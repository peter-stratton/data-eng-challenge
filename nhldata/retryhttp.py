"""
This module is an amalgamation of:
    urllib.util.retry: https://urllib3.readthedocs.io/en/latest/reference/urllib3.util.html#module-urllib3.util.retry
    hashicorp/go-retryabllehttp: https://github.com/hashicorp/go-retryablehttp/blob/master/client.go#L488

The @retry decorator can be applied to any Requests request function that raises an HTTPError

If the backoff factor is set to:

    0 second (DEFAULT) the retries will happen without any sleep between them
    1 second the successive sleeps will be 0.5, 1, 2, 4, 8, 16, 32, 64, 128, 256.
    2 seconds - 1, 2, 4, 8, 16, 32, 64, 128, 256, 512
    10 seconds - 5, 10, 20, 40, 80, 160, 320, 640, 1280, 2560

Jitter is a good idea to add to any system that might be running a pool of workers hitting the same API
in order to prevent a thundering herd: https://en.wikipedia.org/wiki/Thundering_herd_problem
"""
import functools
import logging
import random
import time

import requests

LOG = logging.getLogger(__name__)

# these are the status codes we consider worth retrying
RETRYABLE_CODES = [requests.codes.too_many_requests,  # 429
                   requests.codes.server_error,  # 500
                   requests.codes.bad_gateway,  # 502
                   requests.codes.service_unavailable,  # 503
                   requests.codes.gateway_timeout,  # 504
                   ]


def get_backoff(attempt_num: int, backoff_factor: float, max_jitter_pct: int, max_sleep_seconds: float = None) -> float:
    """
    Calculates a sleep time based on the number of attempts, backoff factor, and jitter

    :param attempt_num: current attempt number
    :param backoff_factor: backoff factor to calculate sleep duration
    :param max_jitter_pct: random percentage to increase backoff by
    :param max_sleep_seconds: the maximum value a backoff can be
    :return: amount of time to sleep between requests
    """
    if max_jitter_pct < 0:
        raise ValueError('max_jitter_pct cannot be less that zero: got %s' % max_jitter_pct)

    # get a random percentage max bounded by the max_jitter_pct argument
    jitter_scale = 1 + (random.random() * max_jitter_pct / 100)

    # calculate an exponential backoff based on the backoff_factor (possibly with jitter)
    jittered_sleep = backoff_factor * (2 ** (attempt_num - 1)) * jitter_scale

    # return jittered_sleep if max_sleep_seconds is None, otherwise return whichever is less
    return min(jittered_sleep, max_sleep_seconds) if max_sleep_seconds else jittered_sleep


# DECORATOR
def retry(max_attempts: int = 5, backoff_factor: int = 0, max_jitter_pct: int = 25, max_sleep_seconds: float = None):
    """
    Retries HTTP requests if a retryable failure status code is raised by Requests

    :param max_attempts: maximum number of retries to allow
    :param backoff_factor: backoff factor to apply between attempts
    :param max_jitter_pct: the maximum percentage to randomly increase the sleep time by
    :param max_sleep_seconds: the maximum amount of time to wait between requests
    :return: None
    """
    def decorator_retry(func):
        @functools.wraps(func)
        def wrapper_retry(*args, **kwargs):
            # save the list of errors we receive for post-mortem analysis
            error_list = []

            for attempt in range(max_attempts):
                # run the http request and catch HTTPErrors
                try:
                    return func(*args, **kwargs)
                except requests.HTTPError as err:
                    LOG.info('Request returned status_code [%s]' % err.response.status_code)
                    error_list.append(err)

                    # if it's not an error we consider worth retryable, re-raise the exception
                    if err.response.status_code not in RETRYABLE_CODES:
                        raise err

                    # if we got a 429, look for Retry-After in the header and use that for the sleep duration
                    # if Retry-After isn't set (sigh), just fall back to get_backoff
                    if err.response.status_code == requests.codes.too_many_requests:
                        sleep_duration = err.response.headers.get('Retry-After') or \
                                         get_backoff(attempt, backoff_factor, max_jitter_pct, max_sleep_seconds)
                    else:
                        sleep_duration = get_backoff(attempt, backoff_factor, max_jitter_pct, max_sleep_seconds)

                    # sleep before trying again
                    LOG.info('Sleeping for: %ss' % float(sleep_duration))
                    time.sleep(float(sleep_duration))

            # if we've made it this far, it's not happening, so just log the codes and re-raise the first error
            for idx, error in enumerate(error_list):
                status_code = error_list[idx].response.status_code
                LOG.error('HTTP Retry Attempt [%s] returned status_code [%s]' % (idx, status_code))
            LOG.error('Re-raising the first error encountered:')
            raise error_list[0]
        return wrapper_retry
    return decorator_retry
