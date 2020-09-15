from unittest.mock import call, patch

import pytest
import requests
import requests_mock

from nhldata.retryhttp import get_backoff, retry

SOME_URL = 'http://some.url'


# helper functions
@retry(max_attempts=5, backoff_factor=0, max_jitter_pct=0)
def get_some_url(url):
    response = requests.get(url)
    response.raise_for_status()
    return response.status_code


@retry(max_attempts=5, backoff_factor=1, max_jitter_pct=0)
def get_some_url_with_backoff(url):
    response = requests.get(url)
    response.raise_for_status()
    return response.status_code


# tests
def test_get_backoff_immediate_no_jitter_no_max():
    want = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    got = []
    attempts = 10
    for attempt in range(attempts):
        got.append(get_backoff(attempt_num=attempt,
                               backoff_factor=0,
                               max_jitter_pct=0))
    assert got == want


def test_get_backoff_f100ms_no_jitter_no_max():
    want = [0.05, 0.1, 0.2, 0.4, 0.8, 1.6, 3.2, 6.4, 12.8, 25.6]
    got = []
    attempts = 10
    for attempt in range(attempts):
        got.append(get_backoff(attempt_num=attempt,
                               backoff_factor=.1,
                               max_jitter_pct=0))
    assert got == want


def test_get_backoff_f1s_no_jitter_no_max():
    want = [.5, 1, 2, 4, 8, 16, 32, 64, 128, 256]
    got = []
    attempts = 10
    for attempt in range(attempts):
        got.append(get_backoff(attempt_num=attempt,
                               backoff_factor=1,
                               max_jitter_pct=0))
    assert got == want


def test_get_backoff_f10s_no_jitter_180_second_max():
    want = [5, 10, 20, 40, 80, 160, 180, 180, 180, 180]
    got = []
    attempts = 10
    for attempt in range(attempts):
        got.append(get_backoff(attempt_num=attempt,
                               backoff_factor=10,
                               max_jitter_pct=0,
                               max_sleep_seconds=180))
    assert got == want


def test_get_backoff_f1s_25pct_jitter_no_max():
    unjittered = [.5, 1, 2, 4, 8, 16, 32, 64, 128, 256]
    jittered = []
    attempts = 10
    jitter_pct = 25
    for attempt in range(attempts):
        jittered.append(get_backoff(attempt_num=attempt,
                                    backoff_factor=1,
                                    max_jitter_pct=jitter_pct,
                                    max_sleep_seconds=0))
    for uval, jval in zip(unjittered, jittered):
        max_jitter = 1 + jitter_pct / 100
        assert uval < jval
        assert jval <= uval * max_jitter


def test_get_backoff_f10s_25pct_jitter_180_second_max():
    unjittered = [5, 10, 20, 40, 80, 160, 180, 180, 180, 180]
    jittered = []
    attempts = 10
    jitter_pct = 25
    max_sleep = 180
    for attempt in range(attempts):
        jittered.append(get_backoff(attempt_num=attempt,
                                    backoff_factor=10,
                                    max_jitter_pct=jitter_pct,
                                    max_sleep_seconds=max_sleep))
    for uval, jval in zip(unjittered, jittered):
        assert uval <= jval

    assert max(jittered) == max_sleep


def test_get_backoff_negative_jitter():
    with pytest.raises(ValueError):
        get_backoff(0, 1, -1)


def test_retry_200_should_not_retry():
    with requests_mock.Mocker() as m:
        m.get(SOME_URL, status_code=200)
        get_some_url(SOME_URL)

        assert m.call_count == 1


def test_retry_500_502_200_should_retry_x3():
    with requests_mock.Mocker() as m:
        m.get(SOME_URL, [
            {'status_code': 500},
            {'status_code': 502},
            {'status_code': 200},
        ])
        get_some_url(SOME_URL)

        assert m.call_count == 3


def test_retry_unsupported_code():
    with requests_mock.Mocker() as m:
        m.get(SOME_URL, status_code=511)  # network_authentication_required
        with pytest.raises(requests.exceptions.HTTPError):
            get_some_url(SOME_URL)

        assert m.call_count == 1


def test_retry_exceeds_attempts(capsys):
    with requests_mock.Mocker() as m:
        m.get(SOME_URL, [
            {'status_code': 500},
            {'status_code': 500},
            {'status_code': 500},
            {'status_code': 500},
            {'status_code': 500},
            {'status_code': 200},  # we shouldn't reach this
        ])
        with pytest.raises(requests.exceptions.HTTPError):
            get_some_url(SOME_URL)
        assert m.call_count == 5


@patch('time.sleep', return_value=None)
def test_retry_sleeps_for_correct_duration(mock_sleep):
    with requests_mock.Mocker() as m:
        m.get(SOME_URL, [
            {'status_code': 500},
            {'status_code': 500},
            {'status_code': 500},
            {'status_code': 500},
            {'status_code': 200},
        ])
        get_some_url_with_backoff(SOME_URL)
        expected_calls = [call(0.5), call(1.0), call(2.0), call(4.0)]

        assert m.call_count == 5
        assert mock_sleep.call_count == 4
        assert mock_sleep.call_args_list == expected_calls


@patch('time.sleep', return_value=None)
def test_retry_429_uses_header_if_available(mock_sleep):
    with requests_mock.Mocker() as m:
        m.get(SOME_URL, [
            {'status_code': 429, 'headers': {'Retry-After': '42'}},
            {'status_code': 200},
        ])
        get_some_url_with_backoff(SOME_URL)
        assert m.call_count == 2
        assert mock_sleep.call_count == 1
        mock_sleep.assert_called_once_with(42)


@patch('time.sleep', return_value=None)
def test_retry_429_uses_backoff_if_header_not_available(mock_sleep):
    with requests_mock.Mocker() as m:
        m.get(SOME_URL, [
            {'status_code': 429},
            {'status_code': 200},
        ])
        get_some_url_with_backoff(SOME_URL)
        assert m.call_count == 2
        assert mock_sleep.call_count == 1
        mock_sleep.assert_called_once_with(0.5)
