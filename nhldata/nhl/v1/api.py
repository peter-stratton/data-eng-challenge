import logging
from datetime import datetime

import requests

from nhldata.retryhttp import retry

LOG = logging.getLogger(__name__)


class NHLApi:
    def __init__(self):
        self.endpoint = "https://statsapi.web.nhl.com/api/v1"

    @retry(max_attempts=5, backoff_factor=1, max_jitter_pct=25)
    def schedule(self, start_date: datetime, end_date: datetime) -> dict:
        """
        returns a dict tree structure that is like
            "dates": [
                {
                    " #.. meta info, one for each requested date ",
                    "games": [
                        { #.. game info },
                        ...
                    ]
                },
                ...
            ]
        """
        return self._get(self._url('schedule'), {'startDate': start_date.strftime('%Y-%m-%d'),
                                                 'endDate': end_date.strftime('%Y-%m-%d')})

    @retry(max_attempts=5, backoff_factor=1, max_jitter_pct=25)
    def boxscore(self, game_id):
        """
        returns a dict tree structure that is like
           "teams": {
                "home": {
                    " #.. other meta ",
                    "players": {
                        $player_id: {
                            #... player info
                        },
                        #...
                    }
                },
                "away": {
                    #... same as "home"
                }
            }
        """
        url = self._url(f'game/{game_id}/boxscore')
        return self._get(url)

    @staticmethod
    def _get(url, params=None):
        response = requests.get(url, params=params)
        response.raise_for_status()
        return response.json()

    def _url(self, path):
        return f'{self.endpoint}/{path}'
