from datetime import datetime

import requests_mock

from nhldata.nhl.v1.api import NHLApi


def test_NHLApi_schedule_with_200():
    endpoint = 'https://statsapi.web.nhl.com/api/v1/schedule?startDate=2020-01-01&endDate=2020-01-02'
    with requests_mock.Mocker() as m:
        m.get(endpoint, json='{}', status_code=200)

        api = NHLApi()
        api.schedule(datetime(2020, 1, 1), datetime(2020, 1, 2))

        assert m.call_count == 1


def test_NHLApi_boxscore_with_200():
    game_id = 'foo123bar'
    endpoint = f'https://statsapi.web.nhl.com/api/v1/game/{game_id}/boxscore'
    with requests_mock.Mocker() as m:
        m.get(endpoint, json='{}', status_code=200)

        api = NHLApi()
        api.boxscore(game_id)

        assert m.call_count == 1