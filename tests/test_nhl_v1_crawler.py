from datetime import datetime
from unittest.mock import Mock

import requests_mock

from nhldata.nhl.v1.api import NHLApi
from nhldata.nhl.v1.crawler import Crawler
from nhldata.nhl.v1.header import header
from nhldata.storage import Storage, StorageKey


def test_extract_game_keys(schedule_data):
    expect = [
        StorageKey(game_year='2020', game_month='09', game_day='13', game_id='2019030314'),
        StorageKey(game_year='2020', game_month='09', game_day='14', game_id='2019030325'),
    ]
    result = Crawler._extract_game_keys(schedule_data)

    assert expect == result


def test_extract_players(game_2019030314_data, game_2019030314_players):
    teams = game_2019030314_data.get('teams')
    result = Crawler._extract_players(teams)
    assert result == game_2019030314_players


def test_crawl(schedule_data, game_2019030314_data):
    databucket = 'testdatabucket'
    jobbucket = 'testjobbucket'
    game_1_id = '2019030314'
    game_2_id = '2019030325'
    header_string = ','.join(header)

    schedule = 'https://statsapi.web.nhl.com/api/v1/schedule?startDate=2020-01-01&endDate=2020-01-02'
    boxscore_1 = f'https://statsapi.web.nhl.com/api/v1/game/{game_1_id}/boxscore'
    boxscore_2 = f'https://statsapi.web.nhl.com/api/v1/game/{game_2_id}/boxscore'

    with requests_mock.Mocker() as m:
        m.get(schedule, json=schedule_data, status_code=200)
        m.get(boxscore_1, json=game_2019030314_data, status_code=200)
        m.get(boxscore_2, json=game_2019030314_data, status_code=200)

        s3_mock = Mock()
        storage = Storage(databucket, jobbucket, s3_mock)
        api = NHLApi()

        crawler = Crawler(api, storage)
        crawler.crawl(datetime(2020, 1, 1), datetime(2020, 1, 2))

        assert s3_mock.put_object.call_count == 2

        call_01_kwargs = s3_mock.put_object.call_args_list[0].kwargs
        call_02_kwargs = s3_mock.put_object.call_args_list[1].kwargs

        assert call_01_kwargs.get('Bucket') == call_02_kwargs.get('Bucket') == databucket
        assert call_01_kwargs.get('Key') == f'2020/09/13/{game_1_id}.csv'
        assert call_02_kwargs.get('Key') == f'2020/09/14/{game_2_id}.csv'

        assert header_string in call_01_kwargs.get('Body')
        assert header_string in call_02_kwargs.get('Body')


def test_crawl_no_games():
    databucket = 'testdatabucket'
    jobbucket = 'testjobbucket'
    schedule = 'https://statsapi.web.nhl.com/api/v1/schedule?startDate=2020-01-01&endDate=2020-01-02'
    with requests_mock.Mocker() as m:
        m.get(schedule, json={'totalGames': 0}, status_code=200)

        s3_mock = Mock()
        storage = Storage(databucket, jobbucket, s3_mock)
        api = NHLApi()

        crawler = Crawler(api, storage)
        crawler.crawl(datetime(2020, 1, 1), datetime(2020, 1, 2))

        assert s3_mock.put_object.call_count == 0
