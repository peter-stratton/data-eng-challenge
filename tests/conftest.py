import json

import boto3
import moto
import pytest

TEST_DATA_BUCKET_NAME = 'testdatabucket'
TEST_JOB_BUCKET_NAME = 'testjobbucket'


@pytest.fixture
def data_bucket():
    with moto.mock_s3():
        boto3.client('s3').create_bucket(Bucket=TEST_DATA_BUCKET_NAME)
        yield boto3.resource('s3').Bucket(TEST_DATA_BUCKET_NAME)


@pytest.fixture
def job_bucket():
    with moto.mock_s3():
        boto3.client('s3').create_bucket(Bucket=TEST_JOB_BUCKET_NAME)
        yield boto3.resource('s3').Bucket(TEST_JOB_BUCKET_NAME)


@pytest.fixture
def schedule_data():
    yield {
        'copyright': 'NHL and the NHL Shield are registered trademarks of the National Hockey League...',
        'totalItems': 2, 'totalEvents': 0, 'totalGames': 2, 'totalMatches': 0, 'wait': 10, 'dates': [
            {'date': '2020-09-13', 'totalItems': 1, 'totalEvents': 0, 'totalGames': 1, 'totalMatches': 0, 'games': [
                {'gamePk': 2019030314, 'link': '/api/v1/game/2019030314/feed/live', 'gameType': 'P',
                 'season': '20192020', 'gameDate': '2020-09-13T19:00:00Z',
                 'status': {'abstractGameState': 'Final', 'codedGameState': '7', 'detailedState': 'Final',
                            'statusCode': '7', 'startTimeTBD': False}, 'teams': {
                    'away': {'leagueRecord': {'wins': 13, 'losses': 4, 'ot': 0, 'type': 'league'}, 'score': 4,
                             'team': {'id': 14, 'name': 'Tampa Bay Lightning', 'link': '/api/v1/teams/14'}},
                    'home': {'leagueRecord': {'wins': 12, 'losses': 8, 'ot': 0, 'type': 'league'}, 'score': 1,
                             'team': {'id': 2, 'name': 'New York Islanders', 'link': '/api/v1/teams/2'}}},
                 'venue': {'id': 5100, 'name': 'Rogers Place', 'link': '/api/v1/venues/5100'},
                 'content': {'link': '/api/v1/game/2019030314/content'}}], 'events': [], 'matches': []},
            {'date': '2020-09-14', 'totalItems': 1, 'totalEvents': 0, 'totalGames': 1, 'totalMatches': 0, 'games': [
                {'gamePk': 2019030325, 'link': '/api/v1/game/2019030325/feed/live', 'gameType': 'P',
                 'season': '20192020', 'gameDate': '2020-09-15T00:00:00Z',
                 'status': {'abstractGameState': 'Preview', 'codedGameState': '1', 'detailedState': 'Scheduled',
                            'statusCode': '1', 'startTimeTBD': False}, 'teams': {
                    'away': {'leagueRecord': {'wins': 12, 'losses': 8, 'ot': 0, 'type': 'league'}, 'score': 0,
                             'team': {'id': 25, 'name': 'Dallas Stars', 'link': '/api/v1/teams/25'}},
                    'home': {'leagueRecord': {'wins': 12, 'losses': 7, 'ot': 0, 'type': 'league'}, 'score': 0,
                             'team': {'id': 54, 'name': 'Vegas Golden Knights', 'link': '/api/v1/teams/54'}}},
                 'venue': {'id': 5100, 'name': 'Rogers Place', 'link': '/api/v1/venues/5100'},
                 'content': {'link': '/api/v1/game/2019030325/content'}}], 'events': [], 'matches': []}]}


@pytest.fixture
def game_2019030314_data():
    yield {'teams': {
        'away': {
            'players': {
                'ID8476826': {
                    'person': {'id': 8476826, 'fullName': 'Yanni Gourde', 'link': '/api/v1/people/8476826',
                               'firstName': 'Yanni',
                               'lastName': 'Gourde', 'primaryNumber': '37', 'birthDate': '1991-12-15', 'currentAge': 28,
                               'birthCity': 'Saint-Narcisse', 'birthStateProvince': 'QC', 'birthCountry': 'CAN',
                               'nationality': 'CAN', 'height': "5' 9''", 'weight': 175, 'active': True,
                               'alternateCaptain': False,
                               'captain': False, 'rookie': False, 'shootsCatches': 'L', 'rosterStatus': 'Y',
                               'currentTeam': {'id': 14, 'name': 'Tampa Bay Lightning', 'link': '/api/v1/teams/14'},
                               'primaryPosition': {'code': 'C', 'name': 'Center', 'type': 'Forward',
                                                   'abbreviation': 'C'}},
                    'jerseyNumber': '37',
                    'position': {'code': 'C', 'name': 'Center', 'type': 'Forward', 'abbreviation': 'C'},
                    'stats': {
                        'skaterStats': {'timeOnIce': '15:14', 'assists': 2, 'goals': 0, 'shots': 2, 'hits': 1,
                                        'powerPlayGoals': 0,
                                        'powerPlayAssists': 0, 'penaltyMinutes': 2, 'faceOffPct': 50, 'faceOffWins': 6,
                                        'faceoffTaken': 12, 'takeaways': 0, 'giveaways': 0, 'shortHandedGoals': 0,
                                        'shortHandedAssists': 0, 'blocked': 0, 'plusMinus': 1, 'evenTimeOnIce': '13:10',
                                        'powerPlayTimeOnIce': '1:01', 'shortHandedTimeOnIce': '1:03'}}}, 'ID8470601': {
                    'person': {'id': 8470601, 'fullName': 'Braydon Coburn', 'link': '/api/v1/people/8470601',
                               'firstName': 'Braydon', 'lastName': 'Coburn', 'primaryNumber': '55',
                               'birthDate': '1985-02-27',
                               'currentAge': 35, 'birthCity': 'Shaunavon', 'birthStateProvince': 'SK',
                               'birthCountry': 'CAN',
                               'nationality': 'CAN', 'height': "6' 5''", 'weight': 224, 'active': True,
                               'alternateCaptain': False,
                               'captain': False, 'rookie': False, 'shootsCatches': 'L', 'rosterStatus': 'Y',
                               'currentTeam': {'id': 14, 'name': 'Tampa Bay Lightning', 'link': '/api/v1/teams/14'},
                               'primaryPosition': {'code': 'D', 'name': 'Defenseman', 'type': 'Defenseman',
                                                   'abbreviation': 'D'}},
                    'jerseyNumber': '55',
                    'position': {'code': 'N/A', 'name': 'Unknown', 'type': 'Unknown', 'abbreviation': 'N/A'},
                    'stats': {}}, 'ID8476883': {
                    'person': {'id': 8476883, 'fullName': 'Andrei Vasilevskiy', 'link': '/api/v1/people/8476883',
                               'firstName': 'Andrei', 'lastName': 'Vasilevskiy', 'primaryNumber': '88',
                               'birthDate': '1994-07-25',
                               'currentAge': 26, 'birthCity': 'Tyumen', 'birthCountry': 'RUS', 'nationality': 'RUS',
                               'height': "6' 3''", 'weight': 216, 'active': True, 'alternateCaptain': False,
                               'captain': False,
                               'rookie': False, 'shootsCatches': 'L', 'rosterStatus': 'Y',
                               'currentTeam': {'id': 14, 'name': 'Tampa Bay Lightning', 'link': '/api/v1/teams/14'},
                               'primaryPosition': {'code': 'G', 'name': 'Goalie', 'type': 'Goalie',
                                                   'abbreviation': 'G'}},
                    'jerseyNumber': '88',
                    'position': {'code': 'G', 'name': 'Goalie', 'type': 'Goalie', 'abbreviation': 'G'},
                    'stats': {
                        'goalieStats': {'timeOnIce': '60:00', 'assists': 1, 'goals': 0, 'pim': 0, 'shots': 27,
                                        'saves': 26,
                                        'powerPlaySaves': 4, 'shortHandedSaves': 0, 'evenSaves': 22,
                                        'shortHandedShotsAgainst': 0, 'evenShotsAgainst': 23,
                                        'powerPlayShotsAgainst': 4,
                                        'decision': 'W', 'savePercentage': 96.29629629629629,
                                        'powerPlaySavePercentage': 100,
                                        'evenStrengthSavePercentage': 95.65217391304348}}}}},
        'home': {
            'players': {
                'ID8474709': {
                    'person': {'id': 8474709, 'fullName': 'Matt Martin', 'link': '/api/v1/people/8474709',
                               'firstName': 'Matt',
                               'lastName': 'Martin', 'primaryNumber': '17', 'birthDate': '1989-05-08', 'currentAge': 31,
                               'birthCity': 'Windsor', 'birthStateProvince': 'ON', 'birthCountry': 'CAN',
                               'nationality': 'CAN',
                               'height': "6' 3''", 'weight': 220, 'active': True, 'alternateCaptain': False,
                               'captain': False,
                               'rookie': False, 'shootsCatches': 'L', 'rosterStatus': 'Y',
                               'currentTeam': {'id': 2, 'name': 'New York Islanders', 'link': '/api/v1/teams/2'},
                               'primaryPosition': {'code': 'L', 'name': 'Left Wing', 'type': 'Forward',
                                                   'abbreviation': 'LW'}},
                    'jerseyNumber': '17',
                    'position': {'code': 'L', 'name': 'Left Wing', 'type': 'Forward', 'abbreviation': 'LW'}, 'stats': {
                        'skaterStats': {'timeOnIce': '9:58', 'assists': 0, 'goals': 0, 'shots': 1, 'hits': 6,
                                        'powerPlayGoals': 0, 'powerPlayAssists': 0, 'penaltyMinutes': 2,
                                        'faceOffWins': 0,
                                        'faceoffTaken': 0, 'takeaways': 0, 'giveaways': 0, 'shortHandedGoals': 0,
                                        'shortHandedAssists': 0, 'blocked': 1, 'plusMinus': -1, 'evenTimeOnIce': '9:58',
                                        'powerPlayTimeOnIce': '0:00', 'shortHandedTimeOnIce': '0:00'}}}, 'ID8477936': {
                    'person': {'id': 8477936, 'fullName': 'Michael Dal Colle', 'link': '/api/v1/people/8477936',
                               'firstName': 'Michael', 'lastName': 'Dal Colle', 'primaryNumber': '28',
                               'birthDate': '1996-06-20', 'currentAge': 24, 'birthCity': 'Woodbridge',
                               'birthStateProvince': 'ON', 'birthCountry': 'CAN', 'nationality': 'CAN',
                               'height': "6' 3''",
                               'weight': 204, 'active': True, 'alternateCaptain': False, 'captain': False,
                               'rookie': False,
                               'shootsCatches': 'L', 'rosterStatus': 'Y',
                               'currentTeam': {'id': 2, 'name': 'New York Islanders', 'link': '/api/v1/teams/2'},
                               'primaryPosition': {'code': 'L', 'name': 'Left Wing', 'type': 'Forward',
                                                   'abbreviation': 'LW'}},
                    'jerseyNumber': '28',
                    'position': {'code': 'N/A', 'name': 'Unknown', 'type': 'Unknown', 'abbreviation': 'N/A'},
                    'stats': {}},
                'ID8473575': {'person': {'id': 8473575, 'fullName': 'Semyon Varlamov', 'link': '/api/v1/people/8473575',
                                         'firstName': 'Semyon', 'lastName': 'Varlamov', 'primaryNumber': '40',
                                         'birthDate': '1988-04-27', 'currentAge': 32, 'birthCity': 'Samara',
                                         'birthCountry': 'RUS', 'nationality': 'RUS', 'height': "6' 2''", 'weight': 205,
                                         'active': True, 'alternateCaptain': False, 'captain': False, 'rookie': False,
                                         'shootsCatches': 'L', 'rosterStatus': 'Y',
                                         'currentTeam': {'id': 2, 'name': 'New York Islanders',
                                                         'link': '/api/v1/teams/2'},
                                         'primaryPosition': {'code': 'G', 'name': 'Goalie', 'type': 'Goalie',
                                                             'abbreviation': 'G'}}, 'jerseyNumber': '40',
                              'position': {'code': 'G', 'name': 'Goalie', 'type': 'Goalie', 'abbreviation': 'G'},
                              'stats': {
                                  'goalieStats': {'timeOnIce': '58:14', 'assists': 0, 'goals': 0, 'pim': 0, 'shots': 35,
                                                  'saves': 32,
                                                  'powerPlaySaves': 8, 'shortHandedSaves': 2, 'evenSaves': 22,
                                                  'shortHandedShotsAgainst': 2, 'evenShotsAgainst': 25,
                                                  'powerPlayShotsAgainst': 8,
                                                  'decision': 'L', 'savePercentage': 91.42857142857143,
                                                  'powerPlaySavePercentage': 100,
                                                  'shortHandedSavePercentage': 100,
                                                  'evenStrengthSavePercentage': 88}}}}}}}


@pytest.fixture
def game_2019030314_players():
    yield [{'player': {
        'person': {'id': 8474709, 'fullName': 'Matt Martin', 'link': '/api/v1/people/8474709', 'firstName': 'Matt',
                   'lastName': 'Martin', 'primaryNumber': '17', 'birthDate': '1989-05-08', 'currentAge': 31,
                   'birthCity': 'Windsor', 'birthStateProvince': 'ON', 'birthCountry': 'CAN', 'nationality': 'CAN',
                   'height': "6' 3''", 'weight': 220, 'active': True, 'alternateCaptain': False, 'captain': False,
                   'rookie': False, 'shootsCatches': 'L', 'rosterStatus': 'Y',
                   'currentTeam': {'id': 2, 'name': 'New York Islanders', 'link': '/api/v1/teams/2'},
                   'primaryPosition': {'code': 'L', 'name': 'Left Wing', 'type': 'Forward', 'abbreviation': 'LW'}},
        'jerseyNumber': '17', 'position': {'code': 'L', 'name': 'Left Wing', 'type': 'Forward', 'abbreviation': 'LW'},
        'stats': {
            'skaterStats': {'timeOnIce': '9:58', 'assists': 0, 'goals': 0, 'shots': 1, 'hits': 6, 'powerPlayGoals': 0,
                            'powerPlayAssists': 0, 'penaltyMinutes': 2, 'faceOffWins': 0, 'faceoffTaken': 0,
                            'takeaways': 0, 'giveaways': 0, 'shortHandedGoals': 0, 'shortHandedAssists': 0,
                            'blocked': 1, 'plusMinus': -1, 'evenTimeOnIce': '9:58', 'powerPlayTimeOnIce': '0:00',
                            'shortHandedTimeOnIce': '0:00'}}}, 'side': 'home'}, {'player': {
        'person': {'id': 8477936, 'fullName': 'Michael Dal Colle', 'link': '/api/v1/people/8477936',
                   'firstName': 'Michael', 'lastName': 'Dal Colle', 'primaryNumber': '28', 'birthDate': '1996-06-20',
                   'currentAge': 24, 'birthCity': 'Woodbridge', 'birthStateProvince': 'ON', 'birthCountry': 'CAN',
                   'nationality': 'CAN', 'height': "6' 3''", 'weight': 204, 'active': True, 'alternateCaptain': False,
                   'captain': False, 'rookie': False, 'shootsCatches': 'L', 'rosterStatus': 'Y',
                   'currentTeam': {'id': 2, 'name': 'New York Islanders', 'link': '/api/v1/teams/2'},
                   'primaryPosition': {'code': 'L', 'name': 'Left Wing', 'type': 'Forward', 'abbreviation': 'LW'}},
        'jerseyNumber': '28', 'position': {'code': 'N/A', 'name': 'Unknown', 'type': 'Unknown', 'abbreviation': 'N/A'},
        'stats': {}}, 'side': 'home'}, {'player': {
        'person': {'id': 8473575, 'fullName': 'Semyon Varlamov', 'link': '/api/v1/people/8473575',
                   'firstName': 'Semyon', 'lastName': 'Varlamov', 'primaryNumber': '40', 'birthDate': '1988-04-27',
                   'currentAge': 32, 'birthCity': 'Samara', 'birthCountry': 'RUS', 'nationality': 'RUS',
                   'height': "6' 2''", 'weight': 205, 'active': True, 'alternateCaptain': False, 'captain': False,
                   'rookie': False, 'shootsCatches': 'L', 'rosterStatus': 'Y',
                   'currentTeam': {'id': 2, 'name': 'New York Islanders', 'link': '/api/v1/teams/2'},
                   'primaryPosition': {'code': 'G', 'name': 'Goalie', 'type': 'Goalie', 'abbreviation': 'G'}},
        'jerseyNumber': '40', 'position': {'code': 'G', 'name': 'Goalie', 'type': 'Goalie', 'abbreviation': 'G'},
        'stats': {'goalieStats': {'timeOnIce': '58:14', 'assists': 0, 'goals': 0, 'pim': 0, 'shots': 35, 'saves': 32,
                                  'powerPlaySaves': 8, 'shortHandedSaves': 2, 'evenSaves': 22,
                                  'shortHandedShotsAgainst': 2, 'evenShotsAgainst': 25, 'powerPlayShotsAgainst': 8,
                                  'decision': 'L', 'savePercentage': 91.42857142857143, 'powerPlaySavePercentage': 100,
                                  'shortHandedSavePercentage': 100, 'evenStrengthSavePercentage': 88}}},
        'side': 'home'}, {'player': {
        'person': {'id': 8476826, 'fullName': 'Yanni Gourde', 'link': '/api/v1/people/8476826', 'firstName': 'Yanni',
                   'lastName': 'Gourde', 'primaryNumber': '37', 'birthDate': '1991-12-15', 'currentAge': 28,
                   'birthCity': 'Saint-Narcisse', 'birthStateProvince': 'QC', 'birthCountry': 'CAN',
                   'nationality': 'CAN', 'height': "5' 9''", 'weight': 175, 'active': True, 'alternateCaptain': False,
                   'captain': False, 'rookie': False, 'shootsCatches': 'L', 'rosterStatus': 'Y',
                   'currentTeam': {'id': 14, 'name': 'Tampa Bay Lightning', 'link': '/api/v1/teams/14'},
                   'primaryPosition': {'code': 'C', 'name': 'Center', 'type': 'Forward', 'abbreviation': 'C'}},
        'jerseyNumber': '37', 'position': {'code': 'C', 'name': 'Center', 'type': 'Forward', 'abbreviation': 'C'},
        'stats': {
            'skaterStats': {'timeOnIce': '15:14', 'assists': 2, 'goals': 0, 'shots': 2, 'hits': 1, 'powerPlayGoals': 0,
                            'powerPlayAssists': 0, 'penaltyMinutes': 2, 'faceOffPct': 50, 'faceOffWins': 6,
                            'faceoffTaken': 12, 'takeaways': 0, 'giveaways': 0, 'shortHandedGoals': 0,
                            'shortHandedAssists': 0, 'blocked': 0, 'plusMinus': 1, 'evenTimeOnIce': '13:10',
                            'powerPlayTimeOnIce': '1:01', 'shortHandedTimeOnIce': '1:03'}}}, 'side': 'away'}, {
        'player': {'person': {'id': 8470601, 'fullName': 'Braydon Coburn', 'link': '/api/v1/people/8470601',
                              'firstName': 'Braydon', 'lastName': 'Coburn', 'primaryNumber': '55',
                              'birthDate': '1985-02-27', 'currentAge': 35, 'birthCity': 'Shaunavon',
                              'birthStateProvince': 'SK', 'birthCountry': 'CAN', 'nationality': 'CAN',
                              'height': "6' 5''", 'weight': 224, 'active': True, 'alternateCaptain': False,
                              'captain': False, 'rookie': False, 'shootsCatches': 'L', 'rosterStatus': 'Y',
                              'currentTeam': {'id': 14, 'name': 'Tampa Bay Lightning',
                                              'link': '/api/v1/teams/14'},
                              'primaryPosition': {'code': 'D', 'name': 'Defenseman', 'type': 'Defenseman',
                                                  'abbreviation': 'D'}}, 'jerseyNumber': '55',
                   'position': {'code': 'N/A', 'name': 'Unknown', 'type': 'Unknown', 'abbreviation': 'N/A'},
                   'stats': {}}, 'side': 'away'}, {'player': {
        'person': {'id': 8476883, 'fullName': 'Andrei Vasilevskiy', 'link': '/api/v1/people/8476883',
                   'firstName': 'Andrei', 'lastName': 'Vasilevskiy', 'primaryNumber': '88', 'birthDate': '1994-07-25',
                   'currentAge': 26, 'birthCity': 'Tyumen', 'birthCountry': 'RUS', 'nationality': 'RUS',
                   'height': "6' 3''", 'weight': 216, 'active': True, 'alternateCaptain': False, 'captain': False,
                   'rookie': False, 'shootsCatches': 'L', 'rosterStatus': 'Y',
                   'currentTeam': {'id': 14, 'name': 'Tampa Bay Lightning', 'link': '/api/v1/teams/14'},
                   'primaryPosition': {'code': 'G', 'name': 'Goalie', 'type': 'Goalie', 'abbreviation': 'G'}},
        'jerseyNumber': '88', 'position': {'code': 'G', 'name': 'Goalie', 'type': 'Goalie', 'abbreviation': 'G'},
        'stats': {'goalieStats': {'timeOnIce': '60:00', 'assists': 1, 'goals': 0, 'pim': 0, 'shots': 27, 'saves': 26,
                                  'powerPlaySaves': 4, 'shortHandedSaves': 0, 'evenSaves': 22,
                                  'shortHandedShotsAgainst': 0, 'evenShotsAgainst': 23, 'powerPlayShotsAgainst': 4,
                                  'decision': 'W', 'savePercentage': 96.29629629629629, 'powerPlaySavePercentage': 100,
                                  'evenStrengthSavePercentage': 95.65217391304348}}}, 'side': 'away'}]


@pytest.fixture
def expected_header():
    yield ['player_person_id',
           'player_jerseyNumber',
           'player_person_active',
           'player_person_alternateCaptain',
           'player_person_birthCity',
           'player_person_birthCountry',
           'player_person_birthDate',
           'player_person_birthStateProvince',
           'player_person_captain',
           'player_person_currentAge',
           'player_person_currentTeam_id',
           'player_person_currentTeam_link',
           'player_person_currentTeam_name',
           'player_person_firstName',
           'player_person_fullName',
           'player_person_height',
           'player_person_lastName',
           'player_person_link',
           'player_person_nationality',
           'player_person_primaryNumber',
           'player_person_primaryPosition_abbreviation',
           'player_person_primaryPosition_code',
           'player_person_primaryPosition_name',
           'player_person_primaryPosition_type',
           'player_person_rookie',
           'player_person_rosterStatus',
           'player_person_shootsCatches',
           'player_person_weight',
           'player_position_abbreviation',
           'player_position_code',
           'player_position_name',
           'player_position_type',
           'player_stats_goalieStats_assists',
           'player_stats_goalieStats_decision',
           'player_stats_goalieStats_evenSaves',
           'player_stats_goalieStats_evenShotsAgainst',
           'player_stats_goalieStats_evenStrengthSavePercentage',
           'player_stats_goalieStats_goals',
           'player_stats_goalieStats_pim',
           'player_stats_goalieStats_powerPlaySavePercentage',
           'player_stats_goalieStats_powerPlaySaves',
           'player_stats_goalieStats_powerPlayShotsAgainst',
           'player_stats_goalieStats_savePercentage',
           'player_stats_goalieStats_saves',
           'player_stats_goalieStats_shortHandedSavePercentage',
           'player_stats_goalieStats_shortHandedSaves',
           'player_stats_goalieStats_shortHandedShotsAgainst',
           'player_stats_goalieStats_shots',
           'player_stats_goalieStats_timeOnIce',
           'player_stats_skaterStats_assists',
           'player_stats_skaterStats_blocked',
           'player_stats_skaterStats_evenTimeOnIce',
           'player_stats_skaterStats_faceOffPct',
           'player_stats_skaterStats_faceOffWins',
           'player_stats_skaterStats_faceoffTaken',
           'player_stats_skaterStats_giveaways',
           'player_stats_skaterStats_goals',
           'player_stats_skaterStats_hits',
           'player_stats_skaterStats_penaltyMinutes',
           'player_stats_skaterStats_plusMinus',
           'player_stats_skaterStats_powerPlayAssists',
           'player_stats_skaterStats_powerPlayGoals',
           'player_stats_skaterStats_powerPlayTimeOnIce',
           'player_stats_skaterStats_shortHandedAssists',
           'player_stats_skaterStats_shortHandedGoals',
           'player_stats_skaterStats_shortHandedTimeOnIce',
           'player_stats_skaterStats_shots',
           'player_stats_skaterStats_takeaways',
           'player_stats_skaterStats_timeOnIce',
           'side']
