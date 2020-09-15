import logging
from datetime import datetime

import pandas as pd

from nhldata.nhl.v1.api import NHLApi
from nhldata.nhl.v1.header import header
from nhldata.storage import Storage, StorageKey

LOG = logging.getLogger(__name__)


class Crawler:
    """
    Retrieves data from the NHL Data Api
    """
    def __init__(self, api: NHLApi, storage: Storage):
        self.api = api
        self.storage = storage

    @staticmethod
    def _extract_game_keys(game_schedule: [dict]) -> [StorageKey]:
        """Extracts the game ids from a NHL Schedule response dictionary"""
        storage_keys = []
        for date in game_schedule.get('dates'):
            for game in date.get('games'):
                storage_keys.append(
                    StorageKey(*(date.get('date').split('-')), str(game.get('gamePk')))
                )
        return storage_keys

    @staticmethod
    def _extract_players(teams: dict):
        """Extracts all of the players from both teams into a flatter data structure"""
        home = [{'player': x, 'side': 'home'} for x in teams.get('home').get('players').values()]
        away = [{'player': x, 'side': 'away'} for x in teams.get('away').get('players').values()]
        return home + away

    def crawl(self, start_date: datetime, end_date: datetime) -> None:
        # get the schedule info
        game_schedule = self.api.schedule(start_date, end_date)

        # return if no games were played between the start_date and end_date
        if game_schedule.get('totalGames') == 0:
            LOG.info('No NHL games found between %s and %s' % (start_date, end_date))
            return

        game_keys = self._extract_game_keys(game_schedule)
        for key in game_keys:
            # get the boxscore data for each game within the start_date and end_date
            game = self.api.boxscore(key.game_id)

            # flatten the dictionary into a dataframe
            players = pd.json_normalize(self._extract_players(game.get('teams')), sep='_')

            # index based on our target schema
            arranged_players = players.reindex(header, axis=1)

            # The API occasionally returns integer fields with a null that Pandas will then cast to Floats in order
            # to use NaN in the null integer fields.  When we attempt to load this game in the database it fails, which
            # causes us to lose ALL the data in that game.  The Int64 datatype supports pandas <NaN> values in integer
            # arrays, so we use that: https://pandas.pydata.org/pandas-docs/version/0.24.2/user_guide/integer_na.html
            # TODO: either predefine a dataframe schema to handle integer fields with nulls or create a xform function
            arranged_players['player_person_currentAge'] = \
                pd.array(arranged_players.player_person_currentAge, dtype='Int64')
            arranged_players['player_person_currentTeam_id'] = \
                pd.array(arranged_players.player_person_currentTeam_id, dtype='Int64')

            self.storage.store_game(key, arranged_players.to_csv(index=False))
