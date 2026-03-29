from operator import concat

from services.sports_stats_api.sports_stats_api_interface import SportsStatsAPIInterface
from repositories.team_snapshots_repository_interface import TeamSnapshotsRepositoryInterface
from repositories.player_snapshots_repository_interface import PlayerSnapshotsRepositoryInterface
from repositories.game_snapshots_repository_interface import GameSnapshotsRepositoryInterface
from repositories.players_games_snapshots_repository_interface import PlayersGamesSnapshotsRepositoryInterface
from services.sports_stats_api.sports_stats_api_interface import SportsStatsAPIInterface
import logging


logger = logging.getLogger(__name__)

class UFCPlayerStatsIngestor():
    def __init__(
            self, sports_stats_api: SportsStatsAPIInterface,  
            team_snapshots_repository: TeamSnapshotsRepositoryInterface,
            player_snapshots_repository: PlayerSnapshotsRepositoryInterface,    
            game_snapshots_repository: GameSnapshotsRepositoryInterface,
            players_games_snapshots_repository: PlayersGamesSnapshotsRepositoryInterface
        ):
        self.sports_stats_api = sports_stats_api
        self.team_snapshots_repository = team_snapshots_repository
        self.player_snapshots_repository = player_snapshots_repository
        self.game_snapshots_repository = game_snapshots_repository
        self.players_games_snapshots_repository = players_games_snapshots_repository

    async def ingest_teams(self):
        pass

    async def get_players_on_team(self, season: int):
        """
        Ingests UFC Players for a given team and season into the database. Retrieves player data from the sports stats API and stores it in the player snapshots repository.
        """

        data = await self.sports_stats_api.get_players_on_team(season=season)
        logging.info(f"Retrieved {len(data['response'])} players for season {season}.")

        for fight in data['response']:
            try:
                for key, value in fight["fighters"].items():
                    self.player_snapshots_repository.insert_player_snapshot('ufc', season, value['id'], value['id'], value['name'].split(" ")[0].lower(), value['name'].split(" ")[1].lower(), 'sports-api')
                    logging.info(f"Inserted player {value['name']} (ID: {value['id']}) into the database.")
            except Exception as e:
                logging.error(f"Error inserting player {value['name']} (ID: {value['id']}): {e}")
                raise e

    async def get_season_games(self, season: int):
        """
        Ingests UFC Games for a given season into the database. Retrieves game data from the sports stats API and stores it in the game snapshots repository. Only games with a status of "Finished" are ingested to ensure that complete data is stored.
        """
        data = await self.sports_stats_api.get_games_from_season(season=season)
        logging.info(f"Retrieved {len(data['response'])} fights for in season {season}.")

        for fight in data['response']:
            try:
                if fight['status']['long'] != 'Finished':
                    logging.info(f"Skipping fight ID {fight['id']} as it is not finished (status: {fight['status']['long']}).")
                    continue

                self.game_snapshots_repository.insert_game_snapshot('ufc', season, fight['id'], fight['date'], fight['status']['long'], 0, 0, 0, 0, 'sports-api')
                logging.info(f"Inserted fight ID {fight['id']} into the database.")
            except Exception as e:
                logging.error(f"Error inserting fight ID {fight['id']}: {e}")
                raise e

    async def get_stats_from_game(self, season: int):

        players = self.player_snapshots_repository.get_all_player_snapshots(season=season, sport_key='ufc')


        players_dict = [
            {
                "player_id": player[2],
            }
            for player in players if player[0] == 'ufc'
        ]

        for player in players_dict:

            data = await self.sports_stats_api.get_players_stats_from_game(player=player['player_id'], season=season)
            logging.info(f"Retrieved stats for player ID {player['player_id']} in season {season}.")

            # In the future, we should only get stats from games that are within the past x timeframe this way we don't have to iterate over as many games.

            print(data)

            for stat in data['response']:
                try:

                    t = self.players_games_snapshots_repository.get_player_game_snapshot(game_id=stat['game']['id'], player_id=player['player_id'], season=season)

                    print(t)
                    
                    self.players_games_snapshots_repository.insert_player_game_snapshot(
                        'ufc',
                        season,
                        stat['game']['id'],
                        player['player_id'],
                        stat['team']['id'],
                        stat['pos'],
                        stat['min'],
                        stat['points'],
                        stat['fgm'],
                        stat['fga'],
                        float(stat['fgp']) if stat['fgp'] not in (None, '', '--') else None,
                        stat['ftm'],
                        stat['fta'],
                        float(stat['ftp']) if stat['ftp'] not in (None, '', '--') else None,
                        stat['tpm'],
                        stat['tpa'],
                        float(stat['tpp']) if stat['tpp'] not in (None, '', '--') else None,
                        stat['offReb'],
                        stat['defReb'],
                        stat['totReb'],
                        stat['assists'],
                        stat['pFouls'],
                        stat['steals'],
                        stat['turnovers'],
                        stat['blocks'],
                        int(stat['plusMinus']) if stat['plusMinus'] not in (None, '', '--') else None,
                        'sports-api'
                    )

                    logging.info(f"Inserted stats for player ID {player['player_id']} in game ID {stat['game']['id']} into the database.")
                except Exception as e:
                    logging.error(f"Error inserting stats for player ID {player} in season {season}: {e}")
                    raise e





        
