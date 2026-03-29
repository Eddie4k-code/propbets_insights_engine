from services.sports_stats_api.sports_stats_api_interface import SportsStatsAPIInterface
from repositories.team_snapshots_repository_interface import TeamSnapshotsRepositoryInterface
from repositories.player_snapshots_repository_interface import PlayerSnapshotsRepositoryInterface
from repositories.game_snapshots_repository_interface import GameSnapshotsRepositoryInterface
from repositories.players_games_snapshots_repository_interface import PlayersGamesSnapshotsRepositoryInterface
from services.sports_stats_api.sports_stats_api_interface import SportsStatsAPIInterface
import logging


logger = logging.getLogger(__name__)

class MLBPlayerStatsIngestor():
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
        """
        Ingests MLB Teams into the database. Retrieves team data from the sports stats API and stores it in the team snapshots repository.
        """
        data = await self.sports_stats_api.get_teams()
        logging.info(f"Retrieved {len(data['data'])} teams.")
        
        for team in data["data"]:
            try:
                self.team_snapshots_repository.insert_team_snapshot('mlb', team['id'], team['display_name'], team['abbreviation'], 'balldontlie')
                logging.info(f"Inserted team {team['display_name']} (ID: {team['id']}) into the database.")
            except Exception as e:
                logging.error(f"Error inserting team {team['display_name']} (ID: {team['id']}): {e}")
                raise e


    async def get_players_on_team(self, season: int):
        """
        Ingests MLB Players for a given team and season into the database. Retrieves player data from the sports stats API and stores it in the player snapshots repository.
        """

        teams = self.team_snapshots_repository.get_all_team_snapshots()
        
        teams_dict = [
            {
                "team_id": team[1],
            }
            for team in teams if team[0] == 'mlb'
        ]

        for team in teams_dict:
            data = await self.sports_stats_api.get_players_on_team(team['team_id'], season)
            logging.info(f"Retrieved {len(data['data'])} players for team ID {team['team_id']} in season {season}.")

            for player in data['data']:
                try:
                    self.player_snapshots_repository.insert_player_snapshot('mlb', season, team['team_id'], player['id'], player['first_name'].lower(), player['last_name'].lower(), 'balldontlie')
                    logging.info(f"Inserted player {player['first_name']} {player['last_name']} (ID: {player['id']}) into the database.")
                except Exception as e:
                    logging.error(f"Error inserting player {player['first_name']} {player['last_name']} (ID: {player['id']}): {e}")
                    raise e

    async def get_season_games(self, season: int):
        """
        Ingests MLB Games for a given season into the database. Retrieves game data from the sports stats API and stores it in the game snapshots repository. Only games with a status of "Finished" are ingested to ensure that complete data is stored.
        """
        data = await self.sports_stats_api.get_games_from_season(season=season)
        logging.info(f"Retrieved {len(data['response'])} games for in season {season}.")

        for game in data['response']:
            try:
                if game['status'] != 'STATUS_FINAL':
                    logging.info(f"Skipping game ID {game['id']} as it is not finished (status: {game['status']}).")
                    continue
                self.game_snapshots_repository.insert_game_snapshot('mlb', season, game['id'], game['date'], game['status'], game['home_team']['id'], game['away_team']['id'], game['scoring_summary'][len(game['scoring_summary']) - 1]["away_score"], game['scoring_summary'][len(game['scoring_summary']) - 1]["home_score"], 'balldontlie')
                logging.info(f"Inserted game ID {game['id']} into the database.")
            except Exception as e:
                logging.error(f"Error inserting game ID {game['id']}: {e}")
                raise e
    # Need to do thids
    async def get_stats_from_game(self, season: int):

        players = self.player_snapshots_repository.get_all_player_snapshots(season=season, sport_key='mlb')


        players_dict = [
            {
                "player_id": player[2],
            }
            for player in players if player[0] == 'mlb'
        ]

        for player in players_dict:

            data = await self.sports_stats_api.get_players_stats_from_game(player=player['player_id'], season=season)
            logging.info(f"Retrieved stats for player ID {player['player_id']} in season {season}.")

            # In the future, we should only get stats from games that are within the past x timeframe this way we don't have to iterate over as many games.

            print(data)

            for stat in data['response']:
                try:
                    self.players_games_snapshots_repository.insert_player_game_snapshot(
                        'mlb',
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
                        'balldontlie'
                    )

                    logging.info(f"Inserted stats for player ID {player['player_id']} in game ID {stat['game']['id']} into the database.")
                except Exception as e:
                    logging.error(f"Error inserting stats for player ID {player} in season {season}: {e}")
                    raise e





        
