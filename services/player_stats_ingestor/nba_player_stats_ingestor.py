from sports_stats_api.sports_stats_api_interface import SportsStatsAPIInterface
from repositories.team_snapshots_repository_interface import TeamSnapshotsRepositoryInterface
from repositories.player_snapshots_repository_interface import PlayerSnapshotsRepositoryInterface
from repositories.game_snapshots_repository_interface import GameSnapshotsRepositoryInterface
import logging


logger = logging.getLogger(__name__)

class NBAPlayerStatsIngestor():
    def __init__(
            self, sports_stats_api: SportsStatsAPIInterface,  
            team_snapshots_repository: TeamSnapshotsRepositoryInterface,
            player_snapshots_repository: PlayerSnapshotsRepositoryInterface,    
            game_snapshots_repository: GameSnapshotsRepositoryInterface
        ):
        self.sports_stats_api = sports_stats_api
        self.team_snapshots_repository = team_snapshots_repository
        self.player_snapshots_repository = player_snapshots_repository
        self.game_snapshots_repository = game_snapshots_repository

    async def ingest_teams(self, season: int):
        """
        Ingests NBA Teams for a given season into the database. Retrieves team data from the sports stats API and stores it in the team snapshots repository.
        """
        data = await self.sports_stats_api.get_teams(season)
        logging.info(f"Retrieved {len(data['response'])} teams for season {season}.")
        
        for team in data["response"]:
            try:
                self.team_snapshots_repository.insert_team_snapshot('nba', season, team['id'], team['name'], team['code'])
                logging.info(f"Inserted team {team['name']} (ID: {team['id']}) into the database.")
            except Exception as e:
                logging.error(f"Error inserting team {team['name']} (ID: {team['id']}): {e}")


    async def get_players_on_team(self, team: int, season: int):
        """
        Ingests NBA Players for a given team and season into the database. Retrieves player data from the sports stats API and stores it in the player snapshots repository.
        """
        data = await self.sports_stats_api.get_players_on_team(team, season)
        logging.info(f"Retrieved {len(data['response'])} players for team ID {team} in season {season}.")

        for player in data['response']:
            try:
                self.player_snapshots_repository.insert_player_snapshot('nba', season, team, player['id'], player['first_name'], player['last_name'], player['abbreviation'])
                logging.info(f"Inserted player {player['first_name']} {player['last_name']} (ID: {player['id']}) into the database.")
            except Exception as e:
                logging.error(f"Error inserting player {player['first_name']} {player['last_name']} (ID: {player['id']}): {e}")

    async def get_season_games(self, season: int):
        """
        Ingests NBA Games for a given season into the database. Retrieves game data from the sports stats API and stores it in the game snapshots repository. Only games with a status of "Finished" are ingested to ensure that complete data is stored.
        """
        data = await self.sports_stats_api.get_games_from_season(season=season)
        logging.info(f"Retrieved {len(data['response'])} games for in season {season}.")

        for game in data['response']:
            try:
                if game['status']['long'] != 'Finished':
                    logging.info(f"Skipping game ID {game['id']} as it is not finished (status: {game['status']['long']}).")
                    continue

                self.game_snapshots_repository.insert_game_snapshot('nba', season, game['id'], game['date']['start'], game['status']['long'], game['teams']['home']['id'], game['teams']['visitors']['id'], game['scores']['home']['points'], game['scores']['visitors']['points'])
                logging.info(f"Inserted game ID {game['id']} into the database.")
            except Exception as e:
                logging.error(f"Error inserting game ID {game['id']}: {e}")

    async def get_stats_from_game(self, game: int):
        data = await self.sports_stats_api.get_players_stats_from_game(game=game)
        
