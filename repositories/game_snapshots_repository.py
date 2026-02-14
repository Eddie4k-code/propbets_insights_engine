from repositories.game_snapshots_repository_interface import GameSnapshotsRepositoryInterface
import psycopg2
from db.initate_connection_interface import InitiateConnectionInterface

class PostgresGameSnapshotsRepository(GameSnapshotsRepositoryInterface):
    def __init__(self, db: InitiateConnectionInterface):
        self.db = db

    def insert_game_snapshot(self, sport_key: str, season: int, game_id: int, game_ts: str, status: str, home_team_id: int, away_team_id: int, home_score: int, away_score: int, provider: str):
        """
        Inserts a game snapshot into the database. This method takes the sport key, season, game ID, game timestamp, status, home team ID, away team ID, home score, and away score as parameters and stores them in the game_snapshots table.
        """
        with self.db.get_cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO game_snapshots (sport_key, season, game_id, game_ts, status, home_team_id, away_team_id, home_score, away_score, provider)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (sport_key, season, game_id, provider) 
                DO UPDATE SET 
                    game_ts = EXCLUDED.game_ts,
                    status = EXCLUDED.status,
                    home_team_id = EXCLUDED.home_team_id,
                    away_team_id = EXCLUDED.away_team_id,
                    home_score = EXCLUDED.home_score,
                    away_score = EXCLUDED.away_score,
                """,
                (sport_key, season, game_id, game_ts, status, home_team_id, away_team_id, home_score, away_score, provider)
            )


