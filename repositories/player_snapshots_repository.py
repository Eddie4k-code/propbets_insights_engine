from repositories.player_snapshots_repository_interface import PlayerSnapshotsRepositoryInterface
from db.initate_connection_interface import InitiateConnectionInterface
from repositories.player_snapshots_repository import PlayerSnapshotsRepositoryInterface
import logging


logger = logging.getLogger(__name__)

class PostgresPlayerSnapshotsRepository(PlayerSnapshotsRepositoryInterface):

    def __init__(self, db: InitiateConnectionInterface):
        self.db = db

    def insert_player_snapshot(self, sport_key: str, season: int, team_id: int, player_id: int, first_name: str, last_name: str, provider: str):
        """Inserts a player snapshot into the database. This method takes the sport key, season, team ID, player ID, name, and abbreviation as parameters and stores them in the player_snapshots table."""
        try:
            with self.db.get_cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO player_snapshots (sport_key, season, player_id, first_name, last_name, team_id, provider)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (sport_key, season, first_name, last_name, provider)
                    DO UPDATE SET 
                        team_id = EXCLUDED.team_id,
                        first_name = EXCLUDED.first_name,
                        last_name = EXCLUDED.last_name
                    """,
                    (sport_key, season, player_id, first_name, last_name, team_id, provider)
                )
        except Exception as e:
            logging.error(f"Error inserting player snapshot: {e}")

    def get_all_player_snapshots(self, season, sport_key: str):
        """Retrieves all player snapshots for a given season from the database."""
        player_snapshot = self.db.execute_query(
            """
            SELECT * FROM player_snapshots WHERE season = %s AND sport_key = %s
            """,
            (season, sport_key)
        )

        return player_snapshot
    
    def get_player_snapshot_by_name(self, first_name, last_name, sport_key: str):
        """Retrieves a player snapshot by player name from the database."""
        player_snapshot = self.db.execute_query(
            """
            SELECT * FROM player_snapshots WHERE first_name = %s AND last_name = %s AND sport_key = %s
            """,
            (first_name, last_name, sport_key)
        )

        return player_snapshot

        