from repositories.player_snapshots_repository_interface import PlayerSnapshotsRepositoryInterface
from db.initate_connection_interface import InitiateConnectionInterface
from repositories.player_snapshots_repository import PlayerSnapshotsRepositoryInterface


class PostgresPlayerSnapshotsRepository(PlayerSnapshotsRepositoryInterface):

    def __init__(self, db: InitiateConnectionInterface):
        self.db = db

    def insert_player_snapshot(self, sport_key: str, season: int, team_id: int, player_id: int, first_name: str, last_name: str, abbreviation: str, provider: str):
        """Inserts a player snapshot into the database. This method takes the sport key, season, team ID, player ID, name, and abbreviation as parameters and stores them in the player_snapshots table."""
        with self.db.get_cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO player_snapshots (sport_key, season, player_id, first_name, last_name, abbreviation, team_id, provider)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (sport_key, season, player_id, first_name, last_name, abbreviation, team_id, provider)
            )


        