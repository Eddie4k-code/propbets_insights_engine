from repositories.player_snapshots_repository_interface import PlayerSnapshotsRepositoryInterface
from db.initate_connection_interface import InitiateConnectionInterface
from repositories.team_snapshots_repository_interface import TeamSnapshotsRepositoryInterface


class PostgresTeamSnapshotsRepository(TeamSnapshotsRepositoryInterface):

    def __init__(self, db: InitiateConnectionInterface):
        self.db = db

    def insert_player_snapshot(self, sport_key: str, season: int, team_id: int, player_id: int, first_name: str, last_name: str, abbreviation: str):
        """Inserts a team snapshot into the database. This method takes the sport key, season, team ID, name, and abbreviation as parameters and stores them in the team_snapshots table."""
        with self.db.get_cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO player_snapshots (sport_key, season, player_id, first_name, last_name, abbreviation, team_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (sport_key, season, team_id, first_name, last_name, abbreviation)
            )


        