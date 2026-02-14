from repositories.team_snapshots_repository_interface import TeamSnapshotsRepositoryInterface
from db.initate_connection_interface import InitiateConnectionInterface


class PostgresTeamSnapshotsRepository(TeamSnapshotsRepositoryInterface):

    def __init__(self, db: InitiateConnectionInterface):
        self.db = db

    def insert_team_snapshot(self, sport_key: str, season: int, team_id: int, name: str, abbreviation: str, provider: str):
        """Inserts a team snapshot into the database. This method takes the sport key, season, team ID, name, and abbreviation as parameters and stores them in the team_snapshots table."""
        with self.db.get_cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO team_snapshots (sport_key, season, team_id, name, abbreviation, provider)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (sport_key, season, team_id) 
                DO UPDATE SET 
                    name = EXCLUDED.name,

                    abbreviation = EXCLUDED.abbreviation
                """,
                (sport_key, season, team_id, name, abbreviation)
            )


        