from repositories.team_snapshots_repository_interface import TeamSnapshotsRepositoryInterface
from db.initate_connection_interface import InitiateConnectionInterface


class PostgresTeamSnapshotsRepository(TeamSnapshotsRepositoryInterface):

    def __init__(self, db: InitiateConnectionInterface):
        self.db = db

    def insert_team_snapshot(self, sport_key: str, team_id: int, name: str, abbreviation: str, provider: str):
        """Inserts a team snapshot into the database. This method takes the sport key, team ID, name, abbreviation, and provider as parameters and stores them in the team_snapshots table."""
        with self.db.get_cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO team_snapshots (sport_key, team_id, name, abbreviation, provider)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (sport_key, team_id, provider)
                DO UPDATE SET 
                    name = EXCLUDED.name,
                    abbreviation = EXCLUDED.abbreviation
                """,
                (sport_key, team_id, name, abbreviation, provider)
            )
    
    def get_all_team_snapshots(self):
        """Retrieves all team snapshots from the database. This method queries the team_snapshots table and returns a list of all team snapshots, including their sport key, team ID, name, abbreviation, and provider."""
        teams = self.db.execute_query("SELECT * FROM team_snapshots")
        return teams


        