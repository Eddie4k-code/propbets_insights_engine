from repositories.prop_snapshots_repository_interface import PostgresPropSnapshotsRepositoryInterface
from db.initate_connection_interface import InitiateConnectionInterface


class PropSnapshotsRepository(PostgresPropSnapshotsRepositoryInterface):

    def __init__(self, db: InitiateConnectionInterface):
        self.db = db

    def insert_prop_snapshot(self, prop_snapshot):
        """Inserts a prop snapshot into the database. This method takes a prop snapshot object as a parameter and stores its attributes in the prop_snapshots table."""
        with self.db.get_cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO prop_snapshots (sport_key, season, game_id, player_id, player_name, team_id, team_name, prop_type, prop_value, odds, provider)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    prop_snapshot.sport_key,
                    prop_snapshot.season,
                    prop_snapshot.game_id,
                    prop_snapshot.player_id,
                    prop_snapshot.player_name,
                    prop_snapshot.team_id,
                    prop_snapshot.team_name,
                    prop_snapshot.prop_type,
                    prop_snapshot.prop_value,
                    prop_snapshot.odds,
                    prop_snapshot.provider
                )
            )
        