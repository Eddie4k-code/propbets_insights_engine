from repositories.prop_snapshots_repository_interface import PostgresPropSnapshotsRepositoryInterface
from db.initate_connection_interface import InitiateConnectionInterface


class PropSnapshotsRepository(PostgresPropSnapshotsRepositoryInterface):

    def __init__(self, db: InitiateConnectionInterface):
        self.db = db

    def insert_prop_snapshot(self, prop_snapshot: dict):
        """Inserts a prop snapshot into the database. This method takes a prop snapshot object as a parameter and stores its attributes in the prop_snapshots table."""
        with self.db.get_cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO daily_prop_snapshots (snapshot_date, snapshot_ts, sport_key, event_id, event_start_time, book_key, market_key, player_key, player_name, outcome_name, line, price, provider, market_last_update)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (snapshot_ts, sport_key, event_id, player_key, market_key, outcome_name, provider)
                DO UPDATE SET
                    line = EXCLUDED.line,
                    price = EXCLUDED.price,
                    market_last_update = EXCLUDED.market_last_update
                """,
                (
                    prop_snapshot["snapshot_date"],
                    prop_snapshot["snapshot_ts"],
                    prop_snapshot["sport_key"],
                    prop_snapshot["event_id"],
                    prop_snapshot["event_start_time"],
                    prop_snapshot["book_key"],
                    prop_snapshot["market_key"],
                    prop_snapshot["player_key"],
                    prop_snapshot["player_name"],
                    prop_snapshot["outcome_name"],
                    prop_snapshot["line"],
                    prop_snapshot["price"],
                    prop_snapshot["provider"],
                    prop_snapshot["market_last_update"],
                )
            )

    def get_snapshots_by_timeframe(self, hours: int, sport_key: str):
        """Retrieves player prop snapshots within a specified timeframe for a given sport."""
        
        interval_str = f"{hours} hours"
        query = f"""
            SELECT *
            FROM daily_prop_snapshots
            WHERE event_start_time >= NOW()
            AND event_start_time <= (NOW() + INTERVAL '{interval_str}')
            AND sport_key = %s
            ORDER BY event_start_time DESC;
        """
        snapshots = self.db.execute_query(query, (sport_key,))
        return snapshots


        