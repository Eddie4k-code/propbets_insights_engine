from repositories.mlb_hit_rate_snapshots_repository_interface import MLBHitRateSnapshotsRepositoryInterface


class PostgresMLBHitRateSnapshotsRepository(MLBHitRateSnapshotsRepositoryInterface):
    def __init__(self, db):
        self.db = db

    def insert_hit_rate_snapshot(self, player_name, prop_type, line, event_start_time, outcome_name, price, book_key, market_last_update, hit_rate_10_game, hit_rate_30_game, hit_rate_60_game, sport_key, tier, recently_hot):
        with self.db.get_cursor() as cursor:
            cursor.execute("""
                INSERT INTO mlb_hit_rate_snapshots (
                    player_name,
                    prop_type,
                    line,
                    event_start_time,
                    outcome_name,
                    price,
                    book_key,
                    market_last_update,
                    hit_rate_10_game,
                    hit_rate_30_game,
                    hit_rate_60_game,
                    sport_key,
                    tier,
                    recently_hot
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (player_name, prop_type, line, event_start_time, outcome_name, price, book_key, market_last_update, hit_rate_10_game, hit_rate_30_game, hit_rate_60_game, sport_key, tier, recently_hot))