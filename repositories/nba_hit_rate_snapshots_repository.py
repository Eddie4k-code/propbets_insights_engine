from repositories.nba_hit_rate_snapshots_repository_interface import NBAHitRateSnapshotsRepositoryInterface
from db.initate_connection_interface import InitiateConnectionInterface


class NBAPostgresHitRateSnapshotsRepository(NBAHitRateSnapshotsRepositoryInterface):
    def __init__(self, db: InitiateConnectionInterface):
        self.db = db

    def insert_hit_rate_snapshot(self, player_name, prop_type, line, outcome_name, event_start_time, price, book_key, market_last_update, hit_rate_10_game, hit_rate_30_game, hit_rate_60_game, sport_key, edge, tier, recently_hot):

        with self.db.get_cursor() as cursor:
            insert_query = """
                INSERT INTO nba_hit_rate_snapshots (
                    player_name, prop_type, line, outcome_name, event_start_time, price, book_key, market_last_update, hit_rate_10_game, hit_rate_30_game, hit_rate_60_game, sport_key, edge, tier, recently_hot
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (player_name, prop_type, line, event_start_time, outcome_name, book_key)
                DO UPDATE SET 
                    price = EXCLUDED.price,
                    market_last_update = EXCLUDED.market_last_update,
                    hit_rate_10_game = EXCLUDED.hit_rate_10_game,
                    hit_rate_30_game = EXCLUDED.hit_rate_30_game,
                    hit_rate_60_game = EXCLUDED.hit_rate_60_game,
                    sport_key = EXCLUDED.sport_key,
                    edge = EXCLUDED.edge,
                    tier = EXCLUDED.tier,
                    recently_hot = EXCLUDED.recently_hot
            """
            cursor.execute(
                insert_query,
                (
                    player_name, prop_type, line, outcome_name, event_start_time, price, book_key, market_last_update, hit_rate_10_game, hit_rate_30_game, hit_rate_60_game, sport_key, edge, tier, recently_hot
                )
            )