from abc import ABC, abstractmethod

class NBAHitRateSnapshotsRepositoryInterface(ABC):
    @abstractmethod
    def insert_hit_rate_snapshot(self, player_name, prop_type, line, event_start_time, outcome_name, price, book_key, market_last_update, hit_rate_10_game, hit_rate_30_game, hit_rate_60_game, sport_key, edge, tier, recently_hot):
        pass

    