from abc import ABC, abstractmethod
from repositories.players_games_snapshots_repository_interface import PlayersGamesSnapshotsRepositoryInterface

class PropHitRateCalculatorInterface(ABC):
    @abstractmethod
    def grab_props():
        pass

    @abstractmethod
    def calculate_hit_rates(props):
        pass

    @abstractmethod
    def calculate_n_game_hit_rate(player_name, prop_type, line, n_games, event_start_time, outcome_name):
        pass

    @abstractmethod
    def calculate_edge(self, hit_rates, price):
        pass

    @abstractmethod
    def create_parlays(self):
        pass

    @abstractmethod
    def calculate_tier(self):
        pass

    @abstractmethod
    def run(self):
        pass