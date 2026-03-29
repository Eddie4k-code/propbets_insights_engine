from abc import ABC, abstractmethod
import psycopg2

class PlayerSnapshotsRepositoryInterface(ABC):
    @abstractmethod
    def insert_player_snapshot(self, sport_key: str, season: int, team_id: int, first_name: str, last_name: str, provider: str):
        pass

    @abstractmethod
    def get_all_player_snapshots(self, season: int, sport_key: str):
        pass

    @abstractmethod
    def get_player_snapshot_by_name(self, first_name: str, last_name: str, sport_key: str):
        pass

