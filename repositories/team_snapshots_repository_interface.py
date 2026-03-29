from abc import ABC, abstractmethod
import psycopg2

class TeamSnapshotsRepositoryInterface(ABC):
    @abstractmethod
    def insert_team_snapshot(self, sport_key: str, team_id: int, name: str, abbreviation: str, provider: str):
        pass

    @abstractmethod
    def get_all_team_snapshots(self, season: int):
        pass

