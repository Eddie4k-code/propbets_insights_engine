from abc import ABC, abstractmethod
import psycopg2

class TeamSnapshotsRepositoryInterface(ABC):
    @abstractmethod
    def insert_team_snapshot(self, sport_key: str, season: int, team_id: int, name: str, abbreviation: str):
        pass

