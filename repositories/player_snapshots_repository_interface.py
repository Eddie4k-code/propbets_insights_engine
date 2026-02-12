from abc import ABC, abstractmethod
import psycopg2

class PlayerSnapshotsRepositoryInterface(ABC):
    @abstractmethod
    def insert_player_snapshot(self, sport_key: str, season: int, team_id: int, first_name: str, last_name: str, abbreviation: str):
        pass

