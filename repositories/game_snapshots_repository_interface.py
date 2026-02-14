from abc import ABC, abstractmethod
import psycopg2

class GameSnapshotsRepositoryInterface(ABC):
    @abstractmethod
    def insert_game_snapshot(self, sport_key: str, season: int, game_id: int, game_ts: str, status: str, home_team_id: int, away_team_id: int, home_score: int, away_score: int, provider: str):
        pass
