from abc import ABC, abstractmethod

class PlayerStatsIngestorInterface(ABC):
    @abstractmethod
    async def ingest_teams(self, season: int):
        pass

    @abstractmethod
    async def get_players_on_team(self, team: int, season: int):
        pass

    @abstractmethod
    async def get_season_games(self, season: int):
        pass

    @abstractmethod
    async def get_stats_from_game(self, season: int, player: int):
        pass
