from abc import ABC, abstractmethod

class PipelineRunnerInterface(ABC):
    @abstractmethod
    async def run_props_pipeline(self, sport: str, hours: int, markets: list, region: str):
        pass

    @abstractmethod
    async def run_team_pipeline(self, season: int):
        pass

    @abstractmethod
    async def run_players_pipeline(self, team: int, season: int):
        pass

    @abstractmethod
    async def run_games_pipeline(self, season: int):
        pass

    @abstractmethod
    async def run_player_stats_pipeline(self, season: int, player: int):
        pass
