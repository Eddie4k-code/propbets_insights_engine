from abc import ABC, abstractmethod

class SportsStatsAPIInterface(ABC):
    @abstractmethod
    async def get_teams(self, season: int):
        pass
    
    @abstractmethod
    async def get_players_on_team(self, team: int, season: int):
        pass

    @abstractmethod
    async def get_games_from_season(self, team: int, season: int):
        pass

    @abstractmethod
    async def get_players_stats_from_game(self, player: int, season: int):
        pass