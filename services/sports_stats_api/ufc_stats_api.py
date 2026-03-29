import http
from services.sports_stats_api.sports_stats_api_interface import SportsStatsAPIInterface
from services.sports_stats_api.sports_stats_api_config import SportsStatsAPIConfig
from http_client.requests_http_client import RequestsHTTPClient
from http_client.http_client import HTTPClientInterface

class UFCStatsAPI(SportsStatsAPIInterface):
    def __init__(self, config: SportsStatsAPIConfig, http_client: HTTPClientInterface):
        self.config = config
        self.http_client = http_client

    async def get_teams(self):
        pass
    
    async def get_players_on_team(self, team: int = None, season: int = None):
        data = await self.http_client.get(self.config.base_url + "/fights", params={"season": season})
        return data
    
    # Really getting fights from season
    async def get_games_from_season(self, season: int):
        data = await self.http_client.get(self.config.base_url + "/fights", params={"season": season})
        return data
    
    # Getting fighters stats from fights
    async def get_players_stats_from_game(self, player: int, season: int):
        data = await self.http_client.get(self.config.base_url + "/fights" + "/statistics" + "/fighters", params={"id": player})
        return data

