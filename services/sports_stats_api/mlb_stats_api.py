from services.sports_stats_api.sports_stats_api_interface import SportsStatsAPIInterface
from services.sports_stats_api.sports_stats_api_config import SportsStatsAPIConfig
from http_client.requests_http_client import RequestsHTTPClient
from http_client.http_client import HTTPClientInterface
import datetime


class MLBStatsAPI(SportsStatsAPIInterface):
    def __init__(self, config: SportsStatsAPIConfig, http_client: HTTPClientInterface):
        self.config = config
        self.http_client = http_client

    async def get_teams(self):
        data = await self.http_client.get(self.config.base_url + "/teams")
        return data

    async def get_players_on_team(self, team, season):
        data = await self.http_client.get(self.config.base_url + "/players", params={"team_ids[]": team})
        return data

# Need to finish this
    async def get_games_from_season(self, team, season):
        data = await self.http_client.get(self.config.base_url + "/games", params={"team_ids[]": team, "seasons[]": season})
        return data
# Need to finish this
    async def get_players_stats_from_game(self, player, season):
        data = await self.http_client.get(self.config.base_url + "/")