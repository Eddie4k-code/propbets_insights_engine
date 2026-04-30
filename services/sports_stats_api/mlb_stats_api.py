from services.sports_stats_api.sports_stats_api_interface import SportsStatsAPIInterface
from services.sports_stats_api.sports_stats_api_config import SportsStatsAPIConfig
from http_client.requests_http_client import RequestsHTTPClient
from http_client.http_client import HTTPClientInterface
import datetime

class MLBStatsAPI(SportsStatsAPIInterface):
    def __init__(self, config: SportsStatsAPIConfig, http_client: HTTPClientInterface):
        self.config = config
        self.http_client = http_client

    async def _fetch_all_pages(self, url, params=None):
        all_data = []
        request_params = dict(params) if params else {}
        while True:
            data = await self.http_client.get(url, params=request_params)
            all_data.extend(data.get("data", []))
            next_cursor = data.get("meta", {}).get("next_cursor")
            if not next_cursor:
                break
            request_params["cursor"] = next_cursor
        return {"data": all_data}

    async def get_teams(self):
        data = await self.http_client.get(self.config.base_url + "/teams")
        return data

    async def get_players_on_team(self, team, season):
        data = await self._fetch_all_pages(self.config.base_url + "/players", params={"team_ids[]": team})
        return data

    async def get_games_from_season(self, season):
        data = await self._fetch_all_pages(self.config.base_url + "/games", params={"seasons[]": season})
        return data

    async def get_players_stats_from_game(self, player, season):
        data = await self._fetch_all_pages(self.config.base_url + "/stats", params={"player_ids[]": player, "seasons[]": season})
        return data