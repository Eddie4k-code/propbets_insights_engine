import http
from services.sports_stats_api.sports_stats_api_interface import SportsStatsAPIInterface
from services.sports_stats_api.sports_stats_api_config import SportsStatsAPIConfig
from http_client.requests_http_client import RequestsHTTPClient
from http_client.http_client import HTTPClientInterface

# Flow Get Teams
# Get Teams Games in past 2 seasons
# Store a mapping in postgres ap_sports_game_id, game_date, home_team_id, away_team id
# Join in your code on game_id


class NBAStatsAPI(SportsStatsAPIInterface):

    def __init__(self, config: SportsStatsAPIConfig, http_client: HTTPClientInterface):
        self.config = config
        self.http_client = http_client

    #/teams
    async def get_teams(self):
        data = await self.http_client.get(self.config.base_url + "/teams")
        return data
    
    async def get_players_on_team(self, team: int, season: int):
        data = await self.http_client.get(self.config.base_url + "/players", params={"season": season, "team": team})
        return data

    async def get_games_from_season(self, season: int):
        data = await self.http_client.get(self.config.base_url + "/games", params={"season": season})
        return data
    
    async def get_players_stats_from_game(self, player: int, season: int):
        data = await self.http_client.get(self.config.base_url + "/players" + "/statistics", params={"id": player, "season": season})
        return data












    