import http
from services.sports_stats_api.sports_stats_api_interface import SportsStatsAPIInterface
from services.sports_stats_api.sports_stats_api_config import SportsAPIConfig
from http_client.requests_http_client import RequestsHTTPClient
from http_client.http_client import HTTPClientInterface

# Flow Get Teams
# Get Teams Games in past 2 seasons
# Store a mapping in postgres ap_sports_game_id, game_date, home_team_id, away_team id
# Join in your code on game_id


class NBAStatsAPI(SportsStatsAPIInterface):

    def __init__(self, sports_api_config: SportsAPIConfig, http_client: HTTPClientInterface):
        self.sports_api_config = sports_api_config
        self.http_client = http_client

    #/teams
    async def get_teams(self, season: int):
        data = await self.http_client.get(self.sports_api_config.base_url + "/teams", params={"season": season})
        
        return data
    
    async def get_players_on_team(self, teamId: int, season: int):
        data = await self.http_client.get(self.sports_api_config.base_url + "/players", params={"team": teamId, "season": season})
        return data

    async def get_games_from_season(self, season: int):
        data = await self.http_client.get(self.sports_api_config.base_url + "/games", params={"season": season})
        return data
    
    async def get_players_stats_from_game(self, player: int, season: int):
        data = await self.http_client.get(self.sports_api_config.base_url + "/players" + "/statistics", params={"player": player, "season": season})
        return data












    