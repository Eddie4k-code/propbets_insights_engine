from http.client import HTTPException
import logging
from random import choices
from fastapi import FastAPI, Query
import uvicorn
from repositories import players_games_snapshots_repository
from services.sports_api.sports_api import SportsAPIConfig, TheOddsAPI
from http_client.requests_http_client import RequestsHTTPClient
import os
import dotenv
from fastapi.middleware.cors import CORSMiddleware
from services.event_ingestor.event_ingestor import EventIngestor
from services.props_ingestor.prop_ingestor import PropIngestor
import asyncio
from db.PostgresConnection import PostgresConnection
from repositories.prop_snapshots_repository import PropSnapshotsRepository
import argparse
import asyncio
from services.pipeline_runner.pipeline_runner_interface import PipelineRunnerInterface
from services.pipeline_runner.pipeline_runner import PipelineRunner
from services.player_stats_ingestor.nba_player_stats_ingestor import NBAPlayerStatsIngestor
from services.sports_stats_api.nba_stats_api import NBAStatsAPI 
from services.sports_stats_api.sports_stats_api_config import SportsAPIConfig
from repositories.game_snapshots_repository import PostgresGameSnapshotsRepository
from repositories.player_snapshots_repository import PostgresPlayerSnapshotsRepository
from repositories.players_games_snapshots_repository import PostgresPlayersGamesSnapshotsRepository
from repositories.team_snapshots_repository import PostgresTeamSnapshotsRepository
from services.sports_api.sports_api import TheOddsAPI
from services.props_ingestor.prop_ingestor import PropIngestor
from services.event_ingestor.event_ingestor import EventIngestor
from services.sports_api.sports_api_config import SportsAPIConfig

dotenv.load_dotenv()

logging = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(description="Run Ingestion Jobs for Sports Prop Betting Data")
    parser.add_argument("job", choices=["events", "props", "teams", "players", "games", "player_stats"], help="The ingestion job to run")
    return parser.parse_args()


async def run_job(job: str, runner: PipelineRunnerInterface, sport: str):
    if job == "props":
        await runner.run_props_pipeline(sport=sport, hours=24, markets=["player_points"], region="us")
    elif job == "teams":
        await runner.run_team_pipeline()
    elif job == "players":
        await runner.run_players_pipeline()
    elif job == "games":
        await runner.run_games_pipeline()
    elif job == "player_stats":
        await runner.run_player_stats_pipeline()
    else:
        logging.error(f"Unknown job type: {job}")


def main():
    args = parse_args()
    runner = PipelineRunner(
        PropIngestor(BetAPI=TheOddsAPI(http_client=RequestsHTTPClient(), config=SportsAPIConfig(api_key=os.getenv("SPORTS_API_KEY"), base_url=os.getenv("SPORTS_API_BASE_URL"))), prop_snapshots_repository=PropSnapshotsRepository(PostgresConnection(connection_string=os.getenv("DATABASE_URL")))),
        NBAPlayerStatsIngestor(
            sports_stats_api=NBAStatsAPI(http_client=RequestsHTTPClient(), sports_api_config=SportsAPIConfig(api_key=os.getenv("SPORTS_STATS_API_KEY"), base_url=os.getenv("SPORTS_STATS_API_BASE_URL"))),  
            team_snapshots_repository=PostgresTeamSnapshotsRepository(PostgresConnection(connection_string=os.getenv("DATABASE_URL"))),
            player_snapshots_repository=PostgresPlayerSnapshotsRepository(PostgresConnection(connection_string=os.getenv("DATABASE_URL"))),
            game_snapshots_repository=PostgresGameSnapshotsRepository(PostgresConnection(connection_string=os.getenv("DATABASE_URL"))),
            players_games_snapshots_repository=PostgresPlayersGamesSnapshotsRepository(PostgresConnection(connection_string=os.getenv("DATABASE_URL")))
        ),
        EventIngestor(BetAPI=TheOddsAPI(http_client=RequestsHTTPClient(), config=SportsAPIConfig(api_key=os.getenv("SPORTS_API_KEY"), base_url=os.getenv("SPORTS_API_BASE_URL")))
    ))


    async def _main():
        await run_job(args.job, runner, 'basketball_ncaab')

    asyncio.run(_main())


if __name__ == "__main__":
    main()

    


# if __name__ == "__main__":
#     uvicorn.run(app, host="0.0.0.0", port=8000)

    