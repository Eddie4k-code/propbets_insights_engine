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
from repositories.game_snapshots_repository import PostgresGameSnapshotsRepository
from repositories.player_snapshots_repository import PostgresPlayerSnapshotsRepository
from repositories.players_games_snapshots_repository import PostgresPlayersGamesSnapshotsRepository
from repositories.team_snapshots_repository import PostgresTeamSnapshotsRepository
from services.sports_api.sports_api import TheOddsAPI
from services.props_ingestor.prop_ingestor import PropIngestor
from services.event_ingestor.event_ingestor import EventIngestor
from services.sports_stats_api.sports_stats_api_config import SportsStatsAPIConfig
from analytics.nba_prop_hit_rate_calculator import NBAPropHitRateCalculator
from analytics.revised_nba_prop_hit_rate_calculator import RevisedNBAPropHitRateCalculator 
from repositories.nba_hit_rate_snapshots_repository import NBAPostgresHitRateSnapshotsRepository
from services.player_stats_ingestor.mlb_player_stats_ingestion import MLBPlayerStatsIngestor
from repositories.pg_mlb_player_games_snapshots_repository import PostgresMLBPlayersGamesSnapshotsRepository
from services.sports_stats_api.mlb_stats_api import MLBStatsAPI
from services.sports_stats_api.ufc_stats_api import UFCStatsAPI
from services.player_stats_ingestor.ufc_player_stats_ingestor import UFCPlayerStatsIngestor
from analytics.mlb_prop_hit_rate_calculator import MLBPropHitRateCalculator
from repositories.mlb_hit_rate_snapshots import PostgresMLBHitRateSnapshotsRepository

dotenv.load_dotenv()

logging = logging.getLogger(__name__)

def parse_args():
    parser = argparse.ArgumentParser(description="Run Ingestion Jobs for Sports Prop Betting Data")
    parser.add_argument("job", choices=["props", "teams", "players", "games", "player_stats", "hit_rates"], help="The ingestion job to run")
    parser.add_argument("sport", choices=["basketball_nba", "baseball_mlb", "hockey_nhl", "ufc"], default="basketball_nba", help="The sport to run the ingestion job for (default: basketball_nba)")
    parser.add_argument("season", type=int, help="The season to run the ingestion job for (required for players, games, and player_stats jobs)")
    return parser.parse_args()

async def run_job(job: str, runner: PipelineRunnerInterface, sport: str, season: int):
    if job == "props":
        markets = []
        if sport == "basketball_nba":
            markets = [m.strip() for m in os.getenv("NBA_MARKETS").split(",")]
        elif sport == "baseball_mlb":
            markets = [m.strip() for m in os.getenv("MLB_MARKETS").split(",")]
        await runner.run_props_pipeline(sport=sport, hours=24, markets=markets, region="us")
    elif job == "teams":
        await runner.run_team_pipeline()
    elif job == "players":
        await runner.run_players_pipeline(season=season)
    elif job == "games":
        await runner.run_games_pipeline(season=season)
    elif job == "player_stats":
        await runner.run_player_stats_pipeline(season=season)
    elif job == "hit_rates":
        await runner.run_hit_rates_pipeline()
    else:
        logging.error(f"Unknown job type: {job}")

def main():
    db = PostgresConnection(connection_string=os.getenv("DATABASE_URL"))
    db.initiate_connection()
    args = parse_args()

    if args.sport == "baseball_mlb":
        runner = PipelineRunner(
            PropIngestor(BetAPI=TheOddsAPI(http_client=RequestsHTTPClient(), config=SportsAPIConfig(api_key=os.getenv("SPORTS_API_KEY"), base_url=os.getenv("SPORTS_API_BASE_URL"))), prop_snapshots_repository=PropSnapshotsRepository(db=db)),
            MLBPlayerStatsIngestor(
                sports_stats_api=MLBStatsAPI(http_client=RequestsHTTPClient(headers={"Authorization": os.getenv("MLB_SPORTS_STATS_API_KEY")}), config=SportsStatsAPIConfig(api_key=os.getenv("MLB_SPORTS_STATS_API_KEY"), base_url=os.getenv("MLB_SPORTS_STATS_API_BASE_URL"))), 
                team_snapshots_repository=PostgresTeamSnapshotsRepository(db=db),
                player_snapshots_repository=PostgresPlayerSnapshotsRepository(db=db),
                game_snapshots_repository=PostgresGameSnapshotsRepository(db=db),
                players_games_snapshots_repository=PostgresMLBPlayersGamesSnapshotsRepository(db=db)
            ),
            EventIngestor(BetAPI=TheOddsAPI(http_client=RequestsHTTPClient(), config=SportsAPIConfig(api_key=os.getenv("SPORTS_API_KEY"), base_url=os.getenv("SPORTS_API_BASE_URL")))),
            MLBPropHitRateCalculator(players_games_snapshots_repository=PostgresMLBPlayersGamesSnapshotsRepository(db=db), props_snapshots_repository=PropSnapshotsRepository(db=db), player_snapshots_repository=PostgresPlayerSnapshotsRepository(db=db), hit_rates_snapshots_repository=PostgresMLBHitRateSnapshotsRepository(db=db))
        )
    elif args.sport == "basketball_nba":
        runner = PipelineRunner(
            PropIngestor(BetAPI=TheOddsAPI(http_client=RequestsHTTPClient(), config=SportsAPIConfig(api_key=os.getenv("SPORTS_API_KEY"), base_url=os.getenv("SPORTS_API_BASE_URL"))), prop_snapshots_repository=PropSnapshotsRepository(db=db)),
            NBAPlayerStatsIngestor(
                sports_stats_api=NBAStatsAPI(http_client=RequestsHTTPClient(headers={"X-Rapidapi-Key": os.getenv("NBA_SPORTS_STATS_API_KEY")}), config=SportsStatsAPIConfig(api_key=os.getenv("NBA_SPORTS_STATS_API_KEY"), base_url=os.getenv("NBA_SPORTS_STATS_API_BASE_URL"))), 
                team_snapshots_repository=PostgresTeamSnapshotsRepository(db=db),
                player_snapshots_repository=PostgresPlayerSnapshotsRepository(db=db), 
                game_snapshots_repository=PostgresGameSnapshotsRepository(db=db),
                players_games_snapshots_repository=PostgresPlayersGamesSnapshotsRepository(db=db)
            ),
            EventIngestor(BetAPI=TheOddsAPI(http_client=RequestsHTTPClient(), config=SportsAPIConfig(api_key=os.getenv("SPORTS_API_KEY"), base_url=os.getenv("SPORTS_API_BASE_URL")))),
            RevisedNBAPropHitRateCalculator(player_games_snapshots_repository=PostgresPlayersGamesSnapshotsRepository(db=db), prop_snapshots_repository=PropSnapshotsRepository(db=db), player_snapshots_repository=PostgresPlayerSnapshotsRepository(db=db), hit_rate_snapshots_repository=NBAPostgresHitRateSnapshotsRepository(db=db))
        )
    else:
        logging.error(f"Unsupported sport: {args.sport}")
        return


    async def _main():
        try:
            await run_job(args.job, runner, args.sport, args.season)
        except Exception as e:
            logging.error(f"Error running job {args.job}: {e.with_traceback(e.__traceback__)}")

    asyncio.run(_main())

if __name__ == "__main__":
    main()