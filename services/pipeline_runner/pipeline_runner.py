from dataclasses import dataclass
from services.props_ingestor.prop_ingestor_interface import PropIngestorInterface
from services.player_stats_ingestor.player_stats_ingestor_interface import PlayerStatsIngestorInterface
from services.event_ingestor.event_ingestor_interface import EventIngestorInterface
import logging

logging = logging.getLogger(__name__)

class PipelineRunner():
    """
    The PipelineRunner class is responsible for orchestrating the execution of various data ingestion pipelines. It serves as a central point for running different pipelines related to prop data, team data, player data, game data, and player stats data. Each method in this class corresponds to a specific pipeline and ensures that the necessary steps are executed in the correct order. This design allows for easy scheduling and management of data ingestion tasks, ensuring that the database remains up-to-date with the latest information from the sports stats API.
    """
    def __init__(
            self,
            props_ingestor: PropIngestorInterface,
            player_stats_ingestor: PlayerStatsIngestorInterface,
            event_ingestor: EventIngestorInterface
    ):
        self.props_ingestor = props_ingestor
        self.player_stats_ingestor = player_stats_ingestor
        self.event_ingestor = event_ingestor

    async def run_props_pipeline(self, sport: str, hours: int, markets: list, region: str):
        """
        Runs the entire pipeline for ingesting daily prop data. This method orchestrates the process of retrieving prop data from the sports stats API and storing it in the database. It can be scheduled to run at regular intervals (e.g., daily) to ensure that the prop data is always up-to-date.
        """
        logging.info("Starting props ingestion pipeline...")
        events = await self.event_ingestor.ingest_events(sport=sport, hours=hours)
        transformed_props = await self.props_ingestor.ingest_props(events=events, markets=markets, region=region, sport=sport)
        self.props_ingestor.insert_transformed_props(transformed_props)
        logging.info("Props ingestion pipeline completed.")


    async def run_team_pipeline(self, season: int):
        """
        Runs the entire pipeline for ingesting team data. This method orchestrates the process of retrieving team data from the sports stats API and storing it in the database. It can be scheduled to run at regular intervals (e.g., daily) to ensure that the team data is always up-to-date.
        """
        logging.info("Starting team ingestion pipeline...")
        await self.player_stats_ingestor.ingest_teams(season=season)

        logging.info("Team ingestion pipeline completed.")

    async def run_players_pipeline(self, team: int, season: int):
        """
        Runs the entire pipeline for ingesting player data. This method orchestrates the process of retrieving player data from the sports stats API and storing it in the database. It can be scheduled to run at regular intervals (e.g., daily) to ensure that the player data is always up-to-date.
        """
        logging.info("Starting player ingestion pipeline...")
        await self.player_stats_ingestor.get_players_on_team(team=team, season=season)

        logging.info("Player ingestion pipeline completed.")

    async def run_games_pipeline(self, season: int):
        """
        Runs the entire pipeline for ingesting game data. This method orchestrates the process of retrieving game data from the sports stats API and storing it in the database. It can be scheduled to run at regular intervals (e.g., daily) to ensure that the game data is always up-to-date.
        """
        logging.info("Starting game ingestion pipeline...")
        await self.player_stats_ingestor.get_season_games(season=season)

        logging.info("Game ingestion pipeline completed.")

    async def run_player_stats_pipeline(self, season: int, player: int):
        """
        Runs the entire pipeline for ingesting player stats data. This method orchestrates the process of retrieving player stats data from the sports stats API and storing it in the database. It can be scheduled to run at regular intervals (e.g., daily) to ensure that the player stats data is always up-to-date.
        """
        logging.info("Starting player stats ingestion pipeline...")
        await self.player_stats_ingestor.get_stats_from_game(season=season, player=player)

        logging.info("Player stats ingestion pipeline completed.")


    


