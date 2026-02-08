from services.event_ingestor.event_ingestor_interface import EventIngestorInterface
from exceptions.ingestion_exception import IngestionException
import logging
from services.sports_api.sports_api_interface import SportsAPIInterface

logger = logging.getLogger(__name__)


class EventIngestor(EventIngestorInterface):
    def __init__(self, BetAPI: SportsAPIInterface):
        super().__init__()
        self.api = BetAPI

    async def ingest_events(self, sport: str, hours: int):
        """
        Ingest events for sports from an external API
        """
        logger.info(f"Starting event ingestion for sport: {sport}")
        
            # Logic to ingest events from an external API
        events = await self.api.get_events(sport, hours=hours)
        
        return events
