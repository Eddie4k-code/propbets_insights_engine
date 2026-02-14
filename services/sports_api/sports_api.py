from services.sports_api.sports_api_interface import SportsAPIInterface
from services.sports_api.sports_api_config import SportsAPIConfig
from http_client.http_client import HTTPClientInterface
from datetime import datetime, timedelta

import logging

logger = logging.getLogger(__name__)


class TheOddsAPI(SportsAPIInterface):
    def __init__(self, config: SportsAPIConfig, http_client: HTTPClientInterface):
        self.config = config
        self.http_client = http_client

    async def _get(self, endpoint: str, params: dict = None):
        url = f"{self.config.base_url}/{endpoint}"
        
        logger.debug(f"Making request to: {url}")
        return await self.http_client.get(url, params=params)

    async def get_events(self, sport: str, hours: int = 24):
        """
        Get events for a sport within a time window.
        
        Args:
            sport: Sport key (e.g., 'basketball_nba')
            hours: Number of hours from now to filter events (default: 24)
        """
        # Calculate time window - format must be YYYY-MM-DDTHH:MM:SSZ
        now = datetime.utcnow()
        commence_time_from = now.strftime("%Y-%m-%dT%H:%M:%SZ")
        commence_time_to = (now + timedelta(hours=hours)).strftime("%Y-%m-%dT%H:%M:%SZ")


        if sport == None:
            raise ValueError("Sport must be provided")
        
        if hours <= 0:
            raise ValueError("Hours must be a positive integer")
        
        if not self.config.api_key:
            raise ValueError("API key must be provided")
        
        # Build URL with time filters
        endpoint = (
            f"sports/{sport}/events?"
            f"apiKey={self.config.api_key}&"
            f"commenceTimeFrom={commence_time_from}&"
            f"commenceTimeTo={commence_time_to}"
        )
        
        logger.info(f"Fetching events for {sport} from {commence_time_from} to {commence_time_to}")
        events = await self._get(endpoint)
        return events
    
    async def get_props_for_event(self, sport: str, event_id: str, region: str, market: str):
        """
        Get prop bets for a specific event.
        
        Args:
            event_id: The ID of the event to fetch prop bets for.
            region: The region to filter prop bets by.
            market: The market to filter prop bets by.
        """
        if not event_id:
            raise ValueError("Event ID must be provided")
        
        if not self.config.api_key:
            raise ValueError("API key must be provided")
        
        if not region: 
            raise ValueError("Region must be provided")
        
        endpoint = (
            f"sports/{sport}/events"
            f"/{event_id}/odds"
            f"?apiKey={self.config.api_key}"
            f"&regions={region}"
            f"&markets={market}"
            f"&oddsFormat=american"
        )

        logger.info(f"Fetching prop bets for event {event_id} in {region} for market {market}")

        
        event_props = await self._get(endpoint)
        

        return event_props


        

        

        
    





