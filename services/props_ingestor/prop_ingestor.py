import datetime
from services.props_ingestor.prop_ingestor_interface import PropIngestorInterface
from services.sports_api.sports_api import TheOddsAPI, SportsAPIInterface
import asyncio
import logging
from repositories.prop_snapshots_repository_interface import PostgresPropSnapshotsRepositoryInterface
import os
import time

logger = logging.getLogger(__name__)

class PropIngestor(PropIngestorInterface):
    """
    Ingests prop data from a sports API. 
    """
    def __init__(self, BetAPI: SportsAPIInterface, prop_snapshots_repository: PostgresPropSnapshotsRepositoryInterface):
        self.BetAPI = BetAPI
        self.prop_snapshots_repository = prop_snapshots_repository

    async def ingest_props(self, events: list, markets: list, sport: str, region: str = "us"):
        """
        Ingests prop data for all events in parallel.
        
        Args:
            events: List of event objects from get_events()
            markets: List of markets to fetch (e.g., ["player_points", "player_rebounds"])
            sport: Sport key for the events
            region: Region to filter props by (default: "us")
        
        Returns:
            List of all props fetched across all events
        """
        logger.info(f"Starting prop ingestion for {len(events)} events with {len(markets)} markets")

        # Batch requests in groups of 10 per second, with a 12-second wait between batches
        batch_size = 30
        all_props = []
        for i in range(0, len(events), batch_size):
            batch = events[i:i+batch_size]
            tasks = [self._fetch_event_props(event, markets, sport, region) for event in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for event, result in zip(batch, results):
                if isinstance(result, Exception):
                    logger.error(f"Failed to fetch props for event {event.get('id')}: {str(result)}")
                else:
                    all_props.extend(result)
            if i + batch_size < len(events):
                logger.info(f"Waiting 12 seconds before next batch to respect rate limits...")
                await asyncio.sleep(12)

        logger.info(f"Completed raw prop ingestion: {len(all_props)} props fetched")

        logger.info("Starting prop processing")
        transformed_props = self.process_props(all_props)
        logger.info(f"Completed prop processing: {len(transformed_props)} props transformed")

        return transformed_props

    
    async def _fetch_event_props(self, event: dict, markets: list, sport: str, region: str):
        """
        Fetch props for a single event across all specified markets.
        
        Args:
            event: Event object with 'id' key
            markets: List of markets to fetch (e.g., ["player_points", "player_rebounds"])
            sport: Sport key
            region: Region filter
        
        Returns:
            List of props for this event
        """
        event_id = event.get('id')
        
        # Ensure markets is a list
        if isinstance(markets, str):
            markets = [markets]
        
        # Fetch props using comma-separated markets in single request
        markets_str = ",".join(markets)
        
        try:
            # Add small delay to respect rate limits
            await asyncio.sleep(7)
            
            props = await self.BetAPI.get_props_for_event(
                sport=sport,
                event_id=event_id,
                region=region,
                market=markets_str
            )
            return props if isinstance(props, list) else [props]
        except Exception as e:
            logger.warning(f"Failed to fetch props for event {event_id}: {str(e)}")
            return []


    def process_props(self, props):
        """
        Processes props into a structured format
        """
        
        if os.getenv("BOOKIES") is None:
            logger.warning("No bookies specified in environment variables.")
            raise ValueError("No bookies specified in environment variables.")

        #Bookies we will only consider
        allowed_bookies = set(os.getenv("BOOKIES", "").split(","))

        # Only Grab props from allowed bookies
        props_filtered_by_bookies = self.filter_by_bookie(props, allowed_bookies)

        # Format the data to match our postgres table
        transformed_props = self.transform_props(props_filtered_by_bookies)


        return transformed_props

        
    def filter_by_bookie(self, props, allowed_bookies):
        """
        Filters props by allowed bookies.
        
        Args:
            props: List of prop events to filter
            allowed_bookies: Set of allowed bookie keys
        
        Returns:
            List of props filtered by allowed bookies
        """
        allowed_bookies = set(allowed_bookies)
        filtered_props = []

        for event in props:
            filtered_event = {
                "event_id": event.get("id"),
                "sport_key": event.get("sport_key"),
                "sport_title": event.get("sport_title"),
                "event_start_time": event.get("commence_time"),
                "bookmakers": []
            }

            for bookmaker in event.get("bookmakers", []):
                if bookmaker.get("key") in allowed_bookies:
                    filtered_event["bookmakers"].append(bookmaker)

            filtered_props.append(filtered_event)

        return filtered_props
    

    def transform_props(self, props):
        """
        Transforms props into a structured format suitable for database insertion.
        """
        transformed_props = []

        for event in props:
            for bookmaker in event.get("bookmakers", []):
                for market in bookmaker.get("markets", []):
                    for outcome in market.get("outcomes", []):
                        transformed_props.append({
                            "snapshot_date": datetime.datetime.now(datetime.timezone.utc),
                            "snapshot_ts": datetime.datetime.now(datetime.timezone.utc).timestamp(),
                            "sport_key": event.get("sport_key"),
                            "event_id": event.get("event_id"),
                            "event_start_time": event.get("event_start_time"),
                            "book_key": bookmaker.get("key"),
                            "market_key": market.get("key"),
                            "player_key": outcome.get(f"{event.get('sport_title').lower()}:{outcome.get('description')}"),
                            "player_name": outcome.get("name"),
                            "outcome_name": outcome.get("description"), #Over Under Yes / No
                            "line": outcome.get("point"),
                            "price": outcome.get("price")
                        })

        return transformed_props
    

    def insert_transformed_props(self, transformed_props):
        """
        Inserts transformed props into the database using the prop snapshots repository.
        """
        for prop in transformed_props:
            try:
                self.prop_snapshots_repository.insert_prop_snapshot(
                    snapshot_ts=prop["snapshot_ts"],
                    sport_key=prop["sport_key"],
                    event_id=prop["event_id"],
                    book_key=prop["book_key"],
                    market_key=prop["market_key"],
                    player_key=prop["player_key"],
                    outcome_name=prop["outcome_name"],
                    line=prop["line"],
                    price=prop["price"],
                    provider="sports-api"
                )
                logger.info(f"Inserted prop for event {prop['event_id']} into the database.")
            except Exception as e:
                logger.error(f"Error inserting prop for event {prop['event_id']}: {str(e)}")
                raise e




        


        