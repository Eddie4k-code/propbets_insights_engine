
from abc import ABC, abstractmethod


class IngestionOrchestratorInterface(ABC):
    """Interface for orchestrating the ingestion of sports data."""
    @abstractmethod
    async def ingest_events(self, sport: str, hours: int):
        pass

    @abstractmethod
    async def ingest_props(self, sport: str, event_id: str, region: str, market: str):
        pass