from abc import ABC, abstractmethod

class EventIngestorInterface(ABC):
    @abstractmethod
    async def ingest_events(self, sport: str, hours: int):
        pass