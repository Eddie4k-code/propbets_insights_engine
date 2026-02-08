from abc import ABC, abstractmethod

class SportsAPIInterface(ABC):
    """Interface for interacting with a sports API."""
    @abstractmethod
    def get_events(self, sport: str, hours: int):
        pass

    @abstractmethod
    def get_props_for_event(self, sport: str, event_id: str, region: str, market: str):
        pass

