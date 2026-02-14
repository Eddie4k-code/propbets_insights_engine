from abc import ABC, abstractmethod

class PropIngestorInterface(ABC):
    @abstractmethod
    async def ingest_props(self, events: list, markets: list, sport: str, region: str = "us"):
        pass

    @abstractmethod
    def process_props(self, props):
        pass

    @abstractmethod
    def filter_by_bookie(self, props, allowed_bookies):
        pass

    @abstractmethod
    def transform_props(self, props):
        pass

    @abstractmethod
    def insert_transformed_props(self, transformed_props):
        pass