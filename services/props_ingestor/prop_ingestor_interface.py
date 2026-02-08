from abc import ABC, abstractmethod

class PropIngestorInterface(ABC):
    @abstractmethod
    def ingest_props(self, data):
        pass

    @abstractmethod
    def process_props(self, props):
        pass