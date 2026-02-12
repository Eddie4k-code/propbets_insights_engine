from abc import ABC, abstractmethod
import psycopg2

class PostgresPropSnapshotsRepositoryInterface(ABC):
    @abstractmethod
    def insert_prop_snapshot(self, prop_snapshot):
        pass

