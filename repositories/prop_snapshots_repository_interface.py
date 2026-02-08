from abc import ABC, abstractmethod
import psycopg2

class PostgresPropSnapshotsRepositoryInterface(ABC):
    @abstractmethod
    async def insert_prop_snapshot(self, prop_snapshot):
        pass

