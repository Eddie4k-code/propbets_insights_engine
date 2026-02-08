from repositories.prop_snapshots_repository_interface import PostgresPropSnapshotsRepositoryInterface
from db.initate_connection_interface import InitiateConnectionInterface


class PropSnapshotsRepository(PostgresPropSnapshotsRepositoryInterface):

    def __init__(self, db: InitiateConnectionInterface):
        self.db = db

    async def insert_prop_snapshot(self, prop_snapshot):
        pass
        