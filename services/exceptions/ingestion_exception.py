# Custom exception for event ingestion errors
class IngestionException(Exception):
    def __init__(self, message: str):
        super().__init__(message)
        self.message = message