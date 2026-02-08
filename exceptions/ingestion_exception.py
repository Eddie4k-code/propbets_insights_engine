class IngestionException(Exception):
    """Exception raised for errors during event ingestion."""
    def __init__(self, message: str):
        super().__init__(message)
        