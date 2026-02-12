class SportsAPIConfig:
    """
    Class for holding config related to the Sports Stats API.
    """
    def __init__(self, api_key: str, base_url: str):
        self.api_key = api_key
        self.base_url = base_url