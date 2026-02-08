import httpx
from http_client.http_client import HTTPClientInterface
import requests
import logging

logger = logging.getLogger(__name__)

class RequestsHTTPClient(HTTPClientInterface):
    def __init__(self, timeout: int = 30):
        super().__init__()
        self.client = httpx.AsyncClient(timeout=timeout)
    
    async def get(self, endpoint: str, params: dict = None):
        try:
            response = await self.client.get(endpoint, params=params)
            
            # Handle empty response
            if not response.text or response.text.strip() == "":
                return []  # Return empty list for empty responses
            
            if response.status_code != 200:
                logger.error(f"GET request to {endpoint} returned status code {response.status_code}")
                raise Exception(f"GET request to {endpoint} returned status code {response.status_code, response.text}")
            return response.json()
        
        except Exception as e:
            logger.error(f"Failed to make GET request to {endpoint}: {e}", exc_info=True)
            raise
       
            
            