import asyncio
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
            
            """ Handle rate limiting with retries and exponential backoff """
            if response.status_code == 429:
                retries = 10
                logger.info(f"Rate limit exceeded for {endpoint}. Response: {response.text}")
                
                for retry in range(retries):
                    wait_time = 2 ** retry  # Exponential backoff
                    logger.info(f"Retrying in {wait_time} seconds... (Attempt {retry + 1}/{retries})")
                    await asyncio.sleep(wait_time)
                    
                    retry_response = await self.client.get(endpoint, params=params)
                    
                    if retry_response.status_code == 200:
                        return retry_response.json()
                    elif retry_response.status_code != 429:
                        logger.error(f"GET request to {endpoint} returned status code {retry_response.status_code} on retry")
                        raise Exception(f"GET request to {endpoint} returned status code {retry_response.status_code, retry_response.text} on retry")
                    
            if response.status_code != 200:
                logger.error(f"GET request to {endpoint} returned status code {response.status_code}. Response: {response.text}")

            return response.json()
        
        except Exception as e:
            logger.error(f"Failed to make GET request to {endpoint}: {e}", exc_info=True)
            raise
       
            
            