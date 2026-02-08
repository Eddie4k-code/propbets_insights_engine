from abc import ABC, abstractmethod

class HTTPClientInterface(ABC):
    @abstractmethod
    async def get(self, endpoint: str, params: dict = None):
        pass