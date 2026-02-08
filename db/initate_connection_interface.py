from abc import ABC, abstractmethod
from contextlib import contextmanager

class InitiateConnectionInterface(ABC):
    @abstractmethod
    def initiate_connection(self):
        pass

    @abstractmethod
    @contextmanager
    def get_connection(self):
        """Get a Connection"""
        pass

    @abstractmethod
    @contextmanager
    def get_cursor(self):
        """Get a Cursor"""


    def close_connection(self):
        """Close the Connection"""
        pass

    @abstractmethod
    def execute_query(self, query: str, params: tuple = ()):
        """Execute a Query"""
        pass