"""Custom exceptions for the betting agent API"""

class BettingAgentException(Exception):
    """Base exception for all betting agent errors"""
    pass


class APIClientError(BettingAgentException):
    """Error communicating with external API"""
    def __init__(self, message: str, status_code: int = None, api_name: str = None):
        self.message = message
        self.status_code = status_code
        self.api_name = api_name
        super().__init__(self.message)


class RateLimitError(APIClientError):
    """API rate limit exceeded"""
    pass


class InvalidSportError(BettingAgentException):
    """Invalid sport key provided"""
    pass


class InvalidMarketError(BettingAgentException):
    """Invalid market type for sport"""
    pass


class DataValidationError(BettingAgentException):
    """Data validation failed"""
    pass
