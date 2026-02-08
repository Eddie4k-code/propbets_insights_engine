from unittest.mock import AsyncMock, Mock
import pytest
from services.sports_api.sports_api_interface import SportsAPIInterface
from services.event_ingestor.event_ingestor import EventIngestor


class TestEventIngestor():
    @pytest.mark.asyncio
    async def test_ingest_events(self):
        """
        Test the ingest_events method of the EventIngestor class.
        """
        mock_api = Mock(spec=SportsAPIInterface)
        mock_api.get_events = AsyncMock(return_value=[{
      "id": "b69d85cac76c01181ada3299bc50ed54",
      "sport_key": "basketball_nba",
      "sport_title": "NBA",
      "commence_time": "2026-02-07T23:10:00Z",
      "home_team": "San Antonio Spurs",
      "away_team": "Dallas Mavericks"}])
        

        ingestor = EventIngestor(BetAPI=mock_api)

        events = await ingestor.ingest_events(sport="basketball_nba", hours=24)

        assert len(events) == 1
        assert events[0]["id"] == "b69d85cac76c01181ada3299bc50ed54"
        assert events[0]["sport_key"] == "basketball_nba"
        assert events[0]["sport_title"] == "NBA"
        assert events[0]["commence_time"] == "2026-02-07T23:10:00Z"
        assert events[0]["home_team"] == "San Antonio Spurs"
        assert events[0]["away_team"] == "Dallas Mavericks"

        mock_api.get_events.assert_awaited_once()