import pytest
from unittest.mock import MagicMock, patch
from analytics.revised_nba_prop_hit_rate_calculator import RevisedNBAPropHitRateCalculator

class DummyRepo:
    def get_snapshots_by_timeframe(self, hours, sport_key):
        return []

def dummy_map_data_to_columns(row, columns):
    return dict(zip(columns, row))

@pytest.fixture
def calculator():
    player_games_repo = MagicMock()
    prop_snapshots_repo = DummyRepo()
    player_snapshots_repo = MagicMock()
    hit_rate_snapshots_repo = MagicMock()
    calc = RevisedNBAPropHitRateCalculator(
        player_games_repo,
        prop_snapshots_repo,
        player_snapshots_repo,
        hit_rate_snapshots_repo
    )
    return calc

def test_grab_props_returns_empty_list_when_no_props(calculator):
    with patch('analytics.revised_nba_prop_hit_rate_calculator.map_data_to_columns', dummy_map_data_to_columns):
        calculator.prop_snapshots_repository.get_snapshots_by_timeframe = MagicMock(return_value=[])
        result = calculator.grab_props()
        assert result == []

def test_grab_props_returns_correct_number_of_props(calculator):
    columns = [
        'snapshot_date', 'snapshot_ts', 'sport_key', 'event_id', 'event_start_time',
        'book_key', 'market_key', 'market_last_update', 'player_key', 'player_name',
        'outcome_name', 'line', 'price', 'provider'
    ]
    row = tuple(range(len(columns)))
    with patch('analytics.revised_nba_prop_hit_rate_calculator.map_data_to_columns', dummy_map_data_to_columns):
        calculator.prop_snapshots_repository.get_snapshots_by_timeframe = MagicMock(return_value=[row, row])
        result = calculator.grab_props()
        assert len(result) == 2
        for d in result:
            assert isinstance(d, dict)
            assert set(d.keys()) == set(columns)

def test_grab_props_skips_malformed_props_and_logs_warning(calculator, caplog):
    columns = [
        'snapshot_date', 'snapshot_ts', 'sport_key', 'event_id', 'event_start_time',
        'book_key', 'market_key', 'market_last_update', 'player_key', 'player_name',
        'outcome_name', 'line', 'price', 'provider'
    ]
    good_row = tuple(range(len(columns)))
    bad_row = tuple(range(5))
    with patch('analytics.revised_nba_prop_hit_rate_calculator.map_data_to_columns', dummy_map_data_to_columns):
        calculator.prop_snapshots_repository.get_snapshots_by_timeframe = MagicMock(return_value=[good_row, bad_row])
        with caplog.at_level('WARNING'):
            result = calculator.grab_props()
            assert len(result) == 1
            assert 'Skipping malformed prop' in caplog.text

def test_grab_props_logs_fetched_count(calculator, caplog):
    columns = [
        'snapshot_date', 'snapshot_ts', 'sport_key', 'event_id', 'event_start_time',
        'book_key', 'market_key', 'market_last_update', 'player_key', 'player_name',
        'outcome_name', 'line', 'price', 'provider'
    ]
    row = tuple(range(len(columns)))
    with patch('analytics.revised_nba_prop_hit_rate_calculator.map_data_to_columns', dummy_map_data_to_columns):
        calculator.prop_snapshots_repository.get_snapshots_by_timeframe = MagicMock(return_value=[row, row, row])
        with caplog.at_level('INFO'):
            calculator.grab_props()
            assert 'Fetched 3 NBA props for processing.' in caplog.text
