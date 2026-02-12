BEGIN;

CREATE TABLE IF NOT EXISTS daily_prop_snapshots (
    snapshot_date DATE NOT NULL,
    snapshot_ts TIMESTAMPTZ NOT NULL,

    sport_key TEXT NOT NULL,
    event_id TEXT NOT NULL,
    event_start_time TIMESTAMPTZ NOT NULL,

    book_key TEXT NOT NULL,
    market_key TEXT NOT NULL, -- e.g player_points, etc.
    market_last_update TIMESTAMPTZ NOT NULL,

    player_key TEXT, -- nullable: not all outcomes are player-specific
    player_name TEXT, -- nullable: not all outcomes are player-specific

    outcome_name TEXT NOT NULL, -- e.g Over / Under / Yes / No
    line NUMERIC,
    price INT,

    PRIMARY KEY (
        snapshot_ts, sport_key, event_id, book_key, market_key,
        COALESCE(player_key,''), outcome_name, COALESCE(line, -999999)
    )
);

-- Find latest odds for a specific player/market
CREATE INDEX IF NOT EXISTS idx_props_player_market_time 
    ON daily_prop_snapshots (player_key, market_key, snapshot_ts DESC) 
    WHERE player_key IS NOT NULL;

-- Find all props for an event over time
CREATE INDEX IF NOT EXISTS idx_props_event_time 
    ON daily_prop_snapshots (event_id, snapshot_ts DESC);

-- Query by date range
CREATE INDEX IF NOT EXISTS idx_props_by_date 
    ON daily_prop_snapshots (snapshot_date, sport_key);

-- Compare odds across books for same market
CREATE INDEX IF NOT EXISTS idx_props_market_comparison 
    ON daily_prop_snapshots (sport_key, event_id, market_key, player_key, snapshot_ts DESC);

COMMIT;