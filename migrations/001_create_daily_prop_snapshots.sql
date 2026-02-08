BEGIN;

CREATE TABLE IF NOT EXISTS daily_prop_snapshots (
    snapshot_date DATE NOT NULL,

    snapshot_ts TIMESTAMPTZ NOT NULL

    sport_key TEXT NOT NULL,
    event_id TEXT NOT NULL,
    event_start_time TIMESTAMPTZ NOT NULL,

    book_key TEXT NOT NULL,
    market_key TEXT NOT NULL, -- e.g player_points, etc.
    market_last_update TIMESTAMPTZ NOT NULL,

    player_key TEXT
    player_name TEXT

    player_key TEXT -- nullable: not all outocmes are player-specific
    player_name TEXT -- nullable: not all outocmes are player-specific

    outcome_name TEXT NOT NULL -- e.g Over / Under / Yes / No
    line NUMERIC
    price INT

    PRIMARY KEY (
    snapshot_ts, sport_key, event_id, book_key, market_key,
    COALESCE(player_key,''), outcome_name, COALESCE(line, -999999)
  )
    



)