CREATE TABLE IF NOT EXISTS mlb_hit_rate_snapshots (
    player_name TEXT NOT NULL,
    prop_type TEXT NOT NULL,
    line TEXT NOT NULL,
    event_start_time TIMESTAMPTZ NOT NULL,
    outcome_name TEXT NOT NULL,
    price NUMERIC NOT NULL,
    book_key TEXT NOT NULL,
    market_last_update TIMESTAMPTZ NOT NULL,
    hit_rate_10_game NUMERIC,
    hit_rate_30_game NUMERIC,
    hit_rate_60_game NUMERIC,
    sport_key TEXT NOT NULL,
    PRIMARY KEY (player_name, prop_type, line, event_start_time, outcome_name, book_key)
);