BEGIN;

CREATE TABLE IF NOT EXISTS games_snapshots (
   sport_key TEXT NOT NULL,,
   season INT NOT NULL,
   game_id INT NOT NULL,
   game_ts TIMESTAMPTZ NOT NULL,
   status TEXT,
   home_team_id INT NOT NULL,
   away_team_id INT NOT NULL,
   home_score INT NOT NULL
   away_score INT NOT NULL,
);

CREATE INDEX IF NOT EXISTS idx_games_by_date ON games_snapshots (sport_key, g);

COMMIT;