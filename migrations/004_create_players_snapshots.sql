BEGIN;

CREATE TABLE IF NOT EXISTS player_snapshots (
   sport_key TEXT NOT NULL,
   season INT NOT NULL,
   player_id INT NOT NULL,
   team_id INT NOT NULL,
   first_name TEXT NOT NULL,
   last_name TEXT NOT NULL,
   PRIMARY KEY (sport_key, season, lower(first_name), lower(last_name))
);

CREATE INDEX IF NOT EXISTS idx_players_name ON player_snapshots (player_id, season, lower(first_name), lower(last_name));

COMMIT;