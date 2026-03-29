BEGIN;

CREATE TABLE IF NOT EXISTS team_snapshots (
    sport_key TEXT NOT NULL,
    team_id INT NOT NULL,
    name TEXT NOT NULL,
    abbreviation TEXT NOT NULL,
    provider TEXT NOT NULL,
    PRIMARY KEY (sport_key, team_id, provider)
);

CREATE INDEX IF NOT EXISTS idx_teams_by_sport_and_team ON team_snapshots (sport_key, team_id);

COMMIT;