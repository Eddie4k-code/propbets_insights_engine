BEGIN;

CREATE TABLE IF NOT EXISTS team_snapshots (
    sport_key TEXT NOT NULL,
    season INT NOT NULL,
    team_id INT NOT NULL,
    name TEXT NO NULL,
    abbreviation TEXT NOT NULL,
    provider TEXT NOT NULL,
    PRIMARY KEY (sport_key, season, team_id, provider),
);

CREATE INDEX IF NOT EXISTS idx_teams_by_season ON team_snapshots (sport_key, team_id, season);

COMMIT;