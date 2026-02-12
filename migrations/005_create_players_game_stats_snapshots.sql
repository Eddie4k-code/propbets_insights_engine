BEGIN;

CREATE TABLE IF NOT EXISTS player_game_stats (
    sport_key TEXT NOT NULL,
    season INT NOT NULL,
    game_id INT NOT NULL,
    player_id INT NOT NULL,
    team_id INT NOT NULL,
    position TEXT,
    minutes TEXT,
    points INT,
    field_goals_made INT,
    field_goals_attempted INT,
    field_goal_percentage NUMERIC,
    free_throws_made INT,
    free_throws_attempted INT,
    free_throw_percentage NUMERIC,
    three_pointers_made INT,
    three_pointers_attempted INT,
    three_pointer_percentage NUMERIC,
    offensive_rebounds INT,
    defensive_rebounds INT,
    total_rebounds INT,
    assists INT,
    personal_fouls INT,
    steals INT,
    turnovers INT,
    blocks INT,
    plus_minus INT,
    PRIMARY KEY (sport_key, season, game_id, player_id)
);

CREATE INDEX IF NOT EXISTS idx_player_stats_by_player 
    ON player_game_stats (sport_key, player_id, season, game_id);

CREATE INDEX IF NOT EXISTS idx_player_stats_by_game 
    ON player_game_stats (sport_key, season, game_id);

COMMIT;