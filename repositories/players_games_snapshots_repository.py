from repositories.players_games_snapshots_repository_interface import PlayersGamesSnapshotsRepositoryInterface
from db.initate_connection_interface import InitiateConnectionInterface

class PostgresPlayersGamesSnapshotsRepository(PlayersGamesSnapshotsRepositoryInterface):
    def __init__(self, db: InitiateConnectionInterface):
        self.db = db

    def insert_player_game_snapshot(
        self,
        sport_key: str,
        season: int,
        game_id: int,
        player_id: int,
        team_id: int,
        position: str,
        minutes: str,
        points: int,
        field_goals_made: int,
        field_goals_attempted: int,
        field_goal_percentage: float,
        free_throws_made: int,
        free_throws_attempted: int,
        free_throw_percentage: float,
        three_pointers_made: int,
        three_pointers_attempted: int,
        three_pointer_percentage: float,
        offensive_rebounds: int,
        defensive_rebounds: int,
        total_rebounds: int,
        assists: int,
        personal_fouls: int,
        steals: int,
        turnovers: int,
        blocks: int,
        plus_minus: int,
        provider: str
    ):
        with self.db.get_cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO players_games_snapshots (
                    sport_key, season, game_id, player_id, team_id, position, minutes, points, 
                    field_goals_made, field_goals_attempted, field_goal_percentage, 
                    free_throws_made, free_throws_attempted, free_throw_percentage, 
                    three_pointers_made, three_pointers_attempted, three_pointer_percentage, 
                    offensive_rebounds, defensive_rebounds, total_rebounds, assists, 
                    personal_fouls, steals, turnovers, blocks, plus_minus, provider
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (sport_key, season, game_id, player_id) 
                DO UPDATE SET 
                    team_id = EXCLUDED.team_id,
                    position = EXCLUDED.position,
                    minutes = EXCLUDED.minutes,
                    points = EXCLUDED.points,
                    field_goals_made = EXCLUDED.field_goals_made,
                    field_goals_attempted = EXCLUDED.field_goals_attempted,
                    field_goal_percentage = EXCLUDED.field_goal_percentage,
                    free_throws_made = EXCLUDED.free_throws_made,
                    free_throws_attempted = EXCLUDED.free_throws_attempted,
                    free_throw_percentage = EXCLUDED.free_throw_percentage,
                    three_pointers_made = EXCLUDED.three_pointers_made,
                    three_pointers_attempted = EXCLUDED.three_pointers_attempted,
                    three_pointer_percentage = EXCLUDED.three_pointer_percentage,
                    offensive_rebounds = EXCLUDED.offensive_rebounds,
                    defensive_rebounds = EXCLUDED.defensive_rebounds,
                    total_rebounds = EXCLUDED.total_rebounds,   
                    assists = EXCLUDED.assists,
                    personal_fouls = EXCLUDED.personal_fouls,
                    steals = EXCLUDED.steals,
                    turnovers = EXCLUDED.turnovers,
                    blocks = EXCLUDED.blocks,
                    plus_minus = EXCLUDED.plus_minus,
                    provider = EXCLUDED.provider
                """,
                (sport_key, season, game_id, player_id, team_id, position, minutes, points, 
                    field_goals_made, field_goals_attempted, field_goal_percentage, 
                    free_throws_made, free_throws_attempted, free_throw_percentage, 
                    three_pointers_made, three_pointers_attempted, three_pointer_percentage, 
                    offensive_rebounds, defensive_rebounds, total_rebounds, assists, 
                    personal_fouls, steals, turnovers, blocks, plus_minus, provider)
            )