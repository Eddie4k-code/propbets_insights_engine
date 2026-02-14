from abc import ABC, abstractmethod

class PlayersGamesSnapshotsRepositoryInterface(ABC):
    @abstractmethod
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
        pass