from abc import ABC, abstractmethod


class MLBPlayersGamesSnapshotsRepositoryInterface(ABC):
    @abstractmethod
    def insert_player_game_snapshot(
        self,
        sport_key: str,
        season: int,
        game_id: int,
        player_id: int,
        team_name: str,
        at_bats: int,
        runs: int,
        hits: int,
        rbi: int,
        hr: int,
        bb: int,
        k: int,
        avg: float,
        obp: float,
        slg: float,
        doubles: int,
        triples: int,
        intentional_walks: int,
        hit_by_pitch: int,
        stolen_bases: int,
        caught_stealing: int,
        plate_appearances: int,
        total_bases: int,
        left_on_base: int,
        fly_outs: int,
        ground_outs: int,
        line_outs: int,
        pop_outs: int,
        air_outs: int,
        gidp: int,
        sac_bunts: int,
        sac_flies: int,
        ip: float,
        p_hits: int,
        p_runs: int,
        er: int,
        p_bb: int,
        p_k: int,
        p_hr: int,
        pitch_count: int,
        strikes: int,
        era: float,
        batters_faced: int,
        pitching_outs: int,
        wins: int,
        losses: int,
        saves: int,
        holds: int,
        blown_saves: int,
        games_started: int,
        wild_pitches: int,
        balks: int,
        pitching_hbp: int,
        inherited_runners: int,
        inherited_runners_scored: int,
        putouts: int,
        assists: int,
        errors: int,
        fielding_chances: int,
        fielding_pct: float,
        provider: str
    ):
        pass

    @abstractmethod
    def get_player_game_snapshot(self, game_id: int, player_id: int, season: int):
        pass

    @abstractmethod
    def get_player_game_snapshots_latest(self, player_id: int):
        pass
