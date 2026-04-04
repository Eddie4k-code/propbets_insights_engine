from repositories.mlb_players_games_snapshots_repository_interface import MLBPlayersGamesSnapshotsRepositoryInterface
from db.initate_connection_interface import InitiateConnectionInterface


class PostgresMLBPlayersGamesSnapshotsRepository(MLBPlayersGamesSnapshotsRepositoryInterface):
    def __init__(self, db: InitiateConnectionInterface):
        self.db = db

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
        with self.db.get_cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO mlb_player_game_stats (
                    sport_key, season, game_id, player_id, team_name,
                    at_bats, runs, hits, rbi, hr, bb, k, avg, obp, slg,
                    doubles, triples, intentional_walks, hit_by_pitch,
                    stolen_bases, caught_stealing, plate_appearances, total_bases,
                    left_on_base, fly_outs, ground_outs, line_outs, pop_outs,
                    air_outs, gidp, sac_bunts, sac_flies,
                    ip, p_hits, p_runs, er, p_bb, p_k, p_hr,
                    pitch_count, strikes, era, batters_faced, pitching_outs,
                    wins, losses, saves, holds, blown_saves, games_started,
                    wild_pitches, balks, pitching_hbp,
                    inherited_runners, inherited_runners_scored,
                    putouts, assists, errors, fielding_chances, fielding_pct,
                    provider
                )
                VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s,
                    %s, %s,
                    %s, %s, %s, %s, %s,
                    %s
                )
                ON CONFLICT (sport_key, season, game_id, player_id, provider)
                DO UPDATE SET
                    team_name = EXCLUDED.team_name,
                    at_bats = EXCLUDED.at_bats,
                    runs = EXCLUDED.runs,
                    hits = EXCLUDED.hits,
                    rbi = EXCLUDED.rbi,
                    hr = EXCLUDED.hr,
                    bb = EXCLUDED.bb,
                    k = EXCLUDED.k,
                    avg = EXCLUDED.avg,
                    obp = EXCLUDED.obp,
                    slg = EXCLUDED.slg,
                    doubles = EXCLUDED.doubles,
                    triples = EXCLUDED.triples,
                    intentional_walks = EXCLUDED.intentional_walks,
                    hit_by_pitch = EXCLUDED.hit_by_pitch,
                    stolen_bases = EXCLUDED.stolen_bases,
                    caught_stealing = EXCLUDED.caught_stealing,
                    plate_appearances = EXCLUDED.plate_appearances,
                    total_bases = EXCLUDED.total_bases,
                    left_on_base = EXCLUDED.left_on_base,
                    fly_outs = EXCLUDED.fly_outs,
                    ground_outs = EXCLUDED.ground_outs,
                    line_outs = EXCLUDED.line_outs,
                    pop_outs = EXCLUDED.pop_outs,
                    air_outs = EXCLUDED.air_outs,
                    gidp = EXCLUDED.gidp,
                    sac_bunts = EXCLUDED.sac_bunts,
                    sac_flies = EXCLUDED.sac_flies,
                    ip = EXCLUDED.ip,
                    p_hits = EXCLUDED.p_hits,
                    p_runs = EXCLUDED.p_runs,
                    er = EXCLUDED.er,
                    p_bb = EXCLUDED.p_bb,
                    p_k = EXCLUDED.p_k,
                    p_hr = EXCLUDED.p_hr,
                    pitch_count = EXCLUDED.pitch_count,
                    strikes = EXCLUDED.strikes,
                    era = EXCLUDED.era,
                    batters_faced = EXCLUDED.batters_faced,
                    pitching_outs = EXCLUDED.pitching_outs,
                    wins = EXCLUDED.wins,
                    losses = EXCLUDED.losses,
                    saves = EXCLUDED.saves,
                    holds = EXCLUDED.holds,
                    blown_saves = EXCLUDED.blown_saves,
                    games_started = EXCLUDED.games_started,
                    wild_pitches = EXCLUDED.wild_pitches,
                    balks = EXCLUDED.balks,
                    pitching_hbp = EXCLUDED.pitching_hbp,
                    inherited_runners = EXCLUDED.inherited_runners,
                    inherited_runners_scored = EXCLUDED.inherited_runners_scored,
                    putouts = EXCLUDED.putouts,
                    assists = EXCLUDED.assists,
                    errors = EXCLUDED.errors,
                    fielding_chances = EXCLUDED.fielding_chances,
                    fielding_pct = EXCLUDED.fielding_pct,
                    provider = EXCLUDED.provider
                """,
                (sport_key, season, game_id, player_id, team_name,
                    at_bats, runs, hits, rbi, hr, bb, k, avg, obp, slg,
                    doubles, triples, intentional_walks, hit_by_pitch,
                    stolen_bases, caught_stealing, plate_appearances, total_bases,
                    left_on_base, fly_outs, ground_outs, line_outs, pop_outs,
                    air_outs, gidp, sac_bunts, sac_flies,
                    ip, p_hits, p_runs, er, p_bb, p_k, p_hr,
                    pitch_count, strikes, era, batters_faced, pitching_outs,
                    wins, losses, saves, holds, blown_saves, games_started,
                    wild_pitches, balks, pitching_hbp,
                    inherited_runners, inherited_runners_scored,
                    putouts, assists, errors, fielding_chances, fielding_pct,
                    provider)
            )

    def get_player_game_snapshot(self, game_id: int, player_id: int, season: int):
        return self.db.execute_query(
            """
            SELECT * FROM mlb_player_game_stats
            WHERE game_id = %s AND player_id = %s AND season = %s
            """,
            (game_id, player_id, season)
        )

    def get_player_game_snapshots_latest(self, player_id: int):
        games = self.db.execute_query(
            """
            SELECT
                g.sport_key, g.season, g.game_id, g.game_ts, g.status,
                g.home_team_id, g.away_team_id, g.home_score, g.away_score,
                p.player_id, p.team_name,
                p.at_bats, p.runs, p.hits, p.rbi, p.hr, p.bb, p.k,
                p.avg, p.obp, p.slg,
                p.doubles, p.triples, p.intentional_walks, p.hit_by_pitch,
                p.stolen_bases, p.caught_stealing, p.plate_appearances, p.total_bases,
                p.left_on_base, p.fly_outs, p.ground_outs, p.line_outs, p.pop_outs,
                p.air_outs, p.gidp, p.sac_bunts, p.sac_flies,
                p.ip, p.p_hits, p.p_runs, p.er, p.p_bb, p.p_k, p.p_hr,
                p.pitch_count, p.strikes, p.era, p.batters_faced, p.pitching_outs,
                p.wins, p.losses, p.saves, p.holds, p.blown_saves, p.games_started,
                p.wild_pitches, p.balks, p.pitching_hbp,
                p.inherited_runners, p.inherited_runners_scored,
                p.putouts, p.assists, p.errors, p.fielding_chances, p.fielding_pct,
                p.provider
            FROM games_snapshots g
            JOIN mlb_player_game_stats p
                ON g.sport_key = p.sport_key
                AND g.game_id = p.game_id
                AND g.provider = p.provider
            WHERE p.player_id = %s AND p.sport_key = 'mlb'
            ORDER BY g.game_ts DESC
            """,
            (player_id,)
        )
        return games
