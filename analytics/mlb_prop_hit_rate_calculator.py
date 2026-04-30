from analytics.prop_hit_rate_calculator import PropHitRateCalculatorInterface
from repositories.mlb_players_games_snapshots_repository_interface import MLBPlayersGamesSnapshotsRepositoryInterface
from repositories.player_snapshots_repository_interface import PlayerSnapshotsRepositoryInterface
from repositories.prop_snapshots_repository_interface import PostgresPropSnapshotsRepositoryInterface
import logging
from repositories.mlb_hit_rate_snapshots_repository_interface import MLBHitRateSnapshotsRepositoryInterface


class MLBPropHitRateCalculator(PropHitRateCalculatorInterface):

    def __init__(self, players_games_snapshots_repository: MLBPlayersGamesSnapshotsRepositoryInterface, props_snapshots_repository: PostgresPropSnapshotsRepositoryInterface, player_snapshots_repository: PlayerSnapshotsRepositoryInterface, hit_rates_snapshots_repository: MLBHitRateSnapshotsRepositoryInterface):
        self.players_games_snapshots_repository = players_games_snapshots_repository
        self.props_snapshots_repository = props_snapshots_repository
        self.player_snapshots_repository = player_snapshots_repository
        self.hit_rates_snapshots_repository = hit_rates_snapshots_repository

    def grab_props(self):
        # Grab all props within 24 hour timeframe window
        props = self.props_snapshots_repository.get_snapshots_by_timeframe(24, 'baseball_mlb') # Within 24 hours, and filter for mlb props only

        columns = [
            'snapshot_date',
            'snapshot_ts',
            'sport_key',
            'event_id',
            'event_start_time',
            'book_key',
            'market_key',
            'market_last_update',
            'player_key',
            'player_name',
            'outcome_name',
            'line',
            'price',
            'provider'
        ]
        prop_dicts = []
        for prop in props:
            prop_dict = {
                'snapshot_date': prop[0],
                'snapshot_ts': prop[1],
                'sport_key': prop[2],
                'event_id': prop[3],
                'event_start_time': prop[4],
                'book_key': prop[5],
                'market_key': prop[6],
                'market_last_update': prop[7],
                'player_key': prop[8],
                'player_name': prop[9],
                'outcome_name': prop[10],
                'line': prop[11],
                'price': prop[12],
                'provider': prop[13]
            }
            prop_dicts.append(prop_dict)



        return prop_dicts

    
    def calculate_hit_rates(self, props):
            for prop in props:
                player_name = prop['player_name']
                prop_type = prop['market_key']
                line = prop['line']
                event_start_time = prop['event_start_time']
                outcome_name = prop['outcome_name']
                price = prop['price']
                book_key = prop['book_key']
                market_last_update = prop['market_last_update']
                sport_key = prop['sport_key']

                last_10_game_hit_rate = self.calculate_n_game_hit_rate(player_name, prop_type, line, 10, event_start_time, outcome_name)
                last_30_game_hit_rate = self.calculate_n_game_hit_rate(player_name, prop_type, line, 30, event_start_time, outcome_name)
                last_60_game_hit_rate = self.calculate_n_game_hit_rate(player_name, prop_type, line, 60, event_start_time, outcome_name)

                # Edge calculation removed (no longer used)
                
                # AVOID PUTTING BAD Data
                if last_10_game_hit_rate == None and last_30_game_hit_rate == None and last_60_game_hit_rate == None:
                    logging.info(f"Skipping player: {player_name}, prop type: {prop_type}, line: {line}, outcome: {outcome_name}, event start time: {event_start_time} due to all hit rates being 0")
                    continue

                logging.info(f"Calculated hit rates for player: {player_name}, prop type: {prop_type}, line: {line}, outcome: {outcome_name}, event start time: {event_start_time}. Last 10 game hit rate: {last_10_game_hit_rate}, Last 30 game hit rate: {last_30_game_hit_rate}, Last 60 game hit rate: {last_60_game_hit_rate}")

                logging.info(f"Calculating Tier based on hit rates for player: {player_name}, prop type: {prop_type}, line: {line}, outcome: {outcome_name}, event start time: {event_start_time}")

                tier_result = self.calculate_tier(last_10_game_hit_rate, last_30_game_hit_rate, last_60_game_hit_rate)
   

                self.insert_hit_rate_into_db(
                    player_name, prop_type, line, event_start_time, outcome_name, price, book_key, market_last_update,
                    last_10_game_hit_rate, last_30_game_hit_rate, last_60_game_hit_rate, sport_key,
                    tier_result['tier'], tier_result['recently_hot']
                )
                logging.info(f"Inserted hit rates into DB for player: {player_name}, prop type: {prop_type}, line: {line}, outcome: {outcome_name}, event start time: {event_start_time}")
        
    def calculate_n_game_hit_rate(self, player_name, prop_type, line, n, event_start_time, outcome_name):
        logging.info(f"Calculating hit rate for {player_name}, prop type: {prop_type}, line: {line}, outcome: {outcome_name}, n: {n}")

        player_snapshot = self.player_snapshots_repository.get_player_snapshot_by_name(player_name.split()[0], " ".join(player_name.split()[1:]), 'mlb') 

        if len(player_snapshot) == 0:
            logging.info(f"No player snapshot found for player: {player_name}")
            return None
        
        player_id = player_snapshot[0][2]

        logging.info(f"Found player snapshot for player: {player_name}, player_id: {player_id}")

        games = self.players_games_snapshots_repository.get_player_game_snapshots_latest(player_id=player_id)

        logging.info(f"Found {len(games)} games for player: {player_name}")

        columns = [
            'sport_key',
            'season',
            'game_id',
            'game_ts',
            'status',
            'home_team_id',
            'away_team_id',
            'home_score',
            'away_score',
            'player_id',
            'team_name',
            'at_bats',
            'runs',
            'hits',
            'rbi',
            'hr',
            'bb',
            'k',
            'avg',
            'obp',
            'slg',
            'doubles',
            'triples',
            'intentional_walks',
            'hit_by_pitch',
            'stolen_bases',
            'caught_stealing',
            'plate_appearances',
            'total_bases',
            'left_on_base',
            'fly_outs',
            'ground_outs',
            'line_outs',
            'pop_outs',
            'air_outs',
            'gidp',
            'sac_bunts',
            'sac_flies',
            'ip',
            'p_hits',
            'p_runs',
            'er',
            'p_bb',
            'p_k',
            'p_hr',
            'pitch_count',
            'strikes',
            'era',
            'batters_faced',
            'pitching_outs',
            'wins',
            'losses',
            'saves',
            'holds',
            'blown_saves',
            'games_started',
            'wild_pitches',
            'balks',
            'pitching_hbp',
            'inherited_runners',
            'inherited_runners_scored',
            'putouts',
            'assists',
            'errors',
            'fielding_chances',
            'fielding_pct',
            'provider',
        ]

        dict_games = []

        for game in games:
            stat_dict = {col: game[idx] for idx, col in enumerate(columns)}
            dict_games.append(stat_dict)


        games_filtered_dnp = [game for game in dict_games if not self.did_not_play(game)]

        if len(games_filtered_dnp) < n:
            logging.info(f"Not enough games to calculate hit rate for player: {player_name}, prop type: {prop_type}, line: {line}, outcome: {outcome_name}. Found {len(games)} games, but need at least {n} games.")
            return None


        n_games_to_consider = games_filtered_dnp[:n]



        if prop_type == 'batter_home_runs':
            return self.batter_home_runs_hit_rate(n_games_to_consider, line, n, outcome_name, player_name)
        if prop_type == 'batter_hits':
            return self.batter_hits_hit_rate(n_games_to_consider, line, n, outcome_name, player_name)
        if prop_type == 'batter_total_bases':
            return self.batter_total_bases_hit_rate(n_games_to_consider, line, n, outcome_name, player_name)
        if prop_type == 'batter_rbis':
            return self.batter_rbis_hit_rate(n_games_to_consider, line, n, outcome_name, player_name)
        if prop_type == 'batter_runs_scored':
            return self.batter_runs_scored_hit_rate(n_games_to_consider, line, n, outcome_name, player_name)
        if prop_type == 'batter_hits_runs_rbis':
            return self.batter_hits_runs_rbis_hit_rate(n_games_to_consider, line, n, outcome_name, player_name)
        if prop_type == 'batter_singles':
            return self.batter_singles_hit_rate(n_games_to_consider, line, n, outcome_name, player_name)
        if prop_type == 'batter_doubles':
            return self.batter_doubles_hit_rate(n_games_to_consider, line, n, outcome_name, player_name)
        if prop_type == 'batter_triples':
            return self.batter_triples_hit_rate(n_games_to_consider, line, n, outcome_name, player_name)
        if prop_type == 'batter_walks':
            return self.batter_walks_hit_rate(n_games_to_consider, line, n, outcome_name, player_name)
        if prop_type == 'batter_strikeouts':
            return self.batter_strikeouts_hit_rate(n_games_to_consider, line, n, outcome_name, player_name)
        if prop_type == 'batter_stolen_bases':
            return self.batter_stolen_bases_hit_rate(n_games_to_consider, line, n, outcome_name, player_name)
        if prop_type == 'pitcher_strikeouts':
            return self.pitcher_strikeouts_hit_rate(n_games_to_consider, line, n, outcome_name, player_name)
        if prop_type == 'pitcher_record_a_win':
            return self.pitcher_record_a_win_hit_rate(n_games_to_consider, line, n, outcome_name, player_name)
        if prop_type == 'pitcher_hits_allowed':
            return self.pitcher_hits_allowed_hit_rate(n_games_to_consider, line, n, outcome_name, player_name)
        if prop_type == 'pitcher_walks':
            return self.pitcher_walks_hit_rate(n_games_to_consider, line, n, outcome_name, player_name)
        if prop_type == 'pitcher_earned_runs':
            return self.pitcher_earned_runs_hit_rate(n_games_to_consider, line, n, outcome_name, player_name)
        if prop_type == 'pitcher_outs':
            return self.pitcher_outs_hit_rate(n_games_to_consider, line, n, outcome_name, player_name)
    def batter_total_bases_hit_rate(self, games, line, n, outcome_name, player_name):
        hits = 0
        for game in games:
            total_bases = game['total_bases'] if game['total_bases'] is not None else 0
            if outcome_name == 'over' and total_bases > line:
                hits += 1
            elif outcome_name == 'under' and total_bases < line:
                hits += 1
        return hits / n if n > 0 else 0

    def batter_rbis_hit_rate(self, games, line, n, outcome_name, player_name):
        hits = 0
        for game in games:
            rbi = game['rbi'] if game['rbi'] is not None else 0
            if outcome_name == 'over' and rbi > line:
                hits += 1
            elif outcome_name == 'under' and rbi < line:
                hits += 1
        return hits / n if n > 0 else 0

    def batter_runs_scored_hit_rate(self, games, line, n, outcome_name, player_name):
        hits = 0
        for game in games:
            runs = game['runs'] if game['runs'] is not None else 0
            if outcome_name == 'over' and runs > line:
                hits += 1
            elif outcome_name == 'under' and runs < line:
                hits += 1
        return hits / n if n > 0 else 0

    def batter_hits_runs_rbis_hit_rate(self, games, line, n, outcome_name, player_name):
        hits = 0
        for game in games:
            hits_val = game['hits'] if game['hits'] is not None else 0
            runs_val = game['runs'] if game['runs'] is not None else 0
            rbi_val = game['rbi'] if game['rbi'] is not None else 0
            val = hits_val + runs_val + rbi_val
            if outcome_name == 'over' and val > line:
                hits += 1
            elif outcome_name == 'under' and val < line:
                hits += 1
        return hits / n if n > 0 else 0

    def batter_singles_hit_rate(self, games, line, n, outcome_name, player_name):
        hits = 0
        for game in games:
            hits_val = game['hits'] if game['hits'] is not None else 0
            doubles = game['doubles'] if game['doubles'] is not None else 0
            triples = game['triples'] if game['triples'] is not None else 0
            hr = game['hr'] if game['hr'] is not None else 0
            singles = hits_val - (doubles + triples + hr)
            if outcome_name == 'over' and singles > line:
                hits += 1
            elif outcome_name == 'under' and singles < line:
                hits += 1
        return hits / n if n > 0 else 0

    def batter_doubles_hit_rate(self, games, line, n, outcome_name, player_name):
        hits = 0
        for game in games:
            doubles = game['doubles'] if game['doubles'] is not None else 0
            if outcome_name == 'over' and doubles > line:
                hits += 1
            elif outcome_name == 'under' and doubles < line:
                hits += 1
        return hits / n if n > 0 else 0

    def batter_triples_hit_rate(self, games, line, n, outcome_name, player_name):
        hits = 0
        for game in games:
            triples = game['triples'] if game['triples'] is not None else 0
            if outcome_name == 'over' and triples > line:
                hits += 1
            elif outcome_name == 'under' and triples < line:
                hits += 1
        return hits / n if n > 0 else 0

    def batter_walks_hit_rate(self, games, line, n, outcome_name, player_name):
        hits = 0
        for game in games:
            bb = game['bb'] if game['bb'] is not None else 0
            if outcome_name == 'over' and bb > line:
                hits += 1
            elif outcome_name == 'under' and bb < line:
                hits += 1
        return hits / n if n > 0 else 0

    def batter_strikeouts_hit_rate(self, games, line, n, outcome_name, player_name):
        hits = 0
        for game in games:
            k = game['k'] if game['k'] is not None else 0
            if outcome_name == 'over' and k > line:
                hits += 1
            elif outcome_name == 'under' and k < line:
                hits += 1
        return hits / n if n > 0 else 0

    def batter_stolen_bases_hit_rate(self, games, line, n, outcome_name, player_name):
        hits = 0
        for game in games:
            stolen_bases = game['stolen_bases'] if game['stolen_bases'] is not None else 0
            if outcome_name == 'over' and stolen_bases > line:
                hits += 1
            elif outcome_name == 'under' and stolen_bases < line:
                hits += 1
        return hits / n if n > 0 else 0

    def pitcher_strikeouts_hit_rate(self, games, line, n, outcome_name, player_name):
        hits = 0
        for game in games:
            p_k = game['p_k'] if game['p_k'] is not None else 0
            if outcome_name == 'over' and p_k > line:
                hits += 1
            elif outcome_name == 'under' and p_k < line:
                hits += 1
        return hits / n if n > 0 else 0

    def pitcher_record_a_win_hit_rate(self, games, line, n, outcome_name, player_name):
        hits = 0
        for game in games:
            wins = game['wins'] if game['wins'] is not None else 0
            # win is 1 if pitcher got the win, 0 otherwise
            if outcome_name == 'yes' and wins > 0:
                hits += 1
            elif outcome_name == 'no' and wins == 0:
                hits += 1
        return hits / n if n > 0 else 0

    def pitcher_hits_allowed_hit_rate(self, games, line, n, outcome_name, player_name):
        hits = 0
        for game in games:
            p_hits = game['p_hits'] if game['p_hits'] is not None else 0
            if outcome_name == 'over' and p_hits > line:
                hits += 1
            elif outcome_name == 'under' and p_hits < line:
                hits += 1
        return hits / n if n > 0 else 0

    def pitcher_walks_hit_rate(self, games, line, n, outcome_name, player_name):
        hits = 0
        for game in games:
            p_bb = game['p_bb'] if game['p_bb'] is not None else 0
            if outcome_name == 'over' and p_bb > line:
                hits += 1
            elif outcome_name == 'under' and p_bb < line:
                hits += 1
        return hits / n if n > 0 else 0

    def pitcher_earned_runs_hit_rate(self, games, line, n, outcome_name, player_name):
        hits = 0
        for game in games:
            er = game['er'] if game['er'] is not None else 0
            if outcome_name == 'over' and er > line:
                hits += 1
            elif outcome_name == 'under' and er < line:
                hits += 1
        return hits / n if n > 0 else 0

    def pitcher_outs_hit_rate(self, games, line, n, outcome_name, player_name):
        hits = 0
        for game in games:
            pitching_outs = game['pitching_outs'] if game['pitching_outs'] is not None else 0
            if outcome_name == 'over' and pitching_outs > line:
                hits += 1
            elif outcome_name == 'under' and pitching_outs < line:
                hits += 1
        return hits / n if n > 0 else 0


    def calculate_edge(self, hit_rates, price):
        pass  # Edge calculation removed (no longer used)

    def create_parlays(self):
        pass

    def calculate_tier(self, hit_rate_10_game, hit_rate_30_game, hit_rate_60_game):

        result = {
            "tier": None,
            "recently_hot": False,
        }


        if hit_rate_10_game is None or hit_rate_30_game is None or hit_rate_60_game is None:
            return result

        if hit_rate_10_game >= .70:
            result['recently_hot'] = True

        if hit_rate_10_game >= 0.70 and hit_rate_30_game >= 0.60 and hit_rate_60_game >= 0.60:
            result['tier'] = 'Strong Quality'
            return result

        if hit_rate_10_game >= 0.70 and hit_rate_30_game >= 0.55 and hit_rate_60_game >= 0.55:
            result['tier'] = 'Good Quality'
            return result

        return result

    def batter_home_runs_hit_rate(self, games, line, n, outcome_name, player_name):
        hits = 0
        for game in games:
            hr = game['hr'] if game['hr'] is not None else 0
            if outcome_name == 'over' and hr > line:
                hits += 1
            elif outcome_name == 'under' and hr < line:
                hits += 1
        return hits / n if n > 0 else 0

    def batter_hits_hit_rate(self, games, line, n, outcome_name, player_name):
        hits = 0
        for game in games:
            hits_val = game['hits'] if game['hits'] is not None else 0
            if outcome_name == 'over' and hits_val > line:
                hits += 1
            elif outcome_name == 'under' and hits_val < line:
                hits += 1
        return hits / n if n > 0 else 0

    def insert_hit_rate_into_db(self, player_name, prop_type, line, event_start_time, outcome_name, price, book_key, market_last_update, last_10_game_hit_rate, last_30_game_hit_rate, last_60_game_hit_rate, sport_key, tier, recently_hot):
        try:
            self.hit_rates_snapshots_repository.insert_hit_rate_snapshot(
                player_name,
                prop_type,
                line,
                event_start_time,
                outcome_name,
                price,
                book_key,
                market_last_update,
                last_10_game_hit_rate,
                last_30_game_hit_rate,
                last_60_game_hit_rate,
                sport_key,
                tier,
                recently_hot
            )
        except Exception as e:
            logging.error(f"Failed to insert hit rate snapshot for {player_name}, {prop_type}, {line}, {event_start_time}: {e}")

    def did_not_play(self, row):
        return (
            row['plate_appearances'] == 0 and
            row['batters_faced'] == 0 and
            row['pitch_count'] == 0 and
            row['putouts'] == 0 and
            row['assists'] == 0
        )

    def run(self):
        props = self.grab_props()
        self.calculate_hit_rates(props)