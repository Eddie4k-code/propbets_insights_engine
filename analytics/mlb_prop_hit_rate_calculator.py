from analytics.prop_hit_rate_calculator import PropHitRateCalculatorInterface
from repositories.mlb_players_games_snapshots_repository_interface import MLBPlayersGamesSnapshotsRepositoryInterface
from repositories.player_snapshots_repository_interface import PlayerSnapshotsRepositoryInterface
from repositories.prop_snapshots_repository_interface import PostgresPropSnapshotsRepositoryInterface
import logging
from repositories.mlb_hit_rate_snapshots_repository_interface import MLBHitRateSnapshotsRepositoryInterface


logging = logging.getLogger(__name__)


class MLBPropHitRateCalculator(PropHitRateCalculatorInterface):

    def __init__(self, players_games_snapshots_repository: MLBPlayersGamesSnapshotsRepositoryInterface, props_snapshots_repository: PostgresPropSnapshotsRepositoryInterface, player_snapshots_repository: PlayerSnapshotsRepositoryInterface, hit_rates_snapshots_repository: MLBHitRateSnapshotsRepositoryInterface):
        self.players_games_snapshots_repository = players_games_snapshots_repository
        self.props_snapshots_repository = props_snapshots_repository
        self.player_snapshots_repository = player_snapshots_repository
        self.hit_rates_snapshots_repository = hit_rates_snapshots_repository

    def grab_props(self):
        # Grab all props within 24 hour timeframe window
        props = self.props_snapshots_repository.get_snapshots_by_timeframe(24, 'mlb') # Within 24 hours, and filter for mlb props only

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

                # Calculate the edge for this particular prop based on the hit rates and the price
                edge = self.calculate_edge(hit_rates={
                    'hit_rate_10_game': last_10_game_hit_rate,
                    'hit_rate_30_game': last_30_game_hit_rate,
                    'hit_rate_60_game': last_60_game_hit_rate
                }, price=price)
                
                # AVOID PUTTING BAD Data
                if last_10_game_hit_rate == None and last_30_game_hit_rate == None and last_60_game_hit_rate == None:
                    logging.info(f"Skipping player: {player_name}, prop type: {prop_type}, line: {line}, outcome: {outcome_name}, event start time: {event_start_time} due to all hit rates being 0")
                    continue


                logging.info(f"Calculated hit rates for player: {player_name}, prop type: {prop_type}, line: {line}, outcome: {outcome_name}, event start time: {event_start_time}. Last 10 game hit rate: {last_10_game_hit_rate}, Last 30 game hit rate: {last_30_game_hit_rate}, Last 60 game hit rate: {last_60_game_hit_rate}")

                logging.info(f"Calculating Tier based on hit rates for player: {player_name}, prop type: {prop_type}, line: {line}, outcome: {outcome_name}, event start time: {event_start_time}")

                tier_result = self.calculate_tier(last_10_game_hit_rate, last_30_game_hit_rate, last_60_game_hit_rate)

                self.insert_hit_rate_into_db(player_name, prop_type, line, event_start_time, outcome_name, price, book_key, market_last_update, last_10_game_hit_rate, last_30_game_hit_rate, last_60_game_hit_rate, sport_key, edge, tier_result['tier'], tier_result['recently_hot'])

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

        if len(games) < n:
            logging.info(f"Not enough games to calculate hit rate for player: {player_name}, prop type: {prop_type}, line: {line}, outcome: {outcome_name}. Found {len(games)} games, but need at least {n} games.")
            return None
        
        n_games = games[:n]

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

        for game in n_games:
            stat_dict = {col: game[idx] for idx, col in enumerate(columns)}
            dict_games.append(stat_dict)


        # TO DO NEXT

        if prop_type == '':
            pass


    def calculate_edge(self, hit_rates, price):
        pass

    def create_parlays(self):
        pass

    def calculate_tier(self):
        pass

    def insert_hit_rate_into_db(self, player_name, prop_type, line, event_start_time, outcome_name, price, book_key, market_last_update, last_10_game_hit_rate, last_30_game_hit_rate, last_60_game_hit_rate, sport_key, edge, tier, recently_hot):
        pass

    def run(self):
        pass