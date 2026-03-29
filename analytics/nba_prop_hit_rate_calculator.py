from analytics.prop_hit_rate_calculator import PropHitRateCalculatorInterface
from repositories.players_games_snapshots_repository import PlayersGamesSnapshotsRepositoryInterface
from repositories.prop_snapshots_repository_interface import PostgresPropSnapshotsRepositoryInterface
from repositories.prop_snapshots_repository_interface import PostgresPropSnapshotsRepositoryInterface
from repositories.player_snapshots_repository_interface import PlayerSnapshotsRepositoryInterface
import logging
from repositories.nba_hit_rate_snapshots_repository_interface import NBAHitRateSnapshotsRepositoryInterface
import itertools

logging = logging.getLogger(__name__)

class NBAPropHitRateCalculator(PropHitRateCalculatorInterface):
    
    def __init__(self, player_games_snapshots_repository: PlayersGamesSnapshotsRepositoryInterface, prop_snapshots_repository: PostgresPropSnapshotsRepositoryInterface, player_snapshots_repository: PlayerSnapshotsRepositoryInterface, hit_rate_snapshots_repository: NBAHitRateSnapshotsRepositoryInterface):
        self.player_games_snapshots_repository = player_games_snapshots_repository
        self.prop_snapshots_repository = prop_snapshots_repository
        self.player_snapshots_repository = player_snapshots_repository
        self.hit_rate_snapshots_repository = hit_rate_snapshots_repository
        
    def grab_props(self):
        # Grab all props within 24 hour timeframe window
        props = self.prop_snapshots_repository.get_snapshots_by_timeframe(24, 'basketball_nba') # Within 24 hours, and filter for nba props only

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
        # For each prop grab the player name, and prop type

        # For each player name, query the players_games_snapshots_repository for the latest 10 games, 20 games, 30 games, and all time.

        # Store the hit rates in the DB with the prop type, player name, and hit rates.

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
        # Find the users player id
        logging.info(f"Calculating hit rate for {player_name}, prop type: {prop_type}, line: {line}, outcome: {outcome_name}, n: {n}")
        # We need to fix this because the names dont always match between the Hisotrical Stats API and the Odds API
        # 
        player_snapshot = self.player_snapshots_repository.get_player_snapshot_by_name(player_name.split()[0], " ".join(player_name.split()[1:]), 'nba') # split the player name into first and last name, and query the player snapshots repository for the player id

        if len(player_snapshot) == 0:
            print(f"No player snapshot found for {player_name}")
            return None

        player_id = player_snapshot[0][2] # player_id is the 3rd column in the player snapshot

        logging.info(f"Found player id {player_id} for player {player_name}")
        # Query the players_games_snapshots_repository for the latest n games for the player
        games = self.player_games_snapshots_repository.get_player_game_snapshots_latest(player_id, 'nba')
        logging.info(f"Found {len(games)} games for player {player_name} with player id {player_id}")

        # Player may not have played n games, so we need to account for that. If they have not played any games, we should return None or some indication that we cannot calculate a hit rate.
        if len(games) < n:
            return None

        n_games = games[:n] 

        
        # Calculate the hit rate for the prop type based on the player's performance in those n games

        prop_type_to_stat_mapping = {
            'player_points': 'player_points'
        }

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
            'player_team_id',
            'position',
            'minutes',
            'points',
            'field_goals_made',
            'field_goals_attempted',
            'field_goal_percentage',
            'free_throws_made',
            'free_throws_attempted',
            'free_throw_percentage',
            'three_pointers_made',
            'three_pointers_attempted',
            'three_pointer_percentage',
            'offensive_rebounds',
            'defensive_rebounds',
            'total_rebounds',
            'assists',
            'personal_fouls',
            'steals',
            'turnovers',
            'blocks',
            'plus_minus',
            'provider'
        ]
        dict_games = []
        for game in n_games:
            stat_dict = {col: game[idx] for idx, col in enumerate(columns)}
            dict_games.append(stat_dict)

        if prop_type == 'player_points':
             return self.points_hit_rate(dict_games, line, n, event_start_time, outcome_name)
        
        elif prop_type == 'player_rebounds':
             return self.rebounds_hit_rate(dict_games, line, n, event_start_time, outcome_name)

        elif prop_type == 'player_threes':
             return self.threes_hit_rate(dict_games, line, n, event_start_time, outcome_name)
        
        elif prop_type == 'player_assists':
             return self.assists_hit_rate(dict_games, line, n, event_start_time, outcome_name)
        
        elif prop_type == 'player_blocks':
            return self.blocks_hit_rate(dict_games, line, n, event_start_time, outcome_name)
        
        elif prop_type == 'player_turnovers':
            return self.turnovers_hit_rate(dict_games, line, n, event_start_time, outcome_name)
        
        elif prop_type == 'player_steals':
            return self.steals_hit_rate(dict_games, line, n, event_start_time, outcome_name)
    
        else:
            return None

    def points_hit_rate(self, games, line, n, event_start_time, outcome_name):
        """Calculates the hit rate for a player points prop based on the player's performance in the last n games."""
        hits = 0
        logging.info(f"Calculating points hit rate for line: {line}, outcome: {outcome_name}, n: {n} games")
        for game in range(0, n):
            if games[game]['minutes'] == 0:
                logging.info(f"Player did not play in game {game}, skipping...")
                continue
            player_points_that_game = games[game]['points']
            logging.info(f"Player scored {player_points_that_game} points in game {game}")
            
            if outcome_name == "over" and player_points_that_game > line:
                hits += 1
            elif outcome_name == "under" and player_points_that_game < line:
                hits += 1

        hit_rate = hits / n if n > 0 else 0

        return hit_rate    


    def rebounds_hit_rate(self, games, line, n, event_start_time, outcome_name):
        hits = 0
        logging.info(f"Calculating rebounds hit rate for line: {line}, outcome: {outcome_name}, n: {n} games")

        for game in range(0, n):
            if games[game]['minutes'] == 0:
                logging.info(f"Player did not play in game {game}, skipping...")
                continue
            player_rebounds_that_game = games[game]['total_rebounds']
            logging.info(f"Player had {player_rebounds_that_game} rebounds in game {game}")

            if outcome_name == "over" and player_rebounds_that_game > line:
                hits += 1
            elif outcome_name == "under" and player_rebounds_that_game < line:
                hits += 1

        hit_rate = hits / n if n > 0 else 0

        return hit_rate


    def assists_hit_rate(self, games, line, n, event_start_time, outcome_name):
        hits = 0

        logging.info(f"Calculating assists hit rate for line: {line}, outcome: {outcome_name}, n: {n} games")

        for game in range(0, n):
            if games[game]['minutes'] == 0:
                logging.info(f"Player did not play in game {game}, skipping...")
                continue
            player_assists_that_game = games[game]['assists']
            logging.info(f"Player had {player_assists_that_game} assists in game {game}")

            if outcome_name == "over" and player_assists_that_game > line:
                hits += 1
            elif outcome_name == "under" and player_assists_that_game < line:
                hits += 1

        hit_rate = hits / n if n > 0 else 0

        return hit_rate
    

    def threes_hit_rate(self, games, line, n, event_start_time, outcome_name):
        hits = 0

        logging.info(f"Calculating threes hit rate for line: {line}, outcome: {outcome_name}, n: {n} games")

        for game in range(0, n):
            if games[game]['minutes'] == 0:
                logging.info(f"Player did not play in game {game}, skipping...")
                continue
            player_threes_that_game = games[game]['three_pointers_made']
            logging.info(f"Player made {player_threes_that_game} three-pointers in game {game}")

            if outcome_name == "over" and player_threes_that_game > line:
                hits += 1
            elif outcome_name == "under" and player_threes_that_game < line:
                hits += 1

        hit_rate = hits / n if n > 0 else 0

        return hit_rate
    
    def blocks_hit_rate(self, games, line, n, event_start_time, outcome_name):
        hits = 0
        logging.info(f"Calculating blocks hit rate for line: {line}, outcome: {outcome_name}, n: {n} games")

        for game in range(0, n):
            if games[game]['minutes'] == 0:
                logging.info(f"Player did not play in game {game}, skipping...")
                continue
            player_blocks_that_game = games[game]['blocks']
            logging.info(f"Player had {player_blocks_that_game} blocks in game {game}")

            if outcome_name == "over" and player_blocks_that_game > line:
                hits += 1
            elif outcome_name == "under" and player_blocks_that_game < line:
                hits += 1

        hit_rate = hits / n if n > 0 else 0

        return hit_rate
    

    def turnovers_hit_rate(self, games, line, n, event_start_time, outcome_name):
        hits = 0

        logging.info(f"Calculating turnovers hit rate for line: {line}, outcome: {outcome_name}, n: {n} games")

        for game in range(0, n):
            if games[game]['minutes'] == 0:
                logging.info(f"Player did not play in game {game}, skipping...")
                continue
            player_turnovers_that_game = games[game]['turnovers']
            logging.info(f"Player had {player_turnovers_that_game} turnovers in game {game}")

            if outcome_name == "over" and player_turnovers_that_game > line:
                hits += 1
            elif outcome_name == "under" and player_turnovers_that_game < line:
                hits += 1

        hit_rate = hits / n if n > 0 else 0

        return hit_rate
    

    def steals_hit_rate(self, games, line, n, event_start_time, outcome_name):
        hits = 0

        logging.info(f"Calculating steals hit rate for line: {line}, outcome: {outcome_name}, n: {n} games")

        for game in range(0, n):
            if games[game]['minutes'] == 0:
                logging.info(f"Player did not play in game {game}, skipping...")
                continue
            player_steals_that_game = games[game]['steals']
            logging.info(f"Player had {player_steals_that_game} steals in game {game}")

            if outcome_name == "over" and player_steals_that_game > line:
                hits += 1
            elif outcome_name == "under" and player_steals_that_game < line:
                hits += 1

        hit_rate = hits / n if n > 0 else 0

        return hit_rate
    

    def insert_hit_rate_into_db(self, player_name, prop_type, line, event_start_time, outcome_name, price, book_key, market_last_update, hit_rate_10_game, hit_rate_30_game, hit_rate_60_game, sport_key, edge, tier, recently_hot):
        try:
            self.hit_rate_snapshots_repository.insert_hit_rate_snapshot(
                player_name,
                prop_type,
                line,
                outcome_name,
                event_start_time,
                price,
                book_key,
                market_last_update,
                hit_rate_10_game,
                hit_rate_30_game,
                hit_rate_60_game,
                sport_key,
                edge,
                tier,
                recently_hot,
            )
        except Exception as e:
            logging.error(f"Failed to insert hit rate snapshot for {player_name}, {prop_type}, {line}, {event_start_time}: {e}")

    """
    Based on a weighted average of the hit rates, we can calculate an edge for the prop. For example, we can weight the 10 game hit rate more than the 30 game hit rate, and the 30 game hit rate more than the 60 game hit rate. We can then compare this weighted average hit rate to the implied probability of the prop (calculated from the price) to determine if there is an edge on the prop.
    
    Model probability = (hit_rate_10_game * 0.5) + (hit_rate_30_game * 0.3) + (hit_rate_60_game * 0.2)
    """
    def calculate_edge(self, hit_rates, price):
            # Convert American odds to implied probability
            def convert_implied_probability_decimal(x):
                if x > 0:
                    decimal_odds = (x / 100) + 1
                else:
                    decimal_odds = (100 / abs(x)) + 1
                return 1 / decimal_odds

            if hit_rates['hit_rate_10_game'] is None or hit_rates['hit_rate_30_game'] is None or hit_rates['hit_rate_60_game'] is None:
                logging.info(f"One of the hit rates is None, cannot calculate edge. Hit rates: {hit_rates}")
                return None

            hit_rate_10_game = hit_rates['hit_rate_10_game']
            hit_rate_30_game = hit_rates['hit_rate_30_game']
            hit_rate_60_game = hit_rates['hit_rate_60_game']

            model_probability = (hit_rate_10_game * 0.5) + (hit_rate_30_game * 0.3) + (hit_rate_60_game * 0.2) 
            implied_probability = convert_implied_probability_decimal(price)
            edge = model_probability - implied_probability

            return edge
    
    def create_parlays(self):
        # Retrieve hit rates within 24 hours (just calculated)
        hit_rates = []

        # Grab all hit rates that meet the threshold of hit_rate_10_game > .70, hit_rate_30_game > .65, hit_rate_60_game > .60 and price > -100
        for hit_Rate in hit_rates:
            if hit_Rate['hit_rate_10_game'] is not None and hit_Rate['hit_rate_30_game'] is not None and hit_Rate['hit_rate_60_game'] is not None:
                if hit_Rate['hit_rate_10_game'] > .70 and hit_Rate['hit_rate_30_game'] > .65 and hit_Rate['hit_rate_60_game'] > .60 and hit_Rate['price'] > -100:
                    hit_rates.append(hit_Rate)

        # Generate all unique 2-leg groups
        two_legs = itertools.combinations(hit_rates, 2)
    
        # Generate all unique 3-leg groups
        three_legs = itertools.combinations(hit_rates, 3)

        # Create a parlay_group object and insert into the DB with parlay_id, created_at, earliest_start_time, latest_start_time, parlay_size, combined_american_odds, sport_key
        for two_leg in two_legs: 
            pass # Create a parlay_group_object for each 2-leg combination and insert into the DB with parlay_id, created_at, earliest_start_time, latest_start_time, parlay_size, combined_american_odds, sport_key

        # Create a parlay_leg_object for each leg and insert into the DB with parlay_leg_id, parlay_id, player_name, prop_type, line, outcome_name, event_start_time, price, book_key, market_last_update, hit_rate_10_game, hit_rate_30_game, hit_rate_60_game, edge
        pass    

    def calculate_tier(self, hit_rate_10_game, hit_rate_30_game, hit_rate_60_game):
        """
        Used for pre identifying potential strong quality bets before even looking at the price. This is based on the idea that a bet with a high hit rate in the last 10 games, as well as solid hit rates in the last 30 and 60 games, is more likely to be a strong bet regardless of the price. This can help us identify potential value bets that may be mispriced by the market.
        """
        if hit_rate_10_game is None or hit_rate_30_game is None or hit_rate_60_game is None:
            return None
        
        result = {
            "tier": None,
            "recently_hot": False,
        }
        
        # Recently Hot
        if hit_rate_10_game >= .70:
            result['recently_hot'] = True

        # Strongest quality: hot recent form with strict medium/long-term support.
        if hit_rate_10_game >= 0.70 and hit_rate_30_game >= 0.60 and hit_rate_60_game >= 0.60:
            result['tier'] = 'Strong Quality'
            return result

        # Good quality: hot recent form with solid medium/long-term support.
        if hit_rate_10_game >= 0.70 and hit_rate_30_game >= 0.55 and hit_rate_60_game >= 0.55:
            result['tier'] = 'Good Quality'
            return result

        return result

    def run(self):
        props = self.grab_props()
        self.calculate_hit_rates(props)