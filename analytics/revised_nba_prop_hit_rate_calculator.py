from analytics.prop_hit_rate_calculator import PropHitRateCalculatorInterface
from repositories.players_games_snapshots_repository_interface import PlayersGamesSnapshotsRepositoryInterface
from repositories.prop_snapshots_repository_interface import PostgresPropSnapshotsRepositoryInterface
from repositories.player_snapshots_repository_interface import PlayerSnapshotsRepositoryInterface
from repositories.nba_hit_rate_snapshots_repository_interface import NBAHitRateSnapshotsRepositoryInterface
import logging
from analytics.utils import map_data_to_columns

class RevisedNBAPropHitRateCalculator(PropHitRateCalculatorInterface):
    def __init__(
        self,
        player_games_snapshots_repository: PlayersGamesSnapshotsRepositoryInterface,
        prop_snapshots_repository: PostgresPropSnapshotsRepositoryInterface,
        player_snapshots_repository: PlayerSnapshotsRepositoryInterface,
        hit_rate_snapshots_repository: NBAHitRateSnapshotsRepositoryInterface
    ):
        self.player_games_snapshots_repository = player_games_snapshots_repository
        self.prop_snapshots_repository = prop_snapshots_repository
        self.player_snapshots_repository = player_snapshots_repository
        self.hit_rate_snapshots_repository = hit_rate_snapshots_repository
        self.logger = logging.getLogger(self.__class__.__name__)

    def grab_props(self):
        """
        Fetch and return NBA prop snapshots for processing.
        """
        props = self.prop_snapshots_repository.get_snapshots_by_timeframe(24, 'basketball_nba')
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
            if len(prop) != len(columns):
                self.logger.warning(f"Skipping malformed prop: {prop}")
                continue
            prop_dict = map_data_to_columns(prop, columns)
            prop_dicts.append(prop_dict)
        self.logger.info(f"Fetched {len(prop_dicts)} NBA props for processing.")
        return prop_dicts
    

    def calculate_hit_rates(self, props):
        for prop in props:

            logging.info(f"Calculating hit rates for prop: {prop['player_name']} - {prop['market_key']} {prop['line']} {prop['outcome_name']}")

            last_10_game_hit_rate = self.calculate_n_game_hit_rate(
                prop['player_name'],
                prop['market_key'],
                prop['line'],
                10,
                prop['event_start_time'],
                prop['outcome_name']
            )

            last_20_game_hit_rate = self.calculate_n_game_hit_rate(
                prop['player_name'],
                prop['market_key'],
                prop['line'],
                20,
                prop['event_start_time'],
                prop['outcome_name']
            )

            last_30_game_hit_rate = self.calculate_n_game_hit_rate(
                prop['player_name'],
                prop['market_key'],
                prop['line'],
                30,
                prop['event_start_time'],
                prop['outcome_name']
            )

            if last_10_game_hit_rate is None and last_20_game_hit_rate is None and last_30_game_hit_rate is None:
                logging.warning(f"Could not calculate any hit rates for prop: {prop['player_name']} - {prop['market_key']} {prop['line']} {prop['outcome_name']}. Skipping database insertion.")
                continue

            tier = self.calculate_tier(last_10_game_hit_rate, last_20_game_hit_rate, last_30_game_hit_rate)

            self.insert_hit_rate_into_db(
                player_name=prop['player_name'],
                prop_type=prop['market_key'],
                line=prop['line'],
                event_start_time=prop['event_start_time'],
                outcome_name=prop['outcome_name'],
                price=prop['price'],
                book_key=prop['book_key'],
                market_last_update=prop['market_last_update'],
                hit_rate_10_game=last_10_game_hit_rate,
                hit_rate_30_game=last_20_game_hit_rate,
                hit_rate_60_game=last_30_game_hit_rate,
                sport_key=prop['sport_key'],
                edge=None,
                tier=tier['tier'] if tier else None,
                recently_hot=tier['recently_hot'] if tier else None
            )

            logging.info(f"Finished processing prop: {prop['player_name']} - {prop['market_key']} {prop['line']} {prop['outcome_name']}")


    def calculate_tier(self, hit_rate_10_game, hit_rate_30_game, hit_rate_60_game):
        """
        Used for pre identifying potential strong quality bets before even looking at the price. This is based on the idea that a bet with a high hit rate in the last 10 games, as well as solid hit rates in the last 30 and 60 games, is more likely to be a strong bet regardless of the price. This can help us identify potential value bets that may be mispriced by the market.
        """
        logging.info(f"Calculating tier based on hit rates - 10 game: {hit_rate_10_game}, 30 game: {hit_rate_30_game}, 60 game: {hit_rate_60_game}")

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

    
    def calculate_n_game_hit_rate(self, player_name, prop_type, line, n_games, event_start_time, outcome_name):
        """
        Calculate the hit rate for a given player prop over the last N games.
        
        Args:
            player_name (str): The name of the player.
            prop_type (str): The type of prop (e.g., 'points', 'rebounds').
            line (float): The line for the prop.
            n_games (int): The number of recent games to consider.
            event_start_time (datetime): The start time of the event.
            outcome_name (str): The name of the outcome (e.g., 'over', 'under').
        Returns:
            float: The calculated hit rate as a percentage.
        """

        logging.info(f"Calculating hit rate for {player_name} - {prop_type} {outcome_name} {line} over the last {n_games} games.")


        logging.info(f"Fetching Player Snapshot for player: {player_name}")

        # We need to make it so that we get player_snapshot tied to season, because we will get multiple player snapshots for a player across seasons.
        player_snapshot = self.player_snapshots_repository.get_player_snapshot_by_name(player_name.split()[0], " ".join(player_name.split()[1:]), 'nba')

        if player_snapshot == []:
            logging.warning(f"No player snapshot found for {player_name}. Cannot calculate hit rate.")
            return None
        
        player_id = player_snapshot[0][2]

        logging.info(f"Fetching all player games for player_id: {player_id}")

        player_games = self.player_games_snapshots_repository.get_player_game_snapshots_latest(player_id, 'nba')

        logging.info(f"Fetched player games for player {player_name} Successfully")

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

        logging.info(f"Mapping player game data to columns for player {player_name}")

        player_game_dicts = [
            map_data_to_columns(game, columns) for game in player_games
        ]

        logging.info(f"Filtering games where the player did had minutes of 0 for player {player_name}")

        player_game_dicts = [game for game in player_game_dicts if game['minutes'] != 0]

        logging.info(f"Checking if there are enough games to calculate hit rate for player {player_name} of {n_games} games")

        n_games_filtered = player_game_dicts[:n_games]

        if len(n_games_filtered) < n_games:
            logging.warning(f"Not enough games to calculate hit rate for player {player_name}. Required: {n_games}, Available: {len(n_games_filtered)}")
            return None
        
        logging.info(f"Calculating hit rate for player {player_name} based on the last {n_games} games")

        
        if prop_type == 'player_points':
             return self.points_hit_rate(n_games_filtered, line, n_games, event_start_time, outcome_name)
        
        elif prop_type == 'player_rebounds':
             return self.rebounds_hit_rate(n_games_filtered, line, n_games, event_start_time, outcome_name)

        elif prop_type == 'player_threes':
             return self.threes_hit_rate(n_games_filtered, line, n_games, event_start_time, outcome_name)
        
        elif prop_type == 'player_assists':
             return self.assists_hit_rate(n_games_filtered, line, n_games, event_start_time, outcome_name)
        
        elif prop_type == 'player_blocks':
            return self.blocks_hit_rate(n_games_filtered, line, n_games, event_start_time, outcome_name)
        
        elif prop_type == 'player_turnovers':
            return self.turnovers_hit_rate(n_games_filtered, line, n_games, event_start_time, outcome_name)
        
        elif prop_type == 'player_steals':
            return self.steals_hit_rate(n_games_filtered, line, n_games, event_start_time, outcome_name)
    
        else:
            return None


    def points_hit_rate(self, games, line, n, event_start_time, outcome_name):
            """
            Calculate the hit rate for a points prop.
        
            Args:
                games (list): A list of dictionaries representing the player's last N games.
                line (float): The line for the points prop.
                n (int): The number of games to consider for the hit rate calculation.
                event_start_time (datetime): The start time of the event.
                outcome_name (str): The name of the outcome (e.g., 'over', 'under').
            Returns:
                float: The calculated hit rate as a percentage.
            """

            logging.info(f"Calculating points hit rate for line {line} and outcome {outcome_name} over the last {n} games.")

            hits = 0

            for game in games:
                if outcome_name == 'over' and game['points'] > line:
                    hits += 1
                elif outcome_name == 'under' and game['points'] < line:
                    hits += 1

            hit_rate = hits / n if n > 0 else 0 

            logging.info(f"Calculated points hit rate: {hit_rate}% for line {line} and outcome {outcome_name} over the last {n} games.")

            return hit_rate

    def rebounds_hit_rate(self, games, line, n, event_start_time, outcome_name):
        """
        Calculate the hit rate for a rebounds prop.
    
        Args:
            games (list): A list of dictionaries representing the player's last N games.
            line (float): The line for the rebounds prop.
            n (int): The number of games to consider for the hit rate calculation.
            event_start_time (datetime): The start time of the event.
            outcome_name (str): The name of the outcome (e.g., 'over', 'under').
        Returns:
            float: The calculated hit rate as a percentage.
        """

        logging.info(f"Calculating rebounds hit rate for line {line} and outcome {outcome_name} over the last {n} games.")

        hits = 0

        for game in games:
            player_rebounds_that_game = game.get('total_rebounds', 0) or 0
            logging.info(f"Player had {player_rebounds_that_game} rebounds in game.")
            if outcome_name == 'over' and player_rebounds_that_game > line:
                hits += 1
            elif outcome_name == 'under' and player_rebounds_that_game < line:
                hits += 1

        hit_rate = hits / n if n > 0 else 0

        logging.info(f"Calculated rebounds hit rate: {hit_rate}% for line {line} and outcome {outcome_name} over the last {n} games.")

        return hit_rate

    def assists_hit_rate(self, games, line, n, event_start_time, outcome_name):
        """
        Calculate the hit rate for an assists prop.
    
        Args:
            games (list): A list of dictionaries representing the player's last N games.
            line (float): The line for the assists prop.
            n (int): The number of games to consider for the hit rate calculation.
            event_start_time (datetime): The start time of the event.
            outcome_name (str): The name of the outcome (e.g., 'over', 'under').
        Returns:
            float: The calculated hit rate as a percentage.
        """

        logging.info(f"Calculating assists hit rate for line {line} and outcome {outcome_name} over the last {n} games.")

        hits = 0

        for game in games:
            player_assists_that_game = game.get('assists', 0) or 0
            logging.info(f"Player had {player_assists_that_game} assists in game.")
            if outcome_name == 'over' and player_assists_that_game > line:
                hits += 1
            elif outcome_name == 'under' and player_assists_that_game < line:
                hits += 1

        hit_rate = hits / n if n > 0 else 0

        logging.info(f"Calculated assists hit rate: {hit_rate}% for line {line} and outcome {outcome_name} over the last {n} games.")

        return hit_rate

    def threes_hit_rate(self, games, line, n, event_start_time, outcome_name):
        """
        Calculate the hit rate for a three-pointers made prop.
    
        Args:
            games (list): A list of dictionaries representing the player's last N games.
            line (float): The line for the threes prop.
            n (int): The number of games to consider for the hit rate calculation.
            event_start_time (datetime): The start time of the event.
            outcome_name (str): The name of the outcome (e.g., 'over', 'under').
        Returns:
            float: The calculated hit rate as a percentage.
        """

        logging.info(f"Calculating threes hit rate for line {line} and outcome {outcome_name} over the last {n} games.")

        hits = 0

        for game in games:
            player_threes_that_game = game.get('three_pointers_made', 0) or 0
            logging.info(f"Player made {player_threes_that_game} three-pointers in game.")
            if outcome_name == 'over' and player_threes_that_game > line:
                hits += 1
            elif outcome_name == 'under' and player_threes_that_game < line:
                hits += 1

        hit_rate = hits / n if n > 0 else 0

        logging.info(f"Calculated threes hit rate: {hit_rate}% for line {line} and outcome {outcome_name} over the last {n} games.")

        return hit_rate

    def blocks_hit_rate(self, games, line, n, event_start_time, outcome_name):
        """
        Calculate the hit rate for a blocks prop.
    
        Args:
            games (list): A list of dictionaries representing the player's last N games.
            line (float): The line for the blocks prop.
            n (int): The number of games to consider for the hit rate calculation.
            event_start_time (datetime): The start time of the event.
            outcome_name (str): The name of the outcome (e.g., 'over', 'under').
        Returns:
            float: The calculated hit rate as a percentage.
        """

        logging.info(f"Calculating blocks hit rate for line {line} and outcome {outcome_name} over the last {n} games.")

        hits = 0

        for game in games:
            player_blocks_that_game = game.get('blocks', 0) or 0
            logging.info(f"Player had {player_blocks_that_game} blocks in game.")
            if outcome_name == 'over' and player_blocks_that_game > line:
                hits += 1
            elif outcome_name == 'under' and player_blocks_that_game < line:
                hits += 1

        hit_rate = hits / n if n > 0 else 0

        logging.info(f"Calculated blocks hit rate: {hit_rate}% for line {line} and outcome {outcome_name} over the last {n} games.")

        return hit_rate

    def turnovers_hit_rate(self, games, line, n, event_start_time, outcome_name):
        """
        Calculate the hit rate for a turnovers prop.
    
        Args:
            games (list): A list of dictionaries representing the player's last N games.
            line (float): The line for the turnovers prop.
            n (int): The number of games to consider for the hit rate calculation.
            event_start_time (datetime): The start time of the event.
            outcome_name (str): The name of the outcome (e.g., 'over', 'under').
        Returns:
            float: The calculated hit rate as a percentage.
        """

        logging.info(f"Calculating turnovers hit rate for line {line} and outcome {outcome_name} over the last {n} games.")

        hits = 0

        for game in games:
            player_turnovers_that_game = game.get('turnovers', 0) or 0
            logging.info(f"Player had {player_turnovers_that_game} turnovers in game.")
            if outcome_name == 'over' and player_turnovers_that_game > line:
                hits += 1
            elif outcome_name == 'under' and player_turnovers_that_game < line:
                hits += 1

        hit_rate = hits / n if n > 0 else 0

        logging.info(f"Calculated turnovers hit rate: {hit_rate}% for line {line} and outcome {outcome_name} over the last {n} games.")

        return hit_rate

    def steals_hit_rate(self, games, line, n, event_start_time, outcome_name):
        """
        Calculate the hit rate for a steals prop.
    
        Args:
            games (list): A list of dictionaries representing the player's last N games.
            line (float): The line for the steals prop.
            n (int): The number of games to consider for the hit rate calculation.
            event_start_time (datetime): The start time of the event.
            outcome_name (str): The name of the outcome (e.g., 'over', 'under').
        Returns:
            float: The calculated hit rate as a percentage.
        """

        logging.info(f"Calculating steals hit rate for line {line} and outcome {outcome_name} over the last {n} games.")

        hits = 0

        for game in games:
            player_steals_that_game = game.get('steals', 0) or 0
            logging.info(f"Player had {player_steals_that_game} steals in game.")
            if outcome_name == 'over' and player_steals_that_game > line:
                hits += 1
            elif outcome_name == 'under' and player_steals_that_game < line:
                hits += 1

        hit_rate = hits / n if n > 0 else 0

        logging.info(f"Calculated steals hit rate: {hit_rate}% for line {line} and outcome {outcome_name} over the last {n} games.")

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


    def calculate_edge(self, hit_rates, price):
        pass

    def create_parlays(self):
        pass
    
    def run(self):
        props = self.grab_props()
        self.calculate_hit_rates(props)



        






