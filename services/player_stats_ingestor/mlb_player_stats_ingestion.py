from services.sports_stats_api.sports_stats_api_interface import SportsStatsAPIInterface
from repositories.team_snapshots_repository_interface import TeamSnapshotsRepositoryInterface
from repositories.player_snapshots_repository_interface import PlayerSnapshotsRepositoryInterface
from repositories.game_snapshots_repository_interface import GameSnapshotsRepositoryInterface
from repositories.players_games_snapshots_repository_interface import PlayersGamesSnapshotsRepositoryInterface
from repositories.mlb_players_games_snapshots_repository_interface import MLBPlayersGamesSnapshotsRepositoryInterface
from services.sports_stats_api.sports_stats_api_interface import SportsStatsAPIInterface
import logging


logger = logging.getLogger(__name__)

class MLBPlayerStatsIngestor():
    def __init__(
            self, sports_stats_api: SportsStatsAPIInterface,  
            team_snapshots_repository: TeamSnapshotsRepositoryInterface,
            player_snapshots_repository: PlayerSnapshotsRepositoryInterface,    
            game_snapshots_repository: GameSnapshotsRepositoryInterface,
            players_games_snapshots_repository: MLBPlayersGamesSnapshotsRepositoryInterface
        ):
        self.sports_stats_api = sports_stats_api
        self.team_snapshots_repository = team_snapshots_repository
        self.player_snapshots_repository = player_snapshots_repository
        self.game_snapshots_repository = game_snapshots_repository
        self.players_games_snapshots_repository = players_games_snapshots_repository
    async def ingest_teams(self):
        """
        Ingests MLB Teams into the database. Retrieves team data from the sports stats API and stores it in the team snapshots repository.
        """
        data = await self.sports_stats_api.get_teams()
        logging.info(f"Retrieved {len(data['data'])} teams.")
        
        for team in data["data"]:
            try:
                self.team_snapshots_repository.insert_team_snapshot('mlb', team['id'], team['display_name'], team['abbreviation'], 'balldontlie')
                logging.info(f"Inserted team {team['display_name']} (ID: {team['id']}) into the database.")
            except Exception as e:
                logging.error(f"Error inserting team {team['display_name']} (ID: {team['id']}): {e}")
                raise e


    async def get_players_on_team(self, season: int):
        """
        Ingests MLB Players for a given team and season into the database. Retrieves player data from the sports stats API and stores it in the player snapshots repository.
        """

        teams = self.team_snapshots_repository.get_all_team_snapshots()
        
        teams_dict = [
            {
                "team_id": team[1],
            }
            for team in teams if team[0] == 'mlb'
        ]

        for team in teams_dict:
            data = await self.sports_stats_api.get_players_on_team(team['team_id'], season)
            logging.info(f"Retrieved {len(data['data'])} players for team ID {team['team_id']} in season {season}.")

            for player in data['data']:
                try:
                    self.player_snapshots_repository.insert_player_snapshot('mlb', season, team['team_id'], player['id'], player['first_name'].lower(), player['last_name'].lower(), 'balldontlie')
                    logging.info(f"Inserted player {player['first_name']} {player['last_name']} (ID: {player['id']}) into the database.")
                except Exception as e:
                    logging.error(f"Error inserting player {player['first_name']} {player['last_name']} (ID: {player['id']}): {e}")
                    raise e

    async def get_season_games(self, season: int):
        """
        Ingests MLB Games for a given season into the database. Retrieves game data from the sports stats API and stores it in the game snapshots repository. Only games with a status of "Finished" are ingested to ensure that complete data is stored.
        """
        data = await self.sports_stats_api.get_games_from_season(season=season)
        logging.info(f"Retrieved {len(data['data'])} games for in season {season}.")

        for game in data['data']:
            try:
                if game['status'] != 'STATUS_FINAL':
                    logging.info(f"Skipping game ID {game['id']} as it is not finished (status: {game['status']}).")
                    continue
                if game['scoring_summary'] is None or len(game['scoring_summary']) == 0:
                    logging.info(f"Skipping game ID {game['id']} as it has no scoring summary.")
                    continue


                self.game_snapshots_repository.insert_game_snapshot('mlb', season, game['id'], game['date'], game['status'], game['home_team']['id'], game['away_team']['id'], game['scoring_summary'][len(game['scoring_summary']) - 1]["away_score"], game['scoring_summary'][len(game['scoring_summary']) - 1]["home_score"], 'balldontlie')
                logging.info(f"Inserted game ID {game['id']} into the database.")
            except Exception as e:
                logging.error(f"Error inserting game ID {game['id']}: {e}")
                raise e
            
    # Need to do this
    async def get_stats_from_game(self, season: int):

        players = self.player_snapshots_repository.get_all_player_snapshots(season=season, sport_key='mlb')

        players_dict = [
            {
                "player_id": player[2],
            }
            for player in players if player[0] == 'mlb'
        ]

        for player in players_dict:
            data = await self.sports_stats_api.get_players_stats_from_game(player=player['player_id'], season=season)
            logging.info(f"Retrieved stats for player ID {player['player_id']} in season {season}.")
            print(data)

            for stat in data['data']:
                try:
                    self.players_games_snapshots_repository.insert_player_game_snapshot(
                        sport_key='mlb',
                        season=season,
                        game_id=stat['game_id'],
                        player_id=player['player_id'],
                        team_name=stat['team_name'],
                        at_bats=stat.get('at_bats') if stat.get('at_bats') is not None else 0,
                        runs=stat.get('runs') if stat.get('runs') is not None else 0,
                        hits=stat.get('hits') if stat.get('hits') is not None else 0,
                        rbi=stat.get('rbi') if stat.get('rbi') is not None else 0,
                        hr=stat.get('hr') if stat.get('hr') is not None else 0,
                        bb=stat.get('bb') if stat.get('bb') is not None else 0,
                        k=stat.get('k') if stat.get('k') is not None else 0,
                        avg=stat.get('avg') if stat.get('avg') is not None else 0,
                        obp=stat.get('obp') if stat.get('obp') is not None else 0,
                        slg=stat.get('slg') if stat.get('slg') is not None else 0,
                        doubles=stat.get('doubles') if stat.get('doubles') is not None else 0,
                        triples=stat.get('triples') if stat.get('triples') is not None else 0,
                        intentional_walks=stat.get('intentional_walks') if stat.get('intentional_walks') is not None else 0,
                        hit_by_pitch=stat.get('hit_by_pitch') if stat.get('hit_by_pitch') is not None else 0,
                        stolen_bases=stat.get('stolen_bases') if stat.get('stolen_bases') is not None else 0,
                        caught_stealing=stat.get('caught_stealing') if stat.get('caught_stealing') is not None else 0,
                        plate_appearances=stat.get('plate_appearances') if stat.get('plate_appearances') is not None else 0,
                        total_bases=stat.get('total_bases') if stat.get('total_bases') is not None else 0,
                        left_on_base=stat.get('left_on_base') if stat.get('left_on_base') is not None else 0,
                        fly_outs=stat.get('fly_outs') if stat.get('fly_outs') is not None else 0,
                        ground_outs=stat.get('ground_outs') if stat.get('ground_outs') is not None else 0,
                        line_outs=stat.get('line_outs') if stat.get('line_outs') is not None else 0,
                        pop_outs=stat.get('pop_outs') if stat.get('pop_outs') is not None else 0,
                        air_outs=stat.get('air_outs') if stat.get('air_outs') is not None else 0,
                        gidp=stat.get('gidp') if stat.get('gidp') is not None else 0,
                        sac_bunts=stat.get('sac_bunts') if stat.get('sac_bunts') is not None else 0,
                        sac_flies=stat.get('sac_flies') if stat.get('sac_flies') is not None else 0,
                        ip=stat.get('ip') if stat.get('ip') is not None else 0,
                        p_hits=stat.get('p_hits') if stat.get('p_hits') is not None else 0,
                        p_runs=stat.get('p_runs') if stat.get('p_runs') is not None else 0,
                        er=stat.get('er') if stat.get('er') is not None else 0,
                        p_bb=stat.get('p_bb') if stat.get('p_bb') is not None else 0,
                        p_k=stat.get('p_k') if stat.get('p_k') is not None else 0,
                        p_hr=stat.get('p_hr') if stat.get('p_hr') is not None else 0,
                        pitch_count=stat.get('pitch_count') if stat.get('pitch_count') is not None else 0,
                        strikes=stat.get('strikes') if stat.get('strikes') is not None else 0,
                        era=stat.get('era') if stat.get('era') is not None else 0,
                        batters_faced=stat.get('batters_faced') if stat.get('batters_faced') is not None else 0,
                        pitching_outs=stat.get('pitching_outs') if stat.get('pitching_outs') is not None else 0,
                        wins=stat.get('wins') if stat.get('wins') is not None else 0,
                        losses=stat.get('losses') if stat.get('losses') is not None else 0,
                        saves=stat.get('saves') if stat.get('saves') is not None else 0,
                        holds=stat.get('holds') if stat.get('holds') is not None else 0,
                        blown_saves=stat.get('blown_saves') if stat.get('blown_saves') is not None else 0,
                        games_started=stat.get('games_started') if stat.get('games_started') is not None else 0,
                        wild_pitches=stat.get('wild_pitches') if stat.get('wild_pitches') is not None else 0,
                        balks=stat.get('balks') if stat.get('balks') is not None else 0,
                        pitching_hbp=stat.get('pitching_hbp') if stat.get('pitching_hbp') is not None else 0,
                        inherited_runners=stat.get('inherited_runners') if stat.get('inherited_runners') is not None else 0,
                        inherited_runners_scored=stat.get('inherited_runners_scored') if stat.get('inherited_runners_scored') is not None else 0,
                        putouts=stat.get('putouts') if stat.get('putouts') is not None else 0,
                        assists=stat.get('assists') if stat.get('assists') is not None else 0,
                        errors=stat.get('errors') if stat.get('errors') is not None else 0,
                        fielding_chances=stat.get('fielding_chances') if stat.get('fielding_chances') is not None else 0,
                        fielding_pct=stat.get('fielding_pct') if stat.get('fielding_pct') is not None else 0,
                        provider='balldontlie'
                    )
                    logging.info(f"Inserted stats for player ID {player['player_id']} in game ID {stat['game_id']} into the database.")
                except Exception as e:
                    logging.error(f"Error inserting stats for player ID {player} in season {season}: {e}")
                    raise e





        
