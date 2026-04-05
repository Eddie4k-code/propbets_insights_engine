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
                        at_bats=stat['at_bats'],
                        runs=stat['runs'],
                        hits=stat['hits'],
                        rbi=stat['rbi'],
                        hr=stat['hr'],
                        bb=stat['bb'],
                        k=stat['k'],
                        avg=stat['avg'],
                        obp=stat['obp'],
                        slg=stat['slg'],
                        doubles=stat['doubles'],
                        triples=stat['triples'],
                        intentional_walks=stat['intentional_walks'],
                        hit_by_pitch=stat['hit_by_pitch'],
                        stolen_bases=stat['stolen_bases'],
                        caught_stealing=stat['caught_stealing'],
                        plate_appearances=stat['plate_appearances'],
                        total_bases=stat['total_bases'],
                        left_on_base=stat['left_on_base'],
                        fly_outs=stat['fly_outs'],
                        ground_outs=stat['ground_outs'],
                        line_outs=stat['line_outs'],
                        pop_outs=stat['pop_outs'],
                        air_outs=stat['air_outs'],
                        gidp=stat['gidp'],
                        sac_bunts=stat['sac_bunts'],
                        sac_flies=stat['sac_flies'],
                        ip=stat['ip'],
                        p_hits=stat['p_hits'],
                        p_runs=stat['p_runs'],
                        er=stat['er'],
                        p_bb=stat['p_bb'],
                        p_k=stat['p_k'],
                        p_hr=stat['p_hr'],
                        pitch_count=stat['pitch_count'],
                        strikes=stat['strikes'],
                        era=stat['era'],
                        batters_faced=stat['batters_faced'],
                        pitching_outs=stat['pitching_outs'],
                        wins=stat['wins'],
                        losses=stat['losses'],
                        saves=stat['saves'],
                        holds=stat['holds'],
                        blown_saves=stat['blown_saves'],
                        games_started=stat['games_started'],
                        wild_pitches=stat['wild_pitches'],
                        balks=stat['balks'],
                        pitching_hbp=stat['pitching_hbp'],
                        inherited_runners=stat['inherited_runners'],
                        inherited_runners_scored=stat['inherited_runners_scored'],
                        putouts=stat['putouts'],
                        assists=stat['assists'],
                        errors=stat['errors'],
                        fielding_chances=stat['fielding_chances'],
                        fielding_pct=stat['fielding_pct'],
                        provider='balldontlie'
                    )
                    logging.info(f"Inserted stats for player ID {player['player_id']} in game ID {stat['game_id']} into the database.")
                except Exception as e:
                    logging.error(f"Error inserting stats for player ID {player} in season {season}: {e}")
                    raise e





        
