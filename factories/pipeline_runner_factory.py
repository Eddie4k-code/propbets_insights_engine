from services.pipeline_runner.pipeline_runner import PipelineRunner
from services.sports_api.sports_api_config import SportsAPIConfig
from services.sports_api.sports_api import TheOddsAPI
from http_client.http_client import HTTPClientInterface
from repositories.prop_snapshots_repository_interface import PostgresPropSnapshotsRepositoryInterface
from repositories.player_snapshots_repository_interface import PlayerSnapshotsRepositoryInterface
from repositories.team_snapshots_repository_interface import TeamSnapshotsRepositoryInterface
from repositories.game_snapshots_repository_interface import GameSnapshotsRepositoryInterface
from repositories.players_games_snapshots_repository_interface import PlayersGamesSnapshotsRepositoryInterface
from repositories.nba_hit_rate_snapshots_repository_interface import NBAHitRateSnapshotsRepositoryInterface
from services.props_ingestor.prop_ingestor import PropIngestor
from services.player_stats_ingestor.nba_player_stats_ingestor import NBAPlayerStatsIngestor
from services.event_ingestor.event_ingestor import EventIngestor
from analytics.nba_prop_hit_rate_calculator import NBAPropHitRateCalculator
from db.initate_connection_interface import InitiateConnectionInterface
from services.sports_stats_api.sports_stats_api_config import SportsStatsAPIConfig
from repositories.game_snapshots_repository_interface import GameSnapshotsRepositoryInterface
from repositories.player_snapshots_repository_interface import PlayerSnapshotsRepositoryInterface
from repositories.players_games_snapshots_repository_interface import PlayersGamesSnapshotsRepositoryInterface
from repositories.team_snapshots_repository_interface import TeamSnapshotsRepositoryInterface
from repositories.prop_snapshots_repository_interface import PostgresPropSnapshotsRepositoryInterface



class PipelineRunnerFactory:
    @staticmethod
    def create_pipeline_runner(
        sport: str,
        db_client: InitiateConnectionInterface,
        http_client: HTTPClientInterface,
        sports_stats_api_config: SportsStatsAPIConfig,
        odds_api_config: SportsAPIConfig,
        prop_snapshots_repository: PostgresPropSnapshotsRepositoryInterface,
        player_snapshots_repository: PlayerSnapshotsRepositoryInterface,
        team_snapshots_repository: TeamSnapshotsRepositoryInterface,
        game_snapshots_repository: GameSnapshotsRepositoryInterface,
        players_games_snapshots_repository: PlayersGamesSnapshotsRepositoryInterface,
        nba_hit_rate_snapshots_repository: NBAHitRateSnapshotsRepositoryInterface
    ) -> PipelineRunner:

        if sport == "basketball_nba":

            # Example config, replace with actual config
            api_config = SportsAPIConfig()
            bet_api = TheOddsAPI(api_config, http_client)

            prop_snapshots_repo = PostgresPropSnapshotsRepositoryInterface(db_client)
            player_snapshots_repo = PlayerSnapshotsRepositoryInterface(db_client)
            team_snapshots_repo = TeamSnapshotsRepositoryInterface(db_client)
            game_snapshots_repo = GameSnapshotsRepositoryInterface(db_client)
            players_games_snapshots_repo = PlayersGamesSnapshotsRepositoryInterface(db_client)
            hit_rate_snapshots_repo = NBAHitRateSnapshotsRepositoryInterface(db_client)

            props_ingestor = PropIngestor(bet_api, prop_snapshots_repo)
            player_stats_ingestor = NBAPlayerStatsIngestor(
                sports_stats_api=bet_api,
                team_snapshots_repository=team_snapshots_repo,
                player_snapshots_repository=player_snapshots_repo,
                game_snapshots_repository=game_snapshots_repo,
                players_games_snapshots_repository=players_games_snapshots_repo
            )
            event_ingestor = EventIngestor(bet_api)
            prop_hit_rate_calculator = NBAPropHitRateCalculator(
                players_games_snapshots_repository=players_games_snapshots_repo,
                prop_snapshots_repository=prop_snapshots_repo,
                player_snapshots_repository=player_snapshots_repo,
                hit_rate_snapshots_repository=hit_rate_snapshots_repo
            )

            return PipelineRunner(
                props_ingestor=props_ingestor,
                player_stats_ingestor=player_stats_ingestor,
                event_ingestor=event_ingestor,
                prop_hit_rate_calculator=prop_hit_rate_calculator
            )
        else:
            raise NotImplementedError(f"Pipeline runner for sport '{sport}' is not implemented.")
