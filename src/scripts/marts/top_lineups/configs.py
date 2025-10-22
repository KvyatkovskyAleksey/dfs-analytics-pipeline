"""Pre-defined configurations for common top lineups mart use cases."""

from dataclasses import dataclass
from typing import Optional


@dataclass
class TopLineupsMartConfig:
    """
    Configuration for top lineups mart generation.

    Attributes:
        sport: Sport name (e.g., "NFL", "NBA", "NHL", "MLB")
        game_type: Game type identifier (e.g., "dk_single_game", "dk_classic", "fd_single_game")
        positions: List of position slot names in the lineup (e.g., ["cpt1", "flex1", "flex2", ...])
        percentile_threshold: Percentile threshold for filtering top lineups (e.g., 0.01 for top 1%)
        output_path: S3 path for the output parquet file (relative to bucket)
        roster_position_mapping: Optional mapping of position slots to required roster_position values
                                 (e.g., {"cpt1": "CPT"} for DK single game captain logic)
                                 If None or empty, no roster_position filtering is applied.
    """

    sport: str
    game_type: str
    positions: list[str]
    percentile_threshold: float
    output_path: str
    roster_position_mapping: Optional[dict[str, str]] = None


# DraftKings Single Game - NFL
DK_SINGLE_GAME_NFL_CONFIG = TopLineupsMartConfig(
    sport="NFL",
    game_type="dk_single_game",
    positions=["cpt1", "flex1", "flex2", "flex3", "flex4", "flex5"],
    percentile_threshold=0.01,
    output_path="top_lineups/NFL/dk_single_game/data.parquet",
    roster_position_mapping={"cpt1": "CPT"},
)

# fixme need to check actual positions before usage
# DraftKings Single Game - NBA
DK_SINGLE_GAME_NBA_CONFIG = TopLineupsMartConfig(
    sport="NBA",
    game_type="dk_single_game",
    positions=["cpt1", "flex1", "flex2", "flex3", "flex4", "flex5"],
    percentile_threshold=0.01,
    output_path="top_lineups/NBA/dk_single_game/data.parquet",
    roster_position_mapping={"cpt1": "CPT"},
)

# fixme need to check actual positions before usage
# DraftKings Single Game - NHL
DK_SINGLE_GAME_NHL_CONFIG = TopLineupsMartConfig(
    sport="NHL",
    game_type="dk_single_game",
    positions=["cpt1", "flex1", "flex2", "flex3", "flex4", "flex5"],
    percentile_threshold=0.01,
    output_path="top_lineups/NHL/dk_single_game/data.parquet",
    roster_position_mapping={"cpt1": "CPT"},
)

DK_CLASSIC_NFL_CONFIG = TopLineupsMartConfig(
    sport="NFL",
    game_type="dk_classic",
    positions=[
        "dst1",
        "flex1",
        "qb1",
        "rb1",
        "rb2",
        "te1",
        "wr1",
        "wr2",
        "wr3",
    ],
    percentile_threshold=0.01,
    output_path="top_lineups/NFL/dk_classic/data.parquet",
)
