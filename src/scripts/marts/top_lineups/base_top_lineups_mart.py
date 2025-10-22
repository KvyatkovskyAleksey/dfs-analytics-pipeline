import logging
import time

from scripts.base_duck_db_processor import BaseDuckDBProcessor
from scripts.marts.top_lineups import configs
from scripts.marts.top_lineups.configs import TopLineupsMartConfig

logger = logging.getLogger("BaseTopLineupsMart")


class BaseTopLineupsMart(BaseDuckDBProcessor):
    """
    Universal base class for generating top lineups marts.

    This class provides a flexible, configurable approach to creating top lineups
    analytics marts that work across different sports, game types, and DFS platforms.

    The mart joins lineup data with player data to enrich each roster slot with
    player details (position, team, salary, player_id).

    Features:
    - Dynamic position handling (works with any lineup structure)
    - Configurable percentile filtering
    - Optional roster_position matching (for captain/MVP/star positions)
    - Outputs player_id, real_position, team, and salary for each slot

    Usage:
        config = TopLineupsMartConfig(
            sport="NFL",
            game_type="dk_single_game",
            positions=["cpt1", "flex1", "flex2", "flex3", "flex4", "flex5"],
            percentile_threshold=0.01,
            output_path="marts/top_lineups/NFL/dk_single_game/data.parquet",
            roster_position_mapping={"cpt1": "CPT"}
        )
        with BaseTopLineupsMart(config=config) as processor:
            processor.process()
    """

    def __init__(self, config: TopLineupsMartConfig, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.config = config
        self._validate_config()

        # Construct data paths
        self.base_dds_stage = f"s3://{self.bucket_name}/dds/"
        self.marts_base_path = f"s3://{self.bucket_name}/marts/"

        self.lineups_path = f"{self.base_dds_stage}{config.sport}/lineups/{config.game_type}/*/data.parquet"
        self.players_path = f"{self.base_dds_stage}{config.sport}/players/{config.game_type}/*/data.parquet"
        self.output_full_path = f"{self.marts_base_path}{config.output_path}"

    def _validate_config(self) -> None:
        """Validate configuration parameters."""
        if not self.config.sport:
            raise ValueError("sport must be provided")
        if not self.config.game_type:
            raise ValueError("game_type must be provided")
        if not self.config.positions:
            raise ValueError("positions list cannot be empty")
        if not 0 < self.config.percentile_threshold <= 1:
            raise ValueError("percentile_threshold must be between 0 and 1")
        if not self.config.output_path:
            raise ValueError("output_path must be provided")

    def _generate_unnest_sql(self) -> tuple[str, str]:
        """
        Generate SQL for unnesting position arrays.

        Returns:
            Tuple of (position_names_sql, position_columns_sql)
        """
        # Generate: 'cpt1', 'flex1', 'flex2', ...
        pos_names = "', '".join(self.config.positions)

        # Generate: pos_cpt1, pos_flex1, pos_flex2, ...
        pos_columns = ", ".join([f"pos_{p}" for p in self.config.positions])

        return pos_names, pos_columns

    def _generate_select_clauses(self) -> str:
        """
        Generate SELECT CASE statements for pivoting position data.

        Returns:
            SQL string with all CASE statements for each position and attribute
        """
        select_clauses = []

        for pos in self.config.positions:
            # Add four columns per position: player_id, position, team, salary
            select_clauses.extend(
                [
                    f"max(CASE WHEN pos_slot = '{pos}' THEN lp.player_id END) as pos_{pos}_player_id",
                    f"max(CASE WHEN pos_slot = '{pos}' THEN p.position END) as pos_{pos}_real",
                    f"max(CASE WHEN pos_slot = '{pos}' THEN p.current_team END) as pos_{pos}_team",
                    f"max(CASE WHEN pos_slot = '{pos}' THEN p.salary END) as pos_{pos}_salary",
                ]
            )

        return ",\n                        ".join(select_clauses)

    def _generate_roster_position_filter(self) -> str:
        """
        Generate SQL filter for roster_position matching.

        If roster_position_mapping is provided, generates a filter like:
            AND ((pos_slot = 'cpt1' AND roster_position = 'CPT')
                 OR (pos_slot NOT IN ('cpt1') AND roster_position NOT IN ('CPT')))

        Returns:
            SQL string for roster_position filtering (empty if no mapping)
        """
        if not self.config.roster_position_mapping:
            return ""

        special_positions = list(self.config.roster_position_mapping.keys())
        special_roster_values = list(self.config.roster_position_mapping.values())

        # Build condition: (pos_slot = 'cpt1' AND roster_position = 'CPT') OR ...
        match_conditions = []
        for slot, roster_pos in self.config.roster_position_mapping.items():
            match_conditions.append(
                f"(lp.pos_slot = '{slot}' AND p.roster_position = '{roster_pos}')"
            )

        # Format lists for SQL IN clause
        special_pos_list = "', '".join(special_positions)
        special_roster_list = "', '".join(special_roster_values)

        # Combine: special positions match their roster_position OR non-special positions
        filter_sql = f"""
                            AND (
                                {' OR '.join(match_conditions)}
                                OR (lp.pos_slot NOT IN ('{special_pos_list}')
                                    AND p.roster_position NOT IN ('{special_roster_list}'))
                            )"""

        return filter_sql

    def _generate_lineup_columns_sql(self) -> str:
        """
        Generate SQL for selecting lineup position columns.

        Returns:
            SQL string like: pos_cpt1, pos_flex1, pos_flex2, ...
        """
        return ",\n                            ".join(
            [f"pos_{p}" for p in self.config.positions]
        )

    def _build_query(self) -> str:
        """
        Build the SQL query for generating the top lineups mart.

        This method can be overridden by subclasses to customize the query
        (e.g., add union_by_name, additional filters, etc.).

        Returns:
            SQL query string
        """
        # Generate dynamic SQL components
        pos_names, pos_columns = self._generate_unnest_sql()
        select_clauses = self._generate_select_clauses()
        roster_filter = self._generate_roster_position_filter()
        lineup_columns = self._generate_lineup_columns_sql()

        query = f"""
            COPY (
                WITH
                    lineups AS (
                        SELECT
                            replace(list_element(string_split(filename, '/'), -2), '-', '')::integer as date_id,
                            contest_id,
                            lineup_rank,
                            total_own,
                            total_salary,
                            {lineup_columns}
                        FROM read_parquet('{self.lineups_path}', filename=true)
                        WHERE lineup_percentile <= {self.config.percentile_threshold}
                    ),
                    -- Unpivot lineup positions to enable single join
                    lineup_positions AS (
                        SELECT
                            date_id,
                            contest_id,
                            lineup_rank,
                            total_own,
                            total_salary,
                            unnest(['{pos_names}']) as pos_slot,
                            unnest([{pos_columns}]) as player_id
                        FROM lineups
                    ),
                    -- Pre-filter to only load players that appear in top lineups
                    needed_players AS (
                        SELECT DISTINCT date_id, player_id
                        FROM lineup_positions
                    ),
                    -- Load only the players we need, with team and salary info
                    players AS (
                        SELECT
                            replace(list_element(string_split(filename, '/'), -2), '-', '')::integer as date_id,
                            p.player_id,
                            p.position,
                            p.current_team,
                            p.salary,
                            p.roster_position
                        FROM read_parquet('{self.players_path}', filename=true) p
                        WHERE (replace(list_element(string_split(filename, '/'), -2), '-', '')::integer, p.player_id) IN
                              (SELECT date_id, player_id FROM needed_players)
                    )
                    -- Single join with roster position matching and pivot back to wide format
                    SELECT
                        contest_id,
                        lineup_rank,
                        max(total_own) as total_own,
                        max(total_salary) as total_salary,
                        {select_clauses}
                    FROM lineup_positions lp
                        JOIN players p ON lp.player_id = p.player_id
                            AND lp.date_id = p.date_id{roster_filter}
                    GROUP BY contest_id, lineup_rank
            ) TO '{self.output_full_path}'
            (FORMAT PARQUET, COMPRESSION 'SNAPPY')
            """
        return query

    def process(self) -> None:
        """Execute the mart generation query."""
        logger.info(
            f"Processing top lineups mart for {self.config.sport} - {self.config.game_type}"
        )
        logger.info(f"  Positions: {self.config.positions}")
        logger.info(f"  Percentile threshold: {self.config.percentile_threshold}")
        logger.info(f"  Output: {self.output_full_path}")

        query = self._build_query()
        print(query)

        self.con.execute(query)

        logger.info(f"✓ Top lineups mart saved to {self.output_full_path}")


if __name__ == "__main__":
    start = time.perf_counter()
    with BaseTopLineupsMart(config=configs.DK_CLASSIC_NFL_CONFIG) as processor:
        processor.process()
    print(f"Total time: {time.perf_counter() - start:.2f}s")
