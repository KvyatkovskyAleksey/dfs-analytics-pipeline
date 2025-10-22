import logging
import time

from scripts.base_duck_db_processor import BaseDuckDBProcessor

logger = logging.getLogger("TeamStacksMart")


class TeamStacksMart(BaseDuckDBProcessor):
    """
    Analyze team stacking patterns in top lineups.

    This mart analyzes:
    - Stack size distributions (2+, 3+, 4+ player stacks)
    - Position combinations within stacks
    - QB correlation patterns (QB+WR, QB+TE, QB+RB)
    - DST stacking frequency and patterns

    Outputs 4 tables:
    - stack_sizes.parquet: Overall stack size distribution
    - team_details.parquet: Per-team breakdown with position combos
    - qb_patterns.parquet: QB stacking correlations
    - dst_patterns.parquet: DST stacking analysis
    """

    def __init__(self, sport: str, game_type: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sport = sport
        self.game_type = game_type

        # Input: top lineups mart
        self.input_path = f"s3://{self.bucket_name}/marts/top_lineups/{sport}/{game_type}/data.parquet"

        # Output: team stacks mart (4 files)
        self.base_output_path = (
            f"s3://{self.bucket_name}/marts/team_stacks/{sport}/{game_type}"
        )
        self.output_paths = {
            "stack_sizes": f"{self.base_output_path}/stack_sizes.parquet",
            "team_details": f"{self.base_output_path}/team_details.parquet",
            "qb_patterns": f"{self.base_output_path}/qb_patterns.parquet",
            "dst_patterns": f"{self.base_output_path}/dst_patterns.parquet",
        }

    def process(self) -> None:
        """Execute the team stacks analysis."""
        logger.info(f"Processing team stacks mart for {self.sport} - {self.game_type}")
        logger.info(f"  Input: {self.input_path}")
        logger.info(f"  Output base: {self.base_output_path}")

        # Create base CTEs used by all analyses
        self._create_base_views()

        # Generate each output table
        self._create_stack_sizes_table()
        self._create_team_details_table()
        self._create_qb_patterns_table()
        self._create_dst_patterns_table()

        logger.info("✓ Team stacks mart processing complete")

    def _create_base_views(self) -> None:
        """Create temporary views for positions and team counts."""
        logger.info("Creating base views...")

        # Create positions_long view: unpivot all positions
        self.con.execute(
            f"""
            CREATE OR REPLACE TEMP VIEW positions_long AS
            SELECT
                contest_id,
                lineup_rank,
                total_own,
                total_salary,
                unnest([
                    'qb1', 'rb1', 'rb2', 'wr1', 'wr2', 'wr3',
                    'te1', 'flex1', 'dst1'
                ]) as position_slot,
                unnest([
                    pos_qb1_team, pos_rb1_team, pos_rb2_team,
                    pos_wr1_team, pos_wr2_team, pos_wr3_team,
                    pos_te1_team, pos_flex1_team, pos_dst1_team
                ]) as team,
                unnest([
                    pos_qb1_real, pos_rb1_real, pos_rb2_real,
                    pos_wr1_real, pos_wr2_real, pos_wr3_real,
                    pos_te1_real, pos_flex1_real, pos_dst1_real
                ]) as position_raw
            FROM read_parquet('{self.input_path}')
        """
        )

        # Create positions_normalized view: normalize position names
        self.con.execute(
            """
            CREATE OR REPLACE TEMP VIEW positions_normalized AS
            SELECT
                *,
                CASE
                    WHEN UPPER(REGEXP_REPLACE(position_raw, '[0-9]', '')) = 'WR' THEN 'WR'
                    WHEN UPPER(REGEXP_REPLACE(position_raw, '[0-9]', '')) = 'RB' THEN 'RB'
                    WHEN UPPER(position_raw) IN ('DEF', 'DST', 'D') THEN 'DST'
                    ELSE UPPER(REGEXP_REPLACE(position_raw, '[0-9]', ''))
                END as position_normalized
            FROM positions_long
            WHERE team IS NOT NULL
        """
        )

        # Create team_counts view: count players per team per lineup
        self.con.execute(
            """
            CREATE OR REPLACE TEMP VIEW team_counts AS
            SELECT
                contest_id,
                lineup_rank,
                total_own,
                team,
                COUNT(*) as stack_size,
                STRING_AGG(DISTINCT position_normalized, ',' ORDER BY position_normalized) as position_combo
            FROM positions_normalized
            GROUP BY contest_id, lineup_rank, total_own, team
            HAVING stack_size >= 2
        """
        )

        logger.info("✓ Base views created")

    def _create_stack_sizes_table(self) -> None:
        """Create stack_sizes.parquet: overall stack size distribution."""
        logger.info("Creating stack_sizes table...")

        query = f"""
            COPY (
                WITH
                    -- Get all stacks per lineup, sorted by size
                    lineup_stacks AS (
                        SELECT
                            contest_id,
                            lineup_rank,
                            total_own,
                            LIST(stack_size ORDER BY stack_size DESC) as stack_sizes
                        FROM team_counts
                        GROUP BY contest_id, lineup_rank, total_own
                    ),
                    -- Generate all "at least N" combinations
                    -- For [4,3,2]: generate 4,3,2 | 4,3 | 4 | 3 | 2
                    expanded_stacks AS (
                        SELECT
                            contest_id,
                            lineup_rank,
                            total_own,
                            stack_sizes,
                            -- Full combination
                            LIST_AGGREGATE(stack_sizes, 'string_agg', ',') as full_combo,
                            -- Individual sizes for separate counting
                            UNNEST(stack_sizes) as individual_size
                        FROM lineup_stacks
                    ),
                    -- Collect all unique combinations
                    all_combos AS (
                        -- Full combinations
                        SELECT
                            full_combo as stack_combination,
                            contest_id,
                            lineup_rank,
                            total_own
                        FROM expanded_stacks

                        UNION ALL

                        -- Prefixes (drop from end)
                        -- For [4,3,2]: also count as 4,3 and 4
                        SELECT
                            LIST_AGGREGATE(stack_sizes[1:len], 'string_agg', ',') as stack_combination,
                            contest_id,
                            lineup_rank,
                            total_own
                        FROM lineup_stacks,
                            UNNEST(RANGE(1, LEN(stack_sizes))) as t(len)
                        WHERE len > 0

                        UNION ALL

                        -- Individual stack sizes
                        SELECT
                            individual_size::VARCHAR as stack_combination,
                            contest_id,
                            lineup_rank,
                            total_own
                        FROM expanded_stacks
                    ),
                    -- Get total lineup count for percentages
                    total_lineups AS (
                        SELECT COUNT(DISTINCT contest_id || '_' || lineup_rank) as total_count
                        FROM read_parquet('{self.input_path}')
                    )
                -- Final aggregation
                SELECT
                    stack_combination,
                    COUNT(DISTINCT contest_id || '_' || lineup_rank) as lineup_count,
                    ROUND(100.0 * COUNT(DISTINCT contest_id || '_' || lineup_rank) / MAX(total_count), 2) as pct_of_lineups,
                    ROUND(AVG(total_own), 2) as avg_total_own,
                    ROUND(AVG(lineup_rank), 1) as avg_lineup_rank
                FROM all_combos, total_lineups
                GROUP BY stack_combination
                ORDER BY lineup_count DESC
            ) TO '{self.output_paths["stack_sizes"]}'
            (FORMAT PARQUET, COMPRESSION 'SNAPPY')
        """

        self.con.execute(query)
        logger.info(f"✓ Stack sizes saved to {self.output_paths['stack_sizes']}")

    def _create_team_details_table(self) -> None:
        """Create team_details.parquet: per-team breakdown."""
        logger.info("Creating team_details table...")

        query = f"""
            COPY (
                WITH
                    -- Get total lineups for percentage calculation
                    total_lineups AS (
                        SELECT COUNT(DISTINCT contest_id || '_' || lineup_rank) as total_count
                        FROM read_parquet('{self.input_path}')
                    ),
                    -- Find most common position combo for each team+stack_size
                    position_combos AS (
                        SELECT
                            team,
                            stack_size,
                            position_combo,
                            COUNT(*) as combo_count,
                            ROW_NUMBER() OVER (PARTITION BY team, stack_size ORDER BY COUNT(*) DESC) as rn
                        FROM team_counts
                        GROUP BY team, stack_size, position_combo
                    )
                -- Main aggregation
                SELECT
                    tc.team,
                    tc.stack_size,
                    COUNT(*) as lineup_count,
                    ROUND(100.0 * COUNT(*) / MAX(tl.total_count), 2) as pct_of_lineups,
                    MAX(CASE WHEN pc.rn = 1 THEN pc.position_combo END) as position_combo,
                    MAX(CASE WHEN pc.rn = 1 THEN pc.combo_count END) as position_combo_count,
                    ROUND(AVG(tc.total_own), 2) as avg_total_own,
                    ROUND(AVG(tc.lineup_rank), 1) as avg_lineup_rank
                FROM team_counts tc
                CROSS JOIN total_lineups tl
                LEFT JOIN position_combos pc
                    ON tc.team = pc.team
                    AND tc.stack_size = pc.stack_size
                    AND pc.rn = 1
                GROUP BY tc.team, tc.stack_size
                ORDER BY tc.team, tc.stack_size DESC
            ) TO '{self.output_paths["team_details"]}'
            (FORMAT PARQUET, COMPRESSION 'SNAPPY')
        """

        self.con.execute(query)
        logger.info(f"✓ Team details saved to {self.output_paths['team_details']}")

    def _create_qb_patterns_table(self) -> None:
        """Create qb_patterns.parquet: QB stacking correlation analysis."""
        logger.info("Creating QB patterns table...")

        query = f"""
            COPY (
                WITH
                    -- First, identify QB team for each lineup
                    lineup_qb_team AS (
                        SELECT
                            contest_id,
                            lineup_rank,
                            total_own,
                            MAX(CASE WHEN position_normalized = 'QB' THEN team END) as qb_team
                        FROM positions_normalized
                        GROUP BY contest_id, lineup_rank, total_own
                        HAVING qb_team IS NOT NULL
                    ),
                    -- Then check which positions match QB team
                    lineup_qb_analysis AS (
                        SELECT
                            qbt.contest_id,
                            qbt.lineup_rank,
                            qbt.total_own,
                            qbt.qb_team,
                            BOOL_OR(pn.position_normalized = 'WR' AND pn.team = qbt.qb_team) as qb_with_wr,
                            BOOL_OR(pn.position_normalized = 'TE' AND pn.team = qbt.qb_team) as qb_with_te,
                            BOOL_OR(pn.position_normalized = 'RB' AND pn.team = qbt.qb_team) as qb_with_rb
                        FROM lineup_qb_team qbt
                        JOIN positions_normalized pn
                            ON qbt.contest_id = pn.contest_id
                            AND qbt.lineup_rank = pn.lineup_rank
                        GROUP BY qbt.contest_id, qbt.lineup_rank, qbt.total_own, qbt.qb_team
                    ),
                    -- Classify each lineup into stack types
                    qb_stack_types AS (
                        SELECT
                            contest_id,
                            lineup_rank,
                            total_own,
                            qb_team,
                            CASE
                                WHEN qb_with_wr AND qb_with_te THEN 'QB + WR + TE'
                                WHEN qb_with_wr AND NOT qb_with_te THEN 'QB + WR (no TE)'
                                WHEN qb_with_te AND NOT qb_with_wr THEN 'QB + TE (no WR)'
                                WHEN qb_with_rb AND NOT qb_with_wr AND NOT qb_with_te THEN 'QB + RB (no pass catchers)'
                                WHEN qb_with_rb AND (qb_with_wr OR qb_with_te) THEN 'QB + RB + pass catchers'
                                WHEN qb_with_wr OR qb_with_te THEN 'QB + pass catchers'
                                ELSE 'QB not stacked'
                            END as stack_type
                        FROM lineup_qb_analysis
                    ),
                    -- Count QB stacked lineups
                    qb_stacked_count AS (
                        SELECT COUNT(*) as qb_stacked_total
                        FROM qb_stack_types
                        WHERE stack_type != 'QB not stacked'
                    )
                -- Aggregate by stack type
                SELECT
                    stack_type,
                    COUNT(*) as lineup_count,
                    ROUND(100.0 * COUNT(*) / MAX(qsc.qb_stacked_total), 2) as pct_of_qb_stacks,
                    ROUND(AVG(total_own), 2) as avg_total_own,
                    ROUND(AVG(lineup_rank), 1) as avg_lineup_rank
                FROM qb_stack_types, qb_stacked_count qsc
                GROUP BY stack_type
                ORDER BY lineup_count DESC
            ) TO '{self.output_paths["qb_patterns"]}'
            (FORMAT PARQUET, COMPRESSION 'SNAPPY')
        """

        self.con.execute(query)
        logger.info(f"✓ QB patterns saved to {self.output_paths['qb_patterns']}")

    def _create_dst_patterns_table(self) -> None:
        """Create dst_patterns.parquet: DST stacking analysis."""
        logger.info("Creating DST patterns table...")

        query = f"""
            COPY (
                WITH
                    -- Identify DST team for each lineup
                    lineup_dst AS (
                        SELECT
                            contest_id,
                            lineup_rank,
                            total_own,
                            MAX(CASE WHEN position_normalized = 'DST' THEN team END) as dst_team
                        FROM positions_normalized
                        GROUP BY contest_id, lineup_rank, total_own
                    ),
                    -- Check if DST is in a stack (2+ players from same team)
                    dst_in_stack AS (
                        SELECT
                            ld.contest_id,
                            ld.lineup_rank,
                            ld.total_own,
                            ld.dst_team,
                            tc.stack_size IS NOT NULL as dst_in_stack,
                            tc.position_combo
                        FROM lineup_dst ld
                        LEFT JOIN team_counts tc
                            ON ld.contest_id = tc.contest_id
                            AND ld.lineup_rank = tc.lineup_rank
                            AND ld.dst_team = tc.team
                        WHERE ld.dst_team IS NOT NULL
                    ),
                    -- For stacked DSTs, find most common other positions
                    dst_position_combos AS (
                        SELECT
                            position_combo,
                            COUNT(*) as combo_count,
                            ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) as rn
                        FROM dst_in_stack
                        WHERE dst_in_stack
                        GROUP BY position_combo
                    )
                -- Aggregate
                SELECT
                    dst_in_stack,
                    COUNT(*) as lineup_count,
                    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as pct_of_lineups,
                    MAX(CASE WHEN dpc.rn = 1 THEN dpc.position_combo END) as most_common_position_combo,
                    MAX(CASE WHEN dpc.rn = 1 THEN dpc.combo_count END) as combo_count,
                    ROUND(AVG(total_own), 2) as avg_total_own,
                    ROUND(AVG(lineup_rank), 1) as avg_lineup_rank
                FROM dst_in_stack
                LEFT JOIN dst_position_combos dpc ON dpc.rn = 1
                GROUP BY dst_in_stack
                ORDER BY dst_in_stack DESC
            ) TO '{self.output_paths["dst_patterns"]}'
            (FORMAT PARQUET, COMPRESSION 'SNAPPY')
        """

        self.con.execute(query)
        logger.info(f"✓ DST patterns saved to {self.output_paths['dst_patterns']}")


if __name__ == "__main__":
    start = time.perf_counter()
    with TeamStacksMart(sport="NFL", game_type="dk_classic") as processor:
        processor.process()
    print(f"Total time: {time.perf_counter() - start:.2f}s")
