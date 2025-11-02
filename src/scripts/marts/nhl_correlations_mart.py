import logging

from scripts.base_duck_db_processor import BaseDuckDBProcessor

logger = logging.getLogger("NHLCorrelationsMart")


class NHLCorrelationsMart(BaseDuckDBProcessor):
    """
    NHL Position Correlations Mart with MoneyPuck Lines Integration.

    Calculates position correlations for NHL players, incorporating line
    combinations from MoneyPuck data. Handles team name mapping between
    Rotogrinders and MoneyPuck sources.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sport = "NHL"
        self.base_dds_stage = f"s3://{self.bucket_name}/dds/"
        self.marts_base_path = f"s3://{self.bucket_name}/marts/"

    def process(self):
        """
        Process NHL correlations with line data integration.

        Joins Rotogrinders player performance data with MoneyPuck line
        assignments and calculates correlations by position, team relationship,
        and line relationship.
        """
        players_path = f"{self.base_dds_stage}{self.sport}/players/*/*/data.parquet"
        lines_path = f"{self.base_dds_stage}{self.sport}/moneypuck/lines/data.parquet"
        mart_path = (
            f"{self.marts_base_path}{self.sport.lower()}_correlations/data.parquet"
        )

        logger.info(f"Processing NHL correlations mart")
        logger.info(f"Players path: {players_path}")
        logger.info(f"Lines path: {lines_path}")

        # Execute the correlation query
        self.con.execute(
            f"""
            COPY (
                -- Team name mapping: MoneyPuck → Rotogrinders
                WITH team_mapping AS (
                    SELECT 'CBJ' as moneypuck_team, 'CLB' as rotogrinders_team UNION ALL
                    SELECT 'LAK', 'LA' UNION ALL
                    SELECT 'NSH', 'NAS' UNION ALL
                    SELECT 'SJS', 'SJ' UNION ALL
                    SELECT 'TBL', 'TB' UNION ALL
                    SELECT 'WSH', 'WAS'
                ),

                -- Normalize MoneyPuck lines to Rotogrinders team names
                moneypuck_lines_normalized AS (
                    SELECT
                        game_date,
                        TRIM(UPPER(last_name)) as last_name_normalized,
                        COALESCE(tm.rotogrinders_team, mp.team) as team,
                        mp.line
                    FROM read_parquet('{lines_path}') mp
                    LEFT JOIN team_mapping tm ON mp.team = tm.moneypuck_team
                ),

                -- Get player performances with normalized last name and position
                player_performances AS (
                    SELECT DISTINCT
                        player_id,
                        full_name,
                        TRIM(UPPER(last_name)) as last_name_normalized,
                        CASE
                            WHEN position IN ('LW', 'RW') THEN 'W'
                            ELSE position
                        END as position,
                        roster_position,
                        current_team,
                        event_id,
                        event_team_id,
                        proj_points,
                        actual_points,
                        -- Extract date from filename: s3://bucket/dds/NHL/players/GAME_TYPE/DATE/data.parquet
                        CAST(SPLIT_PART(filename, '/', -2) AS DATE) as game_date
                    FROM read_parquet('{players_path}', filename=true)
                    WHERE proj_points > 0
                    QUALIFY ROW_NUMBER() OVER (
                        PARTITION BY player_id, event_id, event_team_id
                        ORDER BY proj_points ASC
                    ) = 1
                ),

                -- Join players with their line assignments
                players_with_lines AS (
                    SELECT
                        pp.*,
                        mp.line
                    FROM player_performances pp
                    LEFT JOIN moneypuck_lines_normalized mp
                        ON pp.current_team = mp.team
                        AND pp.game_date = mp.game_date
                        AND (
                            pp.last_name_normalized = mp.last_name_normalized
                            OR UPPER(pp.full_name) LIKE '%' || mp.last_name_normalized || '%'
                        )
                ),

                -- Create player pairs for correlation analysis
                player_pairs AS (
                    SELECT
                        p1.event_id,
                        p1.event_team_id as team_id,
                        p1.position as position_1,
                        p1.line as line_1,
                        p1.actual_points as points_1,
                        p2.position as position_2,
                        p2.line as line_2,
                        p2.actual_points as points_2,
                        CASE
                            WHEN p1.event_team_id = p2.event_team_id THEN 'same_team'
                            ELSE 'opponent_team'
                        END as team_relationship
                    FROM players_with_lines p1
                    JOIN players_with_lines p2
                        ON p1.event_id = p2.event_id
                        AND p1.player_id < p2.player_id  -- Avoid duplicate pairs and self-pairs
                        AND p1.line IS NOT NULL  -- Only include players with line assignments
                        AND p2.line IS NOT NULL  -- Only include players with line assignments
                )

                -- Calculate correlations by position, team, and specific lines
                SELECT
                    position_1,
                    position_2,
                    team_relationship,
                    line_1,
                    line_2,
                    CORR(points_1, points_2) as correlation,
                    COUNT(*) as sample_size,
                    AVG(points_1) as avg_points_1,
                    AVG(points_2) as avg_points_2,
                    STDDEV(points_1) as stddev_points_1,
                    STDDEV(points_2) as stddev_points_2
                FROM player_pairs
                GROUP BY position_1, position_2, team_relationship, line_1, line_2
                HAVING sample_size > 50
                ORDER BY correlation DESC
            ) TO '{mart_path}'
            (FORMAT PARQUET, COMPRESSION 'SNAPPY')
            """
        )

        logger.info(f"✓ NHL correlations mart saved to {mart_path}")


if __name__ == "__main__":
    with NHLCorrelationsMart() as processor:
        processor.process()
