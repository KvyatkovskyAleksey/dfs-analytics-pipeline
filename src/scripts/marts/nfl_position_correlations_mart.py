from scripts.base_duck_db_processor import BaseDuckDBProcessor


class NFLPositionCorrelationsMart(BaseDuckDBProcessor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sport = kwargs.get("sport")
        if not self.sport:
            raise ValueError("Sport must be provided")
        self.base_dds_stage = f"s3://{self.bucket_name}/dds/"

    def process(self):
        players_path = f"{self.base_dds_stage}{self.sport}/players/*/*/data.parquet"
        # Calculate position correlations for same team and opponent team
        df = self.con.execute(
            f"""
            WITH player_performances AS (
                SELECT DISTINCT
                    player_id,
                    full_name,
                    position,
                    roster_position,
                    event_id,
                    event_team_id,
                    proj_points,
                    actual_points
                FROM read_parquet('{players_path}', filename=true)
                WHERE proj_points > 0
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY player_id, event_id, event_team_id
                    ORDER BY proj_points ASC
                ) = 1
            ),

            -- Self-join to create pairs (same team and opponent team)
            player_pairs AS (
                SELECT
                    p1.event_id,
                    p1.event_team_id as team_id,
                    p1.position as position_1,
                    p1.actual_points as points_1,
                    p2.position as position_2,
                    p2.actual_points as points_2,
                    CASE
                        WHEN p1.event_team_id = p2.event_team_id THEN 'same_team'
                        ELSE 'opponent_team'
                    END as team_relationship
                FROM player_performances p1
                JOIN player_performances p2
                    ON p1.event_id = p2.event_id
                    AND p1.player_id < p2.player_id  -- Avoid duplicate pairs and self-pairs
            )

            -- Calculate correlations by position pairs
            SELECT
                position_1,
                position_2,
                team_relationship,
                CORR(points_1, points_2) as correlation,
                COUNT(*) as sample_size
--                 AVG(points_1) as avg_points_position_1,
--                 AVG(points_2) as avg_points_position_2
            FROM player_pairs
            GROUP BY position_1, position_2, team_relationship
                HAVING sample_size > 50
            ORDER BY correlation DESC
            """
        ).df()
        return df


if __name__ == "__main__":
    with NFLPositionCorrelationsMart(sport="NFL") as processor:
        result = processor.process()
        print(result)
