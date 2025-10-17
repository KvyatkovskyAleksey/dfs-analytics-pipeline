import logging

from scripts.base_duck_db_processor import BaseDuckDBProcessor

logger = logging.getLogger("TopUsersMartProcessor")


class TopUsersMartProcessor(BaseDuckDBProcessor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sport = kwargs.get("sport")
        self.base_dds_stage = f"s3://{self.bucket_name}/dds/"
        self.marts_base_path = f"s3://{self.bucket_name}/marts/"

    def process(self):
        users_path = f"{self.base_dds_stage}{self.sport}/users/*/*/data.parquet"
        contests_path = f"{self.base_dds_stage}{self.sport}/contests/*/*/data.parquet"
        mart_path = f"{self.marts_base_path}top_users/data.parquet"

        logger.info(f"Processing top users mart for {self.sport}")

        self.con.execute(
            f"""
            COPY (
                WITH users AS (
                    SELECT
                        contest_id,
                        user_id,
                        total_players,
                        total_rosters,
                        unique_rosters,
                        max_exposure,
                        lineups_cashing,
                        lineups_in_percentile_1,
                        lineups_in_percentile_2,
                        lineups_in_percentile_5,
                        lineups_in_percentile_10,
                        lineups_in_percentile_20,
                        lineups_in_percentile_50,
                        total_entry_cost,
                        total_winning,
                        roi
                    FROM read_parquet('{users_path}')
                ),
                contests AS (
                    SELECT
                        list_element(string_split(filename, '/'), -3) as slate_type,
                        contest_id,
                        CASE
                            WHEN contest_size <= 318 THEN '1_Tiny'
                            WHEN contest_size <= 4444 THEN '2_Small'
                            WHEN contest_size <= 16646 THEN '3_Medium'
                            WHEN contest_size <= 79270 THEN '4_Large'
                            ELSE '5_Massive'
                        END as size_category
                    FROM read_parquet('{contests_path}', filename=true)
                )
                SELECT
                    user_id,
                    slate_type,
                    size_category,
                    ROUND(SUM(unique_rosters)::DOUBLE / SUM(total_rosters)::DOUBLE, 2) as unique_roster_percent,
                    ROUND(AVG(max_exposure), 2) as avg_max_exposure,
                    SUM(lineups_cashing) as total_lineups_cashing,
                    SUM(lineups_in_percentile_1) as total_lineups_in_percentile_1,
                    SUM(lineups_in_percentile_2) as total_lineups_in_percentile_2,
                    SUM(lineups_in_percentile_5) as total_lineups_in_percentile_5,
                    SUM(lineups_in_percentile_10) as total_lineups_in_percentile_10,
                    SUM(lineups_in_percentile_20) as total_lineups_in_percentile_20,
                    SUM(lineups_in_percentile_50) as total_lineups_in_percentile_50,
                    ROUND(SUM(total_entry_cost), 2) as total_entry_cost,
                    ROUND(SUM(total_winning), 2) as total_winning,
                    ROUND(SUM(roi), 2) as roi
                FROM users u
                JOIN contests c ON c.contest_id = u.contest_id
                GROUP BY user_id, slate_type, size_category
                ORDER BY slate_type, size_category, roi DESC
            ) TO '{mart_path}'
            (FORMAT PARQUET, COMPRESSION 'SNAPPY')
        """
        )

        logger.info(f"✓ Top users mart saved to {mart_path}")


if __name__ == "__main__":
    with TopUsersMartProcessor(sport="NFL") as processor:
        processor.process()
