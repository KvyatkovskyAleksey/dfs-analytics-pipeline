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
        mart_path = f"{self.marts_base_path}top_users/data.parquet"

        logger.info(f"Processing top users mart for {self.sport}")

        self.con.execute(
            f"""
            COPY (
                SELECT
                    user_id,
                    slate_type,
                    sum(roi) as roi_sum
                FROM (
                    SELECT
                        user_id as user_id,
                        roi as roi,
                        -- Extract slate_type from a path (dk_classic, dk_single_game, etc)
                        regexp_extract(filename, '/(dk_[^/]+)/', 1) as slate_type
                    FROM read_parquet('{users_path}', filename=true)
                )
                GROUP BY user_id, slate_type
                ORDER BY slate_type, roi_sum DESC
            ) TO '{mart_path}'
            (FORMAT PARQUET, COMPRESSION 'SNAPPY')
        """
        )

        logger.info(f"✓ Top users mart saved to {mart_path}")


if __name__ == "__main__":
    with TopUsersMartProcessor(sport="NFL") as processor:
        processor.process()
