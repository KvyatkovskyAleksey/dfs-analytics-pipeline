from scripts.base_duck_db_processor import BaseDuckDBProcessor


class TopUsersMartProcessor(BaseDuckDBProcessor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sport = kwargs.get("sport")
        self.base_dds_stage = f"s3://{self.bucket_name}/dds/"

    def process(self):
        users_path = f"{self.base_dds_stage}{self.sport}/users/*/*/data.parquet"
        users_df = self.con.execute(
            f"""
            select 
                user_id,
                slate_type,
                sum(roi) as roi_sum
            from (
            SELECT
                user_id as user_id,
                roi as roi,
                -- Extract slate_type from a path (dk_classic, dk_single_game, etc)
                regexp_extract(filename, '/(dk_[^/]+)/', 1) as slate_type
            FROM read_parquet('{users_path}', filename=true)
            )
            group by user_id, slate_type,
            order by slate_type, roi_sum desc
        """
        ).df()
        return users_df


if __name__ == "__main__":
    with TopUsersMartProcessor(sport="NFL") as processor:
        result = processor.process()
        print(result)
