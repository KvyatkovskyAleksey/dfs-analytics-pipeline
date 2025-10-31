import logging

import pandas as pd

from scripts.spiders.rotogrinders_scraper import StagingData
from scripts.staging.base_staging_processor import BaseStagingProcessor

logger = logging.getLogger("DuckDBStagingProcessor")
logger.setLevel(logging.INFO)


class RotogrindersStagingProcessor(BaseStagingProcessor):
    def __init__(self, staging_data: StagingData) -> None:
        """Processor for save data in JSON format to s3 using pandas and gzip for compression"""
        super().__init__()
        self.staging_data = staging_data
        self.sport = staging_data["sport"]
        self.date = staging_data["date"]
        self.base_path = f"s3://{self.bucket_name}/staging/{self.sport}/"

    def save_data_to_s3(self):
        """Save all staging data to S3."""
        self._save_draft_groups()
        self._save_contests_data()
        self._save_events()
        self._save_contest_analyze_data()
        self._save_lineups()

    def _save_lineups(self) -> None:
        for game_type, lineups_data in self.staging_data["lineups_by_slates"].items():
            game_type_df = None
            for slate_id, lineups in lineups_data.items():
                df = pd.DataFrame(lineups)
                if game_type_df is None:
                    game_type_df = df
                # response has no data about slate, so good to add it
                df["slate_id"] = slate_id
                game_type_df = pd.concat([game_type_df, df], ignore_index=True)
            staging_path = (
                self.base_path + f"lineups/{game_type}/{self.date}/data.json.gz"
            )
            self._save_df_to_s3(game_type_df, staging_path)
        logger.info(f"lineups data for {self.date} saved to S3")

    def _save_contest_analyze_data(self) -> None:
        for game_type, analyze_data in self.staging_data[
            "contests_analyze_data"
        ].items():
            s3_path = (
                self.base_path + f"contest_analyze/{game_type}/{self.date}/data.json.gz"
            )
            df = pd.DataFrame(analyze_data)
            self._save_df_to_s3(df, s3_path)
        logger.info(f"analyze data for {self.date} saved to S3")

    def _save_events(self) -> None:
        for game_type, events_data in self.staging_data["events"].items():
            df = pd.DataFrame(events_data)
            staging_path = (
                self.base_path + f"events/{game_type}/{self.date}/data.json.gz"
            )
            self._save_df_to_s3(df, staging_path)
        logger.info(f"events data for {self.date} saved to S3")

    def _save_df_to_s3(self, df: pd.DataFrame, s3_path: str) -> None:
        # Add timestamp column
        df["load_ts"] = pd.Timestamp.now()

        # Write directly to S3 using pandas (avoids DuckDB memory overhead)
        df.to_json(
            s3_path,
            orient="records",  # List of dicts: [{...}, {...}]
            lines=True,  # JSONL format (one JSON per line)
            compression="gzip",
            storage_options={
                "client_kwargs": {"endpoint_url": f"https://{self.s3_endpoint}"},
                "key": self.s3_access_key_id,
                "secret": self.s3_secret_access_key,
            },
        )

    def _save_contests_data(self) -> None:
        for game_type, contests_data in self.staging_data["contests"].items():
            s3_path = self.base_path + f"contests/{game_type}/{self.date}/data.json.gz"
            df = pd.DataFrame(contests_data)
            self._save_df_to_s3(df, s3_path)
        logger.info(f"contests data for {self.date} saved to S3")

    def _save_draft_groups(self) -> None:
        df = pd.DataFrame(self.staging_data["draft_groups"])
        staging_path = self.base_path + f"draft_groups/{self.date}/data.json.gz"
        self._save_df_to_s3(df, staging_path)
        logger.info(f"draft_groups data for {self.date} saved to S3")


if __name__ == "__main__":
    import pickle

    with open("../staging_data.pickle", "rb") as f:
        stage_data = pickle.load(f)
    with RotogrindersStagingProcessor(stage_data) as processor:
        processor.save_data_to_s3()
