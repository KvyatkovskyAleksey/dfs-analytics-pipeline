import logging

import pandas as pd

from scripts.staging.base_staging_processor import BaseStagingProcessor

logger = logging.getLogger("EspnStagingProcessor")
logger.setLevel(logging.INFO)


class EspnStagingProcessor(BaseStagingProcessor):
    def __init__(self, staging_data: dict) -> None:
        """
        Processor for saving ESPN odds data (v1 structure) to S3

        Args:
            staging_data: Dictionary with v1 structure (version, date, sport, scoreboard_response, odds_responses)
        """
        super().__init__()
        self.staging_data = staging_data
        self.sport = staging_data.get("sport")
        self.date = staging_data.get("date")
        self.version = staging_data.get("version", "v1")
        self.base_path = f"s3://{self.bucket_name}/staging/{self.sport}/"

    def save_data_to_s3(self):
        """Save ESPN odds data to S3 (v1 versioned path)."""
        if not self.staging_data or not self.sport or not self.date:
            logger.warning(f"No data to save: missing required fields")
            return

        s3_path = self.base_path + f"espn_odds/{self.version}/{self.date}/data.json.gz"

        series = pd.Series(self.staging_data)
        series.to_json(
            s3_path,
            indent=2,
            storage_options={
                "client_kwargs": {"endpoint_url": f"https://{self.s3_endpoint}"},
                "key": self.s3_access_key_id,
                "secret": self.s3_secret_access_key,
            },
            compression="gzip",
        )

        num_games = len(
            self.staging_data.get("scoreboard_response", {}).get("events", [])
        )
        num_with_odds = sum(
            1 for o in self.staging_data.get("odds_responses", []) if o.get("odds_data")
        )

        logger.info(
            f"ESPN odds data ({self.version}) for {self.sport} on {self.date} saved to S3: {s3_path}"
        )
        logger.info(f"Saved: {num_games} games, {num_with_odds} with detailed odds")
