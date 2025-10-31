from pandas import DataFrame

from scripts.spiders.moneypuck_scraper import MoneyPuckData
from scripts.staging.base_staging_processor import BaseStagingProcessor


class MoneypuckProcessor(BaseStagingProcessor):
    def __init__(self, data: MoneyPuckData) -> None:
        super().__init__()
        self.date = data["date"]
        self.players_data = data["players_data"]
        self.game_events_data = data["game_events_data"]

    def save_data_to_s3(self):
        self._save_players_to_s3()
        self._save_game_events_to_s3()

    def _save_players_to_s3(self):
        df = self.players_data
        path = f"s3://{self.bucket_name}/staging/NHL/moneypuck/players/{self.date}/data.parquet"
        df.to_parquet(
            path,
            storage_options={
                "client_kwargs": {"endpoint_url": f"https://{self.s3_endpoint}"},
                "key": self.s3_access_key_id,
                "secret": self.s3_secret_access_key,
            },
        )

    def _save_game_events_to_s3(self):
        df = self.game_events_data
        path = f"s3://{self.bucket_name}/staging/NHL/moneypuck/game_events/{self.date}/data.parquet"
        df.to_parquet(
            path,
            storage_options={
                "client_kwargs": {"endpoint_url": f"https://{self.s3_endpoint}"},
                "key": self.s3_access_key_id,
                "secret": self.s3_secret_access_key,
            },
        )
