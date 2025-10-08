import json
import logging
import os

import pandas as pd

from scripts.rotogrinders_scraper import Sport

logger = logging.getLogger(__name__)


class DateTracker:
    """Track scraped dates in S3 for idempotent scraping."""

    def __init__(self, sport: Sport):
        self.sport = sport
        self.s3_endpoint = os.getenv("WASABI_ENDPOINT", "s3.us-east-2.wasabisys.com")
        self.bucket_name = os.getenv("WASABI_BUCKET_NAME")
        self.s3_path = (
            f"s3://{self.bucket_name}/staging/metadata/{sport}/scraped_dates.json"
        )

        self.storage_options = {
            "client_kwargs": {"endpoint_url": f"https://{self.s3_endpoint}"},
            "key": os.getenv("WASABI_ACCESS_KEY"),
            "secret": os.getenv("WASABI_SECRET_KEY"),
        }

        self._dates = self._load_dates()

    def _load_dates(self) -> list[str]:
        """Load scraped dates from S3, return an empty list if not exists."""
        try:
            # Use pandas to read JSON from S3
            with pd.io.common.get_handle(
                self.s3_path, mode="r", storage_options=self.storage_options
            ) as handles:
                data = json.load(handles.handle)
                return data if isinstance(data, list) else []
        except FileNotFoundError:
            logger.info(f"No tracking file found at {self.s3_path}, starting fresh")
            return []

    def _save_dates(self) -> None:
        """Save scraped dates to S3."""
        # Write using pandas s3fs
        with pd.io.common.get_handle(
            self.s3_path, mode="w", storage_options=self.storage_options
        ) as handles:
            json.dump(sorted(self._dates), handles.handle, indent=2)

    def is_scraped(self, date: str) -> bool:
        """Check if date already scraped."""
        return date in self._dates

    def mark_scraped(self, date: str) -> None:
        """Mark date as scraped and persist to S3."""
        if date not in self._dates:
            self._dates.append(date)
            self._save_dates()
            logger.info(f"Marked {date} as scraped for {self.sport}")

    def get_all_scraped(self) -> list[str]:
        """Get all scraped dates (sorted)."""
        return sorted(self._dates)
