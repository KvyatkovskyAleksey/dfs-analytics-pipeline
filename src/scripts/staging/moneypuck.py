import logging

from scripts.spiders.moneypuck_scraper import MoneyPuckScraper
from scripts.staging.moneypuck_processor import MoneypuckProcessor

logger = logging.getLogger("Staging")


def load_moneypuck_data_to_staging(date: str):
    """
    Load staging data for MoneyPuck for a specific date.

    Args:
        date: Date in YYYY-MM-DD format
    """
    scraper = MoneyPuckScraper(date)
    scraper.scrape()
    if scraper.data_exists:
        processor = MoneypuckProcessor(scraper.get_data())
        processor.save_data_to_s3()
    else:
        logger.info("No data found for this date.")


if __name__ == "__main__":
    load_moneypuck_data_to_staging("2025-10-07")
