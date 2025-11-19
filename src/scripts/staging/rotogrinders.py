import logging

from scripts.spiders.rotogrinders_scraper import RotogrindersScraper
from scripts.staging.rotogrinders_processor import RotogrindersStagingProcessor
from schemas import Sport

logger = logging.getLogger("Staging")


def load_rotogrinders_data_to_staging(date: str, sport: Sport):
    """
    Load staging data for a specific sport and date.

    Args:
        date: Date in YYYY-MM-DD format
        sport: Sport type (e.g., "NFL", "NBA")
    """
    scraper = RotogrindersScraper(date, sport)
    scraper.scrape()
    if scraper.data_exists:
        processor = RotogrindersStagingProcessor(scraper.get_data())
        processor.save_data_to_s3()
    else:
        logger.info("No data found for this date.")


if __name__ == "__main__":
    load_rotogrinders_data_to_staging("2025-10-02", "NFL")
