import asyncio
import logging

from scripts.spiders.espn_spider import BaseEspnSpider
from scripts.staging.espn_processor import EspnStagingProcessor
from schemas import Sport

logger = logging.getLogger("EspnStaging")


async def load_espn_odds_to_staging(date: str, sport: Sport):
    """
    Load ESPN odds data (v1) to staging for a specific sport and date.

    Fetches:
    - Scoreboard data (all games for the date)
    - Detailed odds for each game

    Args:
        date: Date in YYYY-MM-DD format
        sport: Sport type (e.g., "NFL", "NBA", "NHL", "MLB")
    """
    spider = BaseEspnSpider(sport)

    logger.info(f"Fetching ESPN odds (v1) for {sport} on {date}")
    await spider.fetch_all_odds_for_date(date)

    if spider.data_exists:
        processor = EspnStagingProcessor(spider.get_data())
        processor.save_data_to_s3()
    else:
        logger.info(f"No events found for {sport} on {date}")


if __name__ == "__main__":
    # Example usage
    asyncio.run(load_espn_odds_to_staging("2025-11-20", "NBA"))
