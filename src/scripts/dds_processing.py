"""Wrapper functions for DDS processing to be used in Airflow DAGs."""

import logging

from scripts.dds_processor import DdsProcessor
from scripts.rotogrinders_scraper import Sport

logger = logging.getLogger(__name__)


def process_contests_to_dds(date: str, sport: Sport) -> None:
    """
    Process contests data from staging to DDS.

    Args:
        date: Date in YYYY-MM-DD format
        sport: Sport type (e.g., "NFL", "NBA")
    """
    logger.info(f"Starting contests processing for {sport} on {date}")
    with DdsProcessor(sport=sport, date=date) as processor:
        processor.process_contests()
    logger.info(f"Completed contests processing for {sport} on {date}")


def process_players_to_dds(date: str, sport: Sport) -> None:
    """
    Process players data from staging to DDS.

    Args:
        date: Date in YYYY-MM-DD format
        sport: Sport type (e.g., "NFL", "NBA")
    """
    logger.info(f"Starting players processing for {sport} on {date}")
    with DdsProcessor(sport=sport, date=date) as processor:
        processor.process_players()
    logger.info(f"Completed players processing for {sport} on {date}")


def process_users_lineups_to_dds(date: str, sport: Sport) -> None:
    """
    Process users and lineups data from staging to DDS.

    Args:
        date: Date in YYYY-MM-DD format
        sport: Sport type (e.g., "NFL", "NBA")
    """
    logger.info(f"Starting users_lineups processing for {sport} on {date}")
    with DdsProcessor(sport=sport, date=date) as processor:
        processor.process_users_lineups()
    logger.info(f"Completed users_lineups processing for {sport} on {date}")


def process_lineups_to_dds(date: str, sport: Sport) -> None:
    """
    Process lineups data from staging to DDS.

    Args:
        date: Date in YYYY-MM-DD format
        sport: Sport type (e.g., "NFL", "NBA")
    """
    logger.info(f"Starting lineups processing for {sport} on {date}")
    with DdsProcessor(sport=sport, date=date) as processor:
        processor.process_lineups()
    logger.info(f"Completed lineups processing for {sport} on {date}")


def process_draft_groups_to_dds(date: str, sport: Sport) -> None:
    """
    Process draft groups data from staging to DDS.

    Args:
        date: Date in YYYY-MM-DD format
        sport: Sport type (e.g., "NFL", "NBA")
    """
    logger.info(f"Starting draft_groups processing for {sport} on {date}")
    with DdsProcessor(sport=sport, date=date) as processor:
        processor.process_draft_groups()
    logger.info(f"Completed draft_groups processing for {sport} on {date}")


if __name__ == "__main__":
    process_players_to_dds("2025-09-10", "NFL")
