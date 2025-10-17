"""Wrapper functions for minimum points processing to be used in Airflow DAGs."""

import logging
from typing import Optional

from scripts.min_points_processor import MinPointsProcessor
from scripts.rotogrinders_scraper import Sport

logger = logging.getLogger(__name__)


def process_min_points_to_api(
    sport: Sport,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> dict:
    """
    Process minimum points data and send to external API.

    Queries DDS layer for contest minimum points (minimum points needed to cash)
    and sends the data to a configured API endpoint.

    Args:
        sport: Sport type (e.g., "NFL", "NBA")
        start_date: Optional start date in YYYY-MM-DD format
        end_date: Optional end date in YYYY-MM-DD format
                 If dates not provided, processes all available data

    Returns:
        Dictionary with processing results:
            - sport: Sport processed
            - start_date: Start date (or None)
            - end_date: End date (or None)
            - total_records: Total number of contest records processed
            - records_with_min_points: Number of records with min_points data
            - status: Processing status ("completed")

    Raises:
        Exception: If processing or API send fails

    Example:
        # Process last month
        result = process_min_points_to_api("NFL", "2025-09-01", "2025-09-30")

        # Process all time
        result = process_min_points_to_api("NFL")
    """
    logger.info(
        f"Starting minimum points processing for {sport} "
        f"(date range: {start_date or 'all'} to {end_date or 'all'})"
    )

    with MinPointsProcessor(
        sport=sport, start_date=start_date, end_date=end_date
    ) as processor:
        result = processor.process()

    logger.info(
        f"Completed minimum points processing for {sport}: "
        f"{result['total_records']} total records, "
        f"{result['records_with_min_points']} with min_points"
    )

    return result


if __name__ == "__main__":
    # Example usage for testing
    logging.basicConfig(level=logging.INFO)

    # Test with a specific date range
    result = process_min_points_to_api("NFL", "2023-09-01", "2025-10-30")
    print(f"Processing result: {result}")
