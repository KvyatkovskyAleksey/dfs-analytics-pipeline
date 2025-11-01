"""Wrapper functions for MoneyPuck DDS processing to be used in Airflow DAGs."""

import logging

from scripts.dds.moneypuck_dds_processor import MoneypuckDdsProcessor

logger = logging.getLogger(__name__)


def process_moneypuck_lines_to_dds() -> None:
    """
    Process MoneyPuck staging data to DDS lines table.

    Reads all available staging data from s3://bucket/staging/NHL/moneypuck/players/
    and transforms it into line combinations (forwards, defense, goalies) for 5on5
    situations. Saves to a single DDS file (no date partitioning).

    Output: s3://bucket/dds/NHL/moneypuck/lines/data.parquet
    """
    logger.info("Starting MoneyPuck lines processing")
    with MoneypuckDdsProcessor() as processor:
        processor.process_lines()
    logger.info("Completed MoneyPuck lines processing")


if __name__ == "__main__":
    # Test the processing function
    process_moneypuck_lines_to_dds()
