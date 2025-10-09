from datetime import datetime, timedelta
from typing import MutableSet, cast
import os

from airflow import DAG
from airflow.providers.standard.operators.python import (
    PythonOperator,
    BranchPythonOperator,
)
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk.definitions.taskgroup import TaskGroup
import logging

from scripts.dds_processing import (
    process_players_to_dds,
    process_users_lineups_to_dds,
    process_lineups_to_dds,
)
from scripts.date_tracker import DateTracker
from scripts.rotogrinders_scraper import Sport

logger = logging.getLogger(__name__)

# S3 configuration
BUCKET_NAME = os.getenv("WASABI_BUCKET_NAME")

# Configuration
SPORTS: list[Sport] = ["NFL"]  # Add more sports here: ["NFL", "NBA", "MLB"]
DDS_TABLES = ["players", "users_lineups", "lineups"]

# Mapping of table names to processing functions
PROCESSING_FUNCTIONS = {
    "players": process_players_to_dds,
    "users_lineups": process_users_lineups_to_dds,
    "lineups": process_lineups_to_dds,
}

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


# Task functions
def check_if_processed(sport: Sport, table: str, **context) -> str:
    """Check if date already processed for this sport and table."""
    logical_date = context["logical_date"]
    target_date = (logical_date - timedelta(days=2)).strftime("%Y-%m-%d")

    tracking_path = (
        f"s3://{BUCKET_NAME}/dds/metadata/{sport}/{table}/processed_dates.json"
    )
    tracker = DateTracker(sport=sport, tracking_path=tracking_path)
    is_processed = tracker.is_scraped(target_date)

    if is_processed:
        logger.info(f"{sport}/{table} - Date {target_date} already processed, skipping")
        return f"{sport}.{table}.skip_process_{table}_{sport}"
    else:
        logger.info(f"{sport}/{table} - Date {target_date} not found, will process")
        return f"{sport}.{table}.process_{table}_{sport}"


def process_table_data(sport: Sport, table: str, **context) -> None:
    """Process data for this sport and table."""
    logical_date = context["logical_date"]
    target_date = (logical_date - timedelta(days=2)).strftime("%Y-%m-%d")

    logger.info(f"{sport}/{table} - Starting processing for {target_date}")
    processing_function = PROCESSING_FUNCTIONS[table]
    processing_function(date=target_date, sport=sport)
    logger.info(f"{sport}/{table} - Successfully processed {target_date}")


def mark_table_processed(sport: Sport, table: str, **context) -> None:
    """Mark date as processed in tracking file."""
    logical_date = context["logical_date"]
    target_date = (logical_date - timedelta(days=2)).strftime("%Y-%m-%d")

    tracking_path = (
        f"s3://{BUCKET_NAME}/dds/metadata/{sport}/{table}/processed_dates.json"
    )
    tracker = DateTracker(sport=sport, tracking_path=tracking_path)
    tracker.mark_scraped(target_date)
    logger.info(f"{sport}/{table} - Marked {target_date} as processed")


with DAG(
    "daily_dds_dag",
    default_args=default_args,
    description="Daily processing of data from staging to DDS layer",
    schedule="@daily",
    start_date=datetime(2025, 9, 1),
    catchup=True,
    max_active_runs=1,
    tags=cast(MutableSet, ["dds", "processing", "data-warehouse"]),
) as dag:
    # Create nested TaskGroups dynamically for each sport and table
    for sport in SPORTS:
        with TaskGroup(group_id=sport, dag=dag) as sport_group:
            table_groups = {}  # Store TaskGroup references for sequential dependencies

            for table in DDS_TABLES:
                with TaskGroup(group_id=table, dag=dag) as table_group:

                    # Task 1: Check if already processed
                    check = BranchPythonOperator(
                        task_id=f"check_if_processed_{table}_{sport}",
                        python_callable=check_if_processed,
                        op_kwargs={"sport": sport, "table": table},
                    )

                    # Task 2a: Skip (already processed)
                    skip = EmptyOperator(
                        task_id=f"skip_process_{table}_{sport}",
                    )

                    # Task 2b: Process data
                    process = PythonOperator(
                        task_id=f"process_{table}_{sport}",
                        python_callable=process_table_data,
                        op_kwargs={"sport": sport, "table": table},
                    )

                    # Task 3: Mark as processed (only if processing succeeded)
                    mark = PythonOperator(
                        task_id=f"mark_as_processed_{table}_{sport}",
                        python_callable=mark_table_processed,
                        op_kwargs={"sport": sport, "table": table},
                        trigger_rule="none_failed_min_one_success",
                    )

                    # Dependencies within table group
                    check >> [skip, process]
                    process >> mark

                # Store reference to this table's TaskGroup
                table_groups[table] = table_group

            # Process tables SEQUENTIALLY (one at a time)
            # Individual tasks need up to 24GB, so parallel execution would exceed worker limit
            # Sequential: 1 task × 24GB = safe within 48GB worker limit
            table_groups["players"] >> table_groups["users_lineups"] >> table_groups["lineups"]
