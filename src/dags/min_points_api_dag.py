"""
Airflow DAG for processing minimum points data and sending to external API.

This DAG processes contest minimum points data (minimum points needed to cash)
from the DDS layer and sends it to an external API. It supports two modes:
- last_month: Process previous calendar month (default)
- all_time: Process all available data

The DAG is manually triggered and prevents duplicate runs using DateTracker.
"""

from datetime import datetime, timedelta
from typing import MutableSet, cast
import os
import logging

from airflow import DAG
from airflow.sdk import Param
from airflow.sdk.definitions.param import ParamsDict
from airflow.providers.standard.operators.python import (
    PythonOperator,
    BranchPythonOperator,
)
from airflow.providers.standard.operators.empty import EmptyOperator

from scripts.min_points_processing import process_min_points_to_api
from scripts.date_tracker import DateTracker
from scripts.rotogrinders_scraper import Sport

logger = logging.getLogger(__name__)

# S3 configuration
BUCKET_NAME = os.getenv("WASABI_BUCKET_NAME")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


# Task functions
def calculate_date_range(mode: str, **context) -> dict:
    """
    Calculate start and end dates based on processing mode.

    Args:
        mode: Processing mode ("last_month" or "all_time")
        **context: Airflow context

    Returns:
        Dictionary with start_date and end_date (or None for all_time)
    """
    if mode == "last_month":
        # Calculate previous calendar month
        today = datetime.now()
        first_of_this_month = today.replace(day=1)
        last_month_end = first_of_this_month - timedelta(days=1)
        last_month_start = last_month_end.replace(day=1)

        start_date = last_month_start.strftime("%Y-%m-%d")
        end_date = last_month_end.strftime("%Y-%m-%d")

        logger.info(f"Mode: last_month -> Date range: {start_date} to {end_date}")

    elif mode == "all_time":
        start_date = None
        end_date = None
        logger.info("Mode: all_time -> Processing all available data")

    else:
        raise ValueError(f"Invalid mode: {mode}. Must be 'last_month' or 'all_time'")

    # Push to XCom for downstream tasks
    result = {"start_date": start_date, "end_date": end_date, "mode": mode}
    context["task_instance"].xcom_push(key="date_range", value=result)

    return result


def check_if_processed(sport: Sport, **context) -> str:
    """
    Check if this mode/sport combination was already processed today.

    Args:
        sport: Sport type
        **context: Airflow context

    Returns:
        Task ID to branch to (skip or process)
    """
    # Get date range from previous task
    ti = context["task_instance"]
    date_range = ti.xcom_pull(key="date_range", task_ids="calculate_date_range")
    mode = date_range["mode"]

    # Use today's date as the tracking key
    today = datetime.now().strftime("%Y-%m-%d")

    # Build tracking path specific to mode and sport
    tracking_path = (
        f"s3://{BUCKET_NAME}/metadata/min_points_api/{sport}/{mode}/processed_runs.json"
    )

    tracker = DateTracker(sport=sport, tracking_path=tracking_path)
    is_processed = tracker.is_scraped(today)

    if is_processed:
        logger.info(f"{sport}/{mode} - Already processed for {today}, skipping")
        return "skip_process"
    else:
        logger.info(f"{sport}/{mode} - Not processed for {today}, will process")
        return "process_and_send"


def process_task(sport: Sport, **context) -> None:
    """
    Process minimum points data and send to API.

    Args:
        sport: Sport type
        **context: Airflow context
    """
    # Get date range from XCom
    ti = context["task_instance"]
    date_range = ti.xcom_pull(key="date_range", task_ids="calculate_date_range")

    start_date = date_range["start_date"]
    end_date = date_range["end_date"]
    mode = date_range["mode"]

    logger.info(
        f"Processing {sport} minimum points data "
        f"(mode: {mode}, dates: {start_date} to {end_date})"
    )

    # Call processing function
    result = process_min_points_to_api(
        sport=sport, start_date=start_date, end_date=end_date
    )

    # Push result to XCom for downstream tasks
    ti.xcom_push(key="processing_result", value=result)

    logger.info(f"Processing completed: {result}")


def mark_as_processed(sport: Sport, **context) -> None:
    """
    Mark this run as processed in tracking metadata.

    Args:
        sport: Sport type
        **context: Airflow context
    """
    ti = context["task_instance"]

    # Get date range and result from previous tasks
    date_range = ti.xcom_pull(key="date_range", task_ids="calculate_date_range")
    result = ti.xcom_pull(key="processing_result", task_ids="process_and_send")

    mode = date_range["mode"]
    today = datetime.now().strftime("%Y-%m-%d")

    # Build tracking path
    tracking_path = (
        f"s3://{BUCKET_NAME}/metadata/min_points_api/{sport}/{mode}/processed_runs.json"
    )

    tracker = DateTracker(sport=sport, tracking_path=tracking_path)
    tracker.mark_scraped(today)

    logger.info(
        f"{sport}/{mode} - Marked {today} as processed "
        f"({result.get('total_records', 0)} records)"
    )


# Define DAG
with DAG(
    "min_points_api_dag",
    default_args=default_args,
    description="Process minimum points data and send to external API (manual trigger)",
    schedule=None,  # Manual trigger only
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=cast(MutableSet, ["api", "min_points", "export", "manual"]),
    params=cast(
        ParamsDict,
        {
            "mode": Param(
                default="last_month",
                enum=["last_month", "all_time"],
                description="Processing mode: last_month (previous calendar month) or all_time (all data)",
            ),
            "sport": Param(
                default="NFL",
                type="string",
                description="Sport to process (e.g., NFL, NBA)",
            ),
        },
    ),
) as dag:
    # Task 1: Calculate date range based on mode
    calc_dates = PythonOperator(
        task_id="calculate_date_range",
        python_callable=calculate_date_range,
        op_kwargs={
            "mode": "{{ params.mode }}",
        },
    )

    # Task 2: Check if already processed
    check = BranchPythonOperator(
        task_id="check_if_processed",
        python_callable=check_if_processed,
        op_kwargs={
            "sport": "{{ params.sport }}",
        },
    )

    # Task 3a: Skip (already processed)
    skip = EmptyOperator(
        task_id="skip_process",
    )

    # Task 3b: Process and send to API
    process = PythonOperator(
        task_id="process_and_send",
        python_callable=process_task,
        op_kwargs={
            "sport": "{{ params.sport }}",
        },
    )

    # Task 4: Mark as processed
    mark = PythonOperator(
        task_id="mark_as_processed",
        python_callable=mark_as_processed,
        op_kwargs={
            "sport": "{{ params.sport }}",
        },
        trigger_rule="none_failed_min_one_success",
    )

    # Task 5: Join point
    join = EmptyOperator(
        task_id="join",
        trigger_rule="none_failed_min_one_success",
    )

    # Define dependencies
    calc_dates >> check >> [skip, process]
    process >> mark
    [skip, mark] >> join
