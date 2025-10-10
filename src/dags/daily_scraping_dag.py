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

from scripts.staging import load_data_to_staging
from scripts.date_tracker import DateTracker
from scripts.rotogrinders_scraper import Sport

logger = logging.getLogger(__name__)

# S3 configuration
BUCKET_NAME = os.getenv("WASABI_BUCKET_NAME")

# Configuration
SPORTS: list[Sport] = ["NFL"]  # Add more sports here: ["NFL", "NBA", "MLB"]

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


# Task functions
def check_if_scraped(sport: Sport, **context) -> str:
    """Check if date already scraped for this sport."""
    logical_date = context["logical_date"]
    target_date = (logical_date - timedelta(days=3)).strftime("%Y-%m-%d")

    tracking_path = f"s3://{BUCKET_NAME}/staging/metadata/{sport}/scraped_dates.json"
    tracker = DateTracker(sport=sport, tracking_path=tracking_path)
    is_scraped = tracker.is_scraped(target_date)

    if is_scraped:
        logger.info(f"{sport} - Date {target_date} already scraped, skipping")
        return f"{sport}.skip_scrape_{sport}"
    else:
        logger.info(f"{sport} - Date {target_date} not found, will scrape")
        return f"{sport}.scrape_data_{sport}"


def scrape_and_load(sport: Sport, **context) -> None:
    """Scrape and load data for this sport."""
    logical_date = context["logical_date"]
    target_date = (logical_date - timedelta(days=3)).strftime("%Y-%m-%d")

    logger.info(f"{sport} - Starting scrape for {target_date}")
    load_data_to_staging(date=target_date, sport=sport)
    logger.info(f"{sport} - Successfully scraped and loaded {target_date}")


def mark_date_scraped(sport: Sport, **context) -> None:
    """Mark date as scraped in tracking file."""
    logical_date = context["logical_date"]
    target_date = (logical_date - timedelta(days=3)).strftime("%Y-%m-%d")

    tracking_path = f"s3://{BUCKET_NAME}/staging/metadata/{sport}/scraped_dates.json"
    tracker = DateTracker(sport=sport, tracking_path=tracking_path)
    tracker.mark_scraped(target_date)
    logger.info(f"{sport} - Marked {target_date} as scraped")


with DAG(
    "daily_scraping_dag",
    default_args=default_args,
    description="Daily scraping of Rotogrinders data with conditional execution",
    schedule="@daily",
    start_date=datetime(2025, 9, 1),
    catchup=True,
    max_active_runs=1,
    tags=cast(MutableSet, ["scraping", "rotogrinders", "staging"]),
) as dag:
    # Create TaskGroups dynamically for each sport
    for sport in SPORTS:
        with TaskGroup(group_id=sport, dag=dag) as sport_group:

            # Task 1: Check if already scraped
            check = BranchPythonOperator(
                task_id=f"check_if_scraped_{sport}",
                python_callable=check_if_scraped,
                op_kwargs={"sport": sport},
            )

            # Task 2a: Skip (already scraped)
            skip = EmptyOperator(
                task_id=f"skip_scrape_{sport}",
            )

            # Task 2b: Scrape data
            scrape = PythonOperator(
                task_id=f"scrape_data_{sport}",
                python_callable=scrape_and_load,
                op_kwargs={"sport": sport},
            )

            # Task 3: Mark as scraped (only if scraping succeeded)
            mark = PythonOperator(
                task_id=f"mark_as_scraped_{sport}",
                python_callable=mark_date_scraped,
                op_kwargs={"sport": sport},
                trigger_rule="none_failed_min_one_success",
            )

            # Dependencies
            check >> [skip, scrape]
            scrape >> mark
