from datetime import datetime, timedelta
from typing import MutableSet, cast, TypedDict, Callable
import os

from airflow import DAG
from airflow.providers.standard.operators.python import (
    PythonOperator,
    BranchPythonOperator,
)
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk.definitions.taskgroup import TaskGroup
import logging

from scripts.date_tracker import DateTracker
from scripts.spiders.rotogrinders_scraper import Sport
from scripts.staging.moneypuck import load_moneypuck_data_to_staging

logger = logging.getLogger(__name__)

# S3 configuration
BUCKET_NAME = os.getenv("WASABI_BUCKET_NAME")


class SpiderRunTemplate(TypedDict):
    sport: Sport
    tracking_name: str
    function: Callable


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


# Task functions
def check_if_scraped_extra_spiders(sport: Sport, tracking_name: str, **context) -> str:
    """Check if date already scraped for this sport."""
    logical_date = context["logical_date"]
    target_date = (logical_date - timedelta(days=3)).strftime("%Y-%m-%d")

    tracking_path = f"s3://{BUCKET_NAME}/staging/metadata/{sport}/{tracking_name}.json"
    tracker = DateTracker(sport=sport, tracking_path=tracking_path)
    is_scraped = tracker.is_scraped(target_date)

    if is_scraped:
        logger.info(f"{tracking_name} - Date {target_date} already scraped, skipping")
        return f"{tracking_name}.skip_scrape_{sport}"
    else:
        logger.info(f"{tracking_name} - Date {target_date} not found, will scrape")
        return f"{tracking_name}.scrape_data_{sport}"


def mark_date_scraped(sport: Sport, tracking_name: str, **context) -> None:
    """Mark the date as scraped in the tracking file."""
    logical_date = context["logical_date"]
    target_date = (logical_date - timedelta(days=3)).strftime("%Y-%m-%d")

    tracking_path = f"s3://{BUCKET_NAME}/staging/metadata/{sport}/{tracking_name}.json"
    tracker = DateTracker(sport=sport, tracking_path=tracking_path)
    tracker.mark_scraped(target_date)
    logger.info(f"{sport} - Marked {target_date} as scraped")


def scrape_and_load_extra_spider(spider_function: Callable, **context) -> None:
    """Scrape and load data using the provided spider function."""
    logical_date = context["logical_date"]
    target_date = (logical_date - timedelta(days=3)).strftime("%Y-%m-%d")

    logger.info(f"Starting scrape for {target_date}")
    spider_function(target_date)
    logger.info(f"Successfully scraped and loaded {target_date}")


spiders_to_run: list[SpiderRunTemplate] = [
    SpiderRunTemplate(
        sport="NHL",
        tracking_name="moneypuck",
        function=load_moneypuck_data_to_staging,
    )
]


with DAG(
    "daily_extra_spiders_scraping_dag",
    default_args=default_args,
    description="Daily scraping of extra spiders (MoneyPuck, etc.) with conditional execution",
    schedule="@daily",
    start_date=datetime(2025, 9, 1),
    catchup=True,
    max_active_runs=1,
    tags=cast(MutableSet, ["scraping", "extra_spiders", "staging"]),
) as dag:
    # Create TaskGroups dynamically for each spider
    for spider_run in spiders_to_run:
        sport = spider_run["sport"]
        tracking_name = spider_run["tracking_name"]
        with TaskGroup(group_id=tracking_name, dag=dag) as spider_group:

            # Task 1: Check if already scraped
            check = BranchPythonOperator(
                task_id=f"check_if_scraped_{sport}",
                python_callable=check_if_scraped_extra_spiders,
                op_kwargs={"sport": sport, "tracking_name": tracking_name},
            )

            # Task 2a: Skip (already scraped)
            skip = EmptyOperator(
                task_id=f"skip_scrape_{sport}",
            )

            # Task 2b: Scrape data
            scrape = PythonOperator(
                task_id=f"scrape_data_{sport}",
                python_callable=scrape_and_load_extra_spider,
                op_kwargs={"spider_function": spider_run["function"]},
            )

            # Task 3: Mark as scraped (only if scraping succeeded)
            mark = PythonOperator(
                task_id=f"mark_as_scraped_{sport}",
                python_callable=mark_date_scraped,
                op_kwargs={"sport": sport, "tracking_name": tracking_name},
                trigger_rule="none_failed_min_one_success",
            )

            # Dependencies
            check >> [skip, scrape]
            scrape >> mark
