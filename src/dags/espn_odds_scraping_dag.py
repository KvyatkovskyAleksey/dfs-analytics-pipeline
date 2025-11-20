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

from scripts.staging.espn import load_espn_odds_to_staging
from scripts.date_tracker import DateTracker
from schemas import Sport

logger = logging.getLogger(__name__)

BUCKET_NAME = os.getenv("WASABI_BUCKET_NAME")

SPORTS: list[Sport] = ["NBA", "NFL", "NHL"]

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def check_if_scraped(sport: Sport, **context) -> str:
    """Check if date already scraped. Always scrape if today, skip if backfill date already scraped."""
    logical_date = context["logical_date"]
    target_date = logical_date.strftime("%Y-%m-%d")
    today_date = datetime.now().strftime("%Y-%m-%d")

    if target_date == today_date:
        logger.info(
            f"{sport} - Target date {target_date} is today, will scrape to get latest odds"
        )
        return f"{sport}.scrape_data_{sport}"

    tracking_path = (
        f"s3://{BUCKET_NAME}/staging/metadata/{sport}/espn_odds_scraped_dates.json"
    )
    tracker = DateTracker(sport=sport, tracking_path=tracking_path)
    is_scraped = tracker.is_scraped(target_date)

    if is_scraped:
        logger.info(
            f"{sport} - Backfill date {target_date} already scraped, skipping"
        )
        return f"{sport}.skip_scrape_{sport}"
    else:
        logger.info(f"{sport} - Backfill date {target_date} not found, will scrape")
        return f"{sport}.scrape_data_{sport}"


def scrape_and_load(sport: Sport, **context) -> None:
    """Scrape and load ESPN odds data for this sport."""
    logical_date = context["logical_date"]
    target_date = logical_date.strftime("%Y-%m-%d")

    logger.info(f"{sport} - Starting ESPN odds scrape for {target_date}")
    load_espn_odds_to_staging(date=target_date, sport=sport)
    logger.info(f"{sport} - Successfully scraped and loaded ESPN odds for {target_date}")


def mark_date_scraped(sport: Sport, **context) -> None:
    """Mark date as scraped in tracking file."""
    logical_date = context["logical_date"]
    target_date = logical_date.strftime("%Y-%m-%d")

    tracking_path = (
        f"s3://{BUCKET_NAME}/staging/metadata/{sport}/espn_odds_scraped_dates.json"
    )
    tracker = DateTracker(sport=sport, tracking_path=tracking_path)
    tracker.mark_scraped(target_date)
    logger.info(f"{sport} - Marked {target_date} as scraped")


with DAG(
    "espn_odds_scraping_dag",
    default_args=default_args,
    description="Scraping ESPN odds data twice daily (00:00 and 12:00 UTC)",
    schedule="0 */12 * * *",
    start_date=datetime(2025, 9, 1),
    catchup=True,
    max_active_runs=3,
    tags=cast(MutableSet, ["scraping", "espn", "odds", "staging"]),
) as dag:
    for sport in SPORTS:
        with TaskGroup(group_id=sport, dag=dag) as sport_group:

            check = BranchPythonOperator(
                task_id=f"check_if_scraped_{sport}",
                python_callable=check_if_scraped,
                op_kwargs={"sport": sport},
            )

            skip = EmptyOperator(
                task_id=f"skip_scrape_{sport}",
            )

            scrape = PythonOperator(
                task_id=f"scrape_data_{sport}",
                python_callable=scrape_and_load,
                op_kwargs={"sport": sport},
            )

            mark = PythonOperator(
                task_id=f"mark_as_scraped_{sport}",
                python_callable=mark_date_scraped,
                op_kwargs={"sport": sport},
                trigger_rule="none_failed_min_one_success",
            )

            check >> [skip, scrape]
            scrape >> mark
