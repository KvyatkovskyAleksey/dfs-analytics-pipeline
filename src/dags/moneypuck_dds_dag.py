"""
MoneyPuck DDS Processing DAG

This DAG processes all MoneyPuck staging data into the DDS lines table.
It is designed for manual triggering only (not scheduled) and processes
all available staging data in a single run.
"""

from datetime import datetime, timedelta
from typing import MutableSet, cast

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
import logging

from scripts.dds.moneypuck_dds_processing import process_moneypuck_lines_to_dds

logger = logging.getLogger(__name__)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "moneypuck_dds_processing",
    default_args=default_args,
    description="Process all MoneyPuck staging data to DDS lines table (manual trigger)",
    schedule=None,  # Manual trigger only
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=cast(MutableSet, ["moneypuck", "dds", "nhl", "manual"]),
) as dag:

    # Single task to process all MoneyPuck data
    process_lines = PythonOperator(
        task_id="process_moneypuck_lines",
        python_callable=process_moneypuck_lines_to_dds,
        doc_md="""
        ### Process MoneyPuck Lines

        Transforms MoneyPuck staging data into DDS lines table.

        **Input:**
        - s3://bucket/staging/NHL/moneypuck/players/*/data.parquet

        **Output:**
        - s3://bucket/dds/NHL/moneypuck/lines/data.parquet

        **Processing:**
        - Filters 5on5 situations
        - Extracts forward lines (f1-f4)
        - Extracts defense pairings (d1-d3)
        - Extracts starting goalie (G1)
        - Combines all into single table

        **Note:** Processes ALL available dates in a single run (no date partitioning)
        """,
    )
