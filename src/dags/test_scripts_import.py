"""
Test DAG to verify:
1. Custom packages (duckdb, pandas, s3fs) are installed
2. Scripts from src/scripts are importable via PYTHONPATH
3. Development setup is working correctly
"""

from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator


def test_package_imports():
    """Test that all required packages are installed and importable."""
    print("=" * 60)
    print("Testing package imports...")
    print("=" * 60)

    try:
        import pandas as pd

        print(f"✓ pandas {pd.__version__} imported successfully")
    except ImportError as e:
        print(f"✗ Failed to import pandas: {e}")
        raise

    try:
        import duckdb

        print(f"✓ duckdb {duckdb.__version__} imported successfully")
    except ImportError as e:
        print(f"✗ Failed to import duckdb: {e}")
        raise

    try:
        import s3fs

        print(f"✓ s3fs {s3fs.__version__} imported successfully")
    except ImportError as e:
        print(f"✗ Failed to import s3fs: {e}")
        raise

    try:
        import fsspec

        print(f"✓ fsspec {fsspec.__version__} imported successfully")
    except ImportError as e:
        print(f"✗ Failed to import fsspec: {e}")
        raise

    print("\n✓ All packages imported successfully!")


def test_scripts_import():
    """Test that custom scripts are importable from /opt/scripts."""
    print("=" * 60)
    print("Testing scripts imports from /opt/scripts...")
    print("=" * 60)

    try:
        from scripts.staging.staging_processor import RotogrindersStagingProcessor

        print(f"✓ RotogrindersStagingProcessor imported successfully")
        print(f"  Location: {RotogrindersStagingProcessor.__module__}")
    except ImportError as e:
        print(f"✗ Failed to import RotogrindersStagingProcessor: {e}")
        raise

    try:
        from scripts.exceptions import ImproperlyConfigured

        print(f"✓ ImproperlyConfigured imported successfully")
    except ImportError as e:
        print(f"✗ Failed to import exceptions: {e}")
        raise

    try:
        from scripts.spiders.rotogrinders_scraper import (
            RotogrindersScraper,
            StagingData,
        )

        print(f"✓ RotogrindersScraper and StagingData imported successfully")
    except ImportError as e:
        print(f"✗ Failed to import rotogrinders_scraper: {e}")
        raise

    print("\n✓ All scripts imported successfully!")
    print("✓ PYTHONPATH is configured correctly!")


def test_pythonpath():
    """Display PYTHONPATH to verify configuration."""
    import sys
    import os

    print("=" * 60)
    print("Python environment information:")
    print("=" * 60)
    print(f"Python version: {sys.version}")
    print(f"\nPYTHONPATH from env: {os.getenv('PYTHONPATH', 'Not set')}")
    print(f"\nsys.path entries:")
    for i, path in enumerate(sys.path, 1):
        print(f"  {i}. {path}")


def verify_setup():
    """Run all verification tests."""
    print("\n" + "=" * 60)
    print("AIRFLOW SETUP VERIFICATION")
    print("=" * 60 + "\n")

    test_pythonpath()
    print("\n")
    test_package_imports()
    print("\n")
    test_scripts_import()

    print("\n" + "=" * 60)
    print("✓✓✓ ALL TESTS PASSED! SETUP IS CORRECT! ✓✓✓")
    print("=" * 60 + "\n")


# DAG definition
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}

with DAG(
    dag_id="test_scripts_import",
    default_args=default_args,
    description="Test that packages and scripts are properly installed/mounted",
    schedule=None,  # Manual trigger only
    catchup=False,
    tags=["test", "setup", "development"],
) as dag:

    test_task = PythonOperator(
        task_id="verify_airflow_setup",
        python_callable=verify_setup,
    )
