import logging
import os

import duckdb

from scripts.exceptions import ImproperlyConfigured

logger = logging.getLogger("BaseDuckDBProcessor")
logger.setLevel(logging.INFO)


class BaseDuckDBProcessor:
    def __init__(self, *args, **kwargs) -> None:
        self.s3_endpoint = os.getenv("WASABI_ENDPOINT", "s3.us-east-2.wasabisys.com")
        self.s3_access_key_id = os.getenv("WASABI_ACCESS_KEY")
        self.s3_secret_access_key = os.getenv("WASABI_SECRET_KEY")
        self.bucket_name = os.getenv("WASABI_BUCKET_NAME")
        if (
            not self.s3_access_key_id
            or not self.s3_secret_access_key
            or not self.bucket_name
        ):
            raise ImproperlyConfigured(
                "WASABI_ACCESS_KEY and WASABI_SECRET_KEY environment variables must be set"
            )
        self.base_path = f"s3://{self.bucket_name}/staging/{self.sport}/"

    def __enter__(self) -> "BaseDuckDBProcessor":
        self.con = duckdb.connect()
        self.con.execute(
            f"""
            SET s3_endpoint='{self.s3_endpoint}';
            SET s3_access_key_id='{self.s3_access_key_id}';
            SET s3_secret_access_key='{self.s3_secret_access_key}';
            SET s3_url_style='path';
            SET preserve_insertion_order = false; 
            SET temp_directory = '/tmp/duck_db_tmp_dir.tmp/';
        """
        )
        return self

    logger.info("✓ DuckDB configured with S3 credentials")

    def __exit__(self, exc_type, exc_value, traceback):
        if self.con:
            self.con.close()
