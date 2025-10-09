import logging
import os

import duckdb
import s3fs

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

        # Initialize S3 filesystem for file checks
        self.s3 = s3fs.S3FileSystem(
            client_kwargs={"endpoint_url": f"https://{self.s3_endpoint}"},
            key=self.s3_access_key_id,
            secret=self.s3_secret_access_key,
        )

    def __enter__(self) -> "BaseDuckDBProcessor":
        self.con = duckdb.connect()
        self.con.execute(
            f"""
            SET s3_endpoint='{self.s3_endpoint}';
            SET s3_access_key_id='{self.s3_access_key_id}';
            SET s3_secret_access_key='{self.s3_secret_access_key}';
            SET s3_url_style='path';
            SET preserve_insertion_order = false; 
        """
        )
        return self

    logger.info("✓ DuckDB configured with S3 credentials")

    def __exit__(self, exc_type, exc_value, traceback):
        if self.con:
            self.con.close()

    def _s3_file_exists(self, s3_path: str) -> bool:
        """
        Check if an S3 file exists.

        Args:
            s3_path: Full S3 path (e.g., "s3://bucket/path/to/file.json.gz")

        Returns:
            True if file exists, False otherwise
        """
        try:
            # Remove s3:// prefix for s3fs
            path_without_protocol = s3_path.replace("s3://", "")
            return self.s3.exists(path_without_protocol)
        except Exception as e:
            logger.warning(f"Error checking S3 file {s3_path}: {e}")
            return False
