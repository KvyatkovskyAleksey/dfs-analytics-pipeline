"""Processor for calculating minimum points to cash and sending to API."""

import logging
import os
from typing import Optional, Union, List
from datetime import datetime, timedelta

import pandas as pd
import requests

from scripts.base_duck_db_processor import BaseDuckDBProcessor
from scripts.rotogrinders_scraper import Sport

logger = logging.getLogger(__name__)


class MinPointsProcessor(BaseDuckDBProcessor):
    """
    Process minimum points data from DDS layer and send to external API.

    Queries contests and lineups to determine minimum points needed to cash
    in each contest, then sends formatted data to configured API endpoint.
    """

    def __init__(
        self,
        sport: Sport,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> None:
        """
        Initialize MinPointsProcessor.

        Args:
            sport: Sport type (e.g., "NFL")
            start_date: Optional start date in YYYY-MM-DD format
            end_date: Optional end date in YYYY-MM-DD format
                     If dates not provided, processes all available data
        """
        super().__init__()
        self.sport = sport
        self.start_date = start_date
        self.end_date = end_date

        # Get API configuration from environment
        self.api_url = os.getenv("MIN_POINTS_API_URL")
        self.api_key = os.getenv("MIN_POINTS_API_KEY")

        if not self.api_url:
            logger.warning(
                "MIN_POINTS_API_URL not set - will process data but not send to API"
            )

        logger.info(
            f"Initialized MinPointsProcessor for {sport} "
            f"(date range: {start_date or 'all'} to {end_date or 'all'})"
        )

    def _build_date_path_pattern(self) -> Union[str, List[str]]:
        """
        Build S3 path pattern for date-based filtering.

        Uses path-based filtering to minimize data loading from S3.
        Format: /lineups/*/YYYY-MM-DD/data.parquet

        Returns:
            Path pattern with date wildcards:
            - Single month: "2025-02-*"
            - Multiple months: ["2023-09-*", "2023-10-*", ...]
            - No dates: "*"
        """
        if not self.start_date and not self.end_date:
            # No date filtering - load all dates
            return "*"

        # Parse dates
        start = (
            datetime.strptime(self.start_date, "%Y-%m-%d") if self.start_date else None
        )
        end = datetime.strptime(self.end_date, "%Y-%m-%d") if self.end_date else None

        # If both dates are in the same month, use simple pattern
        if start and end and start.year == end.year and start.month == end.month:
            return f"{start.year}-{start.month:02d}-*"

        # If spanning multiple months, generate list of patterns
        if start and end:
            months = []
            current = start.replace(day=1)
            end_month = end.replace(day=1)

            while current <= end_month:
                months.append(f"{current.year}-{current.month:02d}-*")
                # Move to next month
                if current.month == 12:
                    current = current.replace(year=current.year + 1, month=1)
                else:
                    current = current.replace(month=current.month + 1)

            # Return as list for DuckDB
            return months

        # Fallback to all dates
        return "*"

    def _filter_existing_paths(self, paths: Union[str, List[str]]) -> Union[str, List[str], None]:
        """
        Filter S3 paths to only include those that actually exist.

        Args:
            paths: Single path string or list of path strings (can contain wildcards)

        Returns:
            Filtered paths (same type as input), or None if no paths exist
        """
        if isinstance(paths, str):
            # Single path - check if it exists
            path_without_protocol = paths.replace("s3://", "")
            matches = self.s3.glob(path_without_protocol)
            if matches:
                logger.debug(f"Path exists: {paths} ({len(matches)} files)")
                return paths
            else:
                logger.warning(f"No files found for path: {paths}")
                return None
        else:
            # List of paths - filter to only existing ones
            existing_paths = []
            for path in paths:
                path_without_protocol = path.replace("s3://", "")
                matches = self.s3.glob(path_without_protocol)
                if matches:
                    logger.debug(f"Path exists: {path} ({len(matches)} files)")
                    existing_paths.append(path)
                else:
                    logger.warning(f"No files found for path: {path}")

            if existing_paths:
                logger.info(f"Found data for {len(existing_paths)}/{len(paths)} month patterns")
                return existing_paths
            else:
                logger.warning(f"No files found for any of {len(paths)} paths")
                return None

    def _build_date_filter(self) -> str:
        """
        Build SQL date filter clause for exact date boundaries.

        Used as a secondary filter after path-based filtering to handle
        exact start/end days within months.

        Returns:
            SQL WHERE clause for date filtering, or empty string if no dates
        """
        if not self.start_date and not self.end_date:
            return ""

        # Convert YYYY-MM-DD to YYYYMMDD format for date_id
        filters = []
        if self.start_date:
            date_id_start = self.start_date.replace("-", "")
            filters.append(f"date_id >= '{date_id_start}'")
        if self.end_date:
            date_id_end = self.end_date.replace("-", "")
            filters.append(f"date_id <= '{date_id_end}'")

        return "AND " + " AND ".join(filters) if filters else ""

    def get_min_points_data(self) -> pd.DataFrame:
        """
        Query DDS layer to get minimum and maximum points data for contests.

        Returns:
            DataFrame with columns: slate_id, contest_id, min_points,
                                   max_points, cash_line, lineup_rank, date_id

        Raises:
            Exception: If query fails or data cannot be retrieved
        """
        logger.info(f"Querying minimum points data for {self.sport}...")

        # Build S3 paths with month-based filtering to minimize data loading
        date_pattern = self._build_date_path_pattern()

        # Build path lists based on date pattern type
        if isinstance(date_pattern, list):
            # Multiple months - create list of paths for each month
            dds_contests_path = [
                f"s3://{self.bucket_name}/dds/{self.sport}/contests/*/{month}/data.parquet"
                for month in date_pattern
            ]
            lineups_path = [
                f"s3://{self.bucket_name}/dds/{self.sport}/lineups/*/{month}/data.parquet"
                for month in date_pattern
            ]
        else:
            # Single pattern (string)
            dds_contests_path = f"s3://{self.bucket_name}/dds/{self.sport}/contests/*/{date_pattern}/data.parquet"
            lineups_path = f"s3://{self.bucket_name}/dds/{self.sport}/lineups/*/{date_pattern}/data.parquet"

        logger.debug(f"Using date pattern: {date_pattern}")
        logger.debug(f"Contests path (before filtering): {dds_contests_path}")
        logger.debug(f"Lineups path (before filtering): {lineups_path}")

        # Filter paths to only include those that actually exist on S3
        dds_contests_path = self._filter_existing_paths(dds_contests_path)
        lineups_path = self._filter_existing_paths(lineups_path)

        # If no paths exist, return empty DataFrame
        if not dds_contests_path or not lineups_path:
            logger.warning(
                f"No data found for {self.sport} in date range "
                f"{self.start_date or 'all'} to {self.end_date or 'all'}"
            )
            return pd.DataFrame(
                columns=[
                    "slate_id",
                    "contest_id",
                    "min_points",
                    "max_points",
                    "cash_line",
                    "lineup_rank",
                    "date_id",
                ]
            )

        logger.debug(f"Contests path (after filtering): {dds_contests_path}")
        logger.debug(f"Lineups path (after filtering): {lineups_path}")

        # Build exact date filter for SQL (secondary filter for exact day boundaries)
        date_filter = self._build_date_filter()

        # Format paths for SQL query
        if isinstance(dds_contests_path, list):
            # Convert list to SQL array format: ['path1', 'path2', ...]
            contests_path_sql = str(dds_contests_path)
            lineups_path_sql = str(lineups_path)
        else:
            # Single path - wrap in quotes
            contests_path_sql = f"'{dds_contests_path}'"
            lineups_path_sql = f"'{lineups_path}'"

        # Query to get minimum points for each contest
        query = f"""
        WITH contests AS (
            SELECT
                contest_id,
                contest_group_id AS slate_id,
                cash_line,
                date_id
            FROM read_parquet({contests_path_sql}, union_by_name=true)
            WHERE is_largest_by_size = TRUE
                {date_filter}
        ),
        lineups AS (
            SELECT
                contest_id,
                MIN(points) AS min_points,
                MAX(points) AS max_points,
                MAX(lineup_rank) AS lineup_rank
            FROM read_parquet({lineups_path_sql}, union_by_name=true)
            WHERE is_cashing = TRUE
            GROUP BY contest_id
        )
        SELECT DISTINCT
            contests.slate_id,
            contests.contest_id,
            lineups.min_points,
            lineups.max_points,
            contests.cash_line,
            lineups.lineup_rank,
            contests.date_id
        FROM contests
        INNER JOIN lineups ON contests.contest_id = lineups.contest_id
        ORDER BY contests.date_id DESC, contests.slate_id, contests.contest_id
        """

        logger.debug(f"Executing query:\n{query}")

        try:
            result_df = self.con.execute(query).df()
            logger.info(
                f"Retrieved {len(result_df)} contest records "
                f"(with min_points: {result_df['min_points'].notna().sum()})"
            )
            return result_df
        except Exception as e:
            logger.error(f"Failed to query minimum points data: {e}")
            raise

    def send_to_api(self, data: pd.DataFrame) -> None:
        """
        Send minimum points data to external API.

        Args:
            data: DataFrame with minimum points data

        Raises:
            requests.RequestException: If API request fails
        """
        if not self.api_url:
            logger.warning("Skipping API send - MIN_POINTS_API_URL not configured")
            return

        if data.empty:
            logger.warning("No data to send to API")
            return

        # Prepare payload
        payload = {
            "sport": self.sport,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "total_records": len(data),
            "records_with_min_points": int(data["min_points"].notna().sum()),
            "data": data.to_dict(orient="records"),
        }

        # Prepare headers
        headers = {
            "Content-Type": "application/json",
        }
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"

        # Send request
        logger.info(f"Sending {len(data)} records to API: {self.api_url}")

        try:
            response = requests.post(
                self.api_url,
                json=payload,
                headers=headers,
                timeout=30,
            )

            response.raise_for_status()

            logger.info(
                f"Successfully sent data to API "
                f"(status: {response.status_code}, response: {response.text[:200]})"
            )

        except requests.exceptions.HTTPError as e:
            logger.error(
                f"API request failed with status {e.response.status_code}: "
                f"{e.response.text}"
            )
            raise
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to send data to API: {e}")
            raise

    def process(self) -> dict:
        """
        Main processing method: query data and send to API.

        Returns:
            Dictionary with processing results (record counts, status)

        Raises:
            Exception: If processing fails
        """
        logger.info(
            f"Starting minimum points processing for {self.sport} "
            f"(date range: {self.start_date or 'all'} to {self.end_date or 'all'})"
        )

        # Get data
        data = self.get_min_points_data()

        # Send to API
        self.send_to_api(data)

        result = {
            "sport": self.sport,
            "start_date": self.start_date,
            "end_date": self.end_date,
            "total_records": len(data),
            "records_with_min_points": int(data["min_points"].notna().sum()),
            "status": "completed",
        }

        logger.info(f"Processing completed successfully: {result}")
        return result


if __name__ == "__main__":
    # Example usage for testing
    logging.basicConfig(level=logging.INFO)

    today = datetime.now()
    first_of_this_month = today.replace(day=1)
    last_month_end = first_of_this_month - timedelta(days=1)
    last_month_start = last_month_end.replace(day=1)

    with MinPointsProcessor(
        sport="NFL",
        start_date=last_month_start.strftime("%Y-%m-%d"),
        end_date=last_month_end.strftime("%Y-%m-%d"),
    ) as processor:
        result = processor.process()
        print(f"Result: {result}")
