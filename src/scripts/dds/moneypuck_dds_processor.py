import logging

from scripts.base_duck_db_processor import BaseDuckDBProcessor

logger = logging.getLogger("MoneypuckDdsProcessor")


class MoneypuckDdsProcessor(BaseDuckDBProcessor):
    """Process MoneyPuck staging data to DDS"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.staging_base_path = f"s3://{self.bucket_name}/staging/NHL/moneypuck/"
        self.dds_base_path = f"s3://{self.bucket_name}/dds/NHL/moneypuck/"

    def _check_staging_data_exists(self) -> bool:
        """Check if MoneyPuck staging data exists"""
        players_path = f"{self.staging_base_path}players/*/data.parquet"
        try:
            # Try to read one row to verify data exists
            result = self.con.execute(
                f"SELECT COUNT(*) as count FROM read_parquet('{players_path}', filename=true)"
            ).fetchone()
            count = result[0] if result else 0
            if count > 0:
                logger.info(f"Found {count} rows in MoneyPuck staging data")
                return True
            else:
                logger.warning("No MoneyPuck staging data found")
                return False
        except Exception as e:
            logger.error(f"Error checking staging data: {e}")
            return False

    def process_lines(self) -> None:
        """
        Process MoneyPuck staging data into lines table.

        Transforms player data into line combinations (forwards, defense, goalies)
        for 5on5 situations and saves to a single DDS file.
        """
        logger.info("Processing MoneyPuck lines data")

        # Check if staging data exists
        if not self._check_staging_data_exists():
            logger.warning("Skipping MoneyPuck processing - no staging data available")
            return

        players_path = f"{self.staging_base_path}players/*/data.parquet"
        dds_path = f"{self.dds_base_path}lines/data.parquet"

        logger.info(f"Reading from: {players_path}")
        logger.info(f"Writing to: {dds_path}")

        # Execute the transformation SQL
        self.con.execute(
            f"""
            COPY (
                WITH players AS (
                    SELECT
                        playerName,
                        team,
                        I_F_iceTime,
                        iceTimeRank,
                        position,
                        situation,
                        split_part(filename, '/', 8) as game_date
                    FROM read_parquet('{players_path}', filename=true)
                    WHERE situation = '5on5'
                        AND position IN ('line', 'pairing', 'G')
                ),
                lines_players_raw AS (
                    SELECT
                        *,
                        string_split(playerName, '-') as last_names,
                        'f' || CAST(iceTimeRank AS VARCHAR) as line
                    FROM players
                    WHERE position = 'line' AND iceTimeRank <= 4
                    ORDER BY iceTimeRank ASC
                ),
                lines_players AS (
                    SELECT
                        unnest(last_names) as last_name,
                        team,
                        line,
                        game_date
                    FROM lines_players_raw
                ),
                defense_players_raw AS (
                    SELECT
                        *,
                        string_split(playerName, '-') as last_names,
                        'd' || CAST(iceTimeRank AS VARCHAR) as line
                    FROM players
                    WHERE position = 'pairing' AND iceTimeRank <= 3
                    ORDER BY iceTimeRank ASC
                ),
                defense_players AS (
                    SELECT
                        unnest(last_names) as last_name,
                        team,
                        line,
                        game_date
                    FROM defense_players_raw
                ),
                goalie_players AS (
                    SELECT
                        playerName as last_name,
                        team,
                        'G1' as line,
                        game_date
                    FROM players
                    WHERE position = 'G' AND iceTimeRank = -1
                    ORDER BY I_F_iceTime ASC
                )
                SELECT
                    game_date,
                    trim(last_name) as last_name,
                    team,
                    line
                FROM lines_players
                UNION ALL
                SELECT
                    game_date,
                    trim(last_name) as last_name,
                    team,
                    line
                FROM defense_players
                UNION ALL
                SELECT
                    game_date,
                    trim(last_name) as last_name,
                    team,
                    line
                FROM goalie_players
            )
            TO '{dds_path}'
            (FORMAT PARQUET, COMPRESSION 'SNAPPY')
            """
        )

        logger.info(f"MoneyPuck lines data saved to DDS: {dds_path}")


if __name__ == "__main__":
    # Test the processor
    with MoneypuckDdsProcessor() as processor:
        processor.process_lines()
