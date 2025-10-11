import gzip
import json
import logging
import time
from io import BytesIO

import pandas as pd

from scripts.base_duck_db_processor import BaseDuckDBProcessor
from scripts.rotogrinders_scraper import SlateType

logger = logging.getLogger("DdsProcessor")


class DdsProcessor(BaseDuckDBProcessor):
    """Process staging data to DDS"""

    slate_types: list[SlateType] = ["dk_classic", "dk_single_game"]
    MAX_OBJECT_SIZE = 256 * 1024 * 1024  # we sometimes have large JSON objects,
    # so for handle it we need to increase max object size for read

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.staging_base_path = f"s3://{self.bucket_name}/staging/"
        self.dds_base_path = f"s3://{self.bucket_name}/dds/"
        self.date = kwargs.get("date")
        self.sport = kwargs.get("sport")
        if self.date is None or self.sport is None:
            raise ValueError("Date and sport must be provided")

    def process_contests(self) -> None:
        """Save contests data to DDS stage for a given date and sport"""
        logger.info(f"Processing contests for {self.date} {self.sport}")
        for slate_type in self.slate_types:
            staging_path = f"{self.staging_base_path}{self.sport}/contests/{slate_type}/{self.date}/data.json.gz"

            # Check if a staging file exists before processing
            if not self._s3_file_exists(staging_path):
                logger.warning(
                    f"Skipping {slate_type} for contests - staging file not found: {staging_path}"
                )
                continue

            dds_path = f"{self.dds_base_path}{self.sport}/contests/{slate_type}/{self.date}/data.parquet"
            self.con.execute(
                f"""
                COPY
                    (SELECT
                        (contest_element ->> 'contest_id')::INTEGER as contest_id,
                        contest_element ->> 'contest_name' as contest_name,
                        (contest_element ->> 'contest_size')::INTEGER as contest_size,
                        (contest_element ->> 'entry_cost')::INTEGER as entry_cost,
                        (contest_element ->> 'total_prizes')::INTEGER as total_prizes,
                        (contest_element ->> 'multi_entry_max')::INTEGER as multi_entry_max,
                        (contest_element ->> 'is_largest_by_size')::BOOLEAN as is_largest_by_size,
                        (contest_element ->> 'is_primary')::BOOLEAN as is_primary,
                        contest_element ->> 'tour' as tour,
                        (contest_element ->> 'source_id')::INTEGER as source_id,
                        (contest_element ->> 'sport_event_id')::INTEGER as sport_event_id,
                        (contest_element ->> 'contest_group_id')::INTEGER as contest_group_id,
                        (contest_element ->> 'cash_line')::INTEGER as cash_line,
                        (contest_element ->> 'date_id')::INTEGER as date_id
                    FROM (
                        SELECT
                            unnest(live_contests) as contest_element
                        FROM read_json_auto('{staging_path}', maximum_object_size={self.MAX_OBJECT_SIZE})
                    )
                    ) TO '{dds_path}'
                    (FORMAT PARQUET, COMPRESSION 'SNAPPY')
            """
            )
        logger.info(f"Contests data for {self.date} {self.sport} saved to DDS")

    def process_players(self) -> None:
        """Save players data to DDS stage for a given date and sport"""
        logger.info(f"Processing players for {self.date} {self.sport}")
        for slate_type in self.slate_types:
            staging_path = f"{self.staging_base_path}{self.sport}/contest_analyze/{slate_type}/{self.date}/data.json.gz"

            # Check if a staging file exists before processing
            if not self._s3_file_exists(staging_path):
                logger.warning(
                    f"Skipping {slate_type} for players - staging file not found: {staging_path}"
                )
                continue

            dds_path = f"{self.dds_base_path}{self.sport}/players/{slate_type}/{self.date}/data.parquet"
            self.con.execute(
                f"""
                COPY
                    (SELECT DISTINCT
                        kv.key as player_key,
                        (kv.value->>'playerId')::INTEGER as player_id,
                        kv.value->>'firstName' as first_name,
                        kv.value->>'lastName' as last_name,
                        kv.value->>'fullName' as full_name,
                        (kv.value->>'salary')::INTEGER as salary,
                        kv.value->>'position' as position,
                        kv.value->>'rosterPosition' as roster_position,
                        kv.value->>'currentTeam' as current_team,
                        (kv.value->>'currentTeamId')::INTEGER as current_team_id,
                        (kv.value->>'eventId')::INTEGER as event_id,
                        (kv.value->>'eventTeamId')::INTEGER as event_team_id,
                        kv.value->>'homeVisitor' as home_visitor,
                        kv.value->>'favDog' as fav_dog,
                        (kv.value->>'projPoints')::DOUBLE as proj_points,
                        (kv.value->>'ownership')::DOUBLE as ownership,
                        (kv.value->>'actualPoints')::DOUBLE as actual_points,
                        kv.value->>'statDetails' as stat_details,
                        (kv.value->>'madeCut')::INTEGER as made_cut
                    FROM read_json_auto('{staging_path}', maximum_object_size={self.MAX_OBJECT_SIZE}),
                         json_each(players) as kv
                    ) TO '{dds_path}'
                    (FORMAT PARQUET, COMPRESSION 'SNAPPY')
            """
            )
        logger.info(f"Players data for {self.date} {self.sport} saved to DDS")

    def process_users_lineups_deprecated(self):
        """Save users and their lineup data to the DDS stage for a given date and sport (DEPRECATED - use process_users_lineups instead)"""
        logger.info(f"Processing users and lineups for {self.date} {self.sport}")
        for slate_type in self.slate_types:
            staging_path = f"{self.staging_base_path}{self.sport}/contest_analyze/{slate_type}/{self.date}/data.json.gz"

            # Check if a staging file exists before processing
            if not self._s3_file_exists(staging_path):
                logger.warning(
                    f"Skipping {slate_type} for users_lineups - staging file not found: {staging_path}"
                )
                continue

            users_path = f"{self.dds_base_path}{self.sport}/users/{slate_type}/{self.date}/data.parquet"
            lineups_path = f"{self.dds_base_path}{self.sport}/user_lineups/{slate_type}/{self.date}/data.parquet"

            self.con.execute(
                f"""
                CREATE OR REPLACE TABLE tmp_table AS
                SELECT contest, users
                FROM read_json_auto('{staging_path}', maximum_object_size={self.MAX_OBJECT_SIZE});
                -- first save users 
                COPY 
                    (SELECT DISTINCT
                      contest ->> 'contestId' as contest_id,
                      kv.value ->> 'userId' as user_id,
                      (kv.value ->> 'totalPlayers')::INTEGER as total_players,
                      (kv.value ->> 'totalRosters')::INTEGER as total_rosters,
                      (kv.value ->> 'uniqueRosters')::INTEGER as unique_rosters,
                      (kv.value ->> 'maxExposure')::DOUBLE as max_exposure,
                      (kv.value ->> 'lineupsCashing')::INTEGER as lineups_cashing,
                      (kv.value ->> 'lineupsInPercentile1')::INTEGER as lineups_in_percentile_1,
                      (kv.value ->> 'lineupsInPercentile2')::INTEGER as lineups_in_percentile_2,
                      (kv.value ->> 'lineupsInPercentile5')::INTEGER as lineups_in_percentile_5,
                      (kv.value ->> 'lineupsInPercentile10')::INTEGER as lineups_in_percentile_10,
                      (kv.value ->> 'lineupsInPercentile20')::INTEGER as lineups_in_percentile_20,
                      (kv.value ->> 'lineupsInPercentile50')::INTEGER as lineups_in_percentile_50,
                      (kv.value ->> 'totalEntryCost')::DOUBLE as total_entry_cost,
                      (kv.value ->> 'totalWinning')::DOUBLE as total_winning,
                      (kv.value ->> 'roi')::DOUBLE as roi
                    FROM tmp_table,
                       json_each(users) as kv)
                TO '{users_path}'
                (FORMAT PARQUET, COMPRESSION 'SNAPPY');
                -- then save lineups
                
                -- Create a temporary table with pre-parsed users data
                CREATE OR REPLACE TABLE users_parsed AS
                SELECT
                    contest ->> 'contestId' as contest_id,
                    u.value as user_data
                FROM tmp_table,
                    json_each(users) as u;

                -- Then query lineups from the parsed table
                COPY (
                SELECT
                    contest_id::INTEGER as contest_id,
                    (user_data ->> 'userId')::TEXT as user_id,
                    lineup.value ->> 'lineupHash' as lineup_hash,
                    (lineup.value ->> 'lineupCt')::INTEGER as lineup_ct
                FROM users_parsed,
                    json_each(user_data->'lineups') as lineup
                ) TO '{lineups_path}'
                (FORMAT PARQUET, COMPRESSION 'SNAPPY');
            """
            )
        logger.info(f"Users and lineups data for {self.date} {self.sport} saved to DDS")

    def process_lineups_deprecated(self):
        """Process lineups data to DDS stage for a given date and sport (DEPRECATED - use process_lineups instead)"""
        for slate_type in self.slate_types:
            staging_lineups_path = f"{self.staging_base_path}{self.sport}/lineups/{slate_type}/{self.date}/data.json.gz"

            # Check if a staging file exists before processing
            if not self._s3_file_exists(staging_lineups_path):
                logger.warning(
                    f"Skipping {slate_type} for lineups - staging file not found: {staging_lineups_path}"
                )
                continue

            lineups_path = f"{self.dds_base_path}{self.sport}/lineups/{slate_type}/{self.date}/data.parquet"

            # First, get all unique position keys from the lineup data (we process different slate types,
            # so here slate positions are different and need to handle it)
            positions_query = f"""
                SELECT DISTINCT
                    pos_key.key as position_key
                FROM read_json_auto('{staging_lineups_path}', maximum_object_size={self.MAX_OBJECT_SIZE}),
                    json_each(lineups -> 'lineupPlayers') as pos_key
                ORDER BY position_key
            """

            positions_df = self.con.execute(positions_query).df()
            position_keys = positions_df["position_key"].tolist()

            # Create dynamic position columns
            position_columns = ", ".join(
                [
                    f"(lineups -> 'lineupPlayers' ->> '{pos}')::INTEGER as pos_{pos.lower()}"
                    for pos in position_keys
                ]
            )

            # Now build the main query with dynamic position columns
            query = f"""
                SELECT
                    slate_id as contest_id,
                    (lineups ->> 'lineupHash') as lineup_hash,
                    (lineups ->> 'lineupCt')::INTEGER as lineup_ct,
                    (lineups ->> 'lineupUserCt')::INTEGER as lineup_user_ct,
                    (lineups ->> 'points')::DOUBLE as points,
                    (lineups ->> 'totalSalary')::INTEGER as total_salary,
                    (lineups ->> 'totalOwn')::DOUBLE as total_own,
                    (lineups ->> 'minOwn')::DOUBLE as min_own,
                    (lineups ->> 'maxOwn')::DOUBLE as max_own,
                    (lineups ->> 'avgOwn')::DOUBLE as avg_own,
                    (lineups ->> 'lineupRank')::INTEGER as lineup_rank,
                    (lineups ->> 'isCashing')::BOOLEAN as is_cashing,
                    (lineups ->> 'favoriteCt')::INTEGER as favorite_ct,
                    (lineups ->> 'underdogCt')::INTEGER as underdog_ct,
                    (lineups ->> 'homeCt')::INTEGER as home_ct,
                    (lineups ->> 'visitorCt')::INTEGER as visitor_ct,
                    (lineups ->> 'payout')::DOUBLE as payout,
                    (lineups ->> 'lineupPercentile')::DOUBLE as lineup_percentile,
                    (lineups ->> 'correlatedPlayers')::INTEGER as correlated_players,

                    -- Dynamic position columns
                    {position_columns},

                    -- Keep complex objects as JSON strings
                    lineups -> 'teamStacks' as team_stacks,
                    lineups -> 'gameStacks' as game_stacks,
                    lineups -> 'lineupTrends' as lineup_trends,
                    lineups -> 'entryNameList' as entry_name_list

                FROM read_json_auto('{staging_lineups_path}', maximum_object_size={self.MAX_OBJECT_SIZE})
            """
            # and now need to copy data dds stage
            self.con.execute(
                f"""
                COPY
                    (
                    {query}
                    ) TO '{lineups_path}'
                    (FORMAT PARQUET, COMPRESSION 'SNAPPY')"""
            )

    def process_users_lineups(self):
        """Process users and lineups data to DDS stage for a given date and sport.

        Optimized version that uses pandas for parsing and DuckDB for writing.
        Much faster than the deprecated version because it loads JSON from S3 once and
        avoids DuckDB's slow json_each() on deeply nested structures with hash keys.
        (5-9x faster by tests)
        """
        for slate_type in self.slate_types:
            users_path = f"{self.dds_base_path}{self.sport}/users/{slate_type}/{self.date}/data.parquet"
            lineups_path = f"{self.dds_base_path}{self.sport}/user_lineups/{slate_type}/{self.date}/data.parquet"

            staging_path = f"{self.staging_base_path}{self.sport}/contest_analyze/{slate_type}/{self.date}/data.json.gz"
            # 1 Check if a staging file exists before processing
            if not self._s3_file_exists(staging_path):
                logger.warning(
                    f"Skipping {slate_type} for users_lineups - staging file not found: {staging_path}"
                )
                continue
            with self.s3.open(staging_path.replace("s3://", ""), "rb") as f:
                compressed_data = f.read()
            with gzip.open(BytesIO(compressed_data), "rt") as gz:
                source_df = pd.read_json(gz, lines=True)
            source_df = source_df[["contest", "users"]].copy()
            source_df["contest_id"] = source_df["contest"].apply(
                lambda x: int(x["contestId"])
            )
            source_df["users"] = source_df["users"].apply(lambda x: list(x.values()))
            exploded_source_df = source_df[["contest_id", "users"]].explode("users")
            users_with_lineups_df = exploded_source_df.apply(
                self._extract_user_fields, axis=1
            )
            # 2. Explode lineups (creates one row per lineup)
            lineups_exploded = users_with_lineups_df[
                ["contest_id", "user_id", "lineups"]
            ].explode("lineups")

            # 3. Extract lineup fields
            lineups_df = pd.DataFrame(
                {
                    "contest_id": lineups_exploded["contest_id"],
                    "user_id": lineups_exploded["user_id"],
                    "lineup_hash": lineups_exploded["lineups"].apply(
                        lambda x: x.get("lineupHash") if isinstance(x, dict) else None
                    ),
                    "lineup_ct": lineups_exploded["lineups"].apply(
                        lambda x: x.get("lineupCt") if isinstance(x, dict) else None
                    ),
                }
            )

            # Remove rows with missing lineup data
            lineups_df = lineups_df.dropna(subset=["lineup_hash"])

            # 4. Clean users_df (drop the lineup column and remove duplicates)
            users_df = users_with_lineups_df.drop("lineups", axis=1).drop_duplicates()

            # 5. Write to parquet using DuckDB
            self.con.execute(
                f"COPY (SELECT * FROM users_df) TO '{users_path}' (FORMAT PARQUET, COMPRESSION 'SNAPPY')"
            )
            self.con.execute(
                f"COPY (SELECT * FROM lineups_df) TO '{lineups_path}' (FORMAT PARQUET, COMPRESSION 'SNAPPY')"
            )

        logger.info(f"Users and lineups data for {self.date} {self.sport} saved to DDS")

    def process_lineups(self):
        """Process lineups data to DDS stage for a given date and sport.

        Optimized version that uses pandas for parsing and DuckDB for writing.
        Much faster than the deprecated version because it loads JSON from S3 once and
        avoids DuckDB's slow json_each() on deeply nested structures with hash keys.
        (5-9x faster by tests)
        """
        for slate_type in self.slate_types:
            staging_lineups_path = f"{self.staging_base_path}{self.sport}/lineups/{slate_type}/{self.date}/data.json.gz"

            # Check if a staging file exists before processing
            if not self._s3_file_exists(staging_lineups_path):
                logger.warning(
                    f"Skipping {slate_type} for lineups - staging file not found: {staging_lineups_path}"
                )
                continue

            lineups_path = f"{self.dds_base_path}{self.sport}/lineups/{slate_type}/{self.date}/data.parquet"

            # 1. Load JSON from S3 at once
            with self.s3.open(staging_lineups_path.replace("s3://", ""), "rb") as f:
                compressed_data = f.read()
            with gzip.open(BytesIO(compressed_data), "rt") as gz:
                source_df = pd.read_json(gz, lines=True)

            # 2. Extract slate_id and lineups
            source_df = source_df[["slate_id", "lineups"]].copy()
            source_df.rename(columns={"slate_id": "contest_id"}, inplace=True)

            # 3. Get all unique position keys from the first lineup to determine dynamic columns
            first_lineup = source_df["lineups"].iloc[0]
            lineup_players = first_lineup.get("lineupPlayers", {})
            position_keys = sorted(lineup_players.keys())

            # 4. Extract lineup fields using pandas
            lineups_df = source_df.apply(
                lambda row: self._extract_lineup_fields(row, position_keys), axis=1
            )

            # 5. Write to parquet using DuckDB
            self.con.execute(
                f"COPY (SELECT * FROM lineups_df) TO '{lineups_path}' (FORMAT PARQUET, COMPRESSION 'SNAPPY')"
            )

        logger.info(f"Lineups data for {self.date} {self.sport} saved to DDS")

    @staticmethod
    def _extract_lineup_fields(row, position_keys):
        """Extract lineup fields from a row with nested lineup data"""
        lineup = row["lineups"]

        # Base fields
        result = {
            "contest_id": row["contest_id"],
            "lineup_hash": lineup.get("lineupHash"),
            "lineup_ct": lineup.get("lineupCt"),
            "lineup_user_ct": lineup.get("lineupUserCt"),
            "points": lineup.get("points"),
            "total_salary": lineup.get("totalSalary"),
            "total_own": lineup.get("totalOwn"),
            "min_own": lineup.get("minOwn"),
            "max_own": lineup.get("maxOwn"),
            "avg_own": lineup.get("avgOwn"),
            "lineup_rank": lineup.get("lineupRank"),
            "is_cashing": lineup.get("isCashing"),
            "favorite_ct": lineup.get("favoriteCt"),
            "underdog_ct": lineup.get("underdogCt"),
            "home_ct": lineup.get("homeCt"),
            "visitor_ct": lineup.get("visitorCt"),
            "payout": lineup.get("payout"),
            "lineup_percentile": lineup.get("lineupPercentile"),
            "correlated_players": lineup.get("correlatedPlayers"),
        }

        # Dynamic position columns
        lineup_players = lineup.get("lineupPlayers", {})
        for pos_key in position_keys:
            result[f"pos_{pos_key.lower()}"] = lineup_players.get(pos_key)

        # Complex objects - convert to JSON strings for consistent storage
        result["team_stacks"] = (
            json.dumps(lineup.get("teamStacks")) if lineup.get("teamStacks") else None
        )
        result["game_stacks"] = (
            json.dumps(lineup.get("gameStacks")) if lineup.get("gameStacks") else None
        )
        result["lineup_trends"] = (
            json.dumps(lineup.get("lineupTrends"))
            if lineup.get("lineupTrends")
            else None
        )
        result["entry_name_list"] = (
            json.dumps(lineup.get("entryNameList"))
            if lineup.get("entryNameList")
            else None
        )

        return pd.Series(result)

    @staticmethod
    def _extract_user_fields(row):
        user = row["users"]
        return pd.Series(
            {
                "contest_id": row["contest_id"],
                "user_id": user["userId"],
                "total_players": user.get("totalPlayers"),
                "total_rosters": user.get("totalRosters"),
                "unique_rosters": user.get("uniqueRosters"),
                "max_exposure": user.get("maxExposure"),
                "lineups_cashing": user.get("lineupsCashing"),
                "lineups_in_percentile_1": user.get("lineupsInPercentile1"),
                "lineups_in_percentile_2": user.get("lineupsInPercentile2"),
                "lineups_in_percentile_5": user.get("lineupsInPercentile5"),
                "lineups_in_percentile_10": user.get("lineupsInPercentile10"),
                "lineups_in_percentile_20": user.get("lineupsInPercentile20"),
                "lineups_in_percentile_50": user.get("lineupsInPercentile50"),
                "total_entry_cost": user.get("totalEntryCost"),
                "total_winning": user.get("totalWinning"),
                "roi": user.get("roi"),
                "lineups": list(user["lineups"].values()),  # Keep lineups
            }
        )


if __name__ == "__main__":
    start = time.perf_counter()
    with DdsProcessor(sport="NFL", date="2025-09-07") as processor:
        # processor.process_players()
        # processor.process_users_lineups()
        processor.process_lineups()
    print(f"Script completed in {time.perf_counter() - start:.2f} seconds")
