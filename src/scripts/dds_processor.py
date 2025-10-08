import logging

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

    def process_players(self) -> None:
        """Save players data to DDS stage for a given date and sport"""
        logger.info(f"Processing players for {self.date} {self.sport}")
        for slate_type in self.slate_types:
            staging_path = f"{self.staging_base_path}{self.sport}/contest_analyze/{slate_type}/{self.date}/data.json.gz"
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

    def process_users_lineups(self):
        """Save users and their lineup data to the DDS stage for a given date and sport"""
        logger.info(f"Processing users and lineups for {self.date} {self.sport}")
        for slate_type in self.slate_types:
            staging_path = f"{self.staging_base_path}{self.sport}/contest_analyze/{slate_type}/{self.date}/data.json.gz"
            users_path = f"{self.staging_base_path}{self.sport}/users/{slate_type}/{self.date}/data.parquet"
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

    def process_lineups(self):
        """Process lineups data to DDS stage for a given date and sport"""
        for slate_type in self.slate_types:
            staging_lineups_path = f"{self.staging_base_path}{self.sport}/lineups/{slate_type}/{self.date}/data.json.gz"
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


if __name__ == "__main__":
    with DdsProcessor(sport="NFL", date="2025-09-07") as processor:
        # processor.process_players()
        processor.process_users_lineups()
        # processor.process_lineups()
