import logging
import time

from scripts.marts.top_lineups.base_top_lineups_mart import BaseTopLineupsMart
from scripts.marts.top_lineups import configs

logger = logging.getLogger("NFLClassicTopLineupsMart")


class NFLClassicTopLineupsMart(BaseTopLineupsMart):
    """
    NFL DraftKings Classic top lineups mart with schema evolution support.

    This subclass handles:
    - Schema evolution: Different dates may have different position columns
    - NULL filtering: Drops incomplete lineups where positions are NULL
    - union_by_name: Automatically handles missing columns across files

    The NFL Classic format had position schema changes over time, so we need
    special handling to union data from different seasons/dates.
    """

    def _build_query(self) -> str:
        """
        Build query with union_by_name and NULL filtering for NFL Classic.

        Customizations:
        1. Adds union_by_name=true to read_parquet() calls
        2. Filters out lineups with NULL player_ids (incomplete lineups)
        """
        # Generate dynamic SQL components
        pos_names, pos_columns = self._generate_unnest_sql()
        select_clauses = self._generate_select_clauses()
        roster_filter = self._generate_roster_position_filter()
        lineup_columns = self._generate_lineup_columns_sql()

        # Generate NULL filters for each position
        # This ensures we only keep complete lineups
        null_filters = " AND ".join([f"pos_{p} IS NOT NULL" for p in self.config.positions])

        query = f"""
            COPY (
                WITH
                    lineups AS (
                        SELECT
                            replace(list_element(string_split(filename, '/'), -2), '-', '')::integer as date_id,
                            contest_id,
                            lineup_rank,
                            total_own,
                            total_salary,
                            {lineup_columns}
                        FROM read_parquet('{self.lineups_path}', filename=true, union_by_name=true)
                        WHERE lineup_percentile <= {self.config.percentile_threshold}
                            AND {null_filters}
                    ),
                    -- Unpivot lineup positions to enable single join
                    lineup_positions AS (
                        SELECT
                            date_id,
                            contest_id,
                            lineup_rank,
                            total_own,
                            total_salary,
                            unnest(['{pos_names}']) as pos_slot,
                            unnest([{pos_columns}]) as player_id
                        FROM lineups
                    ),
                    -- Pre-filter to only load players that appear in top lineups
                    needed_players AS (
                        SELECT DISTINCT date_id, player_id
                        FROM lineup_positions
                    ),
                    -- Load only the players we need, with team and salary info
                    players AS (
                        SELECT
                            replace(list_element(string_split(filename, '/'), -2), '-', '')::integer as date_id,
                            p.player_id,
                            p.position,
                            p.current_team,
                            p.salary,
                            p.roster_position
                        FROM read_parquet('{self.players_path}', filename=true, union_by_name=true) p
                        WHERE (replace(list_element(string_split(filename, '/'), -2), '-', '')::integer, p.player_id) IN
                              (SELECT date_id, player_id FROM needed_players)
                    )
                    -- Single join with roster position matching and pivot back to wide format
                    SELECT
                        contest_id,
                        lineup_rank,
                        max(total_own) as total_own,
                        max(total_salary) as total_salary,
                        {select_clauses}
                    FROM lineup_positions lp
                        JOIN players p ON lp.player_id = p.player_id
                            AND lp.date_id = p.date_id{roster_filter}
                    GROUP BY contest_id, lineup_rank
            ) TO '{self.output_full_path}'
            (FORMAT PARQUET, COMPRESSION 'SNAPPY')
            """
        return query


if __name__ == "__main__":
    start = time.perf_counter()
    with NFLClassicTopLineupsMart(config=configs.DK_CLASSIC_NFL_CONFIG) as processor:
        processor.process()
    print(f"Total time: {time.perf_counter() - start:.2f}s")
