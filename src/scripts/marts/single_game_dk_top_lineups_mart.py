import logging

from scripts.base_duck_db_processor import BaseDuckDBProcessor

logger = logging.getLogger("SingleGameDKTopLineupsMart")


class SingleGameDKTopLineupsMart(BaseDuckDBProcessor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.sport = kwargs.get("sport")
        if not self.sport:
            raise ValueError("Sport must be provided")
        self.base_dds_stage = f"s3://{self.bucket_name}/dds/"
        self.marts_base_path = f"s3://{self.bucket_name}/marts/"

    def process(self):
        lineups_path = f"{self.base_dds_stage}{self.sport}/lineups/dk_single_game/*/data.parquet"
        players_path = f"{self.base_dds_stage}{self.sport}/players/dk_single_game/*/data.parquet"
        mart_path = f"{self.marts_base_path}single_game_dk_top_lineups/data.parquet"

        logger.info(f"Processing single game DK top lineups mart for {self.sport}")

        self.con.execute(
            f"""
            COPY (
                WITH
                    lineups AS (
                        SELECT
                            replace(list_element(string_split(filename, '/'), -2), '-', '')::integer as date_id,
                            contest_id,
                            lineup_rank,
                            total_own,
                            total_salary,
                            pos_cpt1,
                            pos_flex1,
                            pos_flex2,
                            pos_flex3,
                            pos_flex4,
                            pos_flex5
                        FROM read_parquet('{lineups_path}', filename=true)
                        WHERE lineup_percentile <= 0.01
                    ),
                    -- Unpivot lineup positions to enable single join
                    lineup_positions AS (
                        SELECT
                            date_id,
                            contest_id,
                            lineup_rank,
                            total_own,
                            total_salary,
                            unnest(['cpt1', 'flex1', 'flex2', 'flex3', 'flex4', 'flex5']) as pos_slot,
                            unnest([pos_cpt1, pos_flex1, pos_flex2, pos_flex3, pos_flex4, pos_flex5]) as player_id
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
                        FROM read_parquet('{players_path}', filename=true) p
                        WHERE (replace(list_element(string_split(filename, '/'), -2), '-', '')::integer, p.player_id) IN
                              (SELECT date_id, player_id FROM needed_players)
                    )
                    -- Single join with roster position matching and pivot back to wide format
                    SELECT
                        contest_id,
                        lineup_rank,
                        max(total_own) as total_own,
                        max(total_salary) as total_salary,
                        max(CASE WHEN pos_slot = 'cpt1' THEN position END) as pos_cpt1_real,
                        max(CASE WHEN pos_slot = 'cpt1' THEN current_team END) as pos_cpt1_team,
                        max(CASE WHEN pos_slot = 'cpt1' THEN salary END) as pos_cpt1_salary,
                        max(CASE WHEN pos_slot = 'flex1' THEN position END) as pos_flex1_real,
                        max(CASE WHEN pos_slot = 'flex1' THEN current_team END) as pos_flex1_team,
                        max(CASE WHEN pos_slot = 'flex1' THEN salary END) as pos_flex1_salary,
                        max(CASE WHEN pos_slot = 'flex2' THEN position END) as pos_flex2_real,
                        max(CASE WHEN pos_slot = 'flex2' THEN current_team END) as pos_flex2_team,
                        max(CASE WHEN pos_slot = 'flex2' THEN salary END) as pos_flex2_salary,
                        max(CASE WHEN pos_slot = 'flex3' THEN position END) as pos_flex3_real,
                        max(CASE WHEN pos_slot = 'flex3' THEN current_team END) as pos_flex3_team,
                        max(CASE WHEN pos_slot = 'flex3' THEN salary END) as pos_flex3_salary,
                        max(CASE WHEN pos_slot = 'flex4' THEN position END) as pos_flex4_real,
                        max(CASE WHEN pos_slot = 'flex4' THEN current_team END) as pos_flex4_team,
                        max(CASE WHEN pos_slot = 'flex4' THEN salary END) as pos_flex4_salary,
                        max(CASE WHEN pos_slot = 'flex5' THEN position END) as pos_flex5_real,
                        max(CASE WHEN pos_slot = 'flex5' THEN current_team END) as pos_flex5_team,
                        max(CASE WHEN pos_slot = 'flex5' THEN salary END) as pos_flex5_salary
                    FROM lineup_positions lp
                        JOIN players p ON lp.player_id = p.player_id
                            AND lp.date_id = p.date_id
                            AND ((lp.pos_slot = 'cpt1' AND p.roster_position = 'CPT')
                                OR (lp.pos_slot != 'cpt1' AND p.roster_position != 'CPT'))
                    GROUP BY contest_id, lineup_rank
            ) TO '{mart_path}'
            (FORMAT PARQUET, COMPRESSION 'SNAPPY')
            """
        )

        logger.info(f"✓ Single game DK top lineups mart saved to {mart_path}")


if __name__ == "__main__":
    with SingleGameDKTopLineupsMart(sport="NFL") as processor:
        processor.process()
