import io
from datetime import datetime
from typing import cast, TypedDict

import pandas as pd
from scripts.spiders.base_spider import BaseSpider


class MoneyPuckData(TypedDict):
    players_data: pd.DataFrame
    game_events_data: pd.DataFrame
    date: str


class MoneyPuckScraper(BaseSpider):
    """
    Scraper for moneypuck.com, here we get info about NHL lines for
    get data about correlations between players
    """

    MAX_REQUESTS_RETRIES = 10

    def __init__(self, date: str):
        super().__init__()
        try:
            datetime.strptime(date, "%Y-%m-%d")
        except ValueError:
            raise ValueError(f"Incorrect date format: {date}")
        self.date = date
        self.players_data_by_game_ids: dict[int, pd.DataFrame] = {}
        self.game_data_by_game_ids: dict[int, pd.DataFrame] = {}

    @property
    def data_exists(self) -> bool:
        """Check if any games data exists for this date"""
        return bool(self.players_data_by_game_ids)

    def scrape(self):
        """Scrape data from moneypuck.com for a specific date"""
        season, game_ids = self._get_games_ids()
        self.get_moneypuck_data(season, game_ids)

    def get_moneypuck_data(self, season: int, game_ids: list[int]) -> dict:
        """Get moneypuck data for a given date"""
        for game_id in game_ids:
            players_game_data_df = self._get_moneypuck_players_data(season, game_id)
            teams_game_data_df = self._get_moneypuck_game_events_data(season, game_id)

            # Only store if data was successfully retrieved
            if players_game_data_df is not None:
                self.players_data_by_game_ids[game_id] = players_game_data_df
            if teams_game_data_df is not None:
                self.game_data_by_game_ids[game_id] = teams_game_data_df

            # Skip this game if both data sources failed
            if players_game_data_df is None and teams_game_data_df is None:
                self.logger.warning(
                    f"Skipping game {game_id} - no data available from MoneyPuck"
                )

        return self.players_data_by_game_ids

    def _get_moneypuck_game_events_data(
        self, season: int, game_id: int
    ) -> pd.DataFrame | None:
        """Scrape game events data for a given game id"""
        url = f"https://moneypuck.com/moneypuck/gameData/{season}/{game_id}.csv"
        response_text = self._make_request(url, parse_json=False, timeout=15)

        # Check if response is valid before parsing
        if not response_text:
            self.logger.warning(
                f"No game events data available for game {game_id} (season {season})"
            )
            return None

        csv_content = io.StringIO(response_text)
        return pd.read_csv(csv_content)

    def _get_moneypuck_players_data(
        self, season: int, game_id: int
    ) -> pd.DataFrame | None:
        """Get moneypuck data for a given game id"""
        url = f"https://moneypuck.com/moneypuck/playerData/games/{season}/{game_id}.csv"
        response_text = self._make_request(url, parse_json=False, timeout=10)

        # Check if response is valid before parsing
        if not response_text:
            self.logger.warning(
                f"No player data available for game {game_id} (season {season})"
            )
            return None

        csv_content = io.StringIO(response_text)
        return pd.read_csv(csv_content)

    def _get_games_ids(self) -> tuple[int, list[int]]:
        """Get game ids for a given date, we get it from api-web.nhle.com which is official NHL api"""
        api_url = "https://api-web.nhle.com/v1/schedule/" + self.date
        response = self._make_request(api_url)
        if not response:
            self.logger(f"No games found for date: {self.date}")
            return 0, []
        try:
            date_games = next(
                data for data in response["gameWeek"] if data["date"] == self.date
            )
            self.logger.info(
                f"Found {len(date_games['games'])} games for date: {self.date}"
            )
        except StopIteration:
            self.logger.info(f"No games found for date: {self.date}")
            return 0, []

        # Check if games list is empty
        if not date_games.get("games"):
            self.logger.info(f"No games in schedule for date: {self.date}")
            return 0, []

        return cast(int, date_games["games"][0]["season"]), [
            cast(int, game["id"]) for game in date_games["games"]
        ]

    def get_data(self) -> MoneyPuckData:
        """Return date and data for a given date as a tuple"""
        # Concatenate data, handling empty dictionaries
        players_data = (
            pd.concat(self.players_data_by_game_ids.values())
            if self.players_data_by_game_ids
            else pd.DataFrame()
        )
        game_events_data = (
            pd.concat(self.game_data_by_game_ids.values())
            if self.game_data_by_game_ids
            else pd.DataFrame()
        )

        return MoneyPuckData(
            date=self.date,
            players_data=players_data,
            game_events_data=game_events_data,
        )


if __name__ == "__main__":
    scraper = MoneyPuckScraper(date="2025-10-07")
    scraper.scrape()
    date, data = scraper.get_data()
