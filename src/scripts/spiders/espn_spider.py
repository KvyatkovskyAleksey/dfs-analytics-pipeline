import asyncio
import logging
import os
import random
from datetime import datetime

import httpx
from schemas import Sport
from scripts.utils.proxy import ProxyManager


class BaseEspnSpider:
    """Base ESPN Spider for fetching odds data."""

    MAX_CONCURRENT_REQUESTS = 16
    MAX_REQUESTS_RETRIES = 3
    logger = logging.getLogger("BaseEspnSpider")
    headers = {
        "accept": "*/*",
        "accept-language": "ru,en;q=0.9",
        "cache-control": "no-cache",
        "pragma": "no-cache",
        "priority": "u=1, i",
        "sec-ch-ua": '"Not)A;Brand";v="8", "Chromium";v="138", "YaBrowser";v="25.8", "Yowser";v="2.5"',
        "sec-ch-ua-platform": '"Linux"',
        "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 YaBrowser/25.8.0.0 Safari/537.36",
    }

    def __init__(self, sport: Sport):
        self.sport = sport
        self.requests_semaphore = asyncio.Semaphore(self.MAX_CONCURRENT_REQUESTS)

        proxy_url = os.getenv("PROXY_URL")
        if proxy_url:
            self.proxy_manager = ProxyManager(proxy_url)
        else:
            self.proxy_manager = None

        self.raw_response = None
        self.date = None

    async def _make_request(
        self, url: str, parse_json: bool = True, timeout: int = 10
    ) -> dict | str | None:
        """Make an async request with retries, semaphore, and proxies if set"""
        async with self.requests_semaphore:
            self.logger.info(f"Making request to {url}")
            attempts = 0
            request_data = None
            while attempts < self.MAX_REQUESTS_RETRIES:
                try:
                    attempts += 1
                    proxy_url = None
                    if self.proxy_manager:
                        proxy_url = self.proxy_manager.get_random_proxy()

                    if not proxy_url:
                        await asyncio.sleep(random.uniform(3, 5))

                    client_kwargs = {"timeout": timeout}
                    if proxy_url:
                        client_kwargs["proxy"] = proxy_url

                    async with httpx.AsyncClient(**client_kwargs) as client:
                        response = await client.get(url, headers=self.headers)
                        if response.status_code == 200:
                            if parse_json:
                                request_data = response.json()
                            else:
                                request_data = response.text
                            break
                except httpx.ProxyError:
                    self.logger.info(f"ProxyError on {url}")
                    continue
                except (httpx.ConnectError, httpx.TimeoutException):
                    self.logger.info(f"ConnectionError or Timeout on {url}")
                    continue
            return request_data

    def _convert_date_format(self, date: str) -> str:
        """Convert date from YYYY-mm-dd to YYYYMMDD format for ESPN API"""
        return date.replace("-", "")

    def _get_sport_path(self) -> dict[str, str]:
        """Get the API path components for the sport"""
        sport_paths = {
            "NFL": {"sport": "football", "league": "nfl"},
            "NBA": {"sport": "basketball", "league": "nba"},
            "NHL": {"sport": "hockey", "league": "nhl"},
            "MLB": {"sport": "baseball", "league": "mlb"},
        }
        return sport_paths.get(self.sport, {})

    async def fetch_game_odds(self, game_id: str) -> dict | None:
        """
        Fetch detailed odds for a specific game from the odds endpoint

        Args:
            game_id: ESPN event/game ID

        Returns:
            Complete odds API response or None if request fails
        """
        sport_path = self._get_sport_path()
        if not sport_path:
            self.logger.error(f"Invalid sport: {self.sport}")
            return None

        base_url = "https://sports.core.api.espn.com/v2/sports"
        url = (
            f"{base_url}/{sport_path['sport']}/leagues/{sport_path['league']}/"
            f"events/{game_id}/competitions/{game_id}/odds"
        )

        odds_data = await self._make_request(url, parse_json=True)

        if odds_data:
            self.logger.info(f"Fetched odds for game {game_id}")
        else:
            self.logger.warning(f"No odds data for game {game_id}")

        return odds_data

    async def fetch_all_odds_for_date(self, date: str) -> dict:
        """
        Main method: Fetch scoreboard and detailed odds for all games on a date

        This implements the v1 data structure with two-step fetch:
        1. Get a scoreboard (all games to date)
        2. Get detailed odds for each game

        Args:
            date: Date in YYYY-mm-dd format

        Returns:
            dict with version, scoreboard, and odds data
        """
        self.date = date

        self.logger.info(f"Fetching scoreboard for {self.sport} on {date}")
        date_formatted = self._convert_date_format(date)
        sport_path = self._get_sport_path()

        if not sport_path:
            self.logger.error(f"Invalid sport: {self.sport}")
            return {}

        base_url = "https://site.api.espn.com/apis/site/v2/sports"
        scoreboard_url = (
            f"{base_url}/{sport_path['sport']}/{sport_path['league']}/"
            f"scoreboard?dates={date_formatted}"
        )

        scoreboard_data = await self._make_request(scoreboard_url, parse_json=True)

        if not scoreboard_data:
            self.logger.warning(f"No scoreboard data for {date}")
            return {}

        events = scoreboard_data.get("events", [])
        game_ids = [event.get("id") for event in events if event.get("id")]

        self.logger.info(f"Found {len(game_ids)} games, fetching odds...")

        odds_tasks = [self.fetch_game_odds(game_id) for game_id in game_ids]
        odds_results = await asyncio.gather(*odds_tasks)

        odds_responses = []
        for game_id, odds_data in zip(game_ids, odds_results):
            event = next((e for e in events if e.get("id") == game_id), {})
            game_name = event.get("name", "Unknown")

            odds_responses.append(
                {"game_id": game_id, "game_name": game_name, "odds_data": odds_data}
            )

        response = {
            "version": "v1",
            "date": date,
            "sport": self.sport,
            "fetch_timestamp": datetime.utcnow().isoformat() + "Z",
            "scoreboard_response": scoreboard_data,
            "odds_responses": odds_responses,
        }

        self.raw_response = response

        self.logger.info(
            f"Fetch complete: {len(game_ids)} games, "
            f"{sum(1 for o in odds_responses if o['odds_data'])} with odds"
        )

        return response

    @property
    def data_exists(self) -> bool:
        """Check if any events exist"""
        return bool(self.raw_response)

    def get_data(self) -> dict:
        """
        Get raw data for the staging processor (v1 structure).

        Returns:
            Dictionary with version, scoreboard, and odds data
        """
        return self.raw_response or {}
