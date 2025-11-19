import logging
from collections import defaultdict
from datetime import datetime
from typing import TypedDict, Literal

from schemas import Sport
from scripts.spiders.base_spider import BaseSpider

SlateType = Literal["dk_classic", "dk_single_game", "fd_anyflex"]


class StagingData(TypedDict):
    draft_groups: dict
    contests: dict[SlateType, list[dict]]
    events: dict[SlateType, list[dict]]
    contests_analyze_data: dict[SlateType, list[dict]]
    lineups_by_slates: dict[SlateType, dict[int, list[dict]]]
    date: str
    sport: Sport


class RotogrindersScraper(BaseSpider):
    """Scraper for scrape data for analytics from rotogrinders.com (ResultsDB)"""

    logger = logging.getLogger("RotogrindersScraper")

    sports_mapping: dict[Sport, int] = {"NFL": 1, "NHL": 4, "NBA": 2}

    partition_naming_by_sources: dict[int, SlateType] = {
        3: "fd_classic",
        4: "dk_classic",
        8: "dk_single_game",
        9: "fd_anyflex",
        11: "yh_classic",
    }
    headers = {
        "accept": "*/*",
        "accept-language": "ru,en;q=0.9",
        "cache-control": "no-cache",
        "origin": "https://terminal.fantasylabs.com",
        "pragma": "no-cache",
        "priority": "u=1, i",
        "referer": "https://terminal.fantasylabs.com/",
        "sec-ch-ua": '"Not)A;Brand";v="8", "Chromium";v="138", "YaBrowser";v="25.8", "Yowser";v="2.5"',
        "sec-ch-ua-platform": '"Linux"',
        "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 YaBrowser/25.8.0.0 Safari/537.36",
    }

    def __init__(self, date: str, sport: Sport):
        super().__init__()
        if sport not in self.sports_mapping:
            raise ValueError(f"Unsupported sport: {sport}")
        try:
            datetime.strptime(date, "%Y-%m-%d")
        except ValueError:
            raise ValueError(f"Incorrect date format: {date}")
        self.date = date
        self.sport: Sport = sport
        self.sport_id = self.sports_mapping[self.sport]
        # here aggregate data for bulk insert to s3 storage
        self.draft_groups_data = None
        self.contests_data = defaultdict(list)
        self.events_data = defaultdict(list)
        self.contests_analyze_data = defaultdict(list)
        self.lineups_by_slates = defaultdict(dict)

    @property
    def data_exists(self) -> bool:
        """Check if any events exist on this date"""
        return bool(self.events_data)

    def scrape(self):
        draft_groups = self._scrape_draft_groups()
        if not draft_groups:
            return
        self.draft_groups_data = draft_groups
        for slate_group in draft_groups["contest-sources"]:
            partition_name = self.partition_naming_by_sources[slate_group["id"]]
            for slate in slate_group["draft_groups"]:
                # sometimes api return data for upcoming dates, so we can skip it for don't duplicate data
                if not slate["contest_start_date"].startswith(self.date):
                    self.logger.info(
                        f"Skip slate, because it's not for scraped date ({self.date} != {slate['contest_start_date']})"
                    )
                    continue

                slate_id = slate["id"]
                slate_contests = self._scrape_slate_contests(slate_id)
                if slate_contests is None:
                    self.logger.info(
                        f"Skip slate {slate_id}, because it doesn't have contests data"
                    )
                    continue
                self.contests_data[partition_name].append(slate_contests)
                slate_events = self._get_events(slate_id)
                if slate_events is None:
                    self.logger.info(
                        f"Skip slate {slate_id}, because it doesn't have events data"
                    )
                    continue
                self.events_data[partition_name].append(slate_events)
                for contest in slate_contests["live_contests"]:
                    contests_analyze_data = self._scrape_contests_analyze_data(
                        contest["contest_id"]
                    )
                    if not contests_analyze_data:
                        self.logger.info(
                            f"Skip contest, because it doesn't have contests analyze data ({contest['contest_id']})"
                        )
                        continue
                    self.contests_analyze_data[partition_name].append(
                        contests_analyze_data
                    )
                    contest_lineups = self._scrape_contest_lineups(
                        contest["contest_id"]
                    )
                    if not contest_lineups:
                        self.logger.info(
                            f"Skip contest, because it doesn't have lineups ({contest['contest_id']})"
                        )
                        continue
                    self.lineups_by_slates[partition_name][
                        contest["contest_id"]
                    ] = contest_lineups

    def get_data(self) -> StagingData:
        return {
            "draft_groups": self.draft_groups_data,
            "contests": self.contests_data,
            "events": self.events_data,
            "contests_analyze_data": self.contests_analyze_data,
            "lineups_by_slates": self.lineups_by_slates,
            "date": self.date,
            "sport": self.sport,
        }

    def _scrape_draft_groups(self) -> dict:
        """Parse draft groups from rotogrinders.com"""
        # contains contest groups which contains data about sites and it's game types
        # ('contest-sources'-> [{},...]
        url = (
            f"https://service.fantasylabs.com/contest-sources/"
            f"?sport_id={self.sports_mapping[self.sport]}&date={self.date}"
        )
        return self._make_request(url)

    def _scrape_slate_contests(self, slate_id: int) -> dict:
        """Scrape contests from slate"""
        url = f"https://service.fantasylabs.com/live-contests/?sport={self.sport}&contest_group_id={slate_id}"
        return self._make_request(url)

    def _get_events(self, slate_id: int) -> dict:
        """Get events for slate"""
        url = f"https://service.fantasylabs.com/live/events/?sport_id={self.sport_id}&contest_group_id={slate_id}"
        return self._make_request(url)

    def _scrape_contests_analyze_data(self, contest_id: int) -> dict:
        """Scrape contests analyze data"""
        date_query_part = self.date.replace("-", "")
        url = f"https://dh5nxc6yx3kwy.cloudfront.net/contests/{self.sport.lower()}/{date_query_part}/{contest_id}/data/"
        return self._make_request(url)

    def _scrape_contest_lineups(self, contest_id: int) -> dict:
        """Scrape lineups for given contest"""
        date_query_part = self.date.replace("-", "")
        url = f"https://dh5nxc6yx3kwy.cloudfront.net/contests/{self.sport.lower()}/{date_query_part}/{contest_id}/lineups/"
        return self._make_request(url)


if __name__ == "__main__":
    scraper = RotogrindersScraper(date="2025-03-05", sport="NHL")
    scraper.scrape()
    data = scraper.get_data()
