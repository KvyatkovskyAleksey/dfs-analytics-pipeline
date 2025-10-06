from collections import defaultdict
from datetime import datetime
import requests


class RotogrindersScraper:
    """Scraper for scrape data for analytics from rotogrinders.com (ResultsDB)"""

    sports_mapping = {"NFL": 1}
    partition_naming_by_sources = {4: "dk_classic", 8: "dk_single_game"}
    headers = {
        "accept": "*/*",
        "accept-language": "ru,en;q=0.9",
        "cache-control": "no-cache",
        "flsessionid": "undefined",
        "origin": "https://terminal.fantasylabs.com",
        "pragma": "no-cache",
        "priority": "u=1, i",
        "referer": "https://terminal.fantasylabs.com/",
        "sec-ch-ua": '"Not)A;Brand";v="8", "Chromium";v="138", "YaBrowser";v="25.8", "Yowser";v="2.5"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Linux"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "user-agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 YaBrowser/25.8.0.0 Safari/537.36",
    }

    def __init__(self, date: str, sport: str):
        if sport not in self.sports_mapping:
            raise ValueError(f"Unsupported sport: {sport}")
        try:
            datetime.strptime(date, "%Y-%m-%d")
        except ValueError:
            raise ValueError(f"Incorrect date format: {date}")
        self.date = date
        self.sport = sport
        self.sport_id = self.sports_mapping[self.sport]
        # here aggregate data for bulk insert to s3 storage
        self.draft_groups_data = None
        self.contests_data = defaultdict(list)
        self.events_data = defaultdict(list)
        self.contests_analyze_data = defaultdict(list)
        self.lineups_by_slates = defaultdict(dict)

    def scrape(self):
        draft_groups = self._scrape_draft_groups()
        self.draft_groups_data = draft_groups
        for slate_group in draft_groups["contest-sources"]:
            partition_name = self.partition_naming_by_sources[slate_group["id"]]
            for slate in slate_group["draft_groups"]:
                slate_id = slate["id"]
                slate_contests = self._scrape_slate_contests(slate_id)
                self.contests_data[partition_name].append(slate_contests)
                slate_events = self._get_events(slate_id)
                self.events_data[partition_name].append(slate_events)
                for contest in slate_contests["live_contests"]:
                    contests_analyze_data = self._scrape_contests_analyze_data(
                        contest["contest_id"]
                    )
                    self.contests_analyze_data[partition_name].append(
                        contests_analyze_data
                    )
                    contest_lineups = self._scrape_contest_lineups(
                        contest["contest_id"]
                    )
                    self.lineups_by_slates[partition_name][contest["contest_id"]] = (
                        contest_lineups["lineups"]
                    )

    def get_data(self):
        return {
            "draft_groups": self.draft_groups_data,
            "contests": self.contests_data,
            "events": self.events_data,
            "contests_analyze_data": self.contests_analyze_data,
            "lineups_by_slates": self.lineups_by_slates,
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

    def _make_request(self, url: str) -> dict:
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        return response.json()

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
    scraper = RotogrindersScraper(date="2025-09-29", sport="NFL")
    scraper.scrape()
    a = 1
