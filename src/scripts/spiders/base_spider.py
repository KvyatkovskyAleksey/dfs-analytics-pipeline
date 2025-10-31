import logging
import os
import random
import time

import requests
from scripts.utils.proxy import ProxyManager


class BaseSpider:
    """Base Spider class."""

    MAX_REQUESTS_RETRIES = 3
    logger = logging.getLogger("BaseSpider")
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

    def __init__(self):
        # we can use a proxy manager if set env value for PROXY_URL, it's recommended
        #  because the site can sometimes block requests
        proxy_url = os.getenv("PROXY_URL")
        if proxy_url:
            self.proxy_manager = ProxyManager(proxy_url)
        else:
            self.proxy_manager = None

    def _make_request(self, url: str, parse_json: bool = True) -> dict | str | None:
        """Make a request with retries and proxies if set"""
        self.logger.info(f"Making request to {url}")
        attempts = 0
        request_data = None
        while attempts < self.MAX_REQUESTS_RETRIES:
            try:
                attempts += 1
                proxies = None
                if self.proxy_manager:
                    proxy = self.proxy_manager.get_random_proxy()
                    proxies = {"http": proxy, "https": proxy}
                if not proxies:
                    # to prevent banning, add delay if no proxies set
                    time.sleep(random.uniform(3, 5))
                response = requests.get(
                    url, headers=self.headers, proxies=proxies, timeout=120
                )
                if response.status_code == 200:
                    if parse_json:
                        request_data = response.json()
                    else:
                        request_data = response.text
                    break
            except requests.exceptions.ProxyError:
                self.logger.info(f"ProxyError on {url}")
                continue
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout):
                self.logger.info(f"ConnectionError on {url}")
                continue
        return request_data
