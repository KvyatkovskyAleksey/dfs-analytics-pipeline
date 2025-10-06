import json
import random
from logging import getLogger

import requests


class ProxyManager:
    def __init__(self, url, use_cached=True, cached_path="/tmp/proxies.json"):
        self.url = url.strip()
        self.cached_path = cached_path
        self.proxies: list[str] = []
        self.logger = getLogger("ProxyManager")
        if use_cached:
            self._load_cached_proxies()
        else:
            self.proxies = self._fetch_proxies()
            self._cache_proxies()

    def get_random_proxy(self):
        return random.choice(self.proxies)

    def __iter__(self):
        return self.proxies.__iter__()

    def __len__(self):
        return len(self.proxies)

    def __getitem__(self, index):
        return self.proxies[index]

    def _cache_proxies(self) -> None:
        with open(self.cached_path, "w") as f:
            json.dump(self.proxies, f)
            self.logger.info(f"Saved {len(self.proxies)} proxies to {self.cached_path}")

    def _load_cached_proxies(self) -> None:
        try:
            with open(self.cached_path, "r") as f:
                self.proxies = json.load(f)
        except FileNotFoundError:
            self.proxies = self._fetch_proxies()
            self._cache_proxies()

    def _fetch_proxies(self):
        response = requests.get(self.url)
        data = [t.strip() for t in response.text.split("\n") if t.strip()]
        output = []
        for row in data:
            host, port, username, password = row.split(":")
            proxy = f"http://{username}:{password}@{host}:{port}"
            output.append(proxy)
        return output


if __name__ == "__main__":
    import os

    proxy_url = os.environ.get("PROXY_URL")
    if not proxy_url:
        raise ValueError("PROXY_URL environment variable is not set")
    proxy_manager = ProxyManager(proxy_url)
