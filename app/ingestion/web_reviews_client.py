"""Web crawler client for collecting editorial review article HTML."""

from __future__ import annotations

from dataclasses import dataclass, field
import logging
import time
from html.parser import HTMLParser
from typing import Iterable
from urllib.parse import urljoin, urlparse
import urllib.robotparser

import requests

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT_SECONDS = 12
DEFAULT_RATE_LIMIT_SECONDS = 1.5
DEFAULT_USER_AGENT = "reviewAnalyzer/0.1 (editorial-web-crawler)"
BLOCKED_STATUS_CODES = {403, 429}
ANTI_BOT_MARKERS = (
    "captcha",
    "access denied",
    "verify you are human",
    "cloudflare",
    "bot detection",
)


class _LinkExtractor(HTMLParser):
    """Extract href values from anchor tags."""

    def __init__(self) -> None:
        super().__init__()
        self.links: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag != "a":
            return
        for key, value in attrs:
            if key == "href" and value:
                self.links.append(value)


@dataclass
class WebReviewsClient:
    """Minimal crawler that respects robots and fetches editorial article pages."""

    user_agent: str = DEFAULT_USER_AGENT
    headers: dict[str, str] | None = None
    timeout: int = DEFAULT_TIMEOUT_SECONDS
    request_delay_seconds: float = DEFAULT_RATE_LIMIT_SECONDS
    _robots_cache: dict[str, urllib.robotparser.RobotFileParser] = field(default_factory=dict)
    _last_request_ts: float | None = None

    def _build_headers(self) -> dict[str, str]:
        merged = {"User-Agent": self.user_agent}
        if self.headers:
            merged.update(self.headers)
        return merged

    def _rate_limit(self) -> None:
        if self.request_delay_seconds <= 0:
            return
        if self._last_request_ts is None:
            return
        elapsed = time.monotonic() - self._last_request_ts
        wait_seconds = self.request_delay_seconds - elapsed
        if wait_seconds > 0:
            time.sleep(wait_seconds)

    def _is_blocked_response(self, response: requests.Response) -> bool:
        if response.status_code in BLOCKED_STATUS_CODES:
            return True
        lowered = (response.text or "").lower()
        return any(marker in lowered for marker in ANTI_BOT_MARKERS)

    def _robots_parser_for(self, site_url: str) -> urllib.robotparser.RobotFileParser | None:
        parsed = urlparse(site_url)
        if not parsed.scheme or not parsed.netloc:
            return None

        origin = f"{parsed.scheme}://{parsed.netloc}"
        if origin in self._robots_cache:
            return self._robots_cache[origin]

        parser = urllib.robotparser.RobotFileParser()
        parser.set_url(f"{origin}/robots.txt")
        try:
            parser.read()
        except Exception as exc:  # noqa: BLE001
            logger.warning("Could not read robots.txt for %s: %s", origin, exc)
            return None

        self._robots_cache[origin] = parser
        return parser

    def _is_allowed_by_robots(self, url: str) -> bool:
        parser = self._robots_parser_for(url)
        if parser is None:
            return True
        try:
            return bool(parser.can_fetch(self.user_agent, url))
        except Exception as exc:  # noqa: BLE001
            logger.warning("robots.txt check failed for %s: %s", url, exc)
            return True

    def _is_same_domain(self, origin_url: str, candidate_url: str) -> bool:
        return urlparse(origin_url).netloc == urlparse(candidate_url).netloc

    def _looks_like_editorial_article(self, url: str) -> bool:
        parsed = urlparse(url)
        path = parsed.path.lower().strip("/")
        if not path:
            return False
        segments = [segment for segment in path.split("/") if segment]
        if len(segments) < 2:
            return False
        article_markers = {"review", "reviews", "article", "news", "features"}
        if any(marker in path for marker in article_markers):
            return True
        # Common long-form URL shape (contains date folder or long slug)
        if any(seg.isdigit() and len(seg) == 4 for seg in segments):
            return True
        return len(segments[-1].split("-")) >= 4

    def _safe_get(self, session: requests.Session, url: str) -> requests.Response | None:
        if not self._is_allowed_by_robots(url):
            logger.warning("Skipping disallowed URL by robots.txt: %s", url)
            return None

        self._rate_limit()
        try:
            response = session.get(url, timeout=self.timeout)
            self._last_request_ts = time.monotonic()
        except requests.RequestException as exc:
            logger.warning("Failed to fetch URL %s: %s", url, exc)
            return None

        if self._is_blocked_response(response):
            logger.warning("Skipping blocked URL %s (status=%s)", url, response.status_code)
            return None

        if response.status_code >= 400:
            logger.warning("Skipping URL %s due to status=%s", url, response.status_code)
            return None

        return response

    def _extract_links(self, base_url: str, html: str) -> list[str]:
        parser = _LinkExtractor()
        parser.feed(html)

        links: list[str] = []
        for raw_link in parser.links:
            absolute = urljoin(base_url, raw_link)
            parsed = urlparse(absolute)
            if parsed.scheme not in {"http", "https"}:
                continue
            cleaned = parsed._replace(fragment="").geturl()
            links.append(cleaned)
        return links

    def discover_candidate_article_urls(
        self,
        *,
        homepage_url: str,
        category_urls: Iterable[str] | None = None,
    ) -> list[str]:
        """Crawl homepage/category pages and discover likely article URLs."""

        allowed_pages = [homepage_url]
        if category_urls:
            allowed_pages.extend(category_urls)

        discovered: list[str] = []
        seen: set[str] = set()

        with requests.Session() as session:
            session.headers.update(self._build_headers())

            for page_url in allowed_pages:
                response = self._safe_get(session, page_url)
                if response is None:
                    continue

                for link in self._extract_links(page_url, response.text):
                    if link in seen:
                        continue
                    if not self._is_same_domain(homepage_url, link):
                        continue
                    if not self._looks_like_editorial_article(link):
                        continue
                    if not self._is_allowed_by_robots(link):
                        logger.warning("Skipping discovered URL disallowed by robots.txt: %s", link)
                        continue

                    seen.add(link)
                    discovered.append(link)

        return discovered

    def fetch_articles(self, article_urls: Iterable[str]) -> dict[str, str]:
        """Fetch article HTML from already discovered candidate URLs."""

        results: dict[str, str] = {}
        with requests.Session() as session:
            session.headers.update(self._build_headers())
            for url in article_urls:
                response = self._safe_get(session, url)
                if response is None:
                    continue
                results[url] = response.text
        return results
