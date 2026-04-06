"""Web crawler client for collecting editorial review article HTML."""

from __future__ import annotations

from dataclasses import dataclass, field
import logging
import re
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
        self.links: list[tuple[str, str]] = []
        self._active_href: str | None = None
        self._active_chunks: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag != "a":
            return
        self._active_href = None
        self._active_chunks = []
        for key, value in attrs:
            if key == "href" and value:
                self._active_href = value
            if key == "title" and value:
                self._active_chunks.append(value)

    def handle_data(self, data: str) -> None:
        if self._active_href and data:
            cleaned = data.strip()
            if cleaned:
                self._active_chunks.append(cleaned)

    def handle_endtag(self, tag: str) -> None:
        if tag != "a" or not self._active_href:
            return
        anchor_text = " ".join(self._active_chunks).strip()
        self.links.append((self._active_href, anchor_text))
        self._active_href = None
        self._active_chunks = []


_TAG_RE = re.compile(r"<[^>]+>")


@dataclass
class _ScoredCandidate:
    url: str
    score: int


@dataclass
class WebReviewsClient:
    """Minimal crawler that respects robots and fetches editorial article pages."""

    user_agent: str = DEFAULT_USER_AGENT
    headers: dict[str, str] | None = None
    timeout: int = DEFAULT_TIMEOUT_SECONDS
    request_delay_seconds: float = DEFAULT_RATE_LIMIT_SECONDS
    _robots_cache: dict[str, urllib.robotparser.RobotFileParser] = field(default_factory=dict)
    _last_request_ts: float | None = None

    @staticmethod
    def _normalize_keywords(keywords: Iterable[str] | None) -> list[str]:
        if not keywords:
            return []
        normalized: list[str] = []
        for keyword in keywords:
            cleaned = str(keyword).strip().casefold()
            if cleaned:
                normalized.append(cleaned)
        return normalized

    @staticmethod
    def _keyword_hits(text: str, keywords: list[str]) -> int:
        lowered = text.casefold()
        return sum(1 for keyword in keywords if keyword in lowered)

    def _score_candidate_url(
        self,
        *,
        url: str,
        anchor_text: str,
        page_body_text: str,
        keywords: list[str],
    ) -> int:
        if not keywords:
            return 0
        title_hits = self._keyword_hits(anchor_text, keywords)
        body_hits = self._keyword_hits(page_body_text, keywords)
        slug_hits = self._keyword_hits(urlparse(url).path.replace("-", " "), keywords)
        # Title relevance should dominate; body context is a medium signal; URL slug is weak.
        return (title_hits * 6) + (body_hits * 3) + slug_hits

    @staticmethod
    def _strip_html(html: str) -> str:
        return _TAG_RE.sub(" ", html)

    @staticmethod
    def _order_with_breadth(candidates: list[_ScoredCandidate]) -> list[str]:
        ranked = [candidate for candidate in candidates if candidate.score > 0]
        broad = [candidate for candidate in candidates if candidate.score <= 0]
        ranked.sort(key=lambda candidate: candidate.score, reverse=True)

        ordered: list[str] = []
        ranked_index = 0
        broad_index = 0

        # Keep keyword matches first while still interleaving broad crawl coverage.
        while ranked_index < len(ranked):
            for _ in range(3):
                if ranked_index >= len(ranked):
                    break
                ordered.append(ranked[ranked_index].url)
                ranked_index += 1
            if broad_index < len(broad):
                ordered.append(broad[broad_index].url)
                broad_index += 1

        while broad_index < len(broad):
            ordered.append(broad[broad_index].url)
            broad_index += 1

        return ordered

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

    def _extract_links(self, base_url: str, html: str) -> list[tuple[str, str]]:
        parser = _LinkExtractor()
        parser.feed(html)

        links: list[tuple[str, str]] = []
        for raw_link, anchor_text in parser.links:
            absolute = urljoin(base_url, raw_link)
            parsed = urlparse(absolute)
            if parsed.scheme not in {"http", "https"}:
                continue
            cleaned = parsed._replace(fragment="").geturl()
            links.append((cleaned, anchor_text))
        return links

    def discover_candidate_article_urls(
        self,
        *,
        homepage_url: str,
        category_urls: Iterable[str] | None = None,
        keywords: Iterable[str] | None = None,
        prioritize_keywords: bool = False,
    ) -> list[str]:
        """Crawl homepage/category pages and discover likely article URLs."""

        allowed_pages = [homepage_url]
        if category_urls:
            allowed_pages.extend(category_urls)

        discovered: list[_ScoredCandidate] = []
        seen: set[str] = set()
        normalized_keywords = self._normalize_keywords(keywords)

        with requests.Session() as session:
            session.headers.update(self._build_headers())

            for page_url in allowed_pages:
                response = self._safe_get(session, page_url)
                if response is None:
                    continue
                page_body_text = self._strip_html(response.text)

                for link, anchor_text in self._extract_links(page_url, response.text):
                    if link in seen:
                        continue
                    if not self._is_same_domain(homepage_url, link):
                        continue
                    if not self._looks_like_editorial_article(link):
                        continue
                    if not self._is_allowed_by_robots(link):
                        logger.warning("Skipping discovered URL disallowed by robots.txt: %s", link)
                        continue

                    score = self._score_candidate_url(
                        url=link,
                        anchor_text=anchor_text,
                        page_body_text=page_body_text,
                        keywords=normalized_keywords,
                    )
                    seen.add(link)
                    discovered.append(_ScoredCandidate(url=link, score=score))
        if not prioritize_keywords:
            return [candidate.url for candidate in discovered]
        return self._order_with_breadth(discovered)

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
