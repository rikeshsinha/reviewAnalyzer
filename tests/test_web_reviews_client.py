from __future__ import annotations

from app.ingestion.web_reviews_client import WebReviewsClient


class _FakeResponse:
    def __init__(self, status_code: int, text: str = ""):
        self.status_code = status_code
        self.text = text


class _FakeSession:
    def __init__(self, response_map: dict[str, _FakeResponse]):
        self.headers = {}
        self._response_map = response_map

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def get(self, url, timeout=10):
        return self._response_map[url]


class _FakeRobotParser:
    def __init__(self, allowed_urls: set[str]):
        self.allowed_urls = allowed_urls

    def set_url(self, url: str) -> None:
        self.robots_url = url

    def read(self) -> None:
        return None

    def can_fetch(self, user_agent: str, url: str) -> bool:
        return url in self.allowed_urls


def test_discovery_respects_robots_and_finds_candidates(monkeypatch) -> None:
    homepage = "https://example.com"
    category = "https://example.com/reviews"
    article_allowed = "https://example.com/reviews/best-watch-2026"
    article_disallowed = "https://example.com/reviews/private-deal"

    response_map = {
        homepage: _FakeResponse(
            200,
            (
                '<a href="/reviews">Reviews</a>'
                f'<a href="{article_allowed}">Allowed article</a>'
                f'<a href="{article_disallowed}">Disallowed article</a>'
            ),
        ),
        category: _FakeResponse(200, f'<a href="{article_allowed}">Allowed article</a>'),
    }
    monkeypatch.setattr("requests.Session", lambda: _FakeSession(response_map))
    monkeypatch.setattr(
        "app.ingestion.web_reviews_client.urllib.robotparser.RobotFileParser",
        lambda: _FakeRobotParser({homepage, category, article_allowed}),
    )

    client = WebReviewsClient(request_delay_seconds=0)
    discovered = client.discover_candidate_article_urls(homepage_url=homepage, category_urls=[category])

    assert discovered == [article_allowed]


def test_blocked_pages_warn_and_are_skipped(monkeypatch, caplog) -> None:
    homepage = "https://example.com"
    article = "https://example.com/reviews/blocked-article"

    response_map = {
        homepage: _FakeResponse(200, f'<a href="{article}">Blocked article</a>'),
        article: _FakeResponse(403, "forbidden"),
    }

    monkeypatch.setattr("requests.Session", lambda: _FakeSession(response_map))
    monkeypatch.setattr(
        "app.ingestion.web_reviews_client.urllib.robotparser.RobotFileParser",
        lambda: _FakeRobotParser({homepage, article}),
    )

    client = WebReviewsClient(request_delay_seconds=0)

    discovered = client.discover_candidate_article_urls(homepage_url=homepage)
    assert discovered == [article]

    with caplog.at_level("WARNING"):
        html_by_url = client.fetch_articles(discovered)

    assert html_by_url == {}
    assert any("Skipping blocked URL" in message for message in caplog.messages)


def test_headers_are_configurable(monkeypatch) -> None:
    homepage = "https://example.com"

    session = _FakeSession({homepage: _FakeResponse(200, "")})
    monkeypatch.setattr("requests.Session", lambda: session)
    monkeypatch.setattr(
        "app.ingestion.web_reviews_client.urllib.robotparser.RobotFileParser",
        lambda: _FakeRobotParser({homepage}),
    )

    client = WebReviewsClient(
        user_agent="custom-agent",
        headers={"Accept-Language": "en-US"},
        request_delay_seconds=0,
    )

    client.discover_candidate_article_urls(homepage_url=homepage)

    assert session.headers["User-Agent"] == "custom-agent"
    assert session.headers["Accept-Language"] == "en-US"


def test_prioritize_keywords_orders_matches_first_and_keeps_breadth(monkeypatch) -> None:
    homepage = "https://example.com"
    samsung = "https://example.com/reviews/samsung-health-review"
    galaxy = "https://example.com/reviews/galaxy-watch-tips"
    broad = "https://example.com/reviews/fitness-trackers-comparison"

    response_map = {
        homepage: _FakeResponse(
            200,
            (
                f'<a href="{samsung}">Samsung Health deep dive</a>'
                f'<a href="{galaxy}">Galaxy Watch setup tips</a>'
                f'<a href="{broad}">Best fitness trackers this year</a>'
            ),
        ),
    }
    monkeypatch.setattr("requests.Session", lambda: _FakeSession(response_map))
    monkeypatch.setattr(
        "app.ingestion.web_reviews_client.urllib.robotparser.RobotFileParser",
        lambda: _FakeRobotParser({homepage, samsung, galaxy, broad}),
    )

    client = WebReviewsClient(request_delay_seconds=0)
    discovered = client.discover_candidate_article_urls(
        homepage_url=homepage,
        keywords=["Samsung Health", "Galaxy"],
        prioritize_keywords=True,
    )

    assert discovered == [samsung, galaxy, broad]
