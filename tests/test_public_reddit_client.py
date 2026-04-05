from __future__ import annotations

from app.ingestion.public_reddit_client import PublicRedditError, fetch_subreddit_new, search_submissions


class _FakeResponse:
    def __init__(self, status_code: int, payload: dict):
        self.status_code = status_code
        self._payload = payload

    def json(self):
        return self._payload


class _FakeSession:
    def __init__(self, responses):
        self.headers = {}
        self._responses = list(responses)
        self.calls = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def get(self, url, params=None, timeout=10):
        self.calls.append({"url": url, "params": params, "timeout": timeout})
        return self._responses.pop(0)


def test_public_json_search_returns_records(monkeypatch) -> None:
    payload = {
        "data": {
            "children": [
                {"data": {"id": "abc1", "title": "hello", "created_utc": 1_710_000_000}},
                {"data": {"id": "abc2", "title": "world", "created_utc": 1_710_000_001}},
            ],
            "after": None,
        }
    }
    monkeypatch.setattr("requests.Session", lambda: _FakeSession([_FakeResponse(200, payload)]))

    records = search_submissions(
        subreddit="android",
        query="battery",
        after_iso=None,
        before_iso=None,
        page_size=10,
        max_pages=1,
        request_delay_seconds=0,
    )

    assert [row["id"] for row in records] == ["abc1", "abc2"]


def test_public_json_search_uses_plain_query_with_restrict_sr(monkeypatch) -> None:
    payload = {"data": {"children": [], "after": None}}
    fake_session = _FakeSession([_FakeResponse(200, payload)])
    monkeypatch.setattr("requests.Session", lambda: fake_session)

    search_submissions(
        subreddit="android",
        query="Samsung Health",
        after_iso=None,
        before_iso=None,
        page_size=10,
        max_pages=1,
        request_delay_seconds=0,
    )

    assert fake_session.calls[0]["params"]["q"] == "Samsung Health"
    assert fake_session.calls[0]["params"]["restrict_sr"] == "on"


def test_public_json_raises_on_non_200(monkeypatch) -> None:
    monkeypatch.setattr("requests.Session", lambda: _FakeSession([_FakeResponse(403, {}), _FakeResponse(403, {})]))

    try:
        search_submissions(
            subreddit="android",
            query="battery",
            after_iso=None,
            before_iso=None,
            max_pages=1,
            request_delay_seconds=0,
        )
        assert False, "Expected PublicRedditError"
    except PublicRedditError as exc:
        assert "status 403" in str(exc)


def test_public_json_falls_back_to_old_reddit_on_primary_host_error(monkeypatch) -> None:
    payload = {
        "data": {
            "children": [{"data": {"id": "abc3", "title": "fallback ok", "created_utc": 1_710_000_002}}],
            "after": None,
        }
    }
    fake_session = _FakeSession([_FakeResponse(403, {}), _FakeResponse(200, payload)])
    monkeypatch.setattr("requests.Session", lambda: fake_session)

    records = search_submissions(
        subreddit="android",
        query="battery",
        after_iso=None,
        before_iso=None,
        max_pages=1,
        request_delay_seconds=0,
    )

    assert [row["id"] for row in records] == ["abc3"]
    assert fake_session.calls[0]["url"].startswith("https://www.reddit.com/")
    assert fake_session.calls[1]["url"].startswith("https://old.reddit.com/")


def test_fetch_subreddit_new_filters_to_date_window(monkeypatch) -> None:
    payload = {
        "data": {
            "children": [
                {"data": {"id": "old1", "created_utc": 1_709_999_999}},
                {"data": {"id": "in1", "created_utc": 1_710_000_000}},
                {"data": {"id": "in2", "created_utc": 1_710_000_100}},
                {"data": {"id": "new1", "created_utc": 1_710_000_201}},
            ],
            "after": None,
        }
    }
    monkeypatch.setattr("requests.Session", lambda: _FakeSession([_FakeResponse(200, payload)]))

    records = fetch_subreddit_new(
        subreddit="android",
        after_iso="2024-03-09T16:00:00+00:00",
        before_iso="2024-03-09T16:03:20+00:00",
        page_size=10,
        max_pages=1,
        request_delay_seconds=0,
    )

    assert [row["id"] for row in records] == ["in1", "in2"]
