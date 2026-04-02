from __future__ import annotations

from app.ingestion.public_reddit_client import PublicRedditError, search_submissions


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

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def get(self, url, params=None, timeout=10):
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


def test_public_json_raises_on_non_200(monkeypatch) -> None:
    monkeypatch.setattr("requests.Session", lambda: _FakeSession([_FakeResponse(403, {})]))

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
