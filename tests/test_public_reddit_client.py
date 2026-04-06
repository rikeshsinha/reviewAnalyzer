from __future__ import annotations

from app.ingestion.public_reddit_client import PublicRedditError, search_submissions


def test_search_submissions_falls_back_to_old_reddit_host(monkeypatch) -> None:
    calls: list[str] = []

    def _fake_request_json(session, url, params, *, timeout):
        calls.append(url)
        if "www.reddit.com" in url:
            raise PublicRedditError("403")
        return {
            "data": {
                "children": [
                    {
                        "data": {
                            "id": "abc123",
                            "created_utc": 1_710_000_000,
                            "title": "t",
                            "selftext": "b",
                        }
                    }
                ],
                "after": None,
            }
        }

    monkeypatch.setattr("app.ingestion.public_reddit_client._request_json", _fake_request_json)

    records = search_submissions(
        subreddit="GalaxyWatch",
        query="Samsung Health",
        after_iso=None,
        before_iso=None,
        max_pages=1,
        request_delay_seconds=0,
    )

    assert records and records[0]["id"] == "abc123"
    assert calls[0].startswith("https://www.reddit.com/")
    assert calls[1].startswith("https://old.reddit.com/")
