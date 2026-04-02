from __future__ import annotations

from app.ingestion.reddit_rss_client import RedditRssError, search_submissions


class _FakeResponse:
    def __init__(self, status_code: int, text: str):
        self.status_code = status_code
        self.text = text


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


def test_search_submissions_parses_atom_and_filters_window(monkeypatch) -> None:
    atom_feed = """<?xml version='1.0' encoding='UTF-8'?>
<feed xmlns='http://www.w3.org/2005/Atom'>
  <entry>
    <id>t3_rss1</id>
    <title>Battery issue</title>
    <updated>2026-03-15T00:00:00+00:00</updated>
    <author><name>alice</name></author>
    <category term='android'/>
    <content type='html'>&lt;p&gt;Battery drains quickly&lt;/p&gt;</content>
    <link rel='alternate' href='https://www.reddit.com/r/android/comments/rss1/test/' />
  </entry>
</feed>
"""
    monkeypatch.setattr("requests.Session", lambda: _FakeSession([_FakeResponse(200, atom_feed)]))

    records = search_submissions(
        subreddit="android",
        query="battery",
        after_iso="2026-03-10T00:00:00+00:00",
        before_iso="2026-03-20T00:00:00+00:00",
        max_pages=1,
        request_delay_seconds=0,
    )

    assert len(records) == 1
    assert records[0]["id"] == "t3_rss1"
    assert records[0]["subreddit"] == "android"
    assert "Battery drains quickly" in records[0]["selftext"]


def test_search_submissions_raises_on_non_200(monkeypatch) -> None:
    monkeypatch.setattr("requests.Session", lambda: _FakeSession([_FakeResponse(403, "")]))

    try:
        search_submissions(
            subreddit="android",
            query="battery",
            after_iso=None,
            before_iso=None,
            max_pages=1,
            request_delay_seconds=0,
        )
        assert False, "Expected RedditRssError"
    except RedditRssError as exc:
        assert "status 403" in str(exc)
