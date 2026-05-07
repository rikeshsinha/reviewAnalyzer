from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from app.ingestion import google_search_client
from app.ingestion.google_search_client import GoogleSearchError, search_google


class FakeResponse:
    def __init__(self, status_code: int, payload: Any) -> None:
        self.status_code = status_code
        self._payload = payload

    def json(self) -> Any:
        return self._payload


def test_search_google_fetches_first_page(monkeypatch: pytest.MonkeyPatch) -> None:
    calls = []

    def fake_get(url: str, params: dict[str, Any], timeout: float) -> FakeResponse:
        calls.append((url, params, timeout))
        return FakeResponse(200, {"items": [{"title": "A", "link": "https://example.com/a"}]})

    monkeypatch.setattr(google_search_client.requests, "get", fake_get)

    results = search_google("Samsung Health", api_key="key", search_engine_id="cx", max_results=1)

    assert results == [{"title": "A", "link": "https://example.com/a"}]
    assert calls[0][1]["num"] == 1
    assert calls[0][1]["start"] == 1


def test_search_google_fetches_two_pages(monkeypatch: pytest.MonkeyPatch) -> None:
    pages = [
        FakeResponse(200, {"items": [{"title": f"A{i}", "link": f"https://example.com/a{i}"} for i in range(10)], "queries": {"nextPage": [{"startIndex": 11}]}}),
        FakeResponse(200, {"items": [{"title": f"B{i}", "link": f"https://example.com/b{i}"} for i in range(10)]}),
    ]

    def fake_get(url: str, params: dict[str, Any], timeout: float) -> FakeResponse:
        del url, timeout
        if params["start"] == 1:
            return pages[0]
        return pages[1]

    monkeypatch.setattr(google_search_client.requests, "get", fake_get)

    results = search_google("Galaxy Ring", api_key="key", search_engine_id="cx", max_results=20)

    assert len(results) == 20
    assert results[10]["title"] == "B0"


def test_search_google_quota_raises_clear_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        google_search_client.requests,
        "get",
        lambda *args, **kwargs: FakeResponse(403, {"error": {"message": "Daily Limit Exceeded"}}),
    )

    with pytest.raises(GoogleSearchError, match="quota exceeded"):
        search_google("q", api_key="key", search_engine_id="cx")


def test_search_google_malformed_response_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        google_search_client.requests,
        "get",
        lambda *args, **kwargs: FakeResponse(200, {"items": {"not": "a list"}}),
    )

    with pytest.raises(GoogleSearchError, match="malformed"):
        search_google("q", api_key="key", search_engine_id="cx")
