"""Google Programmable Search JSON API client."""

from __future__ import annotations

import time
from typing import Any

import requests


class GoogleSearchError(Exception):
    """Raised when Google Search API ingestion cannot fetch valid results."""


_TRANSIENT_STATUSES = {429, 500, 502, 503, 504}
_ENDPOINT = "https://www.googleapis.com/customsearch/v1"


def _extract_error_message(payload: Any, fallback: str) -> str:
    if not isinstance(payload, dict):
        return fallback
    error = payload.get("error")
    if isinstance(error, dict):
        message = error.get("message")
        if isinstance(message, str) and message.strip():
            return message.strip()
        errors = error.get("errors")
        if isinstance(errors, list) and errors and isinstance(errors[0], dict):
            reason = errors[0].get("reason")
            if isinstance(reason, str) and reason.strip():
                return reason.strip()
    return fallback


def _classify_http_error(response: requests.Response, payload: Any) -> GoogleSearchError:
    message = _extract_error_message(payload, f"HTTP {response.status_code} from Google Search API")
    lowered = message.lower()
    if response.status_code == 403 and any(term in lowered for term in ("quota", "limit", "daily")):
        return GoogleSearchError(f"Google Search API quota exceeded: {message}")
    if response.status_code in {400, 403} and any(
        term in lowered for term in ("api key", "key invalid", "bad request")
    ):
        return GoogleSearchError(f"Google Search API key is invalid or unauthorized: {message}")
    if response.status_code in {400, 404} and any(term in lowered for term in ("cx", "search engine", "custom search")):
        return GoogleSearchError(f"Google Search engine ID is invalid: {message}")
    return GoogleSearchError(f"Google Search API request failed ({response.status_code}): {message}")


def _request_page(params: dict[str, Any], *, timeout: float) -> dict[str, Any]:
    last_error: Exception | None = None
    for attempt in range(3):
        try:
            response = requests.get(_ENDPOINT, params=params, timeout=timeout)
        except requests.RequestException as exc:
            last_error = exc
            if attempt < 2:
                time.sleep(0.5 * (2**attempt))
                continue
            raise GoogleSearchError(f"Google Search API request failed: {exc}") from exc

        try:
            payload = response.json()
        except ValueError as exc:
            raise GoogleSearchError("Google Search API returned malformed JSON") from exc

        if response.status_code == 200:
            if not isinstance(payload, dict):
                raise GoogleSearchError("Google Search API returned malformed response: expected object")
            return payload

        if response.status_code in _TRANSIENT_STATUSES and attempt < 2:
            last_error = _classify_http_error(response, payload)
            time.sleep(0.5 * (2**attempt))
            continue
        raise _classify_http_error(response, payload)

    raise GoogleSearchError(f"Google Search API request failed after retries: {last_error}")


def search_google(
    query: str,
    *,
    api_key: str,
    search_engine_id: str,
    max_results: int = 20,
    country: str | None = None,
    language: str | None = None,
    timeout: float = 20.0,
) -> list[dict[str, Any]]:
    """Fetch up to ``max_results`` metadata items for a query via Custom Search JSON API."""

    if not query or not query.strip():
        raise GoogleSearchError("Google Search query must not be empty")
    if not api_key or not api_key.strip():
        raise GoogleSearchError("Missing Google Search API key (GOOGLE_SEARCH_API_KEY)")
    if not search_engine_id or not search_engine_id.strip():
        raise GoogleSearchError("Missing Google Search engine ID (GOOGLE_SEARCH_ENGINE_ID)")
    if max_results <= 0:
        raise GoogleSearchError("Google Search max_results must be a positive integer")

    results: list[dict[str, Any]] = []
    start = 1
    while len(results) < max_results:
        page_size = min(10, max_results - len(results))
        params: dict[str, Any] = {
            "key": api_key,
            "cx": search_engine_id,
            "q": query.strip(),
            "num": page_size,
            "start": start,
        }
        if country:
            params["gl"] = country.strip().lower()
        if language:
            lang = language.strip().lower()
            params["lr"] = lang if lang.startswith("lang_") else f"lang_{lang}"

        payload = _request_page(params, timeout=timeout)
        items = payload.get("items", [])
        if items is None:
            items = []
        if not isinstance(items, list) or not all(isinstance(item, dict) for item in items):
            raise GoogleSearchError("Google Search API returned malformed response: 'items' must be a list")
        if not items:
            break
        results.extend(items)

        queries = payload.get("queries")
        next_page = None
        if isinstance(queries, dict):
            next_pages = queries.get("nextPage")
            if isinstance(next_pages, list) and next_pages and isinstance(next_pages[0], dict):
                next_page = next_pages[0]
        if not next_page:
            break
        next_start = next_page.get("startIndex")
        if not isinstance(next_start, int) or next_start <= start:
            break
        start = next_start

    return results[:max_results]
