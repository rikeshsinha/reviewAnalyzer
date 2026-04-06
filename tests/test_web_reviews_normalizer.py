from __future__ import annotations

from app.ingestion.web_reviews_normalizer import normalize_web_review_article


def test_normalize_web_review_article_maps_to_canonical_schema() -> None:
    html = """
    <html>
      <head>
        <title>Ignored page title</title>
        <meta property="og:title" content="Galaxy Watch 8 review: better battery" />
        <meta name="author" content="Taylor Kim" />
        <meta property="article:published_time" content="2026-03-31T09:45:00Z" />
      </head>
      <body>
        <header>Top navigation and promo links</header>
        <article>
          <p>{text}</p>
        </article>
        <footer>Subscribe now</footer>
      </body>
    </html>
    """.format(text=("Great improvements and some tradeoffs. " * 30))

    raw = {
        "url": "https://www.techradar.com/reviews/galaxy-watch-8-review",
        "html": html,
        "source_link": "https://www.techradar.com/reviews/galaxy-watch-8-review",
    }

    normalized = normalize_web_review_article(raw, min_content_chars=500)

    assert normalized is not None
    assert normalized["source"] == "web_reviews"
    assert normalized["platform"] == "web_reviews"
    assert normalized["subreddit_or_site"] == "techradar.com"
    assert normalized["community_or_channel"] == "techradar.com"
    assert normalized["title"] == "Galaxy Watch 8 review: better battery"
    assert normalized["author"] == "Taylor Kim"
    assert normalized["author_handle"] == "Taylor Kim"
    assert normalized["created_at"] == "2026-03-31T09:45:00+00:00"
    assert normalized["url"] == raw["url"]
    assert normalized["review_text"] == normalized["content"]
    assert len(normalized["content"]) >= 500
    assert normalized["raw_payload"] == raw


def test_normalize_web_review_article_filters_non_editorial_pages() -> None:
    raw = {
        "url": "https://example.com/category/wearables",
        "title": "Wearables Category",
        "content": "Useful summary text " * 80,
    }

    assert normalize_web_review_article(raw, min_content_chars=500) is None


def test_normalize_web_review_article_keeps_editorial_review_pages() -> None:
    raw = {
        "url": "https://example.com/reviews/editorial-roundup-2026",
        "title": "Editorial roundup",
        "content": "Strong editorial review coverage. " * 30,
    }

    normalized = normalize_web_review_article(raw, min_content_chars=500)

    assert normalized is not None
    assert normalized["url"] == raw["url"]


def test_normalize_web_review_article_filters_short_content() -> None:
    raw = {
        "url": "https://example.com/reviews/quick-look",
        "title": "Quick look",
        "content": "Too short",
    }

    assert normalize_web_review_article(raw, min_content_chars=500) is None


def test_normalize_web_review_article_enforces_500_char_minimum() -> None:
    nearly_long_enough = {
        "url": "https://example.com/reviews/almost-long-enough",
        "title": "Almost long enough",
        "content": "a" * 499,
    }
    long_enough = {
        "url": "https://example.com/reviews/long-enough",
        "title": "Long enough",
        "content": "a" * 500,
    }

    assert normalize_web_review_article(nearly_long_enough, min_content_chars=500) is None
    assert normalize_web_review_article(long_enough, min_content_chars=500) is not None
