#!/usr/bin/env python3
"""
Smoke-test the Google Sheets export with a single tweet row.

By default this loads the first tweet from all_tweets.json, enriches it with the
fields expected by the export, writes a single-row worksheet, and prints the
Google Sheets link.
"""

from __future__ import annotations

from scraper import export_to_gsheets, load_config, load_saved_tweets


def build_test_row() -> dict:
    saved = load_saved_tweets()
    if saved:
        tweet = dict(next(iter(saved.values())))
    else:
        tweet = {
            "author_name": "Test Author",
            "author": "@test",
            "text": "Test tweet for Sheets export verification.",
            "url": "https://x.com/test/status/1",
        }

    tweet.setdefault("posted_at", "")
    tweet.setdefault("replies", 0)
    tweet.setdefault("retweets", 0)
    tweet.setdefault("likes", 0)
    tweet.setdefault("impressions", 0)
    tweet.setdefault("score", 0.42)
    tweet.setdefault("llm_topic_fit_score", 0.75)
    tweet.setdefault("llm_philosophical_depth_score", 0.55)
    tweet.setdefault("llm_relevance_score", 0.68)
    tweet.setdefault("relevance_score", tweet["llm_relevance_score"])
    tweet.setdefault("freshness_score", 0.50)
    tweet.setdefault("traction_score", 0.10)
    tweet.setdefault("engagement_rate_score", 0.05)
    tweet.setdefault("quote_style", False)
    tweet.setdefault("quote_style_reason", "")
    tweet.setdefault("quote_docked_to_zero", False)
    tweet.setdefault("composite_score", 0.61)
    tweet.setdefault("llm_reason", "Sheets export smoke test.")
    return tweet


def main() -> int:
    config = load_config()
    url = export_to_gsheets([build_test_row()], config)
    print(url)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
