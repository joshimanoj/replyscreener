#!/usr/bin/env python3
"""
Recover the most recent processed batch from all_tweets.json and export it to Google Sheets.

Current recovery heuristic:
- Tweets with `llm_topic_fit_score` present belong to the most recent local Phi batch.
- Of those, export only tweets where `llm_relevant` is True.
"""

from __future__ import annotations

import json
from pathlib import Path

from scraper import SCRIPT_DIR, export_to_gsheets, load_config


def main() -> int:
    items = json.loads((SCRIPT_DIR / "all_tweets.json").read_text())
    processed = [tweet for tweet in items if "llm_topic_fit_score" in tweet]
    kept = [tweet for tweet in processed if tweet.get("llm_relevant") is True]
    kept.sort(key=lambda tweet: float(tweet.get("composite_score", 0.0) or 0.0), reverse=True)

    print(f"Recovered processed tweets: {len(processed)}")
    print(f"Recovered kept tweets:      {len(kept)}")

    if not kept:
        print("No recoverable kept tweets found.")
        return 1

    url = export_to_gsheets(kept, load_config())
    print(url)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
