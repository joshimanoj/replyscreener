#!/usr/bin/env python3
"""
Recover the most recent interrupted batch from all_tweets.json and export it to Google Sheets.

Recovery heuristic:
- Read the latest completed scrape run from run_history.jsonl.
- Treat tweets added after that run's `total_accumulated` count as the interrupted batch.
- Of those, export only tweets where the Phi stage completed and `llm_relevant` is True.
"""

from __future__ import annotations

import json

from scraper import (
    SCRIPT_DIR,
    append_run_history,
    export_to_gsheets,
    load_config,
    load_last_completed_total,
)


def main() -> int:
    items = json.loads((SCRIPT_DIR / "all_tweets.json").read_text(encoding="utf-8"))
    if isinstance(items, dict):
        ordered = list(items.values())
    else:
        ordered = list(items)

    last_total = load_last_completed_total()
    batch = ordered[last_total:]
    processed = [tweet for tweet in batch if "llm_topic_fit_score" in tweet]
    kept = [tweet for tweet in processed if tweet.get("llm_relevant") is True]
    kept.sort(key=lambda tweet: float(tweet.get("composite_score", 0.0) or 0.0), reverse=True)

    print(f"Last completed total:      {last_total}")
    print(f"Interrupted batch tweets:  {len(batch)}")
    print(f"Recovered processed tweets:{len(processed):>4}")
    print(f"Recovered kept tweets:     {len(kept):>4}")

    if not kept:
        print("No recoverable kept tweets found.")
        return 1

    url = export_to_gsheets(kept, load_config())
    append_run_history({
        "recovered_export": True,
        "source": "export_recovered_last_batch.py",
        "interrupted_batch_size": len(batch),
        "processed_in_batch": len(processed),
        "matching_filter": len(kept),
        "sheet_url": url,
    })
    print(url)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
