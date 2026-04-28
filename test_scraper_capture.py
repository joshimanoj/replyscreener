#!/usr/bin/env python3

import csv
import tempfile
import unittest
from pathlib import Path

from scraper import _ingest_visible_tweets, write_handle_frequency_csv


class ScraperCaptureTests(unittest.TestCase):
    def test_ingest_visible_tweets_tracks_new_duplicates_and_anchor(self):
        all_tweets = {
            "@alpha:existing tweet": {
                "author": "@alpha",
                "text": "existing tweet",
                "url": "https://x.com/alpha/status/1",
                "likes": 1,
            }
        }
        visible_tweets = [
            {
                "author": "alpha",
                "author_name": "Alpha",
                "text": "existing tweet",
                "url": "https://x.com/alpha/status/1",
                "likes": 12,
            },
            {
                "author": "beta",
                "author_name": "Beta",
                "text": "brand new thought",
                "url": "https://x.com/beta/status/2",
                "likes": 3,
            },
            {
                "author": "gamma",
                "author_name": "Gamma",
                "text": "anchor tweet",
                "url": "https://x.com/gamma/status/3",
                "likes": 4,
            },
        ]

        result = _ingest_visible_tweets(
            visible_tweets,
            all_tweets,
            anchor_url="https://x.com/gamma/status/3",
            first_tweet_this_run=None,
        )

        self.assertEqual(result["new_count"], 1)
        self.assertEqual(result["duplicate_count"], 1)
        self.assertTrue(result["anchor_found"])
        self.assertEqual(result["first_tweet_this_run"]["url"], "https://x.com/alpha/status/1")
        self.assertEqual(all_tweets["@alpha:existing tweet"]["likes"], 12)
        self.assertIn("@beta:brand new thought", all_tweets)
        self.assertNotIn("@gamma:anchor tweet", all_tweets)
        self.assertEqual(
            result["visible_urls"],
            {
                "https://x.com/alpha/status/1",
                "https://x.com/beta/status/2",
                "https://x.com/gamma/status/3",
            },
        )

    def test_write_handle_frequency_csv_aggregates_and_skips_empty_handles(self):
        all_tweets = {
            "1": {"author": "alpha", "text": "one"},
            "2": {"author": "@alpha", "text": "two"},
            "3": {"author": "beta", "text": "three"},
            "4": {"author": "", "text": "four"},
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            target = Path(tmpdir) / "handle_frequency.csv"
            write_handle_frequency_csv(all_tweets, destination=target)

            with open(target, newline="", encoding="utf-8") as f:
                rows = list(csv.reader(f))

        self.assertEqual(rows[0], ["handle_id", "frequency"])
        self.assertEqual(rows[1], ["@alpha", "2"])
        self.assertEqual(rows[2], ["@beta", "1"])
        self.assertEqual(len(rows), 3)


if __name__ == "__main__":
    unittest.main()
