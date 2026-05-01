#!/usr/bin/env python3

import tempfile
import unittest
from pathlib import Path
from unittest import mock

import scraper


class ResumeLastBatchTests(unittest.TestCase):
    def test_select_resume_last_batch_returns_only_tweets_after_last_completed_total(self):
        all_tweets = {
            "1": {"text": "one"},
            "2": {"text": "two"},
            "3": {"text": "three"},
            "4": {"text": "four"},
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            history_path = Path(tmpdir) / "run_history.jsonl"
            history_path.write_text(
                "\n".join(
                    [
                        '{"export_only": false, "total_accumulated": 2}',
                        '{"resume_last_batch": true, "total_accumulated": 4}',
                    ]
                ),
                encoding="utf-8",
            )
            with mock.patch.object(scraper, "RUN_HISTORY_FILE", history_path):
                batch = scraper.select_resume_last_batch(all_tweets)

        self.assertEqual([tweet["text"] for tweet in batch], ["three", "four"])

    def test_select_resume_last_batch_returns_all_when_no_completed_run_exists(self):
        all_tweets = {
            "1": {"text": "one"},
            "2": {"text": "two"},
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            history_path = Path(tmpdir) / "run_history.jsonl"
            history_path.write_text(
                '{"export_only": true, "total_accumulated": 2}\n',
                encoding="utf-8",
            )
            with mock.patch.object(scraper, "RUN_HISTORY_FILE", history_path):
                batch = scraper.select_resume_last_batch(all_tweets)

        self.assertEqual([tweet["text"] for tweet in batch], ["one", "two"])


if __name__ == "__main__":
    unittest.main()
