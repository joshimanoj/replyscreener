#!/usr/bin/env python3

import builtins
import unittest
from unittest import mock

import scraper


class ScoringModelRequiredTests(unittest.TestCase):
    def setUp(self):
        scraper._model = None
        scraper._tokenizer = None
        scraper._device = None
        scraper._keep_embs = None
        scraper._skip_embs = None
        scraper._lang_detect = lambda _: "en"
        scraper.LangDetectException = Exception

    def test_load_model_raises_when_transformers_import_fails(self):
        original_import = builtins.__import__

        def fake_import(name, globals=None, locals=None, fromlist=(), level=0):
            if name == "transformers":
                raise ImportError("mocked missing dependency")
            return original_import(name, globals, locals, fromlist, level)

        with mock.patch("builtins.__import__", side_effect=fake_import):
            with self.assertRaises(RuntimeError) as ctx:
                scraper._load_model()

        self.assertIn("Keyword fallback is disabled", str(ctx.exception))
        self.assertIsNone(scraper._model)

    def test_score_tweets_batch_propagates_model_load_failure(self):
        tweets = [
            {
                "text": (
                    "Inner stillness and self-awareness make it easier to loosen the grip of ego "
                    "and see reality more clearly."
                )
            }
        ]

        with mock.patch.object(scraper, "_load_model", side_effect=RuntimeError("model failed")):
            with self.assertRaisesRegex(RuntimeError, "model failed"):
                scraper.score_tweets_batch(tweets)

        self.assertNotIn("score", tweets[0])


if __name__ == "__main__":
    unittest.main()
