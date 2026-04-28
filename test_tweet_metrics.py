#!/usr/bin/env python3
"""
Quick DOM-only smoke test for tweet metrics.

This opens the same persistent Chrome profile as scraper.py, reads a few visible
timeline tweets, and prints the fields used for composite scoring. It does not
call OpenAI, score tweets, save all_tweets.json, or export to Google Sheets.
"""

import argparse
import asyncio
import json

from scraper import PROFILE_DIR, ensure_playwright_node, read_tweets_from_dom, wait_for_timeline_ready
from playwright.async_api import async_playwright


async def read_metric_debug(page, limit: int) -> list[dict]:
    return await page.evaluate(
        """
        (limit) => {
            const articles = Array.from(document.querySelectorAll('article[data-testid="tweet"]')).slice(0, limit);
            return articles.map((article, index) => {
                const text = article.querySelector('[data-testid="tweetText"]')?.innerText?.trim() || '';
                const candidates = [];

                for (const el of article.querySelectorAll('[aria-label], [data-testid], a[href*="analytics"], a[href*="tweet_activity"]')) {
                    const aria = el.getAttribute('aria-label') || '';
                    const testid = el.getAttribute('data-testid') || '';
                    const href = el.getAttribute('href') || '';
                    const visibleText = (el.textContent || '').trim().replace(/\\s+/g, ' ');
                    const combined = `${aria} ${testid} ${href} ${visibleText}`.toLowerCase();

                    if (!combined.includes('view') && !combined.includes('analytics') && !combined.includes('impression')) {
                        continue;
                    }

                    candidates.push({
                        aria,
                        testid,
                        href,
                        text: visibleText.slice(0, 160),
                    });
                }

                return {
                    index: index + 1,
                    text: text.slice(0, 120),
                    candidates,
                };
            });
        }
        """,
        limit,
    )


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=3, help="Number of visible tweets to print")
    parser.add_argument("--debug", action="store_true", help="Print DOM candidates related to views/impressions")
    args = parser.parse_args()

    ensure_playwright_node()

    async with async_playwright() as p:
        PROFILE_DIR.mkdir(exist_ok=True)
        context = await p.chromium.launch_persistent_context(
            user_data_dir=str(PROFILE_DIR),
            channel="chrome",
            headless=False,
            args=["--disable-blink-features=AutomationControlled"],
            viewport={"width": 1280, "height": 900},
        )
        page = await context.new_page()

        print("Opening X home timeline...")
        await page.goto("https://x.com/home", timeout=60000)

        if not await wait_for_timeline_ready(page, timeout_ms=60000):
            print("Timeline did not become ready. Confirm Chrome is logged in to X, then rerun this test.")
            await context.close()
            return

        tweets = (await read_tweets_from_dom(page))[: args.limit]
        print(json.dumps(tweets, indent=2, ensure_ascii=False))

        missing = [i for i, tweet in enumerate(tweets, 1) if tweet.get("impressions") is None]
        if missing:
            print(f"\nImpressions missing for visible tweet(s): {missing}")
            print("Tip: rerun with --debug to print the DOM candidates that mention views/impressions.")
        else:
            print("\nImpressions populated for all printed tweets.")

        if args.debug:
            print("\nDebug candidates related to views/impressions:")
            debug = await read_metric_debug(page, args.limit)
            print(json.dumps(debug, indent=2, ensure_ascii=False))

        await context.close()


if __name__ == "__main__":
    asyncio.run(main())
