#!/usr/bin/env python3
"""
Twitter Timeline Scraper — DOM Edition
---------------------------------------
Run 1 (no anchor): collect up to max_tweets, save first tweet as anchor.
Run 2+ (anchor exists): collect ALL new tweets until anchor is found,
  no count cap - guarantees zero gaps.

Usage:
    python scraper.py               # full scrape + filter + export
    python scraper.py --export-only # skip scraping, just filter + export
    python scraper.py --rank-sheet-only
                                   # rerun Phi-3 ranking for current sheet rows
"""

import argparse
import asyncio
import csv
import importlib.util
import json
import math
import os
import random
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib import request as urlrequest

_lang_detect = None
LangDetectException = Exception


# ─── Paths ───────────────────────────────────────────────────

SCRIPT_DIR       = Path(__file__).parent
TWEETS_FILE      = SCRIPT_DIR / "all_tweets.json"
ANCHOR_FILE      = SCRIPT_DIR / "anchor.json"
PROFILE_DIR      = SCRIPT_DIR / "browser_profile"   # persistent login session
RUN_HISTORY_FILE  = SCRIPT_DIR / "run_history.jsonl"
HANDLE_FREQUENCY_FILE = SCRIPT_DIR / "handle_frequency.csv"


# ─── Config ──────────────────────────────────────────────────

def load_config():
    with open(SCRIPT_DIR / "config.json") as f:
        return json.load(f)


def ensure_playwright_node() -> None:
    """
    Playwright normally uses its bundled Node binary, but some installs can miss
    that file while still shipping the JS driver package. In that case, fall back
    to the system Node.js executable so Playwright can start.
    """
    if os.getenv("PLAYWRIGHT_NODEJS_PATH"):
        return

    spec = importlib.util.find_spec("playwright")
    if spec and spec.origin:
        driver_node = Path(spec.origin).resolve().parent / "driver" / "node"
        if driver_node.exists():
            return

    system_node = shutil.which("node")
    if system_node:
        os.environ["PLAYWRIGHT_NODEJS_PATH"] = system_node


# ─── Anchor helpers ──────────────────────────────────────────

def load_anchor() -> dict | None:
    if ANCHOR_FILE.exists():
        with open(ANCHOR_FILE) as f:
            return json.load(f)
    return None

def save_anchor(tweet: dict):
    with open(ANCHOR_FILE, "w") as f:
        json.dump({
            "url":    tweet.get("url", ""),
            "author": tweet.get("author", ""),
            "text":   tweet.get("text", "")[:80],
        }, f, indent=2)


# ─── Saved tweets helpers ────────────────────────────────────

def load_saved_tweets() -> dict:
    """Load previously collected tweets as dedup dict."""
    if TWEETS_FILE.exists():
        with open(TWEETS_FILE) as f:
            tweets = json.load(f)
        return {f"{t.get('author','')}:{t.get('text','')[:80]}": t for t in tweets}
    return {}

def save_tweets(all_tweets: dict):
    with open(TWEETS_FILE, "w") as f:
        json.dump(list(all_tweets.values()), f, indent=2, ensure_ascii=False)


def _normalize_handle(handle: str) -> str:
    handle = (handle or "").strip()
    if not handle:
        return ""
    return handle if handle.startswith("@") else f"@{handle}"


def append_run_history(record: dict) -> None:
    record["recorded_at"] = datetime.now(timezone.utc).isoformat()
    with open(RUN_HISTORY_FILE, "a") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def write_handle_frequency_csv(all_tweets: dict, destination: Path = HANDLE_FREQUENCY_FILE) -> None:
    counts: dict[str, int] = {}
    for tweet in all_tweets.values():
        handle_id = _normalize_handle(tweet.get("author", ""))
        if not handle_id:
            continue
        counts[handle_id] = counts.get(handle_id, 0) + 1

    with open(destination, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["handle_id", "frequency"])
        for handle_id, frequency in sorted(counts.items(), key=lambda item: (-item[1], item[0])):
            writer.writerow([handle_id, frequency])


def _ingest_visible_tweets(
    visible_tweets: list[dict],
    all_tweets: dict,
    *,
    anchor_url: str | None = None,
    first_tweet_this_run: dict | None = None,
) -> dict:
    new_count = 0
    duplicate_count = 0
    visible_urls = set()
    anchor_found = False
    updated_first_tweet = first_tweet_this_run

    for tweet in visible_tweets:
        text = tweet.get("text", "").strip()
        author = _normalize_handle(tweet.get("author", ""))
        url = tweet.get("url", "")
        if not text:
            continue

        tweet["author"] = author
        if url and url != "unknown":
            visible_urls.add(url)

        if updated_first_tweet is None:
            updated_first_tweet = tweet

        if anchor_url and url != "unknown" and url == anchor_url:
            anchor_found = True
            break

        dedup_key = f"{author}:{text[:80]}"
        if dedup_key not in all_tweets:
            all_tweets[dedup_key] = tweet
            new_count += 1
        else:
            duplicate_count += 1
            existing = all_tweets[dedup_key]
            existing.update({
                "author": author or existing.get("author", ""),
                "author_name": tweet.get("author_name", existing.get("author_name", "")),
                "url": tweet.get("url", existing.get("url", "")),
                "posted_at": tweet.get("posted_at", existing.get("posted_at", "")),
                "replies": tweet.get("replies", existing.get("replies")),
                "retweets": tweet.get("retweets", existing.get("retweets")),
                "likes": tweet.get("likes", existing.get("likes")),
                "impressions": tweet.get("impressions", existing.get("impressions")),
            })

    return {
        "new_count": new_count,
        "duplicate_count": duplicate_count,
        "visible_urls": visible_urls,
        "anchor_found": anchor_found,
        "first_tweet_this_run": updated_first_tweet,
    }


# ─── Challenge / warning detection ──────────────────────────

CHALLENGE_SIGNALS = [
    "confirm it's you",
    "verify your identity",
    "confirm your identity",
    "suspicious activity",
    "unusual activity",
    "enter your phone",
    "enter your email",
]

LOGIN_SIGNALS = [
    "start a new session",
    "log in to twitter",
    "log in to x",
    "sign in to x",
]

AUTH_COOKIE_NAMES = {
    "auth_token",
    "ct0",
    "twid",
    "kdt",
}

def has_saved_x_session() -> bool:
    """
    Best-effort check that the persistent profile contains an authenticated X session.
    A profile directory alone is not enough; guest cookies can exist even when signed out.
    """
    cookies_db = PROFILE_DIR / "Default" / "Cookies"
    if not cookies_db.exists():
        return False

    try:
        import sqlite3
        with sqlite3.connect(f"file:{cookies_db}?mode=ro", uri=True) as con:
            cur = con.cursor()
            placeholders = ",".join("?" for _ in AUTH_COOKIE_NAMES)
            row = cur.execute(
                f"""
                select 1
                from cookies
                where (host_key like '%.x.com' or host_key like 'x.com'
                       or host_key like '%.twitter.com' or host_key like 'twitter.com')
                  and name in ({placeholders})
                limit 1
                """,
                tuple(AUTH_COOKIE_NAMES),
            ).fetchone()
            return row is not None
    except Exception:
        return False

async def wait_for_manual_login():
    """
    Keep the browser open long enough for a first-time or refreshed login.
    This avoids closing Chrome before the user can complete X's sign-in flow.
    """
    print("\n   🔐 No authenticated session detected.")
    print("   Please log in in the opened Chrome window, then come back here and press Enter.")
    await asyncio.to_thread(input, "   Press Enter after X is logged in...")

async def check_for_challenge(page) -> str | None:
    """
    Returns a short description of the challenge if one is detected,
    or None if the page looks normal.
    """
    try:
        body = (await page.inner_text("body")).lower()
    except Exception:
        return None
    for signal in CHALLENGE_SIGNALS:
        if signal in body:
            return signal
    return None


async def check_for_login_required(page) -> str | None:
    """
    Returns a short login signal if X is showing a signed-out/login state.
    This is recoverable and should not be treated like an account challenge.
    """
    try:
        current_url = page.url
        if "/login" in current_url or "/i/flow/login" in current_url:
            return "login url"
        body = (await page.inner_text("body")).lower()
    except Exception:
        return None
    for signal in LOGIN_SIGNALS:
        if signal in body:
            return signal
    return None


# ─── DOM Tweet Extractor ─────────────────────────────────────

async def read_tweets_from_dom(page) -> list[dict]:
    return await page.evaluate("""
        () => {
            const parseCount = (raw) => {
                if (!raw) return null;
                const text = String(raw).trim().toUpperCase().replace(/,/g, '');
                const match = text.match(/^([0-9]*\\.?[0-9]+)\\s*([KMB])?$/);
                if (!match) return null;
                const value = parseFloat(match[1]);
                if (Number.isNaN(value)) return null;
                const multiplier = { K: 1_000, M: 1_000_000, B: 1_000_000_000 };
                return Math.round(value * (multiplier[match[2]] || 1));
            };

            const parseCountFromText = (raw) => {
                if (!raw) return null;
                const match = String(raw).replace(/,/g, '').match(/([0-9]*\\.?[0-9]+)\\s*([KMB])?/i);
                return match ? parseCount(`${match[1]}${match[2] || ''}`) : null;
            };

            const getMetricFromElement = (el, fallbackLabel) => {
                if (!el) return null;

                const countText =
                    el.querySelector('[dir="ltr"] > span')?.textContent ||
                    el.querySelector('span[data-testid="app-text-transition-container"]')?.textContent ||
                    el.textContent ||
                    '';
                const parsed = parseCount(countText);
                if (parsed !== null) return parsed;

                const parsedFromText = parseCountFromText(countText);
                if (parsedFromText !== null) return parsedFromText;

                const aria = el.getAttribute('aria-label') || '';
                if (aria) {
                    const parsedFromAria = parseCountFromText(aria);
                    if (parsedFromAria !== null) return parsedFromAria;
                    if (fallbackLabel && aria.toLowerCase().includes(`0 ${fallbackLabel}`)) {
                        return 0;
                    }
                }
                return fallbackLabel ? 0 : null;
            };

            const getMetric = (article, testId, fallbackLabel) => {
                return getMetricFromElement(article.querySelector(`[data-testid="${testId}"]`), fallbackLabel);
            };

            const getMetricFromGroupLabel = (article, labels) => {
                for (const group of article.querySelectorAll('[role="group"][aria-label]')) {
                    const aria = group.getAttribute('aria-label') || '';
                    const parts = aria.split(/[,•·]/);
                    for (const part of parts) {
                        const lower = part.toLowerCase();
                        if (labels.some(label => lower.includes(label))) {
                            const parsed = parseCountFromText(part);
                            if (parsed !== null) return parsed;
                        }
                    }
                }
                return null;
            };

            const getLabeledMetricFromElement = (el, labels) => {
                if (!el) return null;

                const candidates = [
                    el.getAttribute('aria-label') || '',
                    el.textContent || '',
                ];

                for (const raw of candidates) {
                    const parts = raw.split(/[,•·\\n]/);
                    for (const part of parts) {
                        const lower = part.toLowerCase();
                        if (labels.some(label => lower.includes(label))) {
                            const parsed = parseCountFromText(part);
                            if (parsed !== null) return parsed;
                        }
                    }
                }

                return null;
            };

            const getImpressions = (article) => {
                const labels = ['view', 'views', 'impression', 'impressions'];
                const selectors = [
                    '[data-testid="analytics"]',
                    '[aria-label*="view" i]',
                    '[aria-label*="impression" i]',
                    '[aria-label*="analytics" i]',
                    'a[href*="/analytics"]',
                    'a[href*="/i/tweet_activity"]',
                ];

                for (const selector of selectors) {
                    for (const el of article.querySelectorAll(selector)) {
                        const text = `${el.getAttribute('aria-label') || ''} ${el.textContent || ''}`.toLowerCase();
                        if (!text.includes('view') && !text.includes('analytics') && !text.includes('impression')) continue;

                        const parsed = getLabeledMetricFromElement(el, labels);
                        if (parsed !== null) return parsed;

                        if (selector === '[data-testid="analytics"]') {
                            const fallback = getMetricFromElement(el, null);
                            if (fallback !== null) return fallback;
                        }
                    }
                }

                return getMetricFromGroupLabel(article, labels);
            };

            const articles = document.querySelectorAll('article[data-testid="tweet"]');
            const results = [];

            for (const article of articles) {
                const textEl = article.querySelector('[data-testid="tweetText"]');
                const text = textEl ? textEl.innerText.trim() : '';

                const userNameEl = article.querySelector('[data-testid="User-Name"]');
                let displayName = '', handle = '';
                if (userNameEl) {
                    const nameSpan = userNameEl.querySelector('span span');
                    displayName = nameSpan ? nameSpan.innerText.trim() : '';
                    for (const link of userNameEl.querySelectorAll('a[href^="/"]')) {
                        const href = link.getAttribute('href') || '';
                        if (href.startsWith('/') && !href.includes('/status/')) {
                            handle = '@' + href.substring(1).split('/')[0];
                            break;
                        }
                    }
                }

                let url = 'unknown';
                let postedAt = '';
                const timeEl = article.querySelector('time');
                if (timeEl) {
                    postedAt = timeEl.getAttribute('datetime') || '';
                    const a = timeEl.closest('a');
                    if (a) url = 'https://x.com' + a.getAttribute('href');
                }

                if (!text) continue;

                results.push({
                    author: handle,
                    author_name: displayName,
                    text,
                    url,
                    posted_at: postedAt,
                    replies: getMetric(article, 'reply', 'reply'),
                    retweets: getMetric(article, 'retweet', 'repost'),
                    likes: getMetric(article, 'like', 'like'),
                    impressions: getImpressions(article),
                });
            }
            return results;
        }
    """)


async def wait_for_timeline_ready(page, timeout_ms: int = 120000) -> bool:
    """
    Wait until the X home timeline is visibly loaded enough for scraping.
    Returns True when tweet articles are present, False on timeout.
    """
    deadline = asyncio.get_running_loop().time() + (timeout_ms / 1000)

    while asyncio.get_running_loop().time() < deadline:
        login_signal = await check_for_login_required(page)
        if login_signal:
            print(f"\n   🔐 Login required while waiting: '{login_signal}'")
            return False

        challenge = await check_for_challenge(page)
        if challenge:
            print(f"\n   ⚠️  CHALLENGE DETECTED while waiting: '{challenge}'")
            return False

        try:
            current_url = page.url
            if "/login" in current_url or "/i/flow/" in current_url:
                await asyncio.sleep(2)
                continue

            tweets = await page.locator('article[data-testid="tweet"]').count()
            if tweets > 0:
                return True
        except Exception:
            pass

        await asyncio.sleep(2)

    return False


# ─── Main Agent Loop ─────────────────────────────────────────

async def agent_loop(page, config, all_tweets: dict, anchor: dict | None):
    """
    Scrolls top→bottom collecting tweets.
    - No anchor (run 1): stops at max_tweets.
    - Anchor present: stops when anchor tweet is seen (no count cap).
    Returns (new_tweets_added, first_tweet_of_this_run, run_meta).
    """
    pause_min          = config["scrolling"].get("scroll_pause_min", 1.5)
    pause_max          = config["scrolling"].get("scroll_pause_max", 3.5)
    max_tweets         = config["scrolling"]["max_tweets"]          # first run cap
    max_tweets_anchor  = config["scrolling"].get("max_tweets_anchor", 1000)  # subsequent run cap
    max_iter           = config["scrolling"].get("max_iterations", 300)
    max_session_minutes = float(config["scrolling"].get("max_session_minutes", 18) or 18)
    stale_round_limit = int(config["scrolling"].get("stale_round_limit", 12) or 12)

    anchor_url  = anchor["url"] if anchor else None
    anchor_mode = anchor_url is not None

    first_tweet_this_run = None   # will become the new anchor
    new_count_total      = 0
    stale_rounds         = 0
    anchor_found         = False
    stop_reason          = "max_iterations"
    iterations_completed = 0
    session_started_at = time.monotonic()

    mode_label = f"anchor mode — running until anchor found (cap: {max_tweets_anchor})" if anchor_mode \
                 else f"first run — collecting up to {max_tweets} tweets"
    print(f"\n🤖 Agent loop starting  ({mode_label})\n")

    for iteration in range(1, max_iter + 1):
        iterations_completed = iteration

        if max_session_minutes > 0:
            elapsed_minutes = (time.monotonic() - session_started_at) / 60.0
            if elapsed_minutes >= max_session_minutes:
                stop_reason = "session_time_limit"
                print(f"\n   ⏱️  Session budget reached after {elapsed_minutes:.1f} minutes.")
                break

        # ── Challenge detection (every iteration) ────────────────
        challenge = await check_for_challenge(page)
        if challenge:
            print(f"\n   ⚠️  CHALLENGE DETECTED: '{challenge}'")
            print("   🛑 Stopping scrape to protect your account.")
            print("   👉 Open the browser, resolve the challenge manually, then re-run.")
            save_tweets(all_tweets)
            return new_count_total, first_tweet_this_run, {
                "anchor_mode": anchor_mode,
                "anchor_found": False,
                "stop_reason": "challenge_detected",
                "iterations_completed": iterations_completed,
            }

        # ── Capture before scrolling ─────────────────────────────
        before_scroll = _ingest_visible_tweets(
            await read_tweets_from_dom(page),
            all_tweets,
            anchor_url=anchor_url,
            first_tweet_this_run=first_tweet_this_run,
        )
        first_tweet_this_run = before_scroll["first_tweet_this_run"]
        new_this_iter = before_scroll["new_count"]
        duplicate_this_iter = before_scroll["duplicate_count"]
        visible_urls = set(before_scroll["visible_urls"])
        new_count_total += before_scroll["new_count"]

        if before_scroll["anchor_found"]:
            anchor_found = True
            stop_reason = "anchor_found"
            print(f"\n   🔖 Anchor tweet found — run complete.")
            break

        # ── Human-like scroll then extract ───────────────────────
        await page.mouse.move(
            random.randint(300, 900),
            random.randint(200, 650),
        )

        viewport_height = page.viewport_size["height"] if page.viewport_size else 900
        scroll_px = random.randint(
            max(int(viewport_height * 0.55), 250),
            max(int(viewport_height * 0.75), 350),
        )

        await page.mouse.wheel(0, scroll_px)

        # Base pause with jitter
        await asyncio.sleep(random.uniform(pause_min, pause_max))

        # ~1 in 8 scrolls: simulate pausing to read (4–9 s)
        if random.random() < 0.12:
            await asyncio.sleep(random.uniform(4.0, 9.0))

        # ── Capture after scrolling ──────────────────────────────
        after_scroll = _ingest_visible_tweets(
            await read_tweets_from_dom(page),
            all_tweets,
            anchor_url=anchor_url,
            first_tweet_this_run=first_tweet_this_run,
        )
        first_tweet_this_run = after_scroll["first_tweet_this_run"]
        new_this_iter += after_scroll["new_count"]
        duplicate_this_iter += after_scroll["duplicate_count"]
        visible_urls.update(after_scroll["visible_urls"])
        new_count_total += after_scroll["new_count"]

        if after_scroll["anchor_found"]:
            anchor_found = True
            stop_reason = "anchor_found"
            print(f"\n   🔖 Anchor tweet found — run complete.")
            break

        if new_this_iter > 0:
            stale_rounds = 0
        else:
            stale_rounds += 1

        total = len(all_tweets)
        print(
            f"   [{iteration:3d}/{max_iter}]  "
            f"visible={len(visible_urls):2d}  +{new_this_iter:2d} new  "
            f"dup={duplicate_this_iter:2d}  |  Total: {total}  |  Stale: {stale_rounds}"
            + (f"  |  Seeking anchor..." if anchor_mode else f"/{max_tweets}")
        )

        # Save progress every 5 iters
        if iteration % 5 == 0:
            save_tweets(all_tweets)

        # Run-1 stop: hit tweet target
        if not anchor_mode and total >= max_tweets:
            stop_reason = "target_reached"
            print(f"\n   ✅ Target reached — {total} tweets collected.")
            break

        # Anchor-mode safety cap
        if anchor_mode and new_count_total >= max_tweets_anchor:
            stop_reason = "anchor_mode_tweet_cap"
            print(f"\n   ✅ Anchor-mode cap reached — {new_count_total} new tweets collected.")
            break

        # Safety: too many stale rounds
        if stale_rounds >= stale_round_limit:
            stop_reason = "stale_rounds"
            print(f"\n   ⏹️  No new tweets for {stale_rounds} rounds. Stopping.")
            break

    save_tweets(all_tweets)
    return new_count_total, first_tweet_this_run, {
        "anchor_mode": anchor_mode,
        "anchor_found": anchor_found,
        "stop_reason": stop_reason,
        "iterations_completed": iterations_completed,
        "gap_risk": bool(anchor_mode and not anchor_found),
    }


# ─── Semantic scoring (all-mpnet-base-v2) ────────────────────
# Local model, zero API cost. Significantly better than MiniLM.
# Returns net score (keep_max - skip_max). Filter if > threshold.

MIN_TWEET_LENGTH = 60

KEEP_EXEMPLARS = [
    "habits, impulses, emotional reactions, procrastination, and behavioral patterns that shape our lives",
    "anger, anxiety, overthinking, addiction, and how we manage our emotional states",
    "willpower, self-control, dopamine, craving, and the psychology of discipline",
    "why we scroll mindlessly, get distracted, and operate on autopilot",
    "self-observation, mindfulness, inner life, present moment awareness, and consciousness",
    "introspection, self-reflection, witnessing thoughts, stillness, and meditation",
    "the practice of watching your own mind without judgment",
    "poetic or metaphorical wisdom about acceptance, non-resistance, letting go, and making space for what arises",
    "using nature as a metaphor for equanimity — not clinging to good, not resisting bad, simply holding space",
    "stoic philosophy applied to daily life — Marcus Aurelius, Seneca, Epictetus",
    "Buddhist philosophy: impermanence, non-attachment, dukkha, mindfulness",
    "Taoist wisdom, Lao Tzu, the way of nature, and effortless action",
    "Jungian psychology: shadow work, archetypes, and the unconscious mind",
    "philosophy applied to how we live, think, decide, and find meaning",
    "ancient Greek and Western wisdom traditions applied to the human condition",
    "Vedic philosophy, Upanishads, Advaita Vedanta — Brahman, Atman, and the nature of consciousness",
    "Bhagavad Gita, karma yoga, nishkama karma, and living one's dharma with detachment",
    "Sanskrit wisdom, Hindu philosophy, Sanatan Dharma, and ancient Indian thought",
    "Yoga philosophy, Patanjali, chitta vritti nirodha, and the path to liberation",
    "Swami Vivekananda, Ramana Maharshi, Sri Aurobindo, Nisargadatta — Indian spiritual masters and their teachings",
    "Maya, illusion, and the nature of perceived reality in Advaita Vedanta",
    "the four purusharthas: artha, kama, dharma, moksha — and living a complete life",
    "Yoga Vasishtha, Kashmir Shaivism, Tantra philosophy, and the Indic view of consciousness",
    "devotional wisdom, bhakti, surrender, and the relationship between self and the divine",
    "cognitive biases, mental models, heuristics, and how the mind distorts reality",
    "the psychology of decision-making, irrational behavior, and thinking traps",
    "belief systems, reframing, and how our perception shapes our experience",
    "ego, desire, attachment, suffering, and the path toward liberation",
    "spiritual growth, inner transformation, inner work, and the journey toward self-realization",
    "meaning, purpose, identity, and the search for what truly matters in life",
    "compassion, equanimity, letting go, acceptance, and cultivating inner peace",
    "the root causes of procrastination, self-sabotage, and why we resist growth",
    "human nature, behavioral psychology, and what drives our deepest motivations",
    "comfort zone, growth mindset, limiting beliefs, and silencing the inner critic",
]

SKIP_EXEMPLARS = [
    "breaking news, political events, election results, and government policy updates",
    "sports match scores, tournament results, cricket, football, basketball, league standings",
    "stock market prices, cryptocurrency rates, IPO listings, and financial market analysis",
    "celebrity gossip, movie box office numbers, music album releases, award ceremonies",
    "medical research studies, clinical trials, cardiovascular health data, biomarkers",
    "product promotions, webinar registrations, online course advertisements, buy now offers",
    "personal life updates and social posts with no self-reflection or philosophical angle",
    "tech industry news, startup funding rounds, product launches, company announcements",
    "religious ritual event announcements, festival schedules, puja timings, fasting days",
    "wildlife conservation news, animal sightings, environmental policy reports",
    "travel photos, food posts, lifestyle content with no introspective dimension",
]

_model     = None
_keep_embs = None
_skip_embs = None


def _load_model():
    global _model, _keep_embs, _skip_embs, _lang_detect, LangDetectException
    if _model is not None:
        return
    if _lang_detect is None:
        try:
            from langdetect import detect as _detect
            from langdetect import LangDetectException as _LangDetectException
        except ImportError:
            print("\n❌ langdetect not installed.")
            print("   pip install langdetect\n")
            raise
        _lang_detect = _detect
        LangDetectException = _LangDetectException
    try:
        from sentence_transformers import SentenceTransformer
    except ImportError:
        print("\n❌ sentence-transformers not installed.")
        print("   pip install sentence-transformers\n")
        raise
    print("   🧠 Loading semantic model (first call only)...")
    import transformers; transformers.logging.set_verbosity_error()
    _model     = SentenceTransformer("all-mpnet-base-v2")
    _keep_embs = _model.encode(KEEP_EXEMPLARS, convert_to_tensor=True)
    _skip_embs = _model.encode(SKIP_EXEMPLARS, convert_to_tensor=True)
    print("   ✅ Model ready.\n")


def score_tweets_batch(tweets: list[dict], **_) -> None:
    """Score all unscored tweets in-place using the local mpnet model."""
    to_score = [t for t in tweets if "score" not in t]
    if not to_score:
        return

    _load_model()
    from sentence_transformers import util as st_util

    print(f"   🧠 Scoring {len(to_score)} tweets locally...")
    for tweet in to_score:
        text = tweet.get("text", "")
        if len(text.strip()) < MIN_TWEET_LENGTH:
            tweet["score"] = -999.0
            continue
        try:
            lang = _lang_detect(text)
        except LangDetectException:
            lang = "unknown"
        if lang != "en":
            tweet["score"] = -999.0
            continue
        emb            = _model.encode(text, convert_to_tensor=True)
        keep_score     = float(st_util.cos_sim(emb, _keep_embs).max())
        skip_score     = float(st_util.cos_sim(emb, _skip_embs).max())
        tweet["score"] = keep_score - skip_score


def _clamp(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return max(low, min(high, value))


def _normalize_relevance(raw_score: float, threshold: float) -> float:
    ceiling = threshold + 0.45
    if raw_score <= threshold:
        return 0.0
    return _clamp((raw_score - threshold) / max(ceiling - threshold, 1e-9))


def _parse_posted_at(raw_value: str) -> datetime | None:
    if not raw_value:
        return None
    try:
        return datetime.fromisoformat(raw_value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _freshness_score(posted_at: str, now: datetime) -> float:
    dt = _parse_posted_at(posted_at)
    if dt is None:
        return 0.35
    age_hours = max((now - dt.astimezone(timezone.utc)).total_seconds() / 3600, 0.0)
    return _clamp(math.exp(-age_hours / 18.0))


def _safe_metric(value) -> float:
    if value is None:
        return 0.0
    try:
        return max(float(value), 0.0)
    except (TypeError, ValueError):
        return 0.0


def _quote_style_signals(text: str) -> tuple[bool, str]:
    text = (text or "").strip()
    if not text:
        return False, ""

    compact = " ".join(text.split())
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    lower = compact.lower()

    if len(lines) >= 2 and lines[-1].startswith(("—", "-", "–")):
        return True, "attribution_line"

    if any(
        marker in lower for marker in (
            "quote of the day",
            "daily quote",
            "favorite quote",
            "this quote",
            "a quote that",
        )
    ):
        return True, "explicit_quote_marker"

    if compact.startswith(("\"", "'", "“", "‘")) and compact.endswith(("\"", "'", "”", "’")):
        return True, "wrapped_in_quotes"

    if (
        len(lines) >= 2
        and len(lines[-1].split()) <= 4
        and lines[-1].replace(".", "").istitle()
        and compact.count('"') + compact.count("“") + compact.count("”") >= 2
    ):
        return True, "quote_plus_author"

    if any(name in lower for name in ("marcus aurelius", "seneca", "epictetus", "rumi", "carl jung")):
        if any(symbol in compact for symbol in ("—", "–", "\"", "“", "”")):
            return True, "named_attribution"

    return False, ""


def compute_composite_scores(tweets: list[dict], threshold: float) -> None:
    if not tweets:
        return

    now = datetime.now(timezone.utc)
    traction_raw = []
    rate_raw = []
    freshness_raw = []

    for tweet in tweets:
        likes = _safe_metric(tweet.get("likes"))
        retweets = _safe_metric(tweet.get("retweets"))
        replies = _safe_metric(tweet.get("replies"))
        impressions = _safe_metric(tweet.get("impressions"))

        traction = math.log1p(likes + (2.0 * retweets) + (1.5 * replies))
        rate = math.log1p((1000.0 * (likes + (2.0 * retweets) + replies)) / max(impressions, 1.0)) if impressions > 0 else None
        fresh = _freshness_score(tweet.get("posted_at", ""), now)

        tweet["_traction_raw"] = traction
        tweet["_engagement_rate_raw"] = rate
        tweet["_freshness_raw"] = fresh
        traction_raw.append(traction)
        freshness_raw.append(fresh)
        if rate is not None:
            rate_raw.append(rate)

    traction_max = max(traction_raw) if traction_raw else 1.0
    freshness_max = max(freshness_raw) if freshness_raw else 1.0
    rate_max = max(rate_raw) if rate_raw else None

    for tweet in tweets:
        relevance = _clamp(_safe_metric(tweet.get("llm_relevance_score")))
        if relevance == 0.0:
            relevance = _normalize_relevance(float(tweet.get("score", -999.0)), threshold)
        freshness = tweet["_freshness_raw"] / freshness_max if freshness_max > 0 else 0.0
        traction = tweet["_traction_raw"] / traction_max if traction_max > 0 else 0.0
        rate_raw_value = tweet["_engagement_rate_raw"]
        engagement_rate = (rate_raw_value / rate_max) if (rate_raw_value is not None and rate_max and rate_max > 0) else None

        if engagement_rate is not None:
            composite = (0.50 * relevance) + (0.20 * freshness) + (0.20 * traction) + (0.10 * engagement_rate)
        else:
            composite = (0.55 * relevance) + (0.25 * freshness) + (0.20 * traction)

        is_quote_style, quote_reason = _quote_style_signals(tweet.get("text", ""))
        if is_quote_style:
            composite = 0.0

        tweet["relevance_score"] = round(relevance, 4)
        tweet["freshness_score"] = round(freshness, 4)
        tweet["traction_score"] = round(traction, 4)
        if engagement_rate is not None:
            tweet["engagement_rate_score"] = round(engagement_rate, 4)
        else:
            tweet.pop("engagement_rate_score", None)
        tweet["quote_style"] = is_quote_style
        tweet["quote_style_reason"] = quote_reason
        tweet["quote_docked_to_zero"] = is_quote_style
        tweet["composite_score"] = round(composite, 4)

        del tweet["_traction_raw"]
        del tweet["_engagement_rate_raw"]
        del tweet["_freshness_raw"]


# ─── Two-stage filtering ─────────────────────────────────────
# Stage 1: mpnet pre-filter (loose — only cuts obvious junk)
# Lower threshold catches tangential but relevant tweets
PREFILTER_THRESHOLD = 0.3   # was 0.6

RELEVANCE_SYSTEM_PROMPT = """You decide if a tweet is relevant for @vedaselfhelp to review - \
a handle that bridges Vedic philosophy, Upanishadic wisdom, and modern psychology/self-help.

A tweet is RELEVANT if it touches any of these (explicitly OR implicitly):
- Human psychology: habits, emotions, anxiety, overthinking, attention, behavior, motivation
- Inner life: self-awareness, reflection, consciousness, stillness, presence, ego
- Philosophy: meaning, identity, suffering, acceptance, impermanence, perception, reality
- Self-development: growth, patterns, beliefs, transformation, discipline, purpose
- Vedic/Eastern/Western wisdom traditions (Stoic, Buddhist, Taoist, Hindu, Jungian, etc.)
- The mechanics of the mind: distraction, craving, avoidance, loops, compulsion

A tweet is NOT RELEVANT if it's primarily about:
- News, politics, sports, markets, celebrity, product promotions
- Pure life updates with no reflective or philosophical dimension
- Religious rituals, festival announcements, event schedules

Important: Relevance is about the UNDERLYING THEME, not surface vocabulary.
A tweet about "why I can't stop doomscrolling" is relevant.
A tweet quoting Bhagavad Gita but just sharing a festival date is not.

If relevant=true, score the tweet on TWO dimensions:
1. topic_fit_score:
- How directly the tweet matches the core topics above
- High when the tweet is clearly about psychology, inner life, philosophy, or self-development
- Low when overlap is vague, incidental, or just surface-level

2. philosophical_depth_score:
- How much reflective, introspective, or meaning-oriented substance the tweet has
- High when it contains real insight about mind, self, suffering, awareness, behavior, or meaning
- Low when it is generic advice, slogan-like inspiration, or shallow spiritual language

Then compute an overall relevance_score as a weighted blend:
- relevance_score = 0.65 * topic_fit_score + 0.35 * philosophical_depth_score

Scoring guidance:
- Use the full range from 0.00 to 1.00
- Avoid defaulting to common values like 0.65 or 0.85
- If two tweets differ meaningfully, their scores should differ too
- Only use 0.85+ for unusually strong alignment
- Do NOT score virality, author popularity, recency, likes, or writing quality

Use these anchor meanings:
- 0.00-0.30: not relevant to the handle
- 0.31-0.55: weak or generic overlap
- 0.56-0.75: relevant and strong enough to keep
- 0.76-0.90: strongly aligned with the handle
- 0.91-1.00: exceptional fit

Reply with JSON only:
{"relevant": true/false, "topic_fit_score": 0.0-1.0, "philosophical_depth_score": 0.0-1.0, "relevance_score": 0.0-1.0, "reason": "one short sentence"}"""


def _blend_llm_relevance(result: dict) -> tuple[float, float, float]:
    topic_fit = _clamp(_safe_metric(result.get("topic_fit_score")))
    depth = _clamp(_safe_metric(result.get("philosophical_depth_score")))
    explicit = _clamp(_safe_metric(result.get("relevance_score")))

    blended = (0.65 * topic_fit) + (0.35 * depth)
    if topic_fit > 0.0 or depth > 0.0:
        return round(topic_fit, 4), round(depth, 4), round(blended, 4)
    return round(topic_fit, 4), round(depth, 4), round(explicit, 4)


def _extract_json_object(raw: str) -> dict:
    raw = (raw or "").strip()
    if not raw:
        raise ValueError("Empty model response.")

    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        start = raw.find("{")
        end = raw.rfind("}")
        if start != -1 and end != -1 and end > start:
            candidate = raw[start:end + 1]
            return json.loads(candidate)
        raise


def _ollama_json(host: str, path: str, timeout_seconds: int = 5) -> dict:
    with urlrequest.urlopen(f"{host}{path}", timeout=timeout_seconds) as response:
        return json.loads(response.read().decode("utf-8"))


def _ollama_is_ready(host: str) -> bool:
    try:
        _ollama_json(host, "/api/tags", timeout_seconds=3)
        return True
    except Exception:
        return False


def ensure_ollama_ready(cfg: dict, model: str) -> subprocess.Popen | None:
    host = (cfg.get("host") or "http://127.0.0.1:11434").rstrip("/")
    startup_timeout = max(int(cfg.get("startup_timeout_seconds", 30) or 30), 1)
    auto_start = bool(cfg.get("auto_start", True))

    if _ollama_is_ready(host):
        print(f"   🟢 Ollama already running at {host}")
    else:
        if not auto_start:
            raise RuntimeError(f"Ollama is not running at {host}. Start `ollama serve` or enable llm_filter.auto_start.")
        ollama_bin = shutil.which("ollama")
        if not ollama_bin:
            raise RuntimeError("Ollama CLI not found on PATH. Install Ollama or add it to PATH.")

        log_path = SCRIPT_DIR / "logs" / "ollama-serve.log"
        log_path.parent.mkdir(exist_ok=True)
        print(f"   🚀 Starting Ollama server... logs: {log_path}")
        log_file = open(log_path, "ab")
        process = subprocess.Popen(
            [ollama_bin, "serve"],
            stdout=log_file,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        process._replyscreener_log_file = log_file  # keep the log handle alive with the process

        deadline = time.time() + startup_timeout
        while time.time() < deadline:
            if process.poll() is not None:
                raise RuntimeError(f"`ollama serve` exited early with code {process.returncode}. Check {log_path}.")
            if _ollama_is_ready(host):
                print(f"   ✅ Ollama ready at {host}")
                break
            time.sleep(1)
        else:
            raise RuntimeError(f"Ollama did not become ready within {startup_timeout}s. Check {log_path}.")
    try:
        tags = _ollama_json(host, "/api/tags", timeout_seconds=5)
        models = {
            item.get("name", "")
            for item in tags.get("models", [])
        }
        model_roots = {name.split(":", 1)[0] for name in models if name}
        if model not in models and model.split(":", 1)[0] not in model_roots:
            ollama_bin = shutil.which("ollama")
            if not ollama_bin:
                raise RuntimeError("Ollama CLI not found on PATH. Install Ollama or add it to PATH.")
            print(f"   📦 Pulling missing Ollama model: {model}")
            subprocess.run([ollama_bin, "pull", model], check=True)
    except Exception as exc:
        raise RuntimeError(f"Ollama is running, but model `{model}` could not be verified/pulled: {exc}") from exc

    return locals().get("process")


def llm_filter_tweets(tweets: list[dict], model: str) -> list[dict]:
    """Stage 2: LLM relevance filter for tweets that passed the mpnet pre-filter."""
    cfg = load_config().get("llm_filter", {})
    provider = (cfg.get("provider") or "ollama").strip().lower()
    host = (cfg.get("host") or "http://127.0.0.1:11434").rstrip("/")
    timeout_seconds = max(int(cfg.get("timeout_seconds", 60) or 60), 1)

    if provider != "ollama":
        raise RuntimeError(f"Unsupported llm_filter provider: {provider}")

    ensure_ollama_ready(cfg, model)

    relevant = []
    print(f"   🤖 LLM-filtering {len(tweets)} pre-filtered tweets with {provider}/{model}...")
    for i, tweet in enumerate(tweets, 1):
        try:
            result = None
            last_raw = ""
            for attempt in range(2):
                messages = [
                    {"role": "system", "content": RELEVANCE_SYSTEM_PROMPT},
                    {"role": "user", "content": f"TWEET: {tweet['text']}"},
                ]
                if attempt == 1:
                    messages.append({
                        "role": "user",
                        "content": "Your previous answer was malformed. Reply again with valid JSON only. Keep reason under 12 words.",
                    })

                payload = {
                    "model": model,
                    "stream": False,
                    "format": "json",
                    "messages": messages,
                    "options": {
                        "temperature": 0,
                        "num_predict": 140,
                    },
                }
                req = urlrequest.Request(
                    f"{host}/api/chat",
                    data=json.dumps(payload).encode("utf-8"),
                    headers={"Content-Type": "application/json"},
                    method="POST",
                )
                with urlrequest.urlopen(req, timeout=timeout_seconds) as response:
                    body = json.loads(response.read().decode("utf-8"))
                last_raw = (body.get("message") or {}).get("content", "").strip()
                try:
                    result = _extract_json_object(last_raw)
                    break
                except json.JSONDecodeError:
                    if attempt == 1:
                        raise
            if result is None:
                raise ValueError(f"Could not parse model JSON: {last_raw[:200]}")
            topic_fit_score, philosophical_depth_score, relevance_score = _blend_llm_relevance(result)
            tweet["llm_relevant"] = result.get("relevant", False)
            tweet["llm_reason"]   = result.get("reason", "")
            tweet["llm_topic_fit_score"] = topic_fit_score
            tweet["llm_philosophical_depth_score"] = philosophical_depth_score
            tweet["llm_relevance_score"] = relevance_score
            if tweet["llm_relevant"]:
                if tweet["llm_relevance_score"] <= 0:
                    tweet["llm_relevance_score"] = 0.6
                relevant.append(tweet)
                print(
                    f"      [{i}/{len(tweets)}] ✓  {tweet.get('author','?')} "
                    f"(overall={tweet['llm_relevance_score']:.2f}, topic={topic_fit_score:.2f}, depth={philosophical_depth_score:.2f}) "
                    f"— {tweet['llm_reason']}"
                )
            else:
                print(
                    f"      [{i}/{len(tweets)}] ✗  {tweet.get('author','?')} "
                    f"(overall={tweet['llm_relevance_score']:.2f}, topic={topic_fit_score:.2f}, depth={philosophical_depth_score:.2f}) "
                    f"— {tweet['llm_reason']}"
                )
        except Exception as e:
            # On error, keep the tweet (fail open)
            tweet["llm_relevant"] = True
            tweet["llm_reason"]   = f"[filter error: {e}]"
            tweet["llm_topic_fit_score"] = 0.0
            tweet["llm_philosophical_depth_score"] = 0.0
            tweet["llm_relevance_score"] = _normalize_relevance(float(tweet.get("score", -999.0)), PREFILTER_THRESHOLD)
            relevant.append(tweet)
            print(f"      [{i}/{len(tweets)}] ?  filter error: {e}")
    return relevant


# ─── Google Sheets export ─────────────────────────────────────

GSHEETS_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def _gspread_client(credentials_file: str):
    try:
        import gspread
        from google.oauth2.service_account import Credentials
    except ImportError as exc:
        raise RuntimeError("gspread and google-auth are required for Google Sheets export.") from exc
    creds = Credentials.from_service_account_file(credentials_file, scopes=GSHEETS_SCOPES)
    return gspread.authorize(creds)


def _get_gsheets_client_and_sheet(config: dict):
    gs_cfg           = config["google_sheets"]
    spreadsheet_id   = gs_cfg["spreadsheet_id"]
    credentials_file = str(SCRIPT_DIR / gs_cfg["credentials_file"])
    gc = _gspread_client(credentials_file)
    sh = gc.open_by_key(spreadsheet_id)
    return sh, spreadsheet_id


def _prepare_ranked_worksheet(sh, worksheet_title: str, rows: int, cols: int):
    try:
        ws = sh.worksheet(worksheet_title)
        ws.clear()
        if ws.row_count < rows or ws.col_count < cols:
            ws.resize(rows=max(ws.row_count, rows), cols=max(ws.col_count, cols))
    except Exception:
        ws = sh.add_worksheet(title=worksheet_title, rows=rows, cols=cols)

    for stale_ws in list(sh.worksheets()):
        if stale_ws.id == ws.id:
            continue
        if stale_ws.title.startswith("Run_") or stale_ws.title.startswith("New_"):
            sh.del_worksheet(stale_ws)

    return ws


def _worksheet_from_config_or_gid(sh, config: dict, gid: str | None = None):
    if gid:
        for ws in sh.worksheets():
            if str(ws.id) == str(gid):
                return ws
        raise RuntimeError(f"No worksheet found with gid={gid}")
    worksheet_title = config.get("google_sheets", {}).get("worksheet_title", "Filtered Ranked Tweets")
    return sh.worksheet(worksheet_title)


def _num(value, default=0.0):
    if value in (None, ""):
        return default
    try:
        return float(str(value).replace(",", ""))
    except ValueError:
        return default


def _bool_value(value) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"true", "yes", "1"}


def load_ranked_sheet_tweets(config: dict, gid: str | None = None) -> tuple[list[dict], str]:
    sh, spreadsheet_id = _get_gsheets_client_and_sheet(config)
    ws = _worksheet_from_config_or_gid(sh, config, gid)
    values = ws.get_all_values()
    if not values:
        return [], f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit#gid={ws.id}"

    headers = values[0]
    rows = values[1:]
    idx = {header: pos for pos, header in enumerate(headers)}

    def cell(row, header, default=""):
        pos = idx.get(header)
        if pos is None or pos >= len(row):
            return default
        return row[pos]

    tweets = []
    for row in rows:
        if not any(str(value).strip() for value in row):
            continue
        tweet = {
            "author_name": cell(row, "Author Name"),
            "author": cell(row, "Handle"),
            "text": cell(row, "Tweet Text"),
            "url": cell(row, "Tweet URL"),
            "posted_at": cell(row, "Posted At"),
            "replies": _num(cell(row, "Replies")),
            "retweets": _num(cell(row, "Retweets")),
            "likes": _num(cell(row, "Likes")),
            "impressions": _num(cell(row, "Impressions")),
            "score": _num(cell(row, "Local Prefilter Score"), default=0.0),
            "quote_style": _bool_value(cell(row, "Quote Style")),
            "quote_style_reason": cell(row, "Quote Style Reason"),
            "quote_docked_to_zero": _bool_value(cell(row, "Quote Docked To Zero")),
        }
        if tweet["text"]:
            tweets.append(tweet)

    return tweets, f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit#gid={ws.id}"


def export_to_gsheets(tweets: list[dict], config: dict) -> str:
    sh, spreadsheet_id = _get_gsheets_client_and_sheet(config)
    worksheet_title = config.get("google_sheets", {}).get("worksheet_title", "Filtered Ranked Tweets")

    # Keep a single stable worksheet with only the final ranked output.
    ws = _prepare_ranked_worksheet(
        sh,
        worksheet_title=worksheet_title,
        rows=max(len(tweets) + 10, 50),
        cols=23,
    )

    headers = [
        "#", "Author Name", "Handle", "Tweet Text", "Tweet URL", "Posted At",
        "Replies", "Retweets", "Likes", "Impressions",
        "Local Prefilter Score", "LLM Topic Fit Score", "LLM Philosophical Depth Score", "LLM Relevance Score", "Freshness Score",
        "Traction Score", "Engagement Rate Score", "Quote Style", "Quote Style Reason", "Quote Docked To Zero", "Composite Score", "LLM Reason",
    ]
    ws.append_row(headers, value_input_option="RAW")

    rows = []
    for i, tweet in enumerate(tweets, 1):
        rows.append([
            i,
            tweet.get("author_name", ""),
            tweet.get("author", ""),
            tweet.get("text", ""),
            tweet.get("url", ""),
            tweet.get("posted_at", ""),
            tweet.get("replies", ""),
            tweet.get("retweets", ""),
            tweet.get("likes", ""),
            tweet.get("impressions", ""),
            round(tweet.get("score", 0.0), 4),
            round(tweet.get("llm_topic_fit_score", 0.0), 4),
            round(tweet.get("llm_philosophical_depth_score", 0.0), 4),
            round(tweet.get("relevance_score", 0.0), 4),
            round(tweet.get("freshness_score", 0.0), 4),
            round(tweet.get("traction_score", 0.0), 4),
            round(tweet.get("engagement_rate_score", 0.0), 4) if "engagement_rate_score" in tweet else "",
            tweet.get("quote_style", False),
            tweet.get("quote_style_reason", ""),
            tweet.get("quote_docked_to_zero", False),
            round(tweet.get("composite_score", 0.0), 4),
            tweet.get("llm_reason", ""),
        ])

    if rows:
        ws.append_rows(rows, value_input_option="RAW")

    sheet_url = f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}/edit#gid={ws.id}"
    print(f"   📊 Ranked sheet: {len(tweets)} tweets → {sheet_url}")
    return sheet_url


def rerank_existing_sheet(config: dict, gid: str | None = None) -> str:
    threshold = config.get("filtering", {}).get("semantic_threshold", PREFILTER_THRESHOLD)
    filter_model = config.get("llm_filter", {}).get("model", "phi3:mini")

    print("\n   📥 Loading existing Google Sheet rows...")
    sheet_tweets, source_url = load_ranked_sheet_tweets(config, gid=gid)
    print(f"   📄 Source sheet: {len(sheet_tweets)} rows → {source_url}")
    if not sheet_tweets:
        raise RuntimeError("The selected worksheet has no tweet rows to rank.")

    print(f"\n   🤖 Re-running Phi-3 ranking for existing sheet rows...")
    scored = llm_filter_tweets(sheet_tweets, model=filter_model)
    compute_composite_scores(scored, threshold)
    scored.sort(key=lambda t: t.get("composite_score", 0.0), reverse=True)
    print(f"   🎯 LLM filter matched {len(scored)} / {len(sheet_tweets)} tweets")

    all_tweets = load_saved_tweets()
    by_url = {
        tweet.get("url"): tweet
        for tweet in all_tweets.values()
        if tweet.get("url")
    }
    for tweet in scored:
        saved = by_url.get(tweet.get("url"))
        if saved is not None:
            saved.update({
                "llm_relevant": tweet.get("llm_relevant"),
                "llm_reason": tweet.get("llm_reason", ""),
                "llm_topic_fit_score": tweet.get("llm_topic_fit_score", 0.0),
                "llm_philosophical_depth_score": tweet.get("llm_philosophical_depth_score", 0.0),
                "llm_relevance_score": tweet.get("llm_relevance_score", 0.0),
                "relevance_score": tweet.get("relevance_score", 0.0),
                "freshness_score": tweet.get("freshness_score", 0.0),
                "traction_score": tweet.get("traction_score", 0.0),
                "engagement_rate_score": tweet.get("engagement_rate_score", ""),
                "quote_style": tweet.get("quote_style", False),
                "quote_style_reason": tweet.get("quote_style_reason", ""),
                "quote_docked_to_zero": tweet.get("quote_docked_to_zero", False),
                "composite_score": tweet.get("composite_score", 0.0),
            })
    save_tweets(all_tweets)

    print(f"\n   📤 Rewriting Google Sheet with repaired ranking...")
    sheet_url = export_to_gsheets(scored, config)
    append_run_history({
        "run_ts": datetime.now().strftime("%Y%m%d_%H%M%S"),
        "rank_sheet_only": True,
        "source_sheet_url": source_url,
        "matching_filter": len(scored),
        "sheet_url": sheet_url,
    })
    return sheet_url


# ─── Main ─────────────────────────────────────────────────────

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--export-only", action="store_true",
                        help="Skip scraping - score, filter, and export existing tweets")
    parser.add_argument("--rank-sheet-only", action="store_true",
                        help="Skip scraping/mpnet and rerun Phi-3 ranking on the current Google Sheet rows")
    parser.add_argument("--sheet-gid",
                        help="Optional Google Sheets gid to read for --rank-sheet-only")
    args = parser.parse_args()

    config     = load_config()
    anchor     = load_anchor()
    all_tweets = load_saved_tweets()

    print("=" * 60)
    print("  🐦 Twitter Scraper — DOM Edition")
    print("=" * 60)

    if args.rank_sheet_only:
        print("  Mode:    RANK SHEET ONLY")
        print("=" * 60)
        sheet_url = rerank_existing_sheet(config, gid=args.sheet_gid)
        print("\n" + "=" * 60)
        print("  ✅ Done!")
        print(f"  Sheet:                {sheet_url}")
        print("=" * 60)
        return

    # ── Scraping phase (skipped with --export-only) ───────────────
    if args.export_only:
        print(f"  Mode:    EXPORT ONLY (using {len(all_tweets)} saved tweets)")
        print("=" * 60)
        new_count    = 0
        new_tweet_keys = set()
        scrape_meta = {
            "anchor_mode": bool(anchor),
            "anchor_found": None,
            "stop_reason": "export_only",
            "iterations_completed": 0,
        }
    else:
        if anchor:
            print(f"  Mode:    CATCH-UP (anchor found from previous run)")
            print(f"  Anchor:  {anchor['author']}  {anchor['url']}")
            print(f"  Stored:  {len(all_tweets)} tweets from previous runs")
        else:
            print(f"  Mode:    FIRST RUN (collecting up to {config['scrolling']['max_tweets']} tweets)")
        print("=" * 60)

        keys_before    = set(all_tweets.keys())

        try:
            ensure_playwright_node()
            from playwright.async_api import async_playwright
        except ImportError:
            print("\n❌ playwright is not installed.")
            print("   pip install playwright && playwright install chromium\n")
            return

        async with async_playwright() as p:
            PROFILE_DIR.mkdir(exist_ok=True)
            context = await p.chromium.launch_persistent_context(
                user_data_dir=str(PROFILE_DIR),
                channel="chrome",           # use real installed Chrome, not bundled Chromium
                headless=False,
                args=["--disable-blink-features=AutomationControlled"],
                viewport={"width": 1280, "height": 900},
                # no user_agent override — let Chrome report its real version
            )
            page = await context.new_page()

            print("\n🌐 Opening Twitter/X...")
            await page.goto("https://x.com/home", timeout=60000)

            print("\n" + "=" * 60)
            saved_session = has_saved_x_session()
            if saved_session:
                print("  ✅ Saved X session found — you should already be logged in.")
            elif PROFILE_DIR.exists() and any(PROFILE_DIR.iterdir()):
                print("  ⚠️  Profile exists, but no authenticated X cookies were found.")
                print("     If X opens signed out, log in once in this Chrome window so the profile can refresh.")
            else:
                print("  👋 First run — please log in to Twitter in the browser window.")
            print("  Waiting for your home timeline to load automatically.")
            print("=" * 60)

            if not saved_session:
                await wait_for_manual_login()

            timeline_ready = await wait_for_timeline_ready(page)
            if not timeline_ready and await check_for_login_required(page):
                print("\n   🔐 X is asking for sign-in. Keeping Chrome open so you can refresh the session.")
                await wait_for_manual_login()
                await page.goto("https://x.com/home", timeout=60000)
                timeline_ready = await wait_for_timeline_ready(page)

            if not timeline_ready:
                print("\n   🛑 Timeline did not become ready in time.")
                print("   👉 Confirm Chrome is logged in and X home is accessible, then re-run.")
                await context.close()
                return

            print("\n   ✅ Timeline detected. Taking over...\n")
            await asyncio.sleep(random.uniform(2.5, 4.5))   # human landing pause

            new_count, first_tweet, scrape_meta = await agent_loop(page, config, all_tweets, anchor)

            # Update anchor to the first tweet seen this run
            if first_tweet and first_tweet.get("url", "unknown") != "unknown":
                save_anchor(first_tweet)
                print(f"\n   🔖 New anchor saved: {first_tweet['author']}  {first_tweet['url']}")
            else:
                print("\n   ⚠️  Could not determine anchor tweet (no valid URL seen).")

            new_tweet_keys = set(all_tweets.keys()) - keys_before
            if scrape_meta.get("anchor_mode") and not scrape_meta.get("anchor_found"):
                print(f"\n   ⚠️  Previous anchor was not found. Stop reason: {scrape_meta.get('stop_reason')}")
                print("   This run may not have fully caught up to the previous run.")

            print("\n   Browser closes in 10 seconds...")
            await asyncio.sleep(10)
            await context.close()

    # ── Score & filter (current run's tweets only) ───────────────
    all_list = list(all_tweets.values())

    # In a normal run: process only new tweets collected this session.
    # In --export-only: no scraping happened, so fall back to all saved tweets.
    if args.export_only:
        run_tweets = all_list
    else:
        run_tweets = [all_tweets[k] for k in new_tweet_keys if k in all_tweets]

    llm_filter_cfg = config.get("llm_filter", {})
    filter_model = llm_filter_cfg.get("model", "phi3:mini")

    # ── Stage 1: mpnet pre-filter (loose) ────────────────────────
    threshold = config.get("filtering", {}).get("semantic_threshold", PREFILTER_THRESHOLD)
    print(f"\n   🔍 Scoring {len(run_tweets)} tweets from this run...")
    score_tweets_batch(run_tweets)
    save_tweets(all_tweets)  # persist scores
    write_handle_frequency_csv(all_tweets)

    prefiltered = [t for t in run_tweets if t.get("score", -999) > threshold]
    prefiltered.sort(key=lambda t: t["score"], reverse=True)
    print(f"   🔎 Pre-filter passed: {len(prefiltered)} / {len(run_tweets)} tweets")

    # ── Stage 2: LLM relevance filter ────────────────────────────
    scored = llm_filter_tweets(prefiltered, model=filter_model)
    compute_composite_scores(scored, threshold)
    scored.sort(key=lambda t: t.get("composite_score", 0.0), reverse=True)
    print(f"   🎯 LLM filter matched {len(scored)} / {len(prefiltered)} tweets")

    save_tweets(all_tweets)

    # ── Export to Google Sheets ───────────────────────────────────
    print(f"\n   📤 Exporting to Google Sheets...")
    sheet_url = export_to_gsheets(scored, config)

    append_run_history({
        "run_ts": datetime.now().strftime("%Y%m%d_%H%M%S"),
        "export_only": args.export_only,
        "anchor_mode": scrape_meta.get("anchor_mode"),
        "anchor_found": scrape_meta.get("anchor_found"),
        "gap_risk": scrape_meta.get("gap_risk", False),
        "stop_reason": scrape_meta.get("stop_reason"),
        "iterations_completed": scrape_meta.get("iterations_completed"),
        "new_tweets": new_count,
        "total_accumulated": len(all_list),
        "matching_filter": len(scored),
        "old_anchor_url": anchor.get("url", "") if anchor else "",
        "new_anchor_url": first_tweet.get("url", "") if not args.export_only and first_tweet else "",
        "sheet_url": sheet_url,
    })

    print("\n" + "=" * 60)
    print(f"  ✅ Done!")
    print(f"  New tweets this run:  {new_count}")
    print(f"  Total accumulated:    {len(all_list)}")
    print(f"  Matching filter:      {len(scored)}")
    print(f"  Handle CSV:           {HANDLE_FREQUENCY_FILE}")
    print(f"  Sheet:                {sheet_url}")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
