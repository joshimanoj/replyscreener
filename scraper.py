#!/usr/bin/env python3
"""
Twitter Timeline Scraper — DOM Edition
---------------------------------------
Run 1 (no anchor): collect up to max_tweets, save first tweet as anchor.
Run 2+ (anchor exists): collect ALL new tweets until anchor is found,
  no count cap — guarantees zero gaps.

Usage:
    python scraper.py               # full scrape + filter + export
    python scraper.py --export-only # skip scraping, just filter + reply + export
"""

import argparse
import asyncio
import json
import math
import os
import random
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

try:
    from playwright.async_api import async_playwright
except ImportError:
    print("\n❌ playwright is not installed.")
    print("   pip install playwright && playwright install chromium\n")
    sys.exit(1)

try:
    import gspread
    from google.oauth2.service_account import Credentials
except ImportError:
    print("\n❌ gspread or google-auth not installed.")
    print("   pip install gspread google-auth\n")
    sys.exit(1)

try:
    import openai
except ImportError:
    print("\n❌ openai not installed.")
    print("   pip install openai\n")
    sys.exit(1)

try:
    from langdetect import detect as _lang_detect
    from langdetect import LangDetectException
except ImportError:
    print("\n❌ langdetect not installed.")
    print("   pip install langdetect\n")
    sys.exit(1)


# ─── Paths ───────────────────────────────────────────────────

SCRIPT_DIR       = Path(__file__).parent
TWEETS_FILE      = SCRIPT_DIR / "all_tweets.json"
ANCHOR_FILE      = SCRIPT_DIR / "anchor.json"
REPLY_GUIDE_FILE = SCRIPT_DIR / "tweet_reply_guide"
PROFILE_DIR      = SCRIPT_DIR / "browser_profile"   # persistent login session
HANDLE_STATS_FILE = SCRIPT_DIR / "reply_worthy_handles.json"
RUN_HISTORY_FILE  = SCRIPT_DIR / "run_history.jsonl"


# ─── Config ──────────────────────────────────────────────────

def load_config():
    with open(SCRIPT_DIR / "config.json") as f:
        return json.load(f)


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


def _handle_url(handle: str) -> str:
    handle = _normalize_handle(handle)
    return f"https://x.com/{handle.lstrip('@')}" if handle else ""


def load_reply_worthy_handle_stats() -> dict:
    if not HANDLE_STATS_FILE.exists():
        return {"handles": {}}
    with open(HANDLE_STATS_FILE) as f:
        data = json.load(f)
    if "handles" not in data:
        return {"handles": data if isinstance(data, dict) else {}}
    return data


def save_reply_worthy_handle_stats(stats: dict) -> None:
    stats["updated_at"] = datetime.now(timezone.utc).isoformat()
    with open(HANDLE_STATS_FILE, "w") as f:
        json.dump(stats, f, indent=2, ensure_ascii=False)


def append_run_history(record: dict) -> None:
    record["recorded_at"] = datetime.now(timezone.utc).isoformat()
    with open(RUN_HISTORY_FILE, "a") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def record_reply_worthy_handles(tweets: list[dict], stats: dict) -> dict:
    handles = stats.setdefault("handles", {})

    for tweet in tweets:
        handle = _normalize_handle(tweet.get("author", ""))
        if not handle:
            continue

        entry = handles.setdefault(handle, {
            "handle": handle,
            "handle_url": _handle_url(handle),
            "handle_name": "",
            "follower_count": "",
            "reply_worthy_count": 0,
        })
        entry["handle"] = handle
        entry["handle_url"] = _handle_url(handle)
        entry["handle_name"] = tweet.get("author_name", entry.get("handle_name", ""))
        entry.setdefault("follower_count", "")
        entry["reply_worthy_count"] = int(entry.get("reply_worthy_count") or 0) + 1

    return stats


def annotate_reply_worthy_batch_handles(tweets: list[dict]) -> None:
    counts = Counter(_normalize_handle(tweet.get("author", "")) for tweet in tweets)
    counts.pop("", None)
    for tweet in tweets:
        handle = _normalize_handle(tweet.get("author", ""))
        tweet["reply_worthy_count_this_run"] = counts.get(handle, 0)


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
    max_iter           = config["scrolling"]["max_iterations"]

    anchor_url  = anchor["url"] if anchor else None
    anchor_mode = anchor_url is not None

    first_tweet_this_run = None   # will become the new anchor
    new_count_total      = 0
    stale_rounds         = 0
    anchor_found         = False
    stop_reason          = "max_iterations"
    iterations_completed = 0

    mode_label = f"anchor mode — running until anchor found (cap: {max_tweets_anchor})" if anchor_mode \
                 else f"first run — collecting up to {max_tweets} tweets"
    print(f"\n🤖 Agent loop starting  ({mode_label})\n")

    for iteration in range(1, max_iter + 1):
        iterations_completed = iteration

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

        # ── Human-like scroll then extract ───────────────────────
        # Random mouse nudge before scrolling
        await page.mouse.move(
            random.randint(300, 900),
            random.randint(200, 650),
        )

        # Randomised scroll distance; bigger jump when stale
        if stale_rounds >= 3:
            scroll_px = random.randint(1200, 2000)
        else:
            scroll_px = random.randint(600, 1200)
            if random.random() < 0.08:          # 8% chance of a big skip
                scroll_px = random.randint(1500, 2500)

        await page.mouse.wheel(0, scroll_px)

        # Base pause with jitter
        await asyncio.sleep(random.uniform(pause_min, pause_max))

        # ~1 in 8 scrolls: simulate pausing to read (4–9 s)
        if random.random() < 0.12:
            await asyncio.sleep(random.uniform(4.0, 9.0))

        # ── Extract tweets from DOM ───────────────────────────────
        tweets = await read_tweets_from_dom(page)

        new_this_iter = 0
        for tweet in tweets:
            text   = tweet.get("text", "").strip()
            author = tweet.get("author", "").strip()
            url    = tweet.get("url", "")
            if not text:
                continue

            # Record the very first tweet of this run as new anchor candidate
            if first_tweet_this_run is None:
                first_tweet_this_run = tweet

            # Stop if we've hit the anchor from the previous run
            if anchor_mode and url != "unknown" and url == anchor_url:
                anchor_found = True
                stop_reason = "anchor_found"
                print(f"\n   🔖 Anchor tweet found — run complete.")
                break

            dedup_key = f"{author}:{text[:80]}"
            if dedup_key not in all_tweets:
                all_tweets[dedup_key] = tweet
                new_count_total += 1
                new_this_iter   += 1
            else:
                existing = all_tweets[dedup_key]
                existing.update({
                    "author_name": tweet.get("author_name", existing.get("author_name", "")),
                    "url": tweet.get("url", existing.get("url", "")),
                    "posted_at": tweet.get("posted_at", existing.get("posted_at", "")),
                    "replies": tweet.get("replies", existing.get("replies")),
                    "retweets": tweet.get("retweets", existing.get("retweets")),
                    "likes": tweet.get("likes", existing.get("likes")),
                    "impressions": tweet.get("impressions", existing.get("impressions")),
                })

        if anchor_found:
            break

        if new_this_iter > 0:
            stale_rounds = 0
        else:
            stale_rounds += 1

        total = len(all_tweets)
        print(f"   [{iteration:3d}/{max_iter}]  +{new_this_iter} new  |  "
              f"Total: {total}  |  Stale: {stale_rounds}"
              + (f"  |  Seeking anchor..." if anchor_mode else f"/{max_tweets}"))

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
        if stale_rounds >= 10:
            stop_reason = "stale_rounds"
            print(f"\n   ⏹️  No new tweets for {stale_rounds} rounds. Stopping.")
            break

    save_tweets(all_tweets)
    return new_count_total, first_tweet_this_run, {
        "anchor_mode": anchor_mode,
        "anchor_found": anchor_found,
        "stop_reason": stop_reason,
        "iterations_completed": iterations_completed,
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
    global _model, _keep_embs, _skip_embs
    if _model is not None:
        return
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

        tweet["relevance_score"] = round(relevance, 4)
        tweet["freshness_score"] = round(freshness, 4)
        tweet["traction_score"] = round(traction, 4)
        if engagement_rate is not None:
            tweet["engagement_rate_score"] = round(engagement_rate, 4)
        else:
            tweet.pop("engagement_rate_score", None)
        tweet["composite_score"] = round(composite, 4)

        del tweet["_traction_raw"]
        del tweet["_engagement_rate_raw"]
        del tweet["_freshness_raw"]


def apply_handle_history_adjustments(tweets: list[dict], handle_stats: dict, config: dict) -> None:
    handles = handle_stats.get("handles", {})
    cfg = config.get("ranking", {})
    repeat_penalty_each = _safe_metric(cfg.get("repeat_handle_penalty_each", 0.03))
    repeat_penalty_cap = _safe_metric(cfg.get("repeat_handle_penalty_cap", 0.18))
    popular_followers_floor = _safe_metric(cfg.get("popular_handle_min_followers", 10000))
    poor_follower_engagement_rate = _safe_metric(cfg.get("poor_follower_engagement_rate", 0.001))
    poor_follower_engagement_penalty = _safe_metric(cfg.get("poor_follower_engagement_penalty", 0.10))

    for tweet in tweets:
        handle = _normalize_handle(tweet.get("author", ""))
        entry = handles.get(handle, {}) if handle else {}
        prior_count = int(entry.get("reply_worthy_count") or 0)
        repeat_penalty = min(prior_count * repeat_penalty_each, repeat_penalty_cap)

        followers = _safe_metric(
            tweet.get("author_followers",
                      tweet.get("followers_count",
                                entry.get("follower_count")))
        )
        engagement = (
            _safe_metric(tweet.get("likes"))
            + (2.0 * _safe_metric(tweet.get("retweets")))
            + _safe_metric(tweet.get("replies"))
        )
        follower_engagement_rate = (engagement / followers) if followers > 0 else None

        follower_penalty = 0.0
        if (
            followers >= popular_followers_floor
            and follower_engagement_rate is not None
            and follower_engagement_rate < poor_follower_engagement_rate
        ):
            follower_penalty = poor_follower_engagement_penalty

        total_penalty = repeat_penalty + follower_penalty
        base_composite = _safe_metric(tweet.get("composite_score"))
        tweet["handle_reply_worthy_count"] = prior_count
        tweet["handle_repeat_penalty"] = round(repeat_penalty, 4)
        if follower_engagement_rate is not None:
            tweet["follower_engagement_rate"] = round(follower_engagement_rate, 6)
        else:
            tweet.pop("follower_engagement_rate", None)
        tweet["follower_engagement_penalty"] = round(follower_penalty, 4)
        tweet["composite_score"] = round(_clamp(base_composite - total_penalty), 4)


# ─── Two-stage filtering ─────────────────────────────────────
# Stage 1: mpnet pre-filter (loose — only cuts obvious junk)
# Lower threshold catches tangential but relevant tweets
PREFILTER_THRESHOLD = 0.3   # was 0.6

RELEVANCE_SYSTEM_PROMPT = """You decide if a tweet is worth replying to from @vedaselfhelp — \
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

If relevant=true, also score whether @vedaselfhelp should reply based ONLY on closeness \
of the tweet's content to the handle's character and themes.
Do NOT score virality, author popularity, recency, likes, or whether the tweet is well-written.

Use this relevance_score scale:
- 0.00-0.30: not relevant to the handle
- 0.31-0.55: weak or generic overlap
- 0.56-0.75: relevant and replyable
- 0.76-0.90: strongly aligned with the handle
- 0.91-1.00: ideal fit for @vedaselfhelp's voice and themes

Reply with JSON only:
{"relevant": true/false, "relevance_score": 0.0-1.0, "reason": "one short sentence"}"""


def llm_filter_tweets(tweets: list[dict], model: str) -> list[dict]:
    """Stage 2: LLM relevance filter for tweets that passed the mpnet pre-filter."""
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY environment variable is not set.")
    client = openai.OpenAI(api_key=api_key)

    relevant = []
    print(f"   🤖 LLM-filtering {len(tweets)} pre-filtered tweets...")
    for i, tweet in enumerate(tweets, 1):
        try:
            response = client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": RELEVANCE_SYSTEM_PROMPT},
                    {"role": "user",   "content": f"TWEET: {tweet['text']}"},
                ],
                max_completion_tokens=80,
                temperature=0.0,
                response_format={"type": "json_object"},
            )
            raw = response.choices[0].message.content.strip()
            result = json.loads(raw)
            tweet["llm_relevant"] = result.get("relevant", False)
            tweet["llm_reason"]   = result.get("reason", "")
            tweet["llm_relevance_score"] = round(_clamp(_safe_metric(result.get("relevance_score"))), 4)
            if tweet["llm_relevant"]:
                if tweet["llm_relevance_score"] <= 0:
                    tweet["llm_relevance_score"] = 0.6
                relevant.append(tweet)
                print(f"      [{i}/{len(tweets)}] ✓  {tweet.get('author','?')} ({tweet['llm_relevance_score']:.2f}) — {tweet['llm_reason']}")
            else:
                print(f"      [{i}/{len(tweets)}] ✗  {tweet.get('author','?')} ({tweet['llm_relevance_score']:.2f}) — {tweet['llm_reason']}")
        except Exception as e:
            # On error, keep the tweet (fail open)
            tweet["llm_relevant"] = True
            tweet["llm_reason"]   = f"[filter error: {e}]"
            tweet["llm_relevance_score"] = _normalize_relevance(float(tweet.get("score", -999.0)), PREFILTER_THRESHOLD)
            relevant.append(tweet)
            print(f"      [{i}/{len(tweets)}] ?  filter error: {e}")
    return relevant


# ─── Reply guide ──────────────────────────────────────────────

def load_reply_guide() -> str:
    with open(REPLY_GUIDE_FILE) as f:
        return f.read().strip()


# ─── OpenAI reply generation ──────────────────────────────────

def generate_reply(tweet_text: str, reply_guide: str, model: str = "gpt-5.4") -> str:
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY environment variable is not set.")
    client = openai.OpenAI(api_key=api_key)
    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": reply_guide},
            {"role": "user",   "content": f"TWEET: {tweet_text}"},
        ],
        max_completion_tokens=300,
        temperature=0.7,
    )
    return response.choices[0].message.content.strip()


# ─── Google Sheets export ─────────────────────────────────────

GSHEETS_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def _gspread_client(credentials_file: str):
    creds = Credentials.from_service_account_file(credentials_file, scopes=GSHEETS_SCOPES)
    return gspread.authorize(creds)


def _get_gsheets_client_and_sheet(config: dict):
    gs_cfg           = config["google_sheets"]
    spreadsheet_id   = gs_cfg["spreadsheet_id"]
    credentials_file = str(SCRIPT_DIR / gs_cfg["credentials_file"])
    gc = _gspread_client(credentials_file)
    sh = gc.open_by_key(spreadsheet_id)
    return sh, spreadsheet_id


def export_to_gsheets(tweets: list[dict], config: dict, sheet_title: str) -> str:
    sh, spreadsheet_id = _get_gsheets_client_and_sheet(config)

    # Create filtered+scored worksheet
    ws = sh.add_worksheet(title=sheet_title, rows=max(len(tweets) + 10, 50), cols=24)

    headers = [
        "#", "Author Name", "Handle", "Tweet Text", "Tweet URL", "Posted At",
        "Replies", "Retweets", "Likes", "Impressions",
        "Local Prefilter Score", "LLM Relevance Score", "Freshness Score",
        "Traction Score", "Engagement Rate Score", "Reply-Worthy Count This Run",
        "Historical Reply-Worthy Count", "Handle Repeat Penalty", "Follower Engagement Rate",
        "Follower Engagement Penalty", "Composite Score",
        "LLM Reason", "Suggested Reply",
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
            round(tweet.get("relevance_score", 0.0), 4),
            round(tweet.get("freshness_score", 0.0), 4),
            round(tweet.get("traction_score", 0.0), 4),
            round(tweet.get("engagement_rate_score", 0.0), 4) if "engagement_rate_score" in tweet else "",
            int(tweet.get("reply_worthy_count_this_run", 0) or 0),
            int(tweet.get("handle_reply_worthy_count", 0) or 0),
            round(tweet.get("handle_repeat_penalty", 0.0), 4),
            round(tweet.get("follower_engagement_rate", 0.0), 6) if "follower_engagement_rate" in tweet else "",
            round(tweet.get("follower_engagement_penalty", 0.0), 4),
            round(tweet.get("composite_score", 0.0), 4),
            tweet.get("llm_reason", ""),
            tweet.get("reply", ""),
        ])

    if rows:
        ws.append_rows(rows, value_input_option="RAW")

    sheet_url = (f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}"
                 f"/edit#gid={ws.id}")
    print(f"   📊 Filtered tab: {len(tweets)} tweets → {sheet_url}")
    return sheet_url


def export_raw_dump_to_gsheets(all_tweets: list[dict], config: dict, sheet_title: str) -> str:
    sh, spreadsheet_id = _get_gsheets_client_and_sheet(config)

    # Create raw dump worksheet
    ws = sh.add_worksheet(title=sheet_title, rows=max(len(all_tweets) + 10, 50), cols=14)

    headers = ["#", "Author Name", "Handle", "Tweet Text", "Tweet URL", "Posted At",
               "Replies", "Retweets", "Likes", "Impressions", "Local Prefilter Score",
               "LLM Relevance Score", "Composite Score"]
    ws.append_row(headers, value_input_option="RAW")

    rows = []
    for i, tweet in enumerate(all_tweets, 1):
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
            round(tweet.get("relevance_score", 0.0), 4),
            round(tweet.get("composite_score", 0.0), 4),
        ])

    if rows:
        ws.append_rows(rows, value_input_option="RAW")

    sheet_url = (f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}"
                 f"/edit#gid={ws.id}")
    print(f"   📊 Raw dump tab:  {len(all_tweets)} tweets → {sheet_url}")
    return sheet_url


# ─── Main ─────────────────────────────────────────────────────

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--export-only", action="store_true",
                        help="Skip scraping — score, generate replies, and export existing tweets")
    args = parser.parse_args()

    config     = load_config()
    anchor     = load_anchor()
    all_tweets = load_saved_tweets()
    handle_stats = load_reply_worthy_handle_stats()

    print("=" * 60)
    print("  🐦 Twitter Scraper — DOM Edition")
    print("=" * 60)

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
    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    all_list = list(all_tweets.values())

    # In a normal run: process only new tweets collected this session.
    # In --export-only: no scraping happened, so fall back to all tweets.
    if new_tweet_keys:
        run_tweets = [all_tweets[k] for k in new_tweet_keys if k in all_tweets]
    else:
        run_tweets = all_list

    openai_cfg = config.get("openai", {})
    default_model = openai_cfg.get("model", "gpt-5.4")
    filter_model = openai_cfg.get("filter_model", default_model)
    reply_model = openai_cfg.get("reply_model", default_model)

    # ── Stage 1: mpnet pre-filter (loose) ────────────────────────
    threshold = config.get("filtering", {}).get("semantic_threshold", PREFILTER_THRESHOLD)
    print(f"\n   🔍 Scoring {len(run_tweets)} tweets from this run...")
    score_tweets_batch(run_tweets)
    save_tweets(all_tweets)  # persist scores

    prefiltered = [t for t in run_tweets if t.get("score", -999) > threshold]
    prefiltered.sort(key=lambda t: t["score"], reverse=True)
    print(f"   🔎 Pre-filter passed: {len(prefiltered)} / {len(run_tweets)} tweets")

    # ── Stage 2: LLM relevance filter ────────────────────────────
    scored = llm_filter_tweets(prefiltered, model=filter_model)
    compute_composite_scores(scored, threshold)
    apply_handle_history_adjustments(scored, handle_stats, config)
    scored.sort(key=lambda t: t.get("composite_score", 0.0), reverse=True)
    print(f"   🎯 LLM filter matched {len(scored)} / {len(prefiltered)} tweets")

    # ── Generate replies for high-fit tweets ─────────────────────
    reply_min_relevance = openai_cfg.get("reply_min_relevance_score", 0.65)
    max_replies = openai_cfg.get("max_replies", openai_cfg.get("top_n_replies", 25))
    top = [
        tweet for tweet in scored
        if _safe_metric(tweet.get("llm_relevance_score", tweet.get("relevance_score"))) >= reply_min_relevance
    ][:max_replies]
    annotate_reply_worthy_batch_handles(top)

    if top:
        print(f"\n   💬 Generating replies for {len(top)} tweets with LLM relevance >= {reply_min_relevance}...")
        reply_guide = load_reply_guide()
        for i, tweet in enumerate(top, 1):
            try:
                tweet["reply"] = generate_reply(tweet["text"], reply_guide, reply_model)
                print(f"      [{i}/{len(top)}] ✓  {tweet.get('author', '?')}")
            except Exception as e:
                tweet["reply"] = f"[Error: {e}]"
                print(f"      [{i}/{len(top)}] ✗  {e}")
    else:
        print(f"\n   💬 No tweets met reply threshold LLM relevance >= {reply_min_relevance}.")

    if top:
        record_reply_worthy_handles(top, handle_stats)
        save_reply_worthy_handle_stats(handle_stats)
        repeated = sum(1 for tweet in top if _safe_metric(tweet.get("handle_reply_worthy_count")) > 0)
        print(f"   📇 Stored {len(top)} reply-worthy handles ({repeated} repeats from previous runs).")

    save_tweets(all_tweets)

    # ── Export to Google Sheets ───────────────────────────────────
    print(f"\n   📤 Exporting to Google Sheets...")
    sheet_url = export_to_gsheets(scored, config, f"Run_{ts}")

    if new_tweet_keys:
        export_raw_dump_to_gsheets(run_tweets, config, f"New_{ts}")

    append_run_history({
        "run_ts": ts,
        "export_only": args.export_only,
        "anchor_mode": scrape_meta.get("anchor_mode"),
        "anchor_found": scrape_meta.get("anchor_found"),
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
    print(f"  Sheet:                {sheet_url}")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
