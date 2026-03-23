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
import os
import random
import sys
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


# ─── Challenge / warning detection ──────────────────────────

CHALLENGE_SIGNALS = [
    "confirm it's you",
    "verify your identity",
    "confirm your identity",
    "suspicious activity",
    "unusual activity",
    "enter your phone",
    "enter your email",
    "start a new session",
    "log in to twitter",
    "log in to x",
    "sign in to x",
]

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


# ─── DOM Tweet Extractor ─────────────────────────────────────

async def read_tweets_from_dom(page) -> list[dict]:
    return await page.evaluate("""
        () => {
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
                const timeEl = article.querySelector('time');
                if (timeEl) {
                    const a = timeEl.closest('a');
                    if (a) url = 'https://x.com' + a.getAttribute('href');
                }

                if (text) results.push({ author: handle, author_name: displayName, text, url });
            }
            return results;
        }
    """)


# ─── Main Agent Loop ─────────────────────────────────────────

async def agent_loop(page, config, all_tweets: dict, anchor: dict | None):
    """
    Scrolls top→bottom collecting tweets.
    - No anchor (run 1): stops at max_tweets.
    - Anchor present: stops when anchor tweet is seen (no count cap).
    Returns (new_tweets_added, first_tweet_of_this_run).
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

    mode_label = f"anchor mode — running until anchor found (cap: {max_tweets_anchor})" if anchor_mode \
                 else f"first run — collecting up to {max_tweets} tweets"
    print(f"\n🤖 Agent loop starting  ({mode_label})\n")

    for iteration in range(1, max_iter + 1):

        # ── Challenge detection (every iteration) ────────────────
        challenge = await check_for_challenge(page)
        if challenge:
            print(f"\n   ⚠️  CHALLENGE DETECTED: '{challenge}'")
            print("   🛑 Stopping scrape to protect your account.")
            print("   👉 Open the browser, resolve the challenge manually, then re-run.")
            save_tweets(all_tweets)
            return new_count_total, first_tweet_this_run

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
                print(f"\n   🔖 Anchor tweet found — run complete.")
                break

            dedup_key = f"{author}:{text[:80]}"
            if dedup_key not in all_tweets:
                all_tweets[dedup_key] = tweet
                new_count_total += 1
                new_this_iter   += 1

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
            print(f"\n   ✅ Target reached — {total} tweets collected.")
            break

        # Anchor-mode safety cap
        if anchor_mode and new_count_total >= max_tweets_anchor:
            print(f"\n   ✅ Anchor-mode cap reached — {new_count_total} new tweets collected.")
            break

        # Safety: too many stale rounds
        if stale_rounds >= 10:
            print(f"\n   ⏹️  No new tweets for {stale_rounds} rounds. Stopping.")
            break

    save_tweets(all_tweets)
    return new_count_total, first_tweet_this_run


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

Reply with JSON only: {"relevant": true/false, "reason": "one short sentence"}"""


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
            if tweet["llm_relevant"]:
                relevant.append(tweet)
                print(f"      [{i}/{len(tweets)}] ✓  {tweet.get('author','?')} — {tweet['llm_reason']}")
            else:
                print(f"      [{i}/{len(tweets)}] ✗  {tweet.get('author','?')} — {tweet['llm_reason']}")
        except Exception as e:
            # On error, keep the tweet (fail open)
            tweet["llm_relevant"] = True
            tweet["llm_reason"]   = f"[filter error: {e}]"
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
    ws = sh.add_worksheet(title=sheet_title, rows=max(len(tweets) + 10, 50), cols=10)

    headers = ["#", "Author Name", "Handle", "Tweet Text", "Tweet URL",
               "Similarity Score", "Suggested Reply"]
    ws.append_row(headers, value_input_option="RAW")

    rows = []
    for i, tweet in enumerate(tweets, 1):
        rows.append([
            i,
            tweet.get("author_name", ""),
            tweet.get("author", ""),
            tweet.get("text", ""),
            tweet.get("url", ""),
            round(tweet.get("score", 0.0), 4),
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
    ws = sh.add_worksheet(title=sheet_title, rows=max(len(all_tweets) + 10, 50), cols=8)

    headers = ["#", "Author Name", "Handle", "Tweet Text", "Tweet URL"]
    ws.append_row(headers, value_input_option="RAW")

    rows = []
    for i, tweet in enumerate(all_tweets, 1):
        rows.append([
            i,
            tweet.get("author_name", ""),
            tweet.get("author", ""),
            tweet.get("text", ""),
            tweet.get("url", ""),
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

    print("=" * 60)
    print("  🐦 Twitter Scraper — DOM Edition")
    print("=" * 60)

    # ── Scraping phase (skipped with --export-only) ───────────────
    if args.export_only:
        print(f"  Mode:    EXPORT ONLY (using {len(all_tweets)} saved tweets)")
        print("=" * 60)
        new_count    = 0
        new_tweet_keys = set()
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
            if PROFILE_DIR.exists() and any(PROFILE_DIR.iterdir()):
                print("  ✅ Saved session found — you should already be logged in.")
                print("  Once your timeline is visible, press ENTER to start.")
            else:
                print("  👋 First run — please log in to Twitter in the browser window.")
                print("  Once your timeline is visible, press ENTER to start.")
            print("=" * 60)
            input("\n   Press ENTER when ready → ")

            print("\n   ✅ Taking over! Don't touch the browser...\n")
            await asyncio.sleep(random.uniform(2.5, 4.5))   # human landing pause

            new_count, first_tweet = await agent_loop(page, config, all_tweets, anchor)

            # Update anchor to the first tweet seen this run
            if first_tweet and first_tweet.get("url", "unknown") != "unknown":
                save_anchor(first_tweet)
                print(f"\n   🔖 New anchor saved: {first_tweet['author']}  {first_tweet['url']}")
            else:
                print("\n   ⚠️  Could not determine anchor tweet (no valid URL seen).")

            new_tweet_keys = set(all_tweets.keys()) - keys_before

            print("\n   Browser closes in 10 seconds...")
            await asyncio.sleep(10)
            await context.close()

    # ── Score & filter (current run's tweets only) ───────────────
    from datetime import datetime
    ts       = datetime.now().strftime("%Y%m%d_%H%M%S")
    all_list = list(all_tweets.values())

    # In a normal run: process only new tweets collected this session.
    # In --export-only: no scraping happened, so fall back to all tweets.
    if new_tweet_keys:
        run_tweets = [all_tweets[k] for k in new_tweet_keys if k in all_tweets]
    else:
        run_tweets = all_list

    model = config.get("openai", {}).get("model", "gpt-5.4")

    # ── Stage 1: mpnet pre-filter (loose) ────────────────────────
    threshold = config.get("filtering", {}).get("semantic_threshold", PREFILTER_THRESHOLD)
    print(f"\n   🔍 Scoring {len(run_tweets)} tweets from this run...")
    score_tweets_batch(run_tweets)
    save_tweets(all_tweets)  # persist scores

    prefiltered = [t for t in run_tweets if t.get("score", -999) > threshold]
    prefiltered.sort(key=lambda t: t["score"], reverse=True)
    print(f"   🔎 Pre-filter passed: {len(prefiltered)} / {len(run_tweets)} tweets")

    # ── Stage 2: LLM relevance filter ────────────────────────────
    scored = llm_filter_tweets(prefiltered, model=model)
    scored.sort(key=lambda t: t["score"], reverse=True)
    print(f"   🎯 LLM filter matched {len(scored)} / {len(prefiltered)} tweets")

    # ── Generate replies for top N ────────────────────────────────
    top_n = config.get("openai", {}).get("top_n_replies", 10)
    top   = scored[:top_n]

    if top:
        print(f"\n   💬 Generating replies for top {len(top)} tweets...")
        reply_guide = load_reply_guide()
        for i, tweet in enumerate(top, 1):
            try:
                tweet["reply"] = generate_reply(tweet["text"], reply_guide, model)
                print(f"      [{i}/{len(top)}] ✓  {tweet.get('author', '?')}")
            except Exception as e:
                tweet["reply"] = f"[Error: {e}]"
                print(f"      [{i}/{len(top)}] ✗  {e}")

    # ── Export to Google Sheets ───────────────────────────────────
    print(f"\n   📤 Exporting to Google Sheets...")
    sheet_url = export_to_gsheets(scored, config, f"Run_{ts}")

    if new_tweet_keys:
        export_raw_dump_to_gsheets(run_tweets, config, f"New_{ts}")

    print("\n" + "=" * 60)
    print(f"  ✅ Done!")
    print(f"  New tweets this run:  {new_count}")
    print(f"  Total accumulated:    {len(all_list)}")
    print(f"  Matching filter:      {len(scored)}")
    print(f"  Sheet:                {sheet_url}")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
