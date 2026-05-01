# replyscreener

Automated Twitter timeline scraper that collects tweets, filters them by custom criteria, and exports the best matches to Google Sheets.

## Features

- **Timeline Scraping**: Uses Playwright for browser-based tweet collection with anchor-based gap-free scraping
- **Two-Stage Filtering**: Local `mpnet` prefilter followed by a local LLM relevance pass
- **Google Sheets Export**: Automated export of the final ranked tweets to a single reusable worksheet
- **Free Local Inference**: Uses Ollama with a local small model such as `phi3:mini`
- **Scheduled Runs**: Configured with macOS launchd for automated morning and afternoon scraping

## Tech Stack

- Python, Playwright
- Sentence Transformers + Ollama
- Google Sheets API (gspread)
- macOS launchd (scheduling)

## Setup

1. Create a Python 3.13 virtualenv: `python3.13 -m venv venv`
2. Install dependencies: `./venv/bin/python -m pip install -r requirements.txt`
3. Install Playwright: `./venv/bin/playwright install chromium`
4. Install Ollama and pull a local model such as `phi3:mini`; the scraper starts `ollama serve` automatically when needed
5. Configure `config.json` with Twitter and Google Sheets settings
6. Run: `./venv/bin/python scraper.py`

To repair an existing sheet when a run skipped local LLM ranking:

`python scraper.py --rank-sheet-only --sheet-gid 1619977557`

To resume an interrupted run from the scraped-but-not-filtered batch only:

`python scraper.py --resume-last-batch`

## Evaluate A Sample File

Use the evaluator on a CSV or XLSX sample set before switching your scheduled runs:

`python eval_sample_tweets.py "sample tweets.xlsx"`

If your sample file is in Apple Numbers, export it to CSV or XLSX first.
