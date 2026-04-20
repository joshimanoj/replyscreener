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

1. Install dependencies: `pip install -r requirements.txt`
2. Install Playwright: `playwright install chromium`
3. Install and start Ollama, then pull a local model such as `phi3:mini`
4. Configure `config.json` with Twitter and Google Sheets settings
5. Run: `python scraper.py`

## Evaluate A Sample File

Use the evaluator on a CSV or XLSX sample set before switching your scheduled runs:

`python eval_sample_tweets.py "sample tweets.xlsx"`

If your sample file is in Apple Numbers, export it to CSV or XLSX first.
