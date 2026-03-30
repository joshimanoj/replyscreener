# replyscreener

Automated Twitter timeline scraper that collects tweets, filters them by custom criteria, generates AI-powered reply suggestions, and exports results to Google Sheets.

## Features

- **Timeline Scraping**: Uses Playwright for browser-based tweet collection with anchor-based gap-free scraping
- **Smart Filtering**: Configurable filtering criteria to surface relevant tweets
- **AI Reply Generation**: Generates contextual reply suggestions using LLMs
- **Google Sheets Export**: Automated export of filtered tweets and replies via gspread
- **Scheduled Runs**: Configured with macOS launchd for automated morning and afternoon scraping

## Tech Stack

- Python, Playwright
- OpenAI / LLM APIs
- Google Sheets API (gspread)
- macOS launchd (scheduling)

## Setup

1. Install dependencies: `pip install -r requirements.txt`
2. Install Playwright: `playwright install chromium`
3. Configure `config.json` with API keys and Twitter credentials
4. Run: `python scraper.py`
