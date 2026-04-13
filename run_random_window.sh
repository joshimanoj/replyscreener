#!/bin/zsh

set -euo pipefail

PROJECT_DIR="/Users/manojjoshi/Desktop/scraper"
LOG_DIR="$PROJECT_DIR/logs"
WINDOW_NAME="${1:-window}"
TEST_MODE="${TEST_MODE:-0}"
TEST_SLEEP_SECONDS="${TEST_SLEEP_SECONDS:-30}"
DELAY_SECONDS="${DELAY_SECONDS:-}"
PYTHON_BIN="${PYTHON_BIN:-}"

if [[ -z "$PYTHON_BIN" ]]; then
  if [[ -x "$PROJECT_DIR/venv/bin/python" ]]; then
    PYTHON_BIN="$PROJECT_DIR/venv/bin/python"
  else
    PYTHON_BIN="/usr/bin/env python3"
  fi
fi

mkdir -p "$LOG_DIR"

if [[ -n "$DELAY_SECONDS" ]]; then
  delay_seconds="$DELAY_SECONDS"
else
  delay_seconds=$(( RANDOM % 3600 ))
fi
timestamp="$(date '+%Y-%m-%d %H:%M:%S')"

{
  echo "[$timestamp] $WINDOW_NAME run scheduled with random delay ${delay_seconds}s"
  sleep "$delay_seconds"
  cd "$PROJECT_DIR"

  if [[ "$TEST_MODE" == "1" ]]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] starting dry-run test under caffeinate for ${TEST_SLEEP_SECONDS}s"
    /usr/bin/caffeinate -dimsu /bin/sleep "$TEST_SLEEP_SECONDS"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] dry-run test finished"
  else
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] starting scraper"
    /usr/bin/caffeinate -dimsu $=PYTHON_BIN "$PROJECT_DIR/scraper.py"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] scraper finished"
  fi
} >> "$LOG_DIR/${WINDOW_NAME}.log" 2>&1
