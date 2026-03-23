#!/bin/zsh

set -euo pipefail

PROJECT_DIR="/Users/manojjoshi/Desktop/scraper"
LOG_DIR="$PROJECT_DIR/logs"
WINDOW_NAME="${1:-window}"

mkdir -p "$LOG_DIR"

delay_seconds=$(( RANDOM % 3600 ))
timestamp="$(date '+%Y-%m-%d %H:%M:%S')"

{
  echo "[$timestamp] $WINDOW_NAME run scheduled with random delay ${delay_seconds}s"
  sleep "$delay_seconds"
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] starting scraper"
  cd "$PROJECT_DIR"
  /usr/bin/env python3 "$PROJECT_DIR/scraper.py"
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] scraper finished"
} >> "$LOG_DIR/${WINDOW_NAME}.log" 2>&1
