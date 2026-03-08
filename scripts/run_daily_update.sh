#!/usr/bin/env bash
# Daily market data update wrapper for launchd/cron.
# Activates the venv, runs daily_update.py, logs output.

set -euo pipefail

WAREHOUSE="$HOME/market-warehouse"
VENV="$WAREHOUSE/.venv"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_DIR="$WAREHOUSE/logs"

mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/daily_update_$(date +%Y-%m-%d).log"

echo "=== Daily Update $(date -u '+%Y-%m-%dT%H:%M:%SZ') ===" >> "$LOG_FILE"

source "$VENV/bin/activate"
python "$SCRIPT_DIR/daily_update.py" "$@" >> "$LOG_FILE" 2>&1

echo "=== Done $(date -u '+%Y-%m-%dT%H:%M:%SZ') ===" >> "$LOG_FILE"
