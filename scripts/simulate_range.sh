#!/usr/bin/env bash
# Simulate analysis across a date range.
#
# Usage:
#   ./scripts/simulate_range.sh <project_id> --start <date> --end <date>
#
# Examples:
#   ./scripts/simulate_range.sh test-startup-demo --start 2025-05-01 --end 2026-04-28
#   ./scripts/simulate_range.sh test-startup-demo --start 2025-06-01 --end 2025-06-30

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

exec "$PROJECT_DIR/.venv/bin/python" "$SCRIPT_DIR/simulate_range.py" "$@"
