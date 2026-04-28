#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

set -a
source .env
set +a

source .venv/bin/activate

exec python scripts/simulate.py "$@"
