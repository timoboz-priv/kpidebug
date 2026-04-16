#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

set -a
source .env
set +a

source .venv/bin/activate

exec uvicorn kpidebug.api.server:app --reload --host "${HOST:-0.0.0.0}" --port "${PORT:-8000}"
