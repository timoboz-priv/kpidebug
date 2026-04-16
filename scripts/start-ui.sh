#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

set -a
source .env
set +a

cd ui

PORT="${UI_PORT:-3000}" exec npm start
