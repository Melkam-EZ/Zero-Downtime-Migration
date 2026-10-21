#!/usr/bin/env bash
# Shared helpers. Source this; do not run it.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
PY="$ROOT/.venv/bin/python"
PROXY_URL="${PROXY_URL:-http://127.0.0.1:8080}"
load_env() { [ -f .env ] || { echo "No .env yet: run scripts/up.sh first." >&2; exit 1; }; set -a; source .env; set +a; }
