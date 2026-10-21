#!/usr/bin/env bash
# Migrate to the new stack while a client hammers the proxy. Prints the proof at the end.
source "$(dirname "$0")/lib.sh"; load_env
mkdir -p .state; rm -f .state/stop probe-report.json
echo "== client starts (40 requests/s through the proxy)"
$PY -m probe --url "$PROXY_URL" --rps 40 --verify-dsn "$NEW_DSN" --stop-file .state/stop --report probe-report.json > .state/probe.log 2>&1 &
PROBE=$!
sleep 6
echo "== migration runs: preflight, replicate, canary reads, cutover"
$PY -m migrate run
echo "== serving from the new stack for a few seconds"
sleep 6
touch .state/stop
wait $PROBE || true
echo; echo "== client report"; tail -n +2 .state/probe.log
