#!/usr/bin/env bash
# Start the legacy stack, the modern stack and the proxy. Safe to re-run.
source "$(dirname "$0")/lib.sh"
if [ ! -f .env ]; then
  rand() { head -c 32 /dev/urandom | base64 | tr -dc 'A-Za-z0-9' | head -c 24; }
  P="$(rand)"; A="$(rand)"; R="$(rand)"
  cat > .env <<ENV
PG_PASSWORD=$P
APP_PASSWORD=$A
REPL_PASSWORD=$R
OLD_DSN=postgresql://postgres:$P@127.0.0.1:5433/shop
NEW_DSN=postgresql://postgres:$P@127.0.0.1:5434/shop
OLD_DSN_FROM_NEW="host=db-old port=5432 dbname=shop user=replicator password=$R"
NEW_DSN_FROM_OLD="host=db-new port=5432 dbname=shop user=replicator password=$R"
PROXY_ADMIN=127.0.0.1:9999
LEGACY_URL=http://127.0.0.1:8001
MODERN_URL=http://127.0.0.1:8002
CANARY_STEPS=10,50,100
OBSERVE_SECONDS=10
MIN_REQUESTS=5
ENV
  chmod 600 .env
fi
docker compose up -d --build --wait
docker compose ps --format 'table {{.Service}}\t{{.Status}}'
echo; echo "Up. Try: curl -s http://127.0.0.1:8080/stats   then: scripts/demo.sh"
