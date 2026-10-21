#!/usr/bin/env bash
# Full proof: migrate under load (zero failures, no lost writes), old database refuses writes, then roll back and check.
source "$(dirname "$0")/lib.sh"; load_env
PASS=0; FAIL=0
check() { local label="$1"; shift; if "$@" >/dev/null 2>&1; then echo "PASS  $label"; PASS=$((PASS+1)); else echo "FAIL  $label"; FAIL=$((FAIL+1)); fi; }
served_by() { curl -s -o /dev/null -D - -H 'Content-Type: application/json' -H "Idempotency-Key: verify-$RANDOM$RANDOM" -d '{"customer":"v","amount_cents":1}' "$PROXY_URL"/orders | tr -d '\r' | awk -F': ' 'tolower($1)=="x-app-version"{print $2}'; }
rep() { $PY -c "import json;d=json.load(open('probe-report.json'));print($1)"; }

scripts/demo.sh
echo; echo "== checks after cutover"
check "client saw zero failed requests" test "$(rep "d['failed']")" = 0
check "no acknowledged write was lost" test "$(rep "d['integrity']['lost_writes']")" = 0
check "no stale reads" test "$(rep "d['stale_reads']")" = 0
check "traffic moved from v1 to v2" test "$(rep "int({'v1','v2'} <= {v for b in d['served_by_version_per_5s'].values() for v in b})")" = 1
check "writes are now served by v2" test "$(served_by)" = v2
check "migration phase is cutover_done" test "$($PY -m migrate status | $PY -c 'import sys,json;print(json.load(sys.stdin)["phase"])')" = cutover_done
check "old database refuses application writes" bash -c "! docker compose exec -T db-old psql -U app -d shop -c \"INSERT INTO orders (idempotency_key, customer, amount_cents) VALUES ('direct-write-1','x',1)\""

echo; echo "== rollback"
$PY -m migrate rollback
check "phase is rolled_back" test "$($PY -m migrate status | $PY -c 'import sys,json;print(json.load(sys.stdin)["phase"])')" = rolled_back
check "writes are back on v1" test "$(served_by)" = v1
check "old and new row counts match" test "$(docker compose exec -T db-old psql -U postgres -d shop -Atc 'select count(*) from orders')" = "$(docker compose exec -T db-new psql -U postgres -d shop -Atc 'select count(*) from orders')"

echo; echo "Result: $PASS passed, $FAIL failed.  To run again: scripts/reset.sh"
[ "$FAIL" -eq 0 ]
