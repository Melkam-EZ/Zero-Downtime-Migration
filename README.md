# Zero-Downtime Application Migration

Move a live service from a legacy stack (app v1 + PostgreSQL) to a new one (app v2 + a different PostgreSQL) while a client keeps hammering it, with **zero failed requests, zero lost writes and a rollback path**.

## What it demonstrates
- **Parallel run** of both stacks behind HAProxy, with the traffic split changed live through the runtime API (no reloads).
- **Logical replication** (initial copy + streaming) keeps the new database in step with the old one.
- **Expand/contract schema change:** v2 adds a column; v1 and v2 can both run against their own database safely.
- **Canary on reads** (10% to 50% to 100%), gated on error rate, replication lag, and a shadow comparison of old vs new answers. Any failed gate sends reads back to the old stack.
- **Cutover in about 0.2 s:** pause writes (clients get 503 + Retry-After and retry), drain, make the old database read-only, wait for catch-up, compare a fingerprint of every row, copy sequences, switch.
- **Rollback:** a reverse stream keeps the old database current after cutover, so you can go back without losing the writes made on the new stack.
- **Failure at any cutover step reverts automatically** to "legacy serving writes" (tested at all 7 checkpoints, then retried successfully).
- **Proof by client:** an idempotent, retrying client records every request; at the end every acknowledged write is looked up in the database.

## Run it
See [STEPS.md](STEPS.md): `scripts/setup.sh`, `scripts/up.sh`, `scripts/verify.sh`.

## Layout
`app/` orders service (v1/v2) · `haproxy/` proxy config · `migrate/` the migration tool · `probe/` load client and integrity check · `db/` role setup · `tests/` 21 unit + 18 integration tests · `docs/ARCHITECTURE.md`
