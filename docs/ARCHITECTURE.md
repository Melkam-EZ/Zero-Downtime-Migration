# Architecture

```
                 clients
                    │
                 HAProxy ── runtime API (weights) ◄── python -m migrate
          ┌─────────┴──────────┐
     readers backend      writers backend        reads can be split from the start,
   legacy ◄─► modern     legacy | modern         writes go to exactly ONE stack at a time
        │                     │
   app v1 ── db-old ══ logical replication ══► db-new ── app v2
                 ◄══ reverse stream (after cutover) ══
```

## Phases (each checks its preconditions and can be re-run)
1. **preflight**: 19 checks: Postgres 15+, `wal_level=logical`, free replication slots, every table has a primary key, new schema has every old column, new tables are empty, both apps and all proxy servers are UP, writes currently go only to legacy.
2. **replicate**: publication on old, subscription on new (copy + stream). Waits for every table to reach "ready" and lag to fall.
3. **canary (reads)**: moves reads to the new stack in steps. Each step must see enough traffic, an error rate under the limit, small replication lag, and identical answers for recent orders from both apps. Any failure puts reads back at 0%.
4. **cutover (writes)**: pause writes → drain in-flight → old DB read-only for the app role → wait until the new DB has everything → compare count + md5 of every row → copy sequences → drop forward stream → start reverse stream → flip weights. Any exception before the flip reverts everything and legacy keeps serving.
5. **rollback / finalize**: rollback reverses the process using the reverse stream; finalize drops it (the way back is gone).

## Decisions worth defending
- **One writer at a time.** Splitting writes between two databases needs conflict resolution; pausing writes for ~0.2 s does not. "Zero downtime" here means zero failed requests for a client that retries, which is what real clients do.
- **Idempotency keys** make those retries safe: a retried POST can never create a second order.
- **Read-only via a per-role setting**, not a global one, so the replication workers keep writing while the app role is blocked (existing sessions are terminated so the setting takes effect).
- **Verification beats trust.** After catch-up the tool compares row counts and an md5 over every row on both sides; replication metrics alone are not accepted as proof.
- **Rollback stream carries only columns the old schema knows** (a column-list publication, PostgreSQL 15+). Without this the legacy database rejects the stream because v2 added a column.

## Two production bugs found by the integration tests (kept fixed)
- Catch-up must target `pg_current_wal_insert_lsn()`, not `pg_current_wal_lsn()`: with asynchronous commit a committed transaction can sit beyond the write position, so waiting for the lower number declares "caught up" too early.
- Sequences do not replicate and rows that arrive by replication do not advance them. After copying sequences the tool also lifts each serial sequence to `max(id)`, or the next insert could reuse an existing id.

## Known limits
- Rows written on the new stack in a column the old schema lacks (`currency`) stay only in the new database after a rollback.
- DDL is not replicated; the schema must be created on the new side first (the v2 app does it). Large objects are not replicated.
- Single-table demo schema; foreign keys across tables need the initial-copy ordering considered (logical replication handles it, but test your schema).
- `wal_level=logical` needs a restart on the old database, so do it ahead of the migration window.
- The HAProxy admin socket has full control: keep it on localhost.
