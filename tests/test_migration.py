import threading
import time

import psycopg
import pytest

from migrate.plan import Abort
from probe.probe import Prober

pytestmark = pytest.mark.integration


def run_to_canary_done(mig, stack):
    """The canary judges the new stack on live traffic, so it needs some: run a light background client."""
    mig.preflight(); mig.replicate()
    prober = Prober(stack.proxy.url, rps=30, workers=4, read_delay=1.0)
    prober.start()
    try:
        mig.canary()
    finally:
        prober.stop()


def test_full_migration_under_load_has_zero_failures_and_no_lost_writes(stack):
    prober = Prober(stack.proxy.url, rps=60, workers=8, read_delay=1.0)
    prober.start()
    time.sleep(1.5)
    mig = stack.migration()
    try:
        run_to_canary_done(mig, stack)
        paused = mig.cutover()
        time.sleep(3)                      # keep serving from the new stack for a while
    finally:
        prober.stop()
        mig.close()
    report = prober.report(prober.verify(stack.new.admin_dsn))
    assert report["failed"] == 0, report
    assert report["stale_reads"] == 0
    assert report["integrity"]["lost_writes"] == 0 and report["integrity"]["id_mismatches"] == 0
    assert report["integrity"]["acknowledged_writes"] > 20
    assert paused < 10
    seen = {v for b in report["served_by_version_per_5s"].values() for v in b}
    assert {"v1", "v2"} <= seen, "traffic should have moved from v1 to v2"
    assert mig.phase == "cutover_done"


def admin(cluster):
    return psycopg.connect(cluster.admin_dsn, autocommit=True)


def weights(stack, backend):
    mig = stack.migration()
    try:
        return mig.proxy.weights(backend)
    finally:
        mig.close()


def test_rollback_returns_writes_made_after_cutover_to_the_old_database(stack):
    mig = stack.migration()
    try:
        run_to_canary_done(mig, stack)
        mig.cutover()
        status, body = stack.post("after-cutover-0001", "post-cut", 777)      # lands on the NEW database
        assert status == 201
        with admin(stack.new) as c:
            assert c.execute("SELECT count(*) FROM orders WHERE idempotency_key = 'after-cutover-0001'").fetchone()[0] == 1
        mig.rollback()
        assert mig.phase == "rolled_back"
        with admin(stack.old) as c:
            row = c.execute("SELECT id, amount_cents FROM orders WHERE idempotency_key = 'after-cutover-0001'").fetchone()
        assert row == (body["id"], 777), "the write made on the new stack must have been replicated back"
        assert weights(stack, "writers") == {"legacy": 100, "modern": 0}
        status, again = stack.post("after-rollback-0001", "post-rb", 5)          # legacy accepts writes again, ids do not collide
        assert status == 201 and again["id"] > body["id"]
    finally:
        mig.close()


@pytest.mark.parametrize("checkpoint", ["frozen", "drained", "readonly", "caught_up", "verified", "forward_dropped", "reverse_created"])
def test_a_failure_at_any_cutover_step_leaves_legacy_serving_and_cutover_can_be_retried(stack, checkpoint):
    mig = stack.migration(fault=checkpoint)
    try:
        run_to_canary_done(mig, stack)
        before = stack.count(stack.old)
        with pytest.raises(Abort, match="legacy keeps serving writes"):
            mig.cutover()
        assert mig.phase == "canary_done"
        assert weights(stack, "writers") == {"legacy": 100, "modern": 0}
        status, _ = stack.post(f"after-fault-{checkpoint}", "x", 1)               # writes work immediately, nothing is read-only
        assert status == 201
        assert stack.count(stack.old) == before + 1
        retry = stack.migration()                                                  # same state dir, no fault: must succeed
        try:
            retry.cutover()
            assert retry.phase == "cutover_done"
            with admin(stack.new) as c:
                assert c.execute("SELECT count(*) FROM orders WHERE idempotency_key = %s", (f"after-fault-{checkpoint}",)).fetchone()[0] == 1
        finally:
            retry.close()
    finally:
        mig.close()


def test_sequences_are_carried_over_so_new_ids_never_collide(stack):
    mig = stack.migration()
    try:
        run_to_canary_done(mig, stack)
        with admin(stack.old) as c:
            max_old = c.execute("SELECT max(id) FROM orders").fetchone()[0]
        mig.cutover()
        status, body = stack.post("seq-check-0001", "s", 1)
        assert status == 201 and body["id"] > max_old
    finally:
        mig.close()


def test_the_old_database_refuses_application_writes_after_cutover(stack):
    mig = stack.migration()
    try:
        run_to_canary_done(mig, stack)
        mig.cutover()
        with psycopg.connect(stack.old.app_dsn, autocommit=True) as c:
            with pytest.raises(psycopg.errors.ReadOnlySqlTransaction):
                c.execute("INSERT INTO orders (idempotency_key, customer, amount_cents) VALUES ('direct-0001','x',1)")
    finally:
        mig.close()


def test_canary_reverts_to_zero_percent_when_the_new_stack_is_down(stack):
    mig = stack.migration()
    try:
        mig.preflight(); mig.replicate()
        stack.modern.stop()
        time.sleep(4)                                    # HAProxy health checks mark it DOWN
        with pytest.raises(Abort, match="not UP"):
            mig.canary()
        assert weights(stack, "readers") == {"legacy": 100, "modern": 0}
        assert mig.phase == "synced"
    finally:
        mig.close()
        stack.modern.start()
        time.sleep(3)


def test_canary_stops_when_old_and_new_disagree(stack):
    mig = stack.migration()
    try:
        mig.preflight(); mig.replicate()
        with admin(stack.new) as c:
            c.execute("UPDATE orders SET amount_cents = amount_cents + 1 WHERE id = (SELECT max(id) FROM orders)")
        time.sleep(1)
        prober = Prober(stack.proxy.url, rps=30, workers=4, read_delay=1.0)
        prober.start()
        try:
            with pytest.raises(Abort, match="differ between old and new"):
                mig.canary()
        finally:
            prober.stop()
        assert weights(stack, "readers") == {"legacy": 100, "modern": 0}
    finally:
        mig.close()


def test_cutover_refuses_when_the_databases_differ(stack):
    mig = stack.migration()
    try:
        mig.preflight(); mig.replicate()
        with admin(stack.new) as c:
            c.execute("UPDATE orders SET customer = 'tampered' WHERE id = (SELECT min(id) FROM orders)")
        with pytest.raises(Abort, match="data differs"):
            mig.cutover()
        assert weights(stack, "writers") == {"legacy": 100, "modern": 0}
        status, _ = stack.post("still-writable-0001")
        assert status == 201
    finally:
        mig.close()


def test_preflight_catches_a_missing_table_and_a_dirty_target(stack):
    mig = stack.migration()
    try:
        with admin(stack.new) as c:
            c.execute("INSERT INTO orders (idempotency_key, customer, amount_cents) VALUES ('dirty-0001','x',1)")
        with pytest.raises(Abort, match="preflight"):
            mig.preflight()
        with admin(stack.new) as c:
            c.execute("TRUNCATE orders")
            c.execute("ALTER TABLE orders RENAME TO orders_x")
        with pytest.raises(Abort, match="preflight"):
            mig.preflight()
        with admin(stack.new) as c:
            c.execute("ALTER TABLE orders_x RENAME TO orders")
        mig.preflight()
        assert mig.phase == "preflight_ok"
    finally:
        mig.close()


def test_steps_must_run_in_order_and_replicate_is_repeatable(stack):
    mig = stack.migration()
    try:
        with pytest.raises(Abort, match="needs phase"):
            mig.cutover()
        with pytest.raises(Abort, match="needs phase"):
            mig.rollback()
        mig.preflight(); mig.replicate(); mig.replicate()
        assert mig.phase == "synced"
        st = mig.status()
        assert st["replication"]["forward"]["tables"] == {"r": 1} and st["rows"]
    finally:
        mig.close()


def test_finalize_removes_the_way_back(stack):
    mig = stack.migration()
    try:
        run_to_canary_done(mig, stack)
        mig.cutover()
        mig.finalize()
        assert mig.phase == "finalized"
        with pytest.raises(Abort, match="needs phase"):
            mig.rollback()
        assert mig.status()["replication"] == {}
    finally:
        mig.close()


def test_reset_refuses_after_cutover_without_force(stack):
    mig = stack.migration()
    try:
        run_to_canary_done(mig, stack)
        mig.cutover()
        with pytest.raises(Abort, match="refusing to reset"):
            mig.reset()
    finally:
        mig.close()
