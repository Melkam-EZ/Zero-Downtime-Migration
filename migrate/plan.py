"""The migration, as phases. Every phase checks what must be true before it starts and can be re-run safely.

  preflight -> replicate -> canary (reads) -> cutover (writes) -> finalize, with rollback available until finalize

Writes are only ever accepted by ONE stack. Before cutover that is the legacy stack, and the new database is a replica.
"""
import json
import pathlib
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone

from . import pg
from .proxy import Proxy, num

FWD = "migration_sub"        # new <- old (publisher: old, publication migration_pub)
REV = "rollback_sub"         # old <- new (publisher: new, publication rollback_pub)
FWD_PUB, REV_PUB = "migration_pub", "rollback_pub"
PHASES_BEFORE_CUTOVER = ("init", "preflight_ok", "synced", "canary_done")


class Abort(Exception):
    """A gate failed. The migration stops and leaves the system in a safe, serving state."""


class Migration:
    def __init__(self, cfg, log=print, sleep=time.sleep, clock=time.monotonic, fault=None, proxy=None):
        self.cfg, self.log, self.sleep, self.clock, self.fault = cfg, log, sleep, clock, fault
        self.proxy = proxy or Proxy(cfg.proxy_host, cfg.proxy_port)
        self._old = self._new = None
        self.state_dir = pathlib.Path(cfg.state_dir)
        self.state_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------ plumbing
    @property
    def old(self):
        if self._old is None or self._old.closed:
            self._old = pg.connect(self.cfg.old_dsn)
        return self._old

    @property
    def new(self):
        if self._new is None or self._new.closed:
            self._new = pg.connect(self.cfg.new_dsn)
        return self._new

    def close(self):
        for c in (self._old, self._new):
            if c is not None and not c.closed:
                c.close()

    def _state(self):
        p = self.state_dir / "migration.json"
        return json.loads(p.read_text()) if p.exists() else {"phase": "init"}

    def _set(self, **kw):
        st = {**self._state(), **kw}
        (self.state_dir / "migration.json").write_text(json.dumps(st, indent=2))
        return st

    @property
    def phase(self):
        return self._state()["phase"]

    def _event(self, name, **fields):
        rec = {"ts": datetime.now(timezone.utc).isoformat(timespec="milliseconds"), "event": name, **fields}
        with open(self.state_dir / "events.jsonl", "a") as fh:
            fh.write(json.dumps(rec) + "\n")
        self.log(f"[{name}] " + " ".join(f"{k}={v}" for k, v in fields.items()))

    def _need(self, *phases):
        if self.phase not in phases:
            raise Abort(f"this step needs phase {' or '.join(phases)}, but the migration is in '{self.phase}'")

    def _cp(self, label):
        if self.fault == label:
            raise Abort(f"injected fault at '{label}'")

    def _wait(self, cond, timeout, what):
        end = self.clock() + timeout
        while self.clock() < end:
            if cond():
                return
            self.sleep(0.1)
        raise Abort(f"timed out after {timeout:.0f}s waiting for {what}")

    # ------------------------------------------------------------------ 1. preflight
    def preflight(self):
        checks = []

        def check(name, ok, detail=""):
            checks.append((name, bool(ok), detail))

        try:
            old_i, new_i = pg.info(self.old), pg.info(self.new)
        except Exception as exc:   # noqa: BLE001
            raise Abort(f"cannot connect to the databases: {exc}")
        check("old database reachable", True, f"PostgreSQL {old_i['version']}")
        check("new database reachable", True, f"PostgreSQL {new_i['version']}")
        check("both databases are PostgreSQL 15 or newer (column-list publications for the rollback stream)",
              pg.major_version(self.old) >= 15 and pg.major_version(self.new) >= 15, f"old {old_i['version']}, new {new_i['version']}")
        check("old wal_level is logical (needs a restart to change, do it ahead of time)", old_i["wal_level"] == "logical", old_i["wal_level"])
        check("new wal_level is logical (needed for the rollback stream)", new_i["wal_level"] == "logical", new_i["wal_level"])
        check("old has free replication slots and senders", old_i["slots_free"] >= 2 and old_i["senders_free"] >= 2,
              f"slots free {old_i['slots_free']}, senders free {old_i['senders_free']}")
        check("new has free replication slots and senders", new_i["slots_free"] >= 2 and new_i["senders_free"] >= 2,
              f"slots free {new_i['slots_free']}, senders free {new_i['senders_free']}")

        old_t, new_t = pg.tables(self.old), pg.tables(self.new)
        check("old database has tables", bool(old_t), ", ".join(old_t))
        check("new database has every table (schema created by the new app)", set(old_t) <= set(new_t), f"missing: {sorted(set(old_t) - set(new_t))}")
        for t in old_t:
            check(f"{t}: has a primary key (required for replication)", bool(pg.pk_columns(self.old, t)))
            if t in new_t:
                missing = set(pg.columns(self.old, t)) - set(pg.columns(self.new, t))
                check(f"{t}: new table has every old column", not missing, f"missing: {sorted(missing)}")
        if not pg.subscription_exists(self.new, FWD):
            for t in old_t:
                if t in new_t:
                    check(f"{t}: empty in the new database before the initial copy", pg.row_count(self.new, t) == 0, f"{pg.row_count(self.new, t)} rows")

        try:
            stats = self.proxy.servers()
            for be in ("readers", "writers"):
                for srv in ("legacy", "modern"):
                    row = stats.get((be, srv))
                    check(f"proxy {be}/{srv} is UP", bool(row) and row["status"].startswith("UP"), row["status"] if row else "missing")
            w = {s: int(stats[("writers", s)]["weight"]) for s in ("legacy", "modern") if ("writers", s) in stats}
            check("writes currently go only to legacy", w == {"legacy": 100, "modern": 0} or w.get("modern") == 0 and w.get("legacy", 0) > 0, str(w))
        except Exception as exc:   # noqa: BLE001
            check("proxy runtime API reachable", False, str(exc))
        for name, url in (("legacy", self.cfg.legacy_url), ("modern", self.cfg.modern_url)):
            try:
                ok = urllib.request.urlopen(url + "/healthz", timeout=3).status == 200
            except OSError as exc:
                ok = False
            check(f"{name} app healthy", ok, url)

        for name, ok, detail in checks:
            self.log(f"{'PASS' if ok else 'FAIL'}  {name}" + (f"  [{detail}]" if detail else ""))
        failed = [c[0] for c in checks if not c[1]]
        if failed:
            raise Abort(f"{len(failed)} preflight check(s) failed")
        if self.phase == "init":
            self._set(phase="preflight_ok")
        self._event("preflight_ok", checks=len(checks))
        return checks

    # ------------------------------------------------------------------ 2. replicate
    def replicate(self):
        self._need("init", "preflight_ok", "synced", "canary_done")
        pg.grant_select(self.old, self.cfg.replicator_role)
        pg.create_publication(self.old, FWD_PUB)
        if not pg.subscription_exists(self.new, FWD):
            pg.create_subscription(self.new, FWD, self.cfg.old_from_new, FWD_PUB, copy_data=True)
            self._event("replication_started", note="initial copy + streaming")

        def synced():
            states = pg.sync_states(self.new, FWD)
            return bool(states) and set(states) == {"r"}

        self._wait(synced, self.cfg.sync_timeout, "the initial copy to finish")
        self._wait(lambda: (pg.slot_lag_bytes(self.old, FWD) or 0) <= self.cfg.max_lag_bytes, 60, "replication lag to fall")
        self._set(phase="synced" if self.phase in ("init", "preflight_ok") else self.phase)
        self._event("replication_synced", lag_bytes=pg.slot_lag_bytes(self.old, FWD), **self._counts())
        return self.status()

    def _rollback_columns(self):
        """Columns the OLD schema knows. Columns added by the expand step cannot flow back to the legacy database."""
        new_tables, out = set(pg.tables(self.new)), {}
        for t in pg.tables(self.old):
            if t in new_tables:
                known = set(pg.columns(self.old, t))
                out[t] = [c for c in pg.columns(self.new, t) if c in known]
        return out

    def _counts(self):
        return {f"rows_{t}_old_new": f"{pg.row_count(self.old, t)}/{pg.row_count(self.new, t)}" for t in pg.tables(self.old)}

    # ------------------------------------------------------------------ 3. canary (reads only)
    def canary(self):
        self._need("synced", "canary_done")
        for step in self.cfg.canary_steps:
            self._set_reads(step)
            self._event("canary_step", percent=step)
            try:
                metrics = self._observe(step)
            except Abort as exc:
                self._set_reads(0)
                self._event("canary_failed_reverted", percent=step, reason=str(exc))
                raise
            self._event("canary_step_ok", percent=step, **metrics)
        self._set(phase="canary_done")
        return True

    def _set_reads(self, percent):
        self.proxy.set_weights("readers", {"legacy": 100 - percent, "modern": percent})

    def _observe(self, percent):
        before = self.proxy.servers()
        self.sleep(self.cfg.observe_seconds)
        after = self.proxy.servers()
        key = ("readers", "modern")
        modern = after.get(key)
        if modern is None or not modern["status"].startswith("UP"):
            raise Abort(f"modern server is not UP ({modern['status'] if modern else 'missing'})")
        d = lambda f: num(after[key], f) - num(before[key], f)   # noqa: E731
        # stot counts server connections, which HAProxy may reuse for several requests: use whichever count is larger
        requests = max(d("stot"), sum(d(f"hrsp_{c}xx") for c in "12345"))
        errors = d("hrsp_5xx") + d("econ") + d("eresp")
        if requests < self.cfg.min_requests:
            raise Abort(f"only {requests} requests reached the new stack at {percent}% (need {self.cfg.min_requests}): not enough traffic to judge it")
        rate = errors / requests
        if rate > self.cfg.max_error_rate:
            raise Abort(f"error rate {rate:.1%} on the new stack exceeds {self.cfg.max_error_rate:.1%} ({errors}/{requests})")
        lag = pg.slot_lag_bytes(self.old, FWD) or 0
        if lag > self.cfg.max_lag_bytes:
            raise Abort(f"replication lag {lag} bytes exceeds {self.cfg.max_lag_bytes}")
        mismatches = self._shadow_compare()
        if mismatches:
            raise Abort(f"{len(mismatches)} response(s) differ between old and new stack, e.g. {mismatches[0]}")
        return {"requests": requests, "errors": errors, "lag_bytes": lag}

    def _get(self, url):
        try:
            with urllib.request.urlopen(url, timeout=5) as r:
                return r.status, json.loads(r.read())
        except urllib.error.HTTPError as exc:
            return exc.code, None

    def _shadow_compare(self):
        """Ask both apps directly for the same recent, settled orders and compare the answers."""
        status, recent = self._get(self.cfg.legacy_url + "/orders?limit=30")
        if status != 200:
            return [f"legacy list returned {status}"]
        cutoff = datetime.now(timezone.utc).timestamp() - self.cfg.settle_seconds
        bad = []
        for o in recent:
            if datetime.fromisoformat(o["created_at"]).timestamp() > cutoff:
                continue                       # too fresh: replication may legitimately not have delivered it yet
            st, theirs = self._get(f"{self.cfg.modern_url}/orders/{o['id']}")
            if st != 200:
                bad.append(f"order {o['id']}: modern returned {st}")
                continue
            for field in ("id", "idempotency_key", "customer", "amount_cents"):
                if theirs[field] != o[field]:
                    bad.append(f"order {o['id']}: {field} {o[field]!r} != {theirs[field]!r}")
            if "currency" in theirs and not theirs["currency"]:
                bad.append(f"order {o['id']}: currency missing")
        return bad

    # ------------------------------------------------------------------ 4. cutover (writes)
    def cutover(self):
        self._need("synced", "canary_done")
        if pg.subscription_exists(self.new, FWD) is False:
            raise Abort("forward replication is not running; run 'replicate' first")
        lag = pg.slot_lag_bytes(self.old, FWD) or 0
        if lag > self.cfg.max_lag_bytes:
            raise Abort(f"replication lag {lag} bytes is too high to cut over safely")
        if self.phase == "synced":
            self._set_reads(100)
        started = self.clock()
        self._event("cutover_begin")
        try:
            self._pause_writes()
            self._cp("frozen")
            self._drain("legacy", self.old)
            self._cp("drained")
            pg.set_role_readonly(self.old, self.cfg.app_role, True)
            self._cp("readonly")
            target = pg.current_lsn(self.old)
            if not pg.wait_slot_caught_up(self.old, FWD, target, self.cfg.catchup_timeout, self.sleep):
                raise Abort("the new database did not catch up in time")
            self._cp("caught_up")
            self._verify_identical()
            self._cp("verified")
            self._copy_sequences(self.old, self.new)
            pg.drop_subscription(self.new, FWD)
            pg.drop_publication(self.old, FWD_PUB)
            self._cp("forward_dropped")
            pg.create_publication(self.new, REV_PUB, self._rollback_columns())
            pg.grant_select(self.new, self.cfg.replicator_role)
            pg.create_subscription(self.old, REV, self.cfg.new_from_old, REV_PUB, copy_data=False)
            self._cp("reverse_created")
        except Exception as exc:   # noqa: BLE001
            self._event("cutover_failed_reverting", reason=str(exc))
            self._revert_cutover()
            raise Abort(f"cutover aborted, legacy keeps serving writes: {exc}") from exc
        self.proxy.set_weights("writers", {"modern": 100, "legacy": 0})
        self.proxy.set_weights("readers", {"modern": 100, "legacy": 0})
        paused = self.clock() - started
        self._set(phase="cutover_done", write_pause_seconds=round(paused, 2))
        self._event("cutover_complete", write_pause_seconds=round(paused, 2))
        return paused

    def _pause_writes(self):
        self.proxy.set_weights("writers", {"legacy": 0, "modern": 0})   # proxy answers 503 + Retry-After: 1 to writes; clients retry

    def _drain(self, server, conn):
        def idle():
            row = self.proxy.servers().get(("writers", server))
            return (num(row, "scur") == 0 if row else True) and pg.busy_sessions(conn, self.cfg.app_role) == 0
        self._wait(idle, self.cfg.drain_timeout, f"in-flight writes on {server} to finish")

    def _verify_identical(self):
        bad = []
        for t in pg.tables(self.old):
            cols = [c for c in pg.columns(self.old, t) if c in set(pg.columns(self.new, t))]
            pk = pg.pk_columns(self.old, t)
            a, b = pg.fingerprint(self.old, t, cols, pk), pg.fingerprint(self.new, t, cols, pk)
            if a != b:
                bad.append(f"{t}: old {a} != new {b}")
            self._event("table_verified", table=t, rows=a[0], identical=a == b)
        if bad:
            raise Abort("data differs between old and new: " + "; ".join(bad))

    def _copy_sequences(self, src, dst):
        for name, last in pg.sequences(src).items():
            pg.set_sequence(dst, name, last)
        pg.resync_serial_sequences(dst)      # also covers sequences that lag behind rows which arrived by replication

    def _revert_cutover(self):
        """Put everything back so the legacy stack keeps serving writes and no data is lost."""
        try:
            if pg.subscription_exists(self.old, REV):
                pg.drop_subscription(self.old, REV)
            pg.drop_publication(self.new, REV_PUB)
            if not pg.subscription_exists(self.new, FWD):
                pg.create_publication(self.old, FWD_PUB)
                pg.create_subscription(self.new, FWD, self.cfg.old_from_new, FWD_PUB, copy_data=False)   # data is identical here
            pg.set_role_readonly(self.old, self.cfg.app_role, False)
        finally:
            self.proxy.set_weights("writers", {"legacy": 100, "modern": 0})
            self._event("cutover_reverted")

    # ------------------------------------------------------------------ 5. rollback (new -> old)
    def rollback(self):
        self._need("cutover_done")
        if not pg.subscription_exists(self.old, REV):
            raise Abort("no rollback stream exists (already finalized?): restore from backup instead")
        started = self.clock()
        self._event("rollback_begin")
        try:
            self._pause_writes()
            self._drain("modern", self.new)
            pg.set_role_readonly(self.new, self.cfg.app_role, True)
            target = pg.current_lsn(self.new)
            if not pg.wait_slot_caught_up(self.new, REV, target, self.cfg.catchup_timeout, self.sleep):
                raise Abort("the old database did not catch up in time")
            self._verify_identical()
            self._copy_sequences(self.new, self.old)
            pg.drop_subscription(self.old, REV)
            pg.drop_publication(self.new, REV_PUB)
            pg.set_role_readonly(self.old, self.cfg.app_role, False)
        except Exception as exc:   # noqa: BLE001
            self._event("rollback_failed_reverting", reason=str(exc))
            try:
                pg.set_role_readonly(self.new, self.cfg.app_role, False)
                if not pg.subscription_exists(self.old, REV):
                    pg.create_publication(self.new, REV_PUB, self._rollback_columns())
                    pg.create_subscription(self.old, REV, self.cfg.new_from_old, REV_PUB, copy_data=False)
            finally:
                self.proxy.set_weights("writers", {"modern": 100, "legacy": 0})
            raise Abort(f"rollback aborted, the new stack keeps serving writes: {exc}") from exc
        self.proxy.set_weights("writers", {"legacy": 100, "modern": 0})
        self.proxy.set_weights("readers", {"legacy": 100, "modern": 0})
        paused = self.clock() - started
        self._set(phase="rolled_back", write_pause_seconds=round(paused, 2))
        self._event("rollback_complete", write_pause_seconds=round(paused, 2))
        return paused

    # ------------------------------------------------------------------ 6. finalize / reset / status
    def finalize(self):
        self._need("cutover_done")
        pg.drop_subscription(self.old, REV)
        pg.drop_publication(self.new, REV_PUB)
        self._set(phase="finalized")
        self._event("finalized", note="rollback stream removed; legacy database stays read-only for the application role until you decommission it")

    def reset(self, force=False):
        if self.phase in ("cutover_done", "finalized") and not force:
            raise Abort("refusing to reset after cutover: the new database is now the source of truth (use --force only if you are sure)")
        for conn, sub in ((self.new, FWD), (self.old, REV)):
            pg.drop_subscription(conn, sub)
        pg.drop_publication(self.old, FWD_PUB)
        pg.drop_publication(self.new, REV_PUB)
        for conn in (self.old, self.new):
            pg.set_role_readonly(conn, self.cfg.app_role, False)
        pg.truncate_all(self.new, pg.tables(self.new))
        pg.resync_serial_sequences(self.old)
        self.proxy.set_weights("writers", {"legacy": 100, "modern": 0})
        self.proxy.set_weights("readers", {"legacy": 100, "modern": 0})
        self._set(phase="init")
        self._event("reset")

    def status(self):
        out = {"phase": self.phase, "state": self._state()}
        try:
            out["readers"], out["writers"] = self.proxy.weights("readers"), self.proxy.weights("writers")
        except Exception as exc:   # noqa: BLE001
            out["proxy_error"] = str(exc)
        repl = {}
        if pg.subscription_exists(self.new, FWD):
            repl["forward"] = {"tables": pg.sync_states(self.new, FWD), "lag_bytes": pg.slot_lag_bytes(self.old, FWD)}
        if pg.subscription_exists(self.old, REV):
            repl["rollback"] = {"lag_bytes": pg.slot_lag_bytes(self.new, REV)}
        out["replication"] = repl
        out["rows"] = self._counts()
        return out
