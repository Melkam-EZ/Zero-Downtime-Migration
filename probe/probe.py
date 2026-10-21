"""A well-behaved client under load.

Writes carry an Idempotency-Key and are retried on 503/connection errors (honouring Retry-After), exactly like a good
real-world client would. Reads are tried twice. Every outcome is recorded, and at the end every write the system
acknowledged is looked up in the authoritative database: acknowledged but missing = lost write.
"""
import http.client
import json
import random
import statistics
import threading
import time
import uuid
from urllib.parse import urlparse


class Prober:
    def __init__(self, url, rps=40, workers=8, read_delay=2.0, write_deadline=30.0, timeout=5.0, clock=time.time):
        u = urlparse(url)
        self.host, self.port = u.hostname, u.port or 80
        self.rps, self.workers, self.read_delay, self.write_deadline, self.timeout, self.clock = rps, workers, read_delay, write_deadline, timeout, clock
        self.records, self.acked, self.created = [], {}, []
        self.lock, self.stop_event, self.threads = threading.Lock(), threading.Event(), []
        self.started = None

    # ------------------------------------------------------------------ http
    def _request(self, method, path, body=None, headers=None):
        conn = http.client.HTTPConnection(self.host, self.port, timeout=self.timeout)
        try:
            data = json.dumps(body).encode() if body is not None else None
            conn.request(method, path, body=data, headers={"Content-Type": "application/json", **(headers or {})})
            resp = conn.getresponse()
            raw = resp.read()
            parsed = json.loads(raw) if raw and resp.getheader("Content-Type", "").startswith("application/json") else None
            return resp.status, parsed, resp
        finally:
            conn.close()

    # ------------------------------------------------------------------ operations
    def _write(self):
        key = f"probe-{uuid.uuid4().hex}"
        body = {"customer": f"cust-{random.randint(1, 500)}", "amount_cents": random.randint(100, 50000)}
        start, attempts, status, resp_id, served = self.clock(), 0, 0, None, ""
        while True:
            attempts += 1
            try:
                status, parsed, resp = self._request("POST", "/orders", body, {"Idempotency-Key": key})
                served = resp.getheader("X-App-Version", "") or served
                if status in (200, 201):
                    resp_id = parsed["id"]
                    break
                retry_after = float(resp.getheader("Retry-After", "0") or 0) if status == 503 else None
            except (OSError, http.client.HTTPException, ValueError):
                status, retry_after = 0, 0.0
            if retry_after is None or self.clock() - start > self.write_deadline or self.stop_event.is_set():
                break
            time.sleep(max(retry_after, min(0.25 * attempts, 1.0)))
        ok = resp_id is not None
        if ok:
            with self.lock:
                self.acked[key] = resp_id
                self.created.append((resp_id, self.clock()))
        self._record("write", ok, attempts, status, start, served)

    def _read(self):
        with self.lock:
            old = [i for i, t in self.created if self.clock() - t >= self.read_delay]
        path = f"/orders/{random.choice(old[-200:])}" if old and random.random() < 0.7 else "/orders?limit=5"
        start, attempts, status, served = self.clock(), 0, 0, ""
        for attempts in (1, 2):
            try:
                status, _, resp = self._request("GET", path)
                served = resp.getheader("X-App-Version", "") or served
                if status in (200,):
                    break
                if status == 404:
                    break          # a settled order that is missing is a real problem, not a transient one
            except (OSError, http.client.HTTPException, ValueError):
                status = 0
            time.sleep(0.1)
        self._record("read", status == 200, attempts, status, start, served, stale=(status == 404))

    def _record(self, kind, ok, attempts, status, start, served, stale=False):
        rec = {"t": start, "kind": kind, "ok": ok, "attempts": attempts, "status": status, "ms": round((self.clock() - start) * 1000, 1),
               "version": served, "stale": stale}
        with self.lock:
            self.records.append(rec)

    # ------------------------------------------------------------------ run control
    def _worker(self, idx):
        interval = self.workers / self.rps
        nxt = self.clock() + random.random() * interval
        while not self.stop_event.is_set():
            self._write() if random.random() < 0.3 else self._read()
            nxt += interval
            delay = nxt - self.clock()
            if delay > 0:
                self.stop_event.wait(delay)
            else:
                nxt = self.clock()

    def start(self):
        self.started = self.clock()
        self.threads = [threading.Thread(target=self._worker, args=(i,), daemon=True) for i in range(self.workers)]
        for t in self.threads:
            t.start()

    def stop(self):
        self.stop_event.set()
        for t in self.threads:
            t.join(timeout=self.write_deadline + 5)

    # ------------------------------------------------------------------ verification and report
    def verify(self, dsn):
        """Every acknowledged write must exist in the authoritative database with the same id."""
        import psycopg
        keys = list(self.acked)
        found = {}
        with psycopg.connect(dsn, autocommit=True) as conn:
            for i in range(0, len(keys), 500):
                chunk = keys[i:i + 500]
                found.update(dict(conn.execute("SELECT idempotency_key, id FROM orders WHERE idempotency_key = ANY(%s)", (chunk,)).fetchall()))
        lost = [k for k in keys if k not in found]
        mismatched = [k for k in keys if k in found and found[k] != self.acked[k]]
        return {"acknowledged_writes": len(keys), "lost_writes": len(lost), "id_mismatches": len(mismatched), "lost_examples": lost[:5]}

    def report(self, integrity=None, events=None):
        recs = list(self.records)
        total = len(recs)
        failed = [r for r in recs if not r["ok"]]
        lat = sorted(r["ms"] for r in recs)
        pct = lambda p: lat[min(len(lat) - 1, int(len(lat) * p))] if lat else 0   # noqa: E731
        writes = [r for r in recs if r["kind"] == "write"]
        slow = sorted(writes, key=lambda r: -r["ms"])[:3]
        buckets = {}
        for r in recs:
            b = int((r["t"] - self.started) // 5) * 5
            buckets.setdefault(b, {}).setdefault(r["version"] or "?", 0)
            buckets[b][r["version"] or "?"] += 1
        out = {
            "requests": total, "failed": len(failed), "failed_reads": sum(1 for r in failed if r["kind"] == "read"),
            "failed_writes": sum(1 for r in failed if r["kind"] == "write"),
            "stale_reads": sum(1 for r in recs if r["stale"]), "retried": sum(1 for r in recs if r["attempts"] > 1),
            "latency_ms": {"p50": pct(0.5), "p95": pct(0.95), "p99": pct(0.99), "max": lat[-1] if lat else 0},
            "slowest_writes": [{"at_s": round(r["t"] - self.started, 1), "ms": r["ms"], "attempts": r["attempts"]} for r in slow],
            "served_by_version_per_5s": {str(k): v for k, v in sorted(buckets.items())},
            "integrity": integrity or {}, "events": events or [],
            "mean_ms": round(statistics.mean(lat), 1) if lat else 0,
        }
        out["pass"] = out["failed"] == 0 and out["stale_reads"] == 0 and (not integrity or (integrity["lost_writes"] == 0 and integrity["id_mismatches"] == 0))
        return out


def format_report(rep):
    lines = [f"requests: {rep['requests']}   failed: {rep['failed']} (reads {rep['failed_reads']}, writes {rep['failed_writes']})   "
             f"retried: {rep['retried']}   stale reads: {rep['stale_reads']}",
             "latency ms: " + "  ".join(f"{k}={v}" for k, v in rep["latency_ms"].items()),
             "slowest writes: " + ", ".join(f"{w['ms']:.0f} ms at +{w['at_s']}s ({w['attempts']} attempts)" for w in rep["slowest_writes"])]
    if rep["integrity"]:
        i = rep["integrity"]
        lines.append(f"integrity: {i['acknowledged_writes']} acknowledged writes, {i['lost_writes']} lost, {i['id_mismatches']} id mismatches")
    lines.append("traffic by app version (per 5 s): " + " | ".join(f"+{k}s {v}" for k, v in rep["served_by_version_per_5s"].items()))
    lines.append("RESULT: " + ("PASS: zero failed requests, zero lost writes" if rep["pass"] else "FAIL"))
    return "\n".join(lines)
