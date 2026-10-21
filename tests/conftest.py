import json
import pathlib
import sys
import urllib.request

import pytest

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from migrate.config import Config          # noqa: E402
from migrate.plan import Migration         # noqa: E402
from tests import harness                  # noqa: E402


class Stack:
    """Two real Postgres clusters, the v1 and v2 apps, and HAProxy in front, all on localhost."""

    def __init__(self, tmp):
        self.tmp = tmp
        self.old, self.new = harness.Cluster("old"), harness.Cluster("new")
        self.legacy = self.modern = self.proxy = None

    def start(self):
        self.old.start(); self.new.start()
        self.legacy, self.modern = harness.App("v1", self.old), harness.App("v2", self.new)
        self.legacy.start(); self.modern.start()
        self.proxy = harness.Proxy(self.legacy, self.modern)
        self.proxy.start()

    def stop(self):
        for part in (self.proxy, self.modern, self.legacy):
            if part:
                part.stop()
        self.old.stop(); self.new.stop()

    def config(self, **over):
        kw = dict(old_dsn=self.old.admin_dsn, new_dsn=self.new.admin_dsn, old_from_new=self.old.repl_dsn, new_from_old=self.new.repl_dsn,
                  proxy_host="127.0.0.1", proxy_port=self.proxy.admin_port, legacy_url=self.legacy.url, modern_url=self.modern.url,
                  canary_steps=(25, 100), observe_seconds=1.5, min_requests=3, settle_seconds=0.5, catchup_timeout=20.0,
                  drain_timeout=15.0, sync_timeout=60.0, state_dir=str(self.tmp / "state"))
        kw.update(over)
        return Config(**kw)

    def migration(self, **over):
        fault = over.pop("fault", None)
        return Migration(self.config(**over), log=lambda *_: None, fault=fault)

    # -- helpers
    def post(self, key, customer="c", amount=100, via=None):
        req = urllib.request.Request((via or self.proxy.url) + "/orders", method="POST", data=json.dumps({"customer": customer, "amount_cents": amount}).encode(),
                                     headers={"Content-Type": "application/json", "Idempotency-Key": key})
        with urllib.request.urlopen(req, timeout=5) as r:
            return r.status, json.loads(r.read())

    def seed(self, n=50):
        for i in range(n):
            self.post(f"seed-key-{i:05d}", f"cust-{i % 7}", 100 + i, via=self.legacy.url)

    def count(self, cluster):
        import psycopg
        with psycopg.connect(cluster.admin_dsn, autocommit=True) as c:
            return c.execute("SELECT count(*) FROM orders").fetchone()[0]


@pytest.fixture(scope="module")
def _stack(tmp_path_factory):
    if not harness.available():
        pytest.skip("needs PostgreSQL 16 server binaries, haproxy and root (see tests/harness.py)")
    stack = Stack(tmp_path_factory.mktemp("zdm"))
    stack.start()
    yield stack
    stack.stop()


@pytest.fixture
def stack(_stack):
    """The shared stack, put back to a clean 'before migration' state for every test."""
    mig = _stack.migration()
    try:
        mig.reset(force=True)
    finally:
        mig.close()
    _stack.seed(50)
    yield _stack
