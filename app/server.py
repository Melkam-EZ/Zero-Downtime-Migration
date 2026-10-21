"""Orders service. One codebase, two versions:
  v1 = the legacy service (orders table without currency)
  v2 = the modern service (adds a NOT NULL currency column with a default: an additive "expand" change)
Run: DATABASE_URL=postgresql://... APP_VERSION=v1 PORT=8000 python server.py
"""
import json
import logging
import os
import re
import sys
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import psycopg
from psycopg import errors as pgerr

VERSION = os.environ.get("APP_VERSION", "v1")
NAME = os.environ.get("APP_NAME", f"app-{VERSION}")
DATABASE_URL = os.environ.get("DATABASE_URL", "")
PORT = int(os.environ.get("PORT", "8000"))
CURRENCIES = {"USD", "EUR", "NGN", "GBP"}
MAX_BODY = 4096
KEY_RE = re.compile(r"^[A-Za-z0-9._:-]{8,100}$")
log = logging.getLogger("orders")

SCHEMA = """
CREATE TABLE IF NOT EXISTS orders (
    id              bigserial PRIMARY KEY,
    idempotency_key text        NOT NULL UNIQUE,
    customer        text        NOT NULL,
    amount_cents    integer     NOT NULL CHECK (amount_cents >= 0),
    created_at      timestamptz NOT NULL DEFAULT now()
)"""
EXPAND_V2 = "ALTER TABLE orders ADD COLUMN IF NOT EXISTS currency text NOT NULL DEFAULT 'USD'"


def connect():
    return psycopg.connect(DATABASE_URL, autocommit=True, connect_timeout=3)


def init_schema(retries=60):
    for attempt in range(retries):
        try:
            with connect() as conn:
                conn.execute(SCHEMA)
                if VERSION == "v2":
                    conn.execute(EXPAND_V2)
            return
        except psycopg.Error as exc:
            log.warning("database not ready (%s), retrying", exc)
            time.sleep(1)
    raise SystemExit("database never became ready")


def row_to_dict(row):
    out = {"id": row[0], "idempotency_key": row[1], "customer": row[2], "amount_cents": row[3], "created_at": row[4].isoformat()}
    if VERSION == "v2":
        out["currency"] = row[5]
    return out


COLUMNS = "id, idempotency_key, customer, amount_cents, created_at" + (", currency" if VERSION == "v2" else "")


class Handler(BaseHTTPRequestHandler):
    server_version = "orders"
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):   # keep the proxy's log the single access log
        pass

    def _send(self, status, body, headers=None):
        data = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("X-App-Version", VERSION)
        self.send_header("X-Served-By", NAME)
        for k, v in (headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(data)

    def _unavailable(self, why):
        self._send(503, {"error": why}, {"Retry-After": "1"})

    def do_GET(self):
        url = urlparse(self.path)
        try:
            if url.path == "/healthz":
                with connect() as conn:
                    conn.execute("SELECT 1")
                return self._send(200, {"status": "ok", "version": VERSION})
            if url.path == "/stats":
                with connect() as conn:
                    count, max_id = conn.execute("SELECT count(*), coalesce(max(id), 0) FROM orders").fetchone()
                return self._send(200, {"version": VERSION, "count": count, "max_id": max_id})
            m = re.fullmatch(r"/orders/(\d{1,18})", url.path)
            if m:
                with connect() as conn:
                    row = conn.execute(f"SELECT {COLUMNS} FROM orders WHERE id = %s", (int(m.group(1)),)).fetchone()
                return self._send(200, row_to_dict(row)) if row else self._send(404, {"error": "not found"})
            if url.path == "/orders":
                try:
                    limit = max(1, min(100, int(parse_qs(url.query).get("limit", ["20"])[0])))
                except ValueError:
                    return self._send(400, {"error": "limit must be an integer"})
                with connect() as conn:
                    rows = conn.execute(f"SELECT {COLUMNS} FROM orders ORDER BY id DESC LIMIT %s", (limit,)).fetchall()
                return self._send(200, [row_to_dict(r) for r in rows])
            return self._send(404, {"error": "no such route"})
        except psycopg.Error as exc:
            log.error("GET %s failed: %s", self.path, exc)
            self._unavailable("database unavailable")

    def do_POST(self):
        if urlparse(self.path).path != "/orders":
            return self._send(404, {"error": "no such route"})
        key = self.headers.get("Idempotency-Key", "")
        if not KEY_RE.match(key):
            return self._send(400, {"error": "Idempotency-Key header (8-100 chars: letters, digits, . _ : -) is required"})
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            length = -1
        if not 0 < length <= MAX_BODY:
            return self._send(400, {"error": f"body must be 1-{MAX_BODY} bytes"})
        try:
            body = json.loads(self.rfile.read(length))
            customer, amount = body["customer"], body["amount_cents"]
            if not isinstance(customer, str) or not 0 < len(customer) <= 100:
                raise ValueError("customer must be a string of 1-100 chars")
            if isinstance(amount, bool) or not isinstance(amount, int) or not 0 <= amount <= 10**9:
                raise ValueError("amount_cents must be an integer between 0 and 1000000000")
            currency = body.get("currency", "USD")
            if VERSION == "v2" and currency not in CURRENCIES:
                raise ValueError(f"currency must be one of {sorted(CURRENCIES)}")
        except (ValueError, KeyError, TypeError, json.JSONDecodeError) as exc:
            return self._send(400, {"error": f"invalid body: {exc}"})
        try:
            with connect() as conn:
                if VERSION == "v2":
                    row = conn.execute("INSERT INTO orders (idempotency_key, customer, amount_cents, currency) VALUES (%s,%s,%s,%s) "
                                       "ON CONFLICT (idempotency_key) DO NOTHING RETURNING id", (key, customer, amount, currency)).fetchone()
                else:
                    row = conn.execute("INSERT INTO orders (idempotency_key, customer, amount_cents) VALUES (%s,%s,%s) "
                                       "ON CONFLICT (idempotency_key) DO NOTHING RETURNING id", (key, customer, amount)).fetchone()
                if row:
                    return self._send(201, {"id": row[0], "replayed": False})
                existing = conn.execute("SELECT id FROM orders WHERE idempotency_key = %s", (key,)).fetchone()
                return self._send(200, {"id": existing[0], "replayed": True})
        except pgerr.ReadOnlySqlTransaction:
            self._unavailable("writes are paused on this database")
        except psycopg.Error as exc:
            log.error("POST failed: %s", exc)
            self._unavailable("database unavailable")


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stderr)
    if not DATABASE_URL:
        raise SystemExit("DATABASE_URL is required")
    init_schema()
    srv = ThreadingHTTPServer(("0.0.0.0", PORT), Handler)
    srv.daemon_threads = True
    log.info("%s (%s) listening on :%s", NAME, VERSION, PORT)
    srv.serve_forever()


if __name__ == "__main__":
    main()
