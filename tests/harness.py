"""Starts real Postgres clusters, the two app versions and HAProxy on localhost, for integration tests.
Needs PostgreSQL 16 server binaries and haproxy installed; tests skip when they are missing."""
import os
import pathlib
import shutil
import socket
import subprocess
import sys
import time
import urllib.request

ROOT = pathlib.Path(__file__).resolve().parent.parent
PG_BIN = pathlib.Path(os.environ.get("PG_BIN", "/usr/lib/postgresql/16/bin"))
BASE = pathlib.Path("/tmp/zdm-test")


def available():
    return (PG_BIN / "initdb").exists() and shutil.which("haproxy") is not None and os.geteuid() == 0 and shutil.which("runuser") is not None


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait_for(fn, timeout=30, what="condition"):
    end = time.time() + timeout
    last = None
    while time.time() < end:
        try:
            if fn():
                return
        except Exception as exc:   # noqa: BLE001
            last = exc
        time.sleep(0.2)
    raise TimeoutError(f"timed out waiting for {what}: {last}")


def as_postgres(*cmd, **kw):
    return subprocess.run(["runuser", "-u", "postgres", "--", *map(str, cmd)], check=True, capture_output=True, text=True, **kw)


class Cluster:
    def __init__(self, name):
        self.dir = BASE / name
        self.port = free_port()

    def start(self):
        shutil.rmtree(self.dir, ignore_errors=True)
        self.dir.mkdir(parents=True)
        shutil.chown(BASE, "postgres"); shutil.chown(self.dir, "postgres")
        as_postgres(PG_BIN / "initdb", "-D", self.dir / "data", "-A", "trust", "-U", "postgres")
        conf = (f"port = {self.port}\nlisten_addresses = '127.0.0.1'\nunix_socket_directories = '{self.dir}'\n"
                "wal_level = logical\nmax_wal_senders = 10\nmax_replication_slots = 10\nfsync = off\nsynchronous_commit = off\n")
        with open(self.dir / "data" / "postgresql.auto.conf", "a") as fh:
            fh.write(conf)
        as_postgres(PG_BIN / "pg_ctl", "-D", self.dir / "data", "-l", self.dir / "log.txt", "-w", "start")
        self.sql("CREATE ROLE app LOGIN", "CREATE ROLE replicator LOGIN REPLICATION", "CREATE DATABASE shop OWNER app")

    def sql(self, *statements, db="postgres"):
        args = [a for st in statements for a in ("-c", st)]
        subprocess.run([str(PG_BIN / "psql"), "-h", "127.0.0.1", "-p", str(self.port), "-U", "postgres", "-d", db, "-v", "ON_ERROR_STOP=1", "-q", *args],
                       check=True, capture_output=True, text=True)

    def stop(self):
        subprocess.run(["runuser", "-u", "postgres", "--", str(PG_BIN / "pg_ctl"), "-D", str(self.dir / "data"), "-m", "immediate", "stop"],
                       capture_output=True)

    @property
    def admin_dsn(self):
        return f"postgresql://postgres@127.0.0.1:{self.port}/shop"

    @property
    def app_dsn(self):
        return f"postgresql://app@127.0.0.1:{self.port}/shop"

    @property
    def repl_dsn(self):
        return f"host=127.0.0.1 port={self.port} dbname=shop user=replicator"


class App:
    def __init__(self, version, cluster):
        self.version, self.cluster, self.port, self.proc = version, cluster, free_port(), None

    def start(self):
        env = {**os.environ, "APP_VERSION": self.version, "DATABASE_URL": self.cluster.app_dsn, "PORT": str(self.port), "APP_NAME": f"test-{self.version}"}
        self.proc = subprocess.Popen([sys.executable, str(ROOT / "app" / "server.py")], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        wait_for(self.healthy, 30, f"app {self.version}")

    def healthy(self):
        return urllib.request.urlopen(self.url + "/healthz", timeout=2).status == 200

    @property
    def url(self):
        return f"http://127.0.0.1:{self.port}"

    def stop(self):
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(5)
            except subprocess.TimeoutExpired:
                self.proc.kill()


class Proxy:
    def __init__(self, legacy, modern):
        self.port, self.admin_port, self.proc = free_port(), free_port(), None
        self.legacy, self.modern = legacy, modern

    def start(self):
        env = {**os.environ, "LEGACY_ADDR": f"127.0.0.1:{self.legacy.port}", "MODERN_ADDR": f"127.0.0.1:{self.modern.port}",
               "PROXY_BIND": f"127.0.0.1:{self.port}", "ADMIN_BIND": f"ipv4@127.0.0.1:{self.admin_port}",
               "ERRORFILE": str(ROOT / "haproxy" / "errors-503.http")}
        self.proc = subprocess.Popen(["haproxy", "-f", str(ROOT / "haproxy" / "haproxy.cfg")], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        wait_for(lambda: urllib.request.urlopen(self.url + "/healthz", timeout=2).status == 200, 20, "haproxy")

    @property
    def url(self):
        return f"http://127.0.0.1:{self.port}"

    def stop(self):
        if self.proc and self.proc.poll() is None:
            self.proc.terminate()
            self.proc.wait(5)
