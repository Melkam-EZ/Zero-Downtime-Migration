"""HAProxy runtime API client: read server stats, change weights without a reload."""
import csv
import io
import socket


class ProxyError(Exception):
    pass


class Proxy:
    def __init__(self, host, port, timeout=5.0):
        self.addr, self.timeout = (host, port), timeout

    def command(self, cmd):
        try:
            with socket.create_connection(self.addr, timeout=self.timeout) as s:
                s.sendall(cmd.encode() + b"\n")
                chunks = []
                while True:
                    data = s.recv(65536)
                    if not data:
                        break
                    chunks.append(data)
        except OSError as exc:
            raise ProxyError(f"cannot reach the HAProxy runtime API at {self.addr[0]}:{self.addr[1]}: {exc}") from exc
        return b"".join(chunks).decode()

    def servers(self):
        """{(backend, server): {stat name: value}} for every server line."""
        return parse_stats(self.command("show stat"))

    def set_weights(self, backend, weights):
        for server, weight in weights.items():
            if not 0 <= weight <= 256:
                raise ProxyError(f"weight {weight} out of range")
            out = self.command(f"set weight {backend}/{server} {weight}").strip()
            if out:
                raise ProxyError(f"set weight {backend}/{server} {weight}: {out}")

    def weights(self, backend):
        stats = self.servers()
        return {srv: int(row["weight"] or 0) for (be, srv), row in stats.items() if be == backend}


def parse_stats(text):
    lines = [ln for ln in text.splitlines() if ln.strip()]
    if not lines or not lines[0].startswith("# "):
        raise ProxyError("unexpected 'show stat' output")
    header = lines[0][2:].rstrip(",")
    out = {}
    for row in csv.DictReader(io.StringIO("\n".join([header] + lines[1:]))):
        if row.get("type") == "2":
            out[(row["pxname"], row["svname"])] = row
    return out


def num(row, field):
    try:
        return int(row.get(field) or 0)
    except ValueError:
        return 0
