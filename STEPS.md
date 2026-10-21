# STEPS: Zero-Downtime Application Migration

## Prerequisites (do these first)
- **EC2**: t3.medium (4 GB RAM), **20 GB disk**, Ubuntu 22.04+ or Amazon Linux 2023.
- **Security group**: SSH (22) from your IP only. Everything else listens on 127.0.0.1 inside the instance.
- **No AWS permissions, tokens or accounts needed.** Nothing leaves the instance.
- Versions installed by setup: Docker (distro package) with Compose v2.29.7, Python 3 venv, `psycopg` 3.2.3. Containers: PostgreSQL 16.4, HAProxy 2.8.

## Steps
```bash
git clone <your-repo-url> zero-downtime-migration && cd zero-downtime-migration
scripts/setup.sh        # installs Docker, Compose, Python deps (~3 min)
newgrp docker           # lets you use docker without sudo
make test               # 21 unit tests (the 18 integration tests skip without local Postgres, see below)
scripts/up.sh           # legacy stack, modern stack, proxy (~2 min first time)
scripts/verify.sh       # migrates under live load, checks everything, then rolls back (~2 min)
```
Watch it happen yourself (two terminals):
```bash
scripts/demo.sh                          # terminal 1: client + migration + report
watch -n1 'curl -s localhost:8080/stats' # terminal 2: row count and which version answers
```
Run it again: `scripts/reset.sh`, then `scripts/demo.sh`.

## Step by step by hand
```bash
set -a; source .env; set +a
.venv/bin/python -m migrate preflight     # 19 safety checks
.venv/bin/python -m migrate replicate     # copy + stream to the new database
.venv/bin/python -m migrate canary        # shift reads 10% -> 50% -> 100%, gated on errors and data comparison
.venv/bin/python -m migrate cutover       # pause writes ~0.2 s, verify, switch
.venv/bin/python -m migrate rollback      # or: finalize (removes the way back)
.venv/bin/python -m migrate status
```

## Clean up
`make down` (removes containers, volumes, `.env`).

## Optional: run the integration tests on the instance
They start real Postgres and HAProxy processes (needs root). Ubuntu 24.04:
```bash
sudo apt-get install -y postgresql-16 haproxy
sudo .venv/bin/python -m pytest -q       # 39 tests, ~80 s
```

## If something fails
- `permission denied ... docker.sock`: run `newgrp docker`.
- `scripts/up.sh` hangs on a database: `docker compose logs db-old`.
- Canary aborts with "not enough traffic": start the client first (`scripts/demo.sh` does), or lower `MIN_REQUESTS` in `.env`.
- Any abort is safe: the legacy stack keeps serving. Read the `STOPPED:` line, fix, re-run the same command.
