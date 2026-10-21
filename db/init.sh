#!/bin/bash
# Runs once when a database container is first created (docker-entrypoint-initdb.d).
set -euo pipefail
psql -v ON_ERROR_STOP=1 --username postgres --dbname shop <<SQL
CREATE ROLE app LOGIN PASSWORD '${APP_PASSWORD}';
CREATE ROLE replicator LOGIN REPLICATION PASSWORD '${REPL_PASSWORD}';
ALTER DATABASE shop OWNER TO app;
SQL
