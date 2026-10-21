#!/usr/bin/env bash
source "$(dirname "$0")/lib.sh"
docker compose down -v --remove-orphans
rm -rf .env .state probe-report.json
echo "Everything removed."
