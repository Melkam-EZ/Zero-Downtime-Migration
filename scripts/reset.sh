#!/usr/bin/env bash
# Put the migration back to 'before' so the demo can run again (empties the NEW database only).
source "$(dirname "$0")/lib.sh"; load_env
$PY -m migrate reset --force
