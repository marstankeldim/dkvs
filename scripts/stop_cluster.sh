#!/usr/bin/env bash
# Stops a cluster started by run_cluster.sh.
# Usage: scripts/stop_cluster.sh [data-dir]   (default: ./data)
set -euo pipefail

cd "$(dirname "$0")/.."
DATA_DIR="${1:-data}"

for pidfile in "$DATA_DIR"/node*.pid; do
    [[ -e "$pidfile" ]] || continue
    pid=$(cat "$pidfile")
    if kill "$pid" 2>/dev/null; then
        echo "stopped pid $pid"
    fi
    rm -f "$pidfile"
done
