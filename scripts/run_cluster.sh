#!/usr/bin/env bash
# Starts a local 3-node dkvs cluster in the background.
# Usage: scripts/run_cluster.sh [data-dir]   (default: ./data)
# Stop it with: scripts/stop_cluster.sh
set -euo pipefail

cd "$(dirname "$0")/.."

DATA_DIR="${1:-data}"
BUILD_DIR="${BUILD_DIR:-build}"

if [[ ! -x "$BUILD_DIR/dkvs-server" ]]; then
    echo "error: $BUILD_DIR/dkvs-server not found — build first:" >&2
    echo "  cmake -S . -B build && cmake --build build" >&2
    exit 1
fi

RAFT_PEERS=127.0.0.1:7100,127.0.0.1:7101,127.0.0.1:7102
CLIENT_PEERS=127.0.0.1:6100,127.0.0.1:6101,127.0.0.1:6102

mkdir -p "$DATA_DIR"
for i in 0 1 2; do
    "$BUILD_DIR/dkvs-server" \
        --id "$i" \
        --raft-peers "$RAFT_PEERS" \
        --client-peers "$CLIENT_PEERS" \
        --data-dir "$DATA_DIR/node$i" \
        >>"$DATA_DIR/node$i.log" 2>&1 &
    echo $! > "$DATA_DIR/node$i.pid"
    echo "node $i: pid $(cat "$DATA_DIR/node$i.pid"), client 127.0.0.1:$((6100 + i)), log $DATA_DIR/node$i.log"
done

echo
echo "cluster is up — talk to it with:"
echo "  $BUILD_DIR/dkvs-cli $CLIENT_PEERS"
