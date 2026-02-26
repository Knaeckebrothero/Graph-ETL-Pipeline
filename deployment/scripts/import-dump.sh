#!/usr/bin/env bash
# Import a Neo4j database dump into the Quadlet-managed instance.
#
# Usage:
#   ./import-dump.sh                          # uses backups/neo4j.dump
#   ./import-dump.sh /path/to/neo4j.dump
#
# Neo4j Community Edition requires the database to be stopped for load.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

DUMP_FILE="${1:-$PROJECT_ROOT/backups/neo4j.dump}"
IMAGE="docker.io/neo4j:5-community"

# Quadlet names its volumes with a "systemd-" prefix
DATA_VOLUME="systemd-neo4j-data"

# --- pre-flight checks -------------------------------------------------------

if [[ ! -f "$DUMP_FILE" ]]; then
    echo "ERROR: Dump file not found: $DUMP_FILE" >&2
    echo "Usage: $0 [/path/to/neo4j.dump]" >&2
    exit 1
fi

DUMP_ABS="$(realpath "$DUMP_FILE")"
DUMP_DIR="$(dirname "$DUMP_ABS")"
DUMP_NAME="$(basename "$DUMP_ABS")"

echo "Dump file: $DUMP_ABS ($(du -h "$DUMP_ABS" | cut -f1))"

# --- stop neo4j ---------------------------------------------------------------

SERVICE_ACTIVE=false
if systemctl --user is-active neo4j.service &>/dev/null; then
    SERVICE_ACTIVE=true
    echo "Stopping neo4j service..."
    systemctl --user stop neo4j.service
    # Wait for container to fully stop
    sleep 2
fi

# Also handle compose-managed container
if podman container exists neo4j-etl 2>/dev/null; then
    echo "Stopping existing neo4j-etl container..."
    podman stop neo4j-etl 2>/dev/null || true
    podman rm neo4j-etl 2>/dev/null || true
    sleep 2
fi

# --- ensure data volume exists ------------------------------------------------

if ! podman volume exists "$DATA_VOLUME" 2>/dev/null; then
    echo "Creating data volume: $DATA_VOLUME"
    podman volume create "$DATA_VOLUME"
fi

# --- load dump ----------------------------------------------------------------

echo "Loading dump into Neo4j data volume..."

# The dump dir needs to be readable by the neo4j user (uid 7474) inside the
# container. Mount with :z for SELinux relabeling on Fedora.
podman run --rm \
    -v "$DATA_VOLUME":/data \
    -v "$DUMP_DIR":/dumps:z \
    "$IMAGE" \
    neo4j-admin database load neo4j --from-path=/dumps --overwrite-destination=true

echo "Dump loaded successfully."

# --- restart neo4j ------------------------------------------------------------

if [[ "$SERVICE_ACTIVE" == true ]]; then
    echo "Restarting neo4j service..."
    systemctl --user start neo4j.service
else
    echo "Neo4j service was not running before import."
    echo "Start it with: systemctl --user start neo4j.service"
    exit 0
fi

# --- wait for healthy ---------------------------------------------------------

echo "Waiting for Neo4j to start (up to 60s)..."
for i in $(seq 1 30); do
    if curl -sf http://localhost:7474 &>/dev/null; then
        echo "Neo4j is up! Verifying data..."
        # Quick node count via HTTP API
        RESULT=$(curl -sf \
            -H "Content-Type: application/json" \
            -u neo4j:neo4j_dev \
            -d '{"statements":[{"statement":"MATCH (n) RETURN count(n) AS nodes"}]}' \
            http://localhost:7474/db/neo4j/tx/commit 2>/dev/null) || true
        if [[ -n "$RESULT" ]]; then
            echo "  $RESULT" | python3 -c "
import sys, json
try:
    r = json.load(sys.stdin)
    nodes = r['results'][0]['data'][0]['row'][0]
    print(f'  Nodes in graph: {nodes:,}')
except Exception:
    print('  (could not parse node count)')
" 2>/dev/null || echo "  (node count check skipped)"
        fi
        exit 0
    fi
    sleep 2
done

echo "WARNING: Neo4j did not respond within 60s. Check logs:"
echo "  journalctl --user -u neo4j -f"
exit 1
