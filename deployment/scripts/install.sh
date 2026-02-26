#!/usr/bin/env bash
# Install Neo4j as a rootless Podman Quadlet (systemd-managed container).
# Run as the regular user (not root).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
QUADLET_SRC="$SCRIPT_DIR/../quadlet"
QUADLET_DST="$HOME/.config/containers/systemd"

# --- pre-flight checks -------------------------------------------------------

if [[ $EUID -eq 0 ]]; then
    echo "ERROR: Run this as your regular user, not root." >&2
    exit 1
fi

if ! command -v podman &>/dev/null; then
    echo "ERROR: podman not found. Install it with: sudo dnf install podman" >&2
    exit 1
fi

# Verify lingering is enabled (must be done by an admin beforehand)
if [[ "$(loginctl show-user "$USER" --property=Linger --value 2>/dev/null)" != "yes" ]]; then
    echo "ERROR: Lingering is not enabled for $USER." >&2
    echo "An admin must run:  sudo loginctl enable-linger $USER" >&2
    exit 1
fi

# --- stop existing compose container if running ------------------------------

if podman container exists neo4j-etl 2>/dev/null; then
    echo "Stopping existing neo4j-etl container..."
    podman stop neo4j-etl 2>/dev/null || true
    podman rm neo4j-etl 2>/dev/null || true
fi

# --- install quadlet files ----------------------------------------------------

mkdir -p "$QUADLET_DST"

echo "Copying Quadlet files to $QUADLET_DST ..."
cp "$QUADLET_SRC"/neo4j-data.volume   "$QUADLET_DST/"
cp "$QUADLET_SRC"/neo4j-logs.volume   "$QUADLET_DST/"
cp "$QUADLET_SRC"/neo4j-import.volume "$QUADLET_DST/"
cp "$QUADLET_SRC"/neo4j.container     "$QUADLET_DST/"

echo "Reloading systemd user daemon..."
systemctl --user daemon-reload

echo "Enabling and starting neo4j service..."
systemctl --user enable --now neo4j.service

# --- wait for healthy ---------------------------------------------------------

echo "Waiting for Neo4j to start (up to 60s)..."
for i in $(seq 1 30); do
    if curl -sf http://localhost:7474 &>/dev/null; then
        echo "Neo4j is up!"
        echo ""
        systemctl --user status neo4j.service --no-pager
        echo ""
        echo "Browser:  http://localhost:7474"
        echo "Bolt:     bolt://localhost:7687"
        echo "Auth:     neo4j / neo4j_dev"
        exit 0
    fi
    sleep 2
done

echo "WARNING: Neo4j did not respond within 60s. Check logs:"
echo "  journalctl --user -u neo4j -f"
exit 1
