#!/usr/bin/env bash
# Start the database tool for the proof-of-concept run: one property-graph cell (poc-pg), the curated
# schema, Thesis B's validator as the checker. Stop with Ctrl-C; the commit hook is removed on stop.
#
# Needs the development Neo4j (deploy/harness/podman-compose.yml) and a checkout of the branch
# thesis-b/f3-spec beside this repository (VALIDATOR_CHECKOUT names another place).
# Keys are created once in run/keys.env; records and warnings go to run/records/.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/../../.." && pwd)"
validator="${VALIDATOR_CHECKOUT:-$root/../Graph-ETL-Pipeline-thesis-b}"
[ -d "$validator/experiments/thesis-b/validator" ] || { echo "no validator in $validator" >&2; exit 1; }
mkdir -p "$here/run"
keys="$here/run/keys.env"
if [ ! -f "$keys" ]; then
  key() { python3 -c 'import secrets; print(secrets.token_hex(16))'; }
  printf 'DBTOOL_CONTROL_KEY=%s\nDBTOOL_KEY_POC_PG=%s\n' "$(key)" "$(key)" > "$keys"
  chmod 600 "$keys"
fi
set -a; . "$keys"; set +a
export HARNESS_NEO4J_PASSWORD="${HARNESS_NEO4J_PASSWORD:-harness_dev}"
export PYTHONPATH="$validator/experiments/thesis-b"
export KG_VALIDATOR_ROOT="$root"
cd "$here/run"
exec python3 "$root/experiments/harness/dbtool/serve.py" "$here/cells.poc.json"
