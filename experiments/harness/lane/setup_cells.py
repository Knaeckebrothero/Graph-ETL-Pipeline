#!/usr/bin/env python3
"""Create, once per cell, what SRW needs to run the cell's jobs: a project, the connector `graph` (the
cell's MCP wrapper with the cell's key) and the connector `run-query-env` (the two variables of
`run_query` in the workspace), and the workspace template of the proof of concept.

    python3 setup_cells.py [--host host.k3d.internal] [cell ...]      # default: every enabled cell

Every cell has its own SRW account (token `SRW_TOKEN_<CELL>` in deploy/pilot/run/srw.env), because SRW
allows one connector of a name and type per account, and every cell's connector is named `graph`: the
model sees the tool as `mcp__graph__cypher` or `mcp__graph__sparql` in every cell.

Writes deploy/pilot/run/setup/<cell>.json (the ids). A cell that already has that file is left alone.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import CELLS_JSON, KEYS, PILOT_RUN, SrwClient, key_of_cell, psql, token_of_cell  # noqa: E402

WORKSPACE_IMAGE = "srw-registry:5000/thesis-poc-workspace@sha256:ef1d7574623ec71cca4d299286658caa852ef81a33ba88abecbde3b1a7ffc53a"
TEMPLATE = "thesis-poc-workspace"


def ensure(cell: dict, host: str) -> dict:
    target = PILOT_RUN / "setup" / f"{cell['id']}.json"
    if target.exists():
        return json.loads(target.read_text(encoding="utf-8"))
    client = SrwClient(token_of_cell(cell["id"]))
    key = key_of_cell(cell["id"])
    user_id = psql("srw-postgres-0", "srw", f"select id from users where email = 'thesis-{cell['id']}@example.invalid';")[0]
    code, project = client.call("POST", "/projects", {
        "user_id": user_id, "name": cell["id"], "description": ""})
    if code not in (200, 201):
        raise SystemExit(f"{cell['id']}: project not created: {code} {project}")
    code, mcp = client.call("POST", "/datasources", {
        "name": "graph", "type": "mcp",
        "description": "",          # the workspace README shows it to the agent: nothing about the cell or the study
        "connection_url": f"http://{host}:{cell['mcp_port']}/mcp",
        "credentials": {"transport": "http", "auth": {"type": "headers", "headers": {"X-Tool-Key": key}}},
    })
    if code not in (200, 201):
        raise SystemExit(f"{cell['id']}: connector graph not created: {code} {mcp}")
    code, env = client.call("POST", "/datasources", {
        "name": "run-query-env", "type": "credentials",
        "description": "",
        "credentials": {"env_vars": {"RUN_QUERY_URL": f"http://{host}:28080/cells/{cell['id']}/tool", "RUN_QUERY_KEY": key}},
    })
    if code not in (200, 201):
        raise SystemExit(f"{cell['id']}: connector run-query-env not created: {code} {env}")
    manifest = (f"apiVersion: srw/v1alpha1\nkind: WorkspaceTemplate\nmetadata:\n  name: {TEMPLATE}\n"
                f"  scope: {{kind: Account, name: me}}\nspec:\n  backend: sandbox\n  environment:\n    image: {WORKSPACE_IMAGE}\n")
    code, applied = client.call("POST", "/manifests/apply", {"source": manifest})
    if code not in (200, 201):
        raise SystemExit(f"{cell['id']}: workspace template not applied: {code} {applied}")
    out = {"cell": cell["id"], "project_id": project["id"], "mcp_id": mcp["id"], "env_id": env["id"],
           "mcp_url": f"http://{host}:{cell['mcp_port']}/mcp", "template": TEMPLATE, "workspace_image": WORKSPACE_IMAGE}
    target.parent.mkdir(exist_ok=True)
    target.write_text(json.dumps(out, indent=1) + "\n", encoding="utf-8")
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("cells", nargs="*")
    parser.add_argument("--host", default="host.k3d.internal", help="the name SRW's pods reach the bundle by")
    args = parser.parse_args()
    config = json.loads(CELLS_JSON.read_text(encoding="utf-8"))
    cells = [cell for cell in config["cells"] if cell.get("enabled") and (not args.cells or cell["id"] in args.cells)]
    for cell in cells:
        print(json.dumps(ensure(cell, args.host)))
    return 0


if __name__ == "__main__":
    sys.exit(main())
