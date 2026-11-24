#!/usr/bin/env python3
"""Entry point of the pilot image: the database tool, or the MCP wrapper of one cell.

    entrypoint.py tool /config/tool-config.json    # waits until every cell's store answers, then serves
    entrypoint.py mcp [port]                       # the wrapper that offers `cypher`; DBTOOL_URL is the tool address of the cell
    entrypoint.py mcp-sparql [port]                # the wrapper that offers `sparql`, for an RDF cell
    entrypoint.py info                             # what this image was built from
"""

from __future__ import annotations

import json
import os
import runpy
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent


def answers(cell: dict) -> None:
    """One request to the cell's store; raises when it does not answer."""
    password = os.environ.get(cell.get("password_env", ""), cell.get("password", ""))
    if cell["arm"] == "rdf":
        import base64
        import urllib.request

        login = base64.b64encode(f"{cell.get('user', 'admin')}:{password}".encode()).decode()
        request = urllib.request.Request(cell["endpoint"].rstrip("/") + "/sparql", data=b"ASK { }", method="POST",
                                         headers={"Content-Type": "application/sparql-query", "Authorization": f"Basic {login}"})
        with urllib.request.urlopen(request, timeout=10) as response:
            response.read()
        return
    from neo4j import GraphDatabase

    with GraphDatabase.driver(cell["uri"], auth=(cell.get("user", "neo4j"), password)) as driver:
        with driver.session(database=cell.get("database", "neo4j")) as session:
            session.run("RETURN 1").consume()


def wait_for_stores(config: dict, wait_s: float) -> None:
    deadline = time.monotonic() + wait_s
    for cell_id, cell in config["cells"].items():
        while True:
            try:
                answers(cell)
                print(f"the store of {cell_id} answers", flush=True)
                break
            except Exception as error:
                if time.monotonic() > deadline:
                    raise SystemExit(f"the store of {cell_id} ({cell.get('uri') or cell.get('endpoint')}) did not answer "
                                     f"within {wait_s:.0f} s: {type(error).__name__}: {error}")
                time.sleep(2)


def main() -> int:
    mode = sys.argv[1] if len(sys.argv) > 1 else ""
    if mode == "info":
        print((HERE / "BUILD_INFO.json").read_text(encoding="utf-8"))
        return 0
    if mode == "tool" and len(sys.argv) == 3:
        config = json.loads(Path(sys.argv[2]).read_text(encoding="utf-8"))
        wait_for_stores(config, float(os.environ.get("PILOT_STORE_WAIT_S", "300")))
        sys.argv = ["serve.py", sys.argv[2]]
        runpy.run_path(str(HERE / "dbtool" / "serve.py"), run_name="__main__")
        return 0
    if mode in ("mcp", "mcp-sparql"):
        script = "cypher_mcp.py" if mode == "mcp" else "sparql_mcp.py"
        sys.argv = [script] + sys.argv[2:3]
        runpy.run_path(str(HERE / "mcp" / script), run_name="__main__")
        return 0
    print(__doc__)
    return 2


if __name__ == "__main__":
    sys.exit(main())
