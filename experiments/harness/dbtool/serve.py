#!/usr/bin/env python3
"""Start the database tool from a configuration file.

    python experiments/harness/dbtool/serve.py experiments/harness/dbtool/cells.example.json

Passwords and the control key come from environment variables named in the file.
"""

from __future__ import annotations

import json
import os
import signal
import sys
import threading
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dbtool.checker import load_checker  # noqa: E402
from dbtool.service import Service  # noqa: E402


def main() -> int:
    if len(sys.argv) != 2:
        print(__doc__)
        return 2
    config = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
    host, _, port = config.get("listen", "127.0.0.1:18080").partition(":")
    service = Service(host, int(port), control_key=os.environ.get(config.get("control_key_env", ""), ""),
                      hook_address=config.get("hook_address", ""))
    for cell_id, cell in config["cells"].items():
        common = dict(
            checker=load_checker(cell.get("checker", config.get("checker"))),
            mode=cell.get("mode", "reject"), log_dir=config.get("log_dir", "dbtool-records"),
            realisation=cell.get("realisation"), max_rows=cell.get("max_rows", 200),
            timeout_s=cell.get("timeout_s", 60.0),
            warn_deleted=cell.get("warn_deleted", config.get("warn_deleted", 25)),
            tool_key=os.environ.get(cell.get("tool_key_env", ""), ""))
        password = os.environ.get(cell.get("password_env", ""), cell.get("password", ""))
        if cell["arm"] == "pg":
            service.add_neo4j_cell(cell_id, cell["uri"], cell.get("user", "neo4j"), password,
                                   database=cell.get("database", "neo4j"), **common)
        elif cell["arm"] == "rdf":
            # endpoint: the dataset's address; change_log: the dataset's change log as this process reads it
            service.add_fuseki_cell(cell_id, cell["endpoint"], cell.get("user", "admin"), password,
                                    change_log=cell["change_log"], namespace=cell.get("namespace"), **common)
        else:
            raise SystemExit(f"{cell_id}: arm is pg or rdf, not {cell['arm']!r}")
    service.start()
    print(f"database tool on {service.address}, cells: {', '.join(sorted(service.cells))}", flush=True)
    stop = threading.Event()
    for name in (signal.SIGINT, signal.SIGTERM):
        signal.signal(name, lambda *_: stop.set())
    stop.wait()
    service.stop(remove_hooks=config.get("remove_hooks_on_stop", False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
