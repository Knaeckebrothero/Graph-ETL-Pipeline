#!/usr/bin/env python3
"""Empty the graph of the proof-of-concept cell. Only while the database tool is stopped.

    python3 experiments/harness/poc/reset.py

Removes a commit hook that a killed tool may have left behind, waits until the store has dropped it,
deletes every node, and prints what is left (0).
"""

import json
import os
import sys
import time
import urllib.request
from pathlib import Path

from neo4j import GraphDatabase

config = json.loads((Path(__file__).parent / "cells.poc.json").read_text(encoding="utf-8"))
port = config["listen"].rpartition(":")[2]
try:
    urllib.request.urlopen(f"http://127.0.0.1:{port}/health", timeout=2)
    sys.exit("the database tool is running; stop it first")
except OSError:
    pass

for cell_id, cell in config["cells"].items():
    password = os.environ.get(cell["password_env"], "harness_dev")
    with GraphDatabase.driver(cell["uri"], auth=(cell["user"], password)) as driver:
        with driver.session(database="system") as session:
            session.run("CALL apoc.trigger.drop($db, $name)", db=cell["database"], name=f"dbtool-{cell_id}").consume()
        time.sleep(3)                                   # the store reads its hooks once a second
        with driver.session(database=cell["database"]) as session:
            session.run("MATCH (n) DETACH DELETE n").consume()
            left = session.run("MATCH (n) RETURN count(n) AS n").single()["n"]
    print(f"{cell_id}: {left} nodes left")
