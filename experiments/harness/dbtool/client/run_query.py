#!/usr/bin/env python3
"""run_query: the database tool, from inside the workspace.

The same call as the agent's tool, for scripts and for the shell. Every statement goes through the
same check and into the same record.

    from run_query import run_query
    answer = run_query("MATCH (w:WasteItem) RETURN w.name AS name")
    answer = run_query("UNWIND $rows AS row MERGE (:WasteItem {name: row.name})",
                       parameters={"rows": [{"name": "Paint can"}, {"name": "Jar"}]},
                       passage="the sentence or table row of the unit this rests on")
    answer["status"]      # result | stored | rejected | already_present | error | refused

    run_query 'MATCH (n) RETURN count(n) AS n'
    run_query --passage '...' --param rows='[{"name": "Jar"}]' 'UNWIND $rows AS row MERGE (:WasteItem {name: row.name})'
    run_query --file statement.cypher --params-file rows.json --passage '...'

The shell command prints the answer as JSON and ends with 0 for result, stored and already_present,
with 1 for rejected, error and refused. Standard library only.

RUN_QUERY_URL is the address of the tool for this workspace; RUN_QUERY_KEY its key, when it has one.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request

FINE = ("result", "stored", "already_present")


def run_query(statement, parameters=None, passage=None, locator=None, id=None, timeout=120):
    """Run one statement. Returns the tool's answer as a dict; never raises for a rejected or failed statement."""
    url = os.environ.get("RUN_QUERY_URL")
    if not url:
        return {"status": "error", "message": "RUN_QUERY_URL is not set: this workspace has no database tool."}
    body = {"statement": statement}
    for name, value in (("parameters", parameters), ("passage", passage), ("locator", locator), ("id", id)):
        if value is not None:
            body[name] = value
    headers = {"Content-Type": "application/json"}
    if os.environ.get("RUN_QUERY_KEY"):
        headers["X-Tool-Key"] = os.environ["RUN_QUERY_KEY"]
    try:
        data = json.dumps(body, ensure_ascii=False).encode("utf-8")
    except (TypeError, ValueError) as error:
        return {"status": "error", "message": f"the parameters cannot be written as JSON: {error}"}
    request = urllib.request.Request(url, data=data, method="POST", headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as error:
        try:
            return json.loads(error.read().decode("utf-8"))
        except ValueError:
            return {"status": "error", "message": f"the database tool answered {error.code}"}
    except (urllib.error.URLError, TimeoutError, OSError) as error:
        return {"status": "error", "message": f"the database tool cannot be reached: {error}"}


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="run_query", description="Run one statement through the database tool.")
    parser.add_argument("statement", nargs="?", help="the statement; '-' reads it from standard input")
    parser.add_argument("--file", help="read the statement from this file")
    parser.add_argument("--param", action="append", default=[], metavar="NAME=JSON",
                        help="one parameter; the value is JSON (a bare word is taken as text)")
    parser.add_argument("--params-file", help="a JSON file with all parameters as one object")
    parser.add_argument("--passage", help="the passage of the unit a write rests on")
    parser.add_argument("--locator", help="where in the unit the passage stands (section, row)")
    parser.add_argument("--id", help="the id the tool returned for this write, when this is a correction")
    args = parser.parse_args(argv)

    if args.file:
        with open(args.file, encoding="utf-8") as handle:
            statement = handle.read()
    elif args.statement == "-":
        statement = sys.stdin.read()
    elif args.statement:
        statement = args.statement
    else:
        parser.error("give a statement, '-' or --file")
    parameters = {}
    if args.params_file:
        with open(args.params_file, encoding="utf-8") as handle:
            parameters.update(json.load(handle))
    for item in args.param:
        name, separator, value = item.partition("=")
        if not separator:
            parser.error(f"--param takes NAME=JSON, not {item!r}")
        try:
            parameters[name] = json.loads(value)
        except ValueError:
            parameters[name] = value
    answer = run_query(statement, parameters or None, args.passage, args.locator, args.id)
    print(json.dumps(answer, ensure_ascii=False, indent=1))
    return 0 if answer.get("status") in FINE else 1


if __name__ == "__main__":
    sys.exit(main())
