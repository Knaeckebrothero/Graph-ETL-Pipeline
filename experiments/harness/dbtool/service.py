"""The database tool: one service for one or more cells.

For the agent (later wrapped as a tool of the agent framework):
    POST /cells/<cell>/tool   {"statement": "...", "parameters": {...}, "passage": "...", "locator": "...", "id": "..."}

The same address serves the tool call of the agent framework and `run_query` inside the workspace
(client/run_query.py). When a cell has a tool key, the call has to carry it (`X-Tool-Key`), so that a
workspace reaches its own cell only.

For the harness (needs the control key when one is set):
    POST   /cells/<cell>/unit   {"unit_id", "run_id", "source_id", "stratum"}   open a unit
    DELETE /cells/<cell>/unit                                                   close it
    POST   /check/<cell>        called by the store's commit hook (property graph)
    GET    /health

A property-graph cell is checked by a hook inside the store, before the commit (neo4j_cell.py). An RDF
cell has no such hook: the store logs what a statement changed, the tool reads that, and a rejected
statement is taken out again (fuseki_cell.py). Both end in the same verdict (`Cell.judge`) and the same record.

Every call of the tool is appended to <log_dir>/<cell>.jsonl, in order. What a person should look at
(a large delete, a write that went around the tool, a hook that is not working, a checker that failed)
is also appended to <log_dir>/warnings.jsonl, one file for all cells, and printed on standard error.
"""

from __future__ import annotations

import datetime
import hashlib
import json
import sys
import threading
import time
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

try:
    from . import checker as checking, guard
    from .fuseki_cell import FusekiCell, triple_id
    from .neo4j_cell import TOKEN_KEY, Neo4jCell, changes_from_hook, is_empty
except ImportError:  # run as a script
    import checker as checking, guard
    from fuseki_cell import FusekiCell, triple_id
    from neo4j_cell import TOKEN_KEY, Neo4jCell, changes_from_hook, is_empty

TOOL_NAME = {"pg": "cypher", "rdf": "sparql"}
SYNTAX_ERRORS = ("Neo.ClientError.Statement.SyntaxError", "Fuseki.ParseError")


@dataclass
class Verdict:
    """What the checker said about one statement's changes, after the mode was applied."""
    rejections: list[dict] = field(default_factory=list)
    observations: list[dict] = field(default_factory=list)
    not_checkable: list[str] = field(default_factory=list)
    rejected: bool = False
    fault: bool = False                          # the checker itself failed


def now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


class Cell:
    """One cell: its store, its checker, its mode, the unit that is open, and its record file."""

    def __init__(self, cell_id: str, store, checker, mode: str, log_dir: Path, realisation: str | None = None,
                 warn_deleted: int = 25, tool_key: str = ""):
        if mode not in ("reject", "log_only"):
            raise ValueError(f"mode is reject or log_only, not {mode!r}")
        self.cell_id, self.store, self.checker, self.mode = cell_id, store, checker, mode
        self.realisation = realisation
        self.warn_deleted = warn_deleted         # a statement that deletes this many elements is reported
        self.tool_key = tool_key                 # when set, a tool call has to carry it
        self.unit_created: set[str] = set()      # element ids the open unit created and has not deleted again
        self.unit: dict | None = None
        self.fault: str | None = None            # set when a write got into the store unchecked
        self.retries: dict[tuple[str, str], int] = {}
        self.writes = 0                          # write attempts in the open unit that got a new id
        self.lock = threading.Lock()             # one statement at a time
        log_dir.mkdir(parents=True, exist_ok=True)
        self.log_path = log_dir / f"{cell_id}.jsonl"
        self.warnings_path = log_dir / "warnings.jsonl"
        if hasattr(store, "attach"):             # a store without a commit hook asks for the verdict itself
            store.attach(judge=self.judge, report=self._reported)

    # ------------------------------------------------------------------ records

    def record(self, record_type: str, **fields: Any) -> dict:
        unit = self.unit or {}
        entry = {"record_type": record_type, "ts": now(), "cell_id": self.cell_id,
                 "run_id": unit.get("run_id"), "unit_id": unit.get("unit_id"),
                 "source_id": unit.get("source_id"), "stratum": unit.get("stratum"), **fields}
        with self.log_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(entry, ensure_ascii=False, default=str) + "\n")
        return entry

    def warn(self, kind: str, text: str, **fields: Any) -> None:
        """Something a person should look at. Never changes what the agent is told or what is stored."""
        entry = self.record("warning", kind=kind, text=text, **fields)
        with self.warnings_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(entry, ensure_ascii=False, default=str) + "\n")
        print(f"dbtool warning [{self.cell_id}, unit {entry['unit_id']}] {kind}: {text}", file=sys.stderr, flush=True)

    # ------------------------------------------------------------------ units

    def open_unit(self, unit: dict) -> None:
        if not unit.get("unit_id"):
            raise ValueError("a unit needs a unit_id")
        with self.lock:
            if not self.store.ready():               # every unit starts with a store that was seen checking
                raise RuntimeError(self.store.not_ready)
            self.unit, self.retries, self.writes, self.fault = dict(unit), {}, 0, None
            self.unit_created = set()
            self.record("unit_open")

    def close_unit(self) -> None:
        with self.lock:
            if self.unit:
                self.record("unit_close")
            self.unit = None

    # ------------------------------------------------------------------ the tool

    def tool(self, statement: str, item_id: str | None = None, passage: str | None = None,
             locator: str | None = None, parameters: dict | None = None) -> dict:
        if parameters is not None and not isinstance(parameters, dict):
            return {"status": "error", "message": "parameters is a map from names to values"}
        with self.lock:
            return self._tool(statement, item_id, passage, locator, parameters)

    def _tool(self, statement: str, given_id: str | None, passage: str | None, locator: str | None,
              parameters: dict | None) -> dict:
        started = time.monotonic()
        tool_name = TOOL_NAME[self.store.arm]
        evidence = {"quote": passage, "locator": locator} if passage is not None or locator is not None else None
        payload = statement or "" if not parameters else json.dumps(
            {"statement": statement, "parameters": parameters}, sort_keys=True, ensure_ascii=False, default=str)
        common = {"tool_name": tool_name, "statement": statement, "parameters": parameters or None,
                  "payload_sha256": hashlib.sha256(payload.encode("utf-8")).hexdigest(),
                  "item_id": given_id, "evidence": evidence}
        latency = lambda: int((time.monotonic() - started) * 1000)

        if self.unit is None:
            return {"status": "error", "message": "No unit is open. This is a fault of the harness, not of the statement."}
        if self.fault:
            return {"status": "error", "message": self.fault}
        if parameters and self.store.arm == "rdf":
            message = "SPARQL takes no parameters. Write the values into the statement."
            self.record("statement_error", engine_error=message, engine_error_code="DbTool.Parameters",
                        latency_ms=latency(), **common)
            return {"status": "error", "message": message}
        refusal = guard.refuse(statement, self.store.arm)
        if refusal:
            self.record("refused", reason=refusal, latency_ms=latency(), **common)
            return {"status": "refused", "message": f"Not passed on to the store: {refusal}."}

        # The tool names a write at its first attempt and returns the name; the agent sends it back with a
        # correction. The name is only used up if the statement turns out to be a write.
        item_id = given_id or f"{self.unit['unit_id']}-w{self.writes + 1:03d}"
        context = {"cell_id": self.cell_id, "arm": self.store.arm, "realisation": self.realisation,
                   "unit_id": self.unit["unit_id"], "item_id": item_id, "evidence": passage, "statement": statement,
                   "parameters": parameters or None}
        outcome = self.store.run(statement, context, parameters)

        if outcome.kind == "unchecked":
            self.fault = ("The store is not checking writes at the moment. This is a fault of the harness, "
                          "not of the statement. Nothing more can be run in this unit.")
            if self.store.arm == "pg":
                self.record("hook_missing", latency_ms=latency(), **common)
                self.warn("hook_missing", "a write was committed without being checked; the unit has to be repeated",
                          statement=statement)
            else:                                # RDF: the log gave nothing, or an undo did not go through
                self.record(outcome.error_code or "log_missing", reason=outcome.error, latency_ms=latency(), **common)
                self.warn(outcome.error_code or "log_missing", f"{outcome.error}; the unit has to be repeated",
                          statement=statement)
            return {"status": "error", "message": self.fault}

        if outcome.kind == "result":
            self.record("graph_read", rows=len(outcome.rows), truncated=outcome.truncated, latency_ms=latency(), **common)
            return {"status": "result", "rows": outcome.rows, "row_count": len(outcome.rows), "truncated": outcome.truncated}

        if outcome.kind == "error" and given_id is None and evidence is None:
            self.record("statement_error", engine_error=outcome.error, engine_error_code=outcome.error_code,
                        latency_ms=latency(), **common)
            return {"status": "error", "message": outcome.error}

        # everything below is a write attempt
        if given_id is None:
            self.writes += 1
        key = (self.unit["unit_id"], item_id)
        retry_index = self.retries.get(key, 0)
        self.retries[key] = retry_index + 1
        attempt = {"retry_index": retry_index, "validator_mode": self.mode, "engine_error": None,
                   "element_ids": [], "rejections": [], **common, "item_id": item_id,
                   "observations": outcome.observations, "validator_not_checkable": outcome.not_checkable}

        if outcome.kind == "error":
            unreadable = outcome.error_code in SYNTAX_ERRORS
            attempt.update(validator_result="reject" if unreadable else "pass", engine_result="error",
                           engine_error=outcome.error, accepted=False,
                           rejections=[{"rule_id": "CS0", "element": "statement",
                                        "expected": "a statement the store can read",
                                        "found": outcome.error_code}] if unreadable else [])
            self.record("insert_attempt", latency_ms=latency(), **attempt)
            return {"status": "error", "id": item_id, "message": outcome.error}

        if outcome.kind == "unchanged":
            attempt.update(validator_result="pass", engine_result="already_present", accepted=True,
                           element_ids=outcome.existing)
            self.record("insert_attempt", latency_ms=latency(), **attempt)
            return {"status": "already_present", "id": item_id, "rows": outcome.rows,
                    "message": "The statement changed nothing: what it writes is already in the graph."}

        changes = outcome.changes or {}
        if "triples" in changes:                 # RDF: a triple has no id, so it is named by the hash of its line
            element_ids = [triple_id(line) for line in changes["triples"]["added"]]
        else:
            element_ids = [n["key"] for n in changes.get("nodes", [])] + [r["key"] for r in changes.get("relationships", [])]
        summary = summarise(changes)
        if outcome.kind == "rejected":
            attempt.update(validator_result="reject", engine_result="not_sent", accepted=False,
                           rejections=outcome.rejections)
            self.record("insert_attempt", changes=changes, latency_ms=latency(), **attempt)
            return {"status": "rejected", "id": item_id, "message": checking.message(outcome.rejections),
                    "rejections": outcome.rejections,
                    "note": "Nothing of this statement was stored. Send the corrected statement with this id."}

        # stored; in log-only mode the checker may still have found faults
        attempt.update(validator_result="reject" if outcome.rejections else "pass", engine_result="ok",
                       accepted=True, rejections=outcome.rejections, element_ids=element_ids)
        self.record("insert_attempt", changes=changes, latency_ms=latency(), **attempt)
        self._watch_deletes(changes, item_id, statement)
        return {"status": "stored", "id": item_id, **summary, "rows": outcome.rows}

    def _watch_deletes(self, changes: dict, item_id: str, statement: str) -> None:
        """Deleting is allowed. A statement that deletes a lot is reported, with how much of it older units wrote."""
        if "triples" in changes:                 # RDF: counted in triples
            gone = changes["triples"]["removed"]
            from_earlier_units = [line for line in gone if line not in self.unit_created]
            self.unit_created.difference_update(gone)
            self.unit_created.update(changes["triples"]["added"])
            if len(gone) >= self.warn_deleted:
                size = self.store.size()
                self.warn("bulk_delete",
                          f"one statement deleted {len(gone)} triples, {len(from_earlier_units)} of them written by "
                          f"earlier units; the graph now holds {size['triples']} triples",
                          item_id=item_id, statement=statement, deleted_triples=len(gone),
                          deleted_from_earlier_units=len(from_earlier_units), graph_after=size)
            return
        deleted = changes.get("deleted", {})
        gone = list(deleted.get("nodes", [])) + list(deleted.get("relationships", []))
        from_earlier_units = [key for key in gone if key not in self.unit_created]
        self.unit_created.difference_update(gone)
        self.unit_created.update(n["key"] for n in changes.get("nodes", []) if n["change"] == "created")
        self.unit_created.update(r["key"] for r in changes.get("relationships", []) if r["change"] == "created")
        if len(gone) >= self.warn_deleted:
            size = self.store.size()
            self.warn("bulk_delete",
                      f"one statement deleted {len(deleted.get('nodes', []))} nodes and "
                      f"{len(deleted.get('relationships', []))} relationships, {len(from_earlier_units)} of them "
                      f"written by earlier units; the graph now holds {size['nodes']} nodes and "
                      f"{size['relationships']} relationships",
                      item_id=item_id, statement=statement, deleted_nodes=len(deleted.get("nodes", [])),
                      deleted_relationships=len(deleted.get("relationships", [])),
                      deleted_from_earlier_units=len(from_earlier_units), graph_after=size)

    # ------------------------------------------------------------------ the verdict

    def judge(self, changes: dict) -> Verdict:
        """Hand one statement's changes to the checker and apply the mode. A checker that fails lets nothing through."""
        try:
            rejections = checking.normalise(self.checker.check(changes, self.store))
        except Exception as error:
            self.warn("checker_fault", f"the checker failed with {type(error).__name__}: {error}; nothing was stored",
                      statement=changes.get("statement"))
            return Verdict(rejections=[{"rule_id": "CHECKER-FAULT", "element": "checker", "expected": "a verdict",
                                        "found": f"{type(error).__name__}: {error}"}], rejected=True, fault=True)
        # A checker may keep what it counted without rejecting, and say which rules it could not decide.
        verdict = Verdict(rejections=rejections)
        observations = getattr(self.checker, "observations", None)
        if isinstance(observations, list):
            verdict.observations = list(observations)
            observations.clear()
        not_checkable = getattr(self.checker, "not_checkable", None)
        verdict.not_checkable = list(not_checkable()) if callable(not_checkable) else []
        verdict.rejected = bool(rejections) and self.mode == "reject"
        return verdict

    def _reported(self, kind: str, text: str, changes: dict | None = None, **fields: Any) -> None:
        """What a store without a commit hook found on its own: a write past the tool, a log without a mark."""
        if kind == "unattributed_write":
            self.record("unattributed_write", changes=changes or {}, **fields)
        self.warn(kind, text, **({} if kind == "unattributed_write" else fields))

    # ------------------------------------------------------------------ the commit hook

    def check(self, payload: dict) -> dict:
        """Called while the store waits to commit. Answers keep or reject."""
        token = (payload.get("meta") or {}).get(TOKEN_KEY)
        pending = self.store.pending.get(token) if token else None
        if pending is None:
            self.record("unattributed_write", changes=changes_from_hook(payload))
            self.warn("unattributed_write", "a write reached the store without passing the tool; it was undone")
            return {"reject": True, "message": "A write reached the store without passing the database tool."}
        if pending.kind == "admin":
            return {"reject": False, "message": ""}
        if pending.kind == "canary":
            pending.rejected = True
            return {"reject": True, "message": "canary"}
        changes = {**pending.context, **changes_from_hook(payload)}
        pending.changes = changes
        if is_empty(changes):                           # created and removed again: nothing to check
            return {"reject": False, "message": ""}
        verdict = self.judge(changes)
        pending.rejections, pending.rejected = verdict.rejections, verdict.rejected
        pending.observations, pending.not_checkable = verdict.observations, verdict.not_checkable
        if verdict.fault:
            return {"reject": True, "message": "The checker failed; nothing was stored."}
        return {"reject": pending.rejected, "message": checking.message(pending.rejections) if pending.rejected else ""}


def summarise(changes: dict) -> dict:
    created_nodes = [n for n in changes.get("nodes", []) if n["change"] == "created"]
    created_relationships = [r for r in changes.get("relationships", []) if r["change"] == "created"]
    summary = {
        "created": {"nodes": len(created_nodes), "relationships": len(created_relationships)},
        "changed": sum(1 for n in changes.get("nodes", []) if n["change"] == "changed")
        + sum(1 for r in changes.get("relationships", []) if r["change"] == "changed"),
        "deleted": len(changes.get("deleted", {}).get("nodes", [])) + len(changes.get("deleted", {}).get("relationships", [])),
    }
    if "triples" in changes:                     # RDF: the same counted in triples
        summary["triples"] = {"added": len(changes["triples"]["added"]), "removed": len(changes["triples"]["removed"])}
    return summary


class Service:
    def __init__(self, host: str = "127.0.0.1", port: int = 18080, control_key: str = "",
                 hook_address: str = ""):
        self.cells: dict[str, Cell] = {}
        self.control_key = control_key
        self.hook_address = hook_address.rstrip("/")   # where the store reaches the tool, if not the listen address
        self.ready = False                       # true once every cell's hook was seen working
        service = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *args: Any) -> None:
                pass

            def _body(self) -> dict:
                if self.headers.get("Content-Length"):
                    raw = self.rfile.read(int(self.headers["Content-Length"]))
                elif "chunked" in (self.headers.get("Transfer-Encoding") or "").lower():
                    raw = b""                              # the store's hook sends its body in chunks
                    while True:
                        size = int(self.rfile.readline().strip() or b"0", 16)
                        if size == 0:
                            self.rfile.readline()
                            break
                        raw += self.rfile.read(size)
                        self.rfile.readline()
                else:
                    raw = b""
                return json.loads(raw) if raw.strip() else {}

            def _send(self, status: int, body: dict) -> None:
                data = json.dumps(body, ensure_ascii=False, default=str).encode("utf-8")
                self.send_response(status)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

            def _handle(self, method: str) -> None:
                parts = [part for part in self.path.split("?")[0].split("/") if part]
                try:
                    body = self._body() if method == "POST" else {}
                    if method == "GET" and parts == ["health"]:
                        return self._send(200, {"status": "ok" if service.ready else "starting",
                                                "cells": sorted(service.cells)})
                    control = len(parts) == 2 and parts[0] == "check" or len(parts) == 3 and parts[2] == "unit"
                    if control and service.control_key and self.headers.get("X-Harness-Key") != service.control_key:
                        return self._send(403, {"status": "error", "message": "control key missing or wrong"})
                    if method == "POST" and len(parts) == 2 and parts[0] == "check" and parts[1] in service.cells:
                        return self._send(200, service.cells[parts[1]].check(body))
                    if len(parts) == 3 and parts[0] == "cells" and parts[1] in service.cells:
                        cell = service.cells[parts[1]]
                        if method == "POST" and parts[2] == "tool":
                            if cell.tool_key and self.headers.get("X-Tool-Key") != cell.tool_key:
                                return self._send(403, {"status": "error", "message": "tool key missing or wrong"})
                            return self._send(200, cell.tool(body.get("statement", ""), body.get("id"), body.get("passage"),
                                                             body.get("locator"), body.get("parameters")))
                        if method == "POST" and parts[2] == "unit":
                            cell.open_unit(body)
                            return self._send(200, {"status": "ok"})
                        if method == "DELETE" and parts[2] == "unit":
                            cell.close_unit()
                            return self._send(200, {"status": "ok"})
                    self._send(404, {"status": "error", "message": "unknown address"})
                except Exception as error:
                    self._send(500, {"status": "error", "message": f"{type(error).__name__}: {error}"})

            def do_GET(self) -> None:
                self._handle("GET")

            def do_POST(self) -> None:
                self._handle("POST")

            def do_DELETE(self) -> None:
                self._handle("DELETE")

        self.server = ThreadingHTTPServer((host, port), Handler)
        self.server.daemon_threads = True
        # a tool that listens on every interface is reached by the store on this machine through localhost
        self.address = f"http://{'127.0.0.1' if host in ('0.0.0.0', '') else host}:{self.server.server_address[1]}"

    def add_neo4j_cell(self, cell_id: str, uri: str, user: str, password: str, database: str = "neo4j",
                       checker=None, mode: str = "reject", log_dir: Path | str = ".", realisation: str | None = None,
                       max_rows: int = 200, timeout_s: float = 60.0, warn_deleted: int = 25,
                       tool_key: str = "") -> Cell:
        store = Neo4jCell(cell_id, uri, user, password, database, max_rows=max_rows, timeout_s=timeout_s)
        cell = self.cells[cell_id] = Cell(cell_id, store, checker or checking.NoChecker(), mode, Path(log_dir),
                                          realisation, warn_deleted=warn_deleted, tool_key=tool_key)
        return cell

    def add_fuseki_cell(self, cell_id: str, endpoint: str, user: str, password: str, change_log: Path | str,
                        checker=None, mode: str = "reject", log_dir: Path | str = ".", realisation: str | None = None,
                        namespace: str | None = None, max_rows: int = 200, timeout_s: float = 60.0,
                        warn_deleted: int = 25, tool_key: str = "") -> Cell:
        """An RDF cell. `endpoint` is the dataset's address, `change_log` the dataset's change log as this
        process can read it. The tool's mark of how far that log is dealt with is kept beside the records."""
        Path(log_dir).mkdir(parents=True, exist_ok=True)
        store = FusekiCell(cell_id, endpoint, user, password, change_log,
                           state_path=Path(log_dir) / f"{cell_id}.settled", namespace=namespace,
                           max_rows=max_rows, timeout_s=timeout_s)
        cell = self.cells[cell_id] = Cell(cell_id, store, checker or checking.NoChecker(), mode, Path(log_dir),
                                          realisation, warn_deleted=warn_deleted, tool_key=tool_key)
        return cell

    def start(self) -> None:
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        for cell_id, cell in self.cells.items():       # the hook can only be tested once the service answers
            cell.store.start(f"{self.hook_address or self.address}/check/{cell_id}", self.control_key)
        self.ready = True

    def stop(self, remove_hooks: bool = False) -> None:
        for cell in self.cells.values():
            if remove_hooks and hasattr(cell.store, "remove_hook"):
                cell.store.remove_hook()
            cell.store.close()
        self.server.shutdown()
        self.server.server_close()
