"""The database tool: one service for one or more cells.

For the agent (later wrapped as a tool of the agent framework):
    POST /cells/<cell>/tool   {"statement": "...", "id": "...", "passage": "..."}

For the harness (needs the control key when one is set):
    POST   /cells/<cell>/unit   {"unit_id", "run_id", "source_id", "stratum"}   open a unit
    DELETE /cells/<cell>/unit                                                   close it
    POST   /check/<cell>        called by the store's commit hook
    GET    /health

Every call of the tool is appended to <log_dir>/<cell>.jsonl, in order.
"""

from __future__ import annotations

import datetime
import hashlib
import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

try:
    from . import checker as checking, guard
    from .neo4j_cell import TOKEN_KEY, Neo4jCell, changes_from_hook, is_empty
except ImportError:  # run as a script
    import checker as checking, guard
    from neo4j_cell import TOKEN_KEY, Neo4jCell, changes_from_hook, is_empty

TOOL_NAME = {"pg": "cypher", "rdf": "sparql"}
SYNTAX_ERRORS = ("Neo.ClientError.Statement.SyntaxError",)


def now() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


class Cell:
    """One cell: its store, its checker, its mode, the unit that is open, and its record file."""

    def __init__(self, cell_id: str, store, checker, mode: str, log_dir: Path, realisation: str | None = None):
        if mode not in ("reject", "log_only"):
            raise ValueError(f"mode is reject or log_only, not {mode!r}")
        self.cell_id, self.store, self.checker, self.mode = cell_id, store, checker, mode
        self.realisation = realisation
        self.unit: dict | None = None
        self.fault: str | None = None            # set when a write got into the store unchecked
        self.retries: dict[tuple[str, str], int] = {}
        self.lock = threading.Lock()             # one statement at a time
        log_dir.mkdir(parents=True, exist_ok=True)
        self.log_path = log_dir / f"{cell_id}.jsonl"

    # ------------------------------------------------------------------ records

    def record(self, record_type: str, **fields: Any) -> None:
        unit = self.unit or {}
        entry = {"record_type": record_type, "ts": now(), "cell_id": self.cell_id,
                 "run_id": unit.get("run_id"), "unit_id": unit.get("unit_id"),
                 "source_id": unit.get("source_id"), "stratum": unit.get("stratum"), **fields}
        with self.log_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(entry, ensure_ascii=False, default=str) + "\n")

    # ------------------------------------------------------------------ units

    def open_unit(self, unit: dict) -> None:
        if not unit.get("unit_id"):
            raise ValueError("a unit needs a unit_id")
        with self.lock:
            if not self.store.hook_is_active():      # every unit starts with a hook that was seen working
                raise RuntimeError(f"the commit hook of {self.cell_id} is not active; no unit was opened")
            self.unit, self.retries, self.fault = dict(unit), {}, None
            self.record("unit_open")

    def close_unit(self) -> None:
        with self.lock:
            if self.unit:
                self.record("unit_close")
            self.unit = None

    # ------------------------------------------------------------------ the tool

    def tool(self, statement: str, item_id: str | None = None, passage: str | None = None) -> dict:
        with self.lock:
            return self._tool(statement, item_id, passage)

    def _tool(self, statement: str, item_id: str | None, passage: str | None) -> dict:
        started = time.monotonic()
        tool_name = TOOL_NAME[self.store.arm]
        common = {"tool_name": tool_name, "statement": statement,
                  "payload_sha256": hashlib.sha256((statement or "").encode("utf-8")).hexdigest(),
                  "item_id": item_id, "evidence": passage}
        latency = lambda: int((time.monotonic() - started) * 1000)

        if self.unit is None:
            return {"status": "error", "message": "No unit is open. This is a fault of the harness, not of the statement."}
        if self.fault:
            return {"status": "error", "message": self.fault}
        refusal = guard.refuse(statement, self.store.arm)
        if refusal:
            self.record("refused", reason=refusal, latency_ms=latency(), **common)
            return {"status": "refused", "message": f"Not passed on to the store: {refusal}."}

        context = {"cell_id": self.cell_id, "arm": self.store.arm, "realisation": self.realisation,
                   "unit_id": self.unit["unit_id"], "item_id": item_id, "evidence": passage, "statement": statement}
        outcome = self.store.run(statement, context)

        if outcome.kind == "unchecked":
            self.fault = ("The store is not checking writes at the moment. This is a fault of the harness, "
                          "not of the statement. Nothing more can be run in this unit.")
            self.record("hook_missing", latency_ms=latency(), **common)
            return {"status": "error", "message": self.fault}

        if outcome.kind == "result":
            self.record("graph_read", rows=len(outcome.rows), truncated=outcome.truncated, latency_ms=latency(), **common)
            return {"status": "result", "rows": outcome.rows, "row_count": len(outcome.rows), "truncated": outcome.truncated}

        if outcome.kind == "error" and item_id is None and passage is None:
            self.record("statement_error", engine_error=outcome.error, engine_error_code=outcome.error_code,
                        latency_ms=latency(), **common)
            return {"status": "error", "message": outcome.error}

        # everything below is a write attempt
        key = (self.unit["unit_id"], item_id or "")
        retry_index = self.retries.get(key, 0) if item_id else 0
        if item_id:
            self.retries[key] = retry_index + 1
        attempt = {"retry_index": retry_index, "validator_mode": self.mode, "engine_error": None,
                   "element_ids": [], "rejections": [], **common}

        if outcome.kind == "error":
            unreadable = outcome.error_code in SYNTAX_ERRORS
            attempt.update(validator_result="reject" if unreadable else "pass", engine_result="error",
                           engine_error=outcome.error, accepted=False,
                           rejections=[{"rule_id": "CS0", "element": "statement",
                                        "expected": "a statement the store can read",
                                        "found": outcome.error_code}] if unreadable else [])
            self.record("insert_attempt", latency_ms=latency(), **attempt)
            return {"status": "error", "message": outcome.error}

        if outcome.kind == "unchanged":
            attempt.update(validator_result="pass", engine_result="already_present", accepted=True)
            self.record("insert_attempt", latency_ms=latency(), **attempt)
            return {"status": "already_present", "rows": outcome.rows,
                    "message": "The statement changed nothing: what it writes is already in the graph."}

        changes = outcome.changes or {}
        element_ids = [n["key"] for n in changes.get("nodes", [])] + [r["key"] for r in changes.get("relationships", [])]
        summary = summarise(changes)
        if outcome.kind == "rejected":
            attempt.update(validator_result="reject", engine_result="not_sent", accepted=False,
                           rejections=outcome.rejections)
            self.record("insert_attempt", changes=changes, latency_ms=latency(), **attempt)
            return {"status": "rejected", "message": checking.message(outcome.rejections),
                    "rejections": outcome.rejections, "note": "Nothing of this statement was stored."}

        # stored; in log-only mode the checker may still have found faults
        attempt.update(validator_result="reject" if outcome.rejections else "pass", engine_result="ok",
                       accepted=True, rejections=outcome.rejections, element_ids=element_ids)
        self.record("insert_attempt", changes=changes, latency_ms=latency(), **attempt)
        return {"status": "stored", **summary, "rows": outcome.rows}

    # ------------------------------------------------------------------ the commit hook

    def check(self, payload: dict) -> dict:
        """Called while the store waits to commit. Answers keep or reject."""
        token = (payload.get("meta") or {}).get(TOKEN_KEY)
        pending = self.store.pending.get(token) if token else None
        if pending is None:
            self.record("unattributed_write", changes=changes_from_hook(payload))
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
        try:
            pending.rejections = checking.normalise(self.checker.check(changes, self.store))
        except Exception as error:                      # a broken checker must not let writes through
            pending.rejections = [{"rule_id": "CHECKER-FAULT", "element": "checker",
                                   "expected": "a verdict", "found": f"{type(error).__name__}: {error}"}]
            pending.rejected = True
            return {"reject": True, "message": "The checker failed; nothing was stored."}
        pending.rejected = bool(pending.rejections) and self.mode == "reject"
        return {"reject": pending.rejected, "message": checking.message(pending.rejections) if pending.rejected else ""}


def summarise(changes: dict) -> dict:
    created_nodes = [n for n in changes.get("nodes", []) if n["change"] == "created"]
    created_relationships = [r for r in changes.get("relationships", []) if r["change"] == "created"]
    return {
        "created": {"nodes": len(created_nodes), "relationships": len(created_relationships)},
        "changed": sum(1 for n in changes.get("nodes", []) if n["change"] == "changed")
        + sum(1 for r in changes.get("relationships", []) if r["change"] == "changed"),
        "deleted": len(changes.get("deleted", {}).get("nodes", [])) + len(changes.get("deleted", {}).get("relationships", [])),
    }


class Service:
    def __init__(self, host: str = "127.0.0.1", port: int = 18080, control_key: str = ""):
        self.cells: dict[str, Cell] = {}
        self.control_key = control_key
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
                            return self._send(200, cell.tool(body.get("statement", ""), body.get("id"), body.get("passage")))
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
        self.address = f"http://{host}:{self.server.server_address[1]}"

    def add_neo4j_cell(self, cell_id: str, uri: str, user: str, password: str, database: str = "neo4j",
                       checker=None, mode: str = "reject", log_dir: Path | str = ".", realisation: str | None = None,
                       max_rows: int = 200, timeout_s: float = 60.0) -> Cell:
        store = Neo4jCell(cell_id, uri, user, password, database, max_rows=max_rows, timeout_s=timeout_s)
        cell = self.cells[cell_id] = Cell(cell_id, store, checker or checking.NoChecker(), mode, Path(log_dir), realisation)
        return cell

    def start(self) -> None:
        threading.Thread(target=self.server.serve_forever, daemon=True).start()
        for cell_id, cell in self.cells.items():       # the hook can only be tested once the service answers
            cell.store.install_hook(f"{self.address}/check/{cell_id}", self.control_key)
        self.ready = True

    def stop(self, remove_hooks: bool = False) -> None:
        for cell in self.cells.values():
            if remove_hooks:
                cell.store.remove_hook()
            cell.store.close()
        self.server.shutdown()
        self.server.server_close()
