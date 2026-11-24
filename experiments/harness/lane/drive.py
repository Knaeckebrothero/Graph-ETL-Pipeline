#!/usr/bin/env python3
"""Run one pass: a list of units through a list of cells, one SRW job per unit.

    python3 drive.py --label s4dev --cells pilot-s4-pg --units dev5
    python3 drive.py --label pg --cells pilot-s4-pg pilot-s3-pg pilot-s1-pg --units pilot
    python3 drive.py --label all6 --cells all --units pilot

Inside a cell the units run one after another, in the given order, each as its own job with a fresh
context: the unit is opened at the database tool before the job and closed after the job has ended,
however it ended. Different cells run side by side (one thread per cell).

Per unit: fill the two prompt texts for the cell's arm and schema, open the unit, create the job, wait
until it has ended, close the unit, export SRW's records of the job. At the start and the end of the
pass: the station's stack id. Writes deploy/pilot/run/lane/<run id>/ (pass.json, jobs.jsonl,
prompts/, srw/).

Carried over from the proof of concept's drive_poc.py and poc_variants.py (variant `agent`: a job
without phases), made to work for several cells. Reads deploy/pilot/run/srw.env (one token per cell),
keys.env, setup/<cell>.json (written by setup_cells.py).
"""

from __future__ import annotations

import argparse
import datetime
import json
import sys
import threading
import time
import traceback
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import common  # noqa: E402
from common import (DEV5, OUT, PROMPTS_DIR, SCHEMAS, SrwClient, cell_of, development_unit, export, now,  # noqa: E402
                    pilot_units, setup_of, stack_id, token_of_cell, tokens_so_far, tool_control, tool_health)

sys.path.insert(0, str(PROMPTS_DIR))
import prompts  # noqa: E402  (the thesis repository's own filler; read, never changed)

MODEL = "DeepSeek-V4.1-Flash"            # SRW's "Local Router" entry for the DGX Station; no fallback
TERMINAL = {"completed", "failed", "cancelled"}
PARKED = {"pending_review", "waiting_for_reply", "reviewing", "waiting", "paused"}
# SRW has no token limit: the driver stops a job above --max-tokens (input plus output, summed from SRW's model-call
# records while the job runs) and reports one above --token-report (decided 23 and 24 November 2026).

FILE_TOOLS = ["read_file", "write_file", "edit_file", "list_files", "delete_file", "search_files",
              "file_exists", "move_file", "rename_file", "copy_file", "create_directory", "delete_directory"]
SHELL_TOOLS = ["run_command", "cancel_command", "shell_read"]
OFF = {name: [] for name in ("skills", "research", "browser_direct", "citation", "communication", "knowledge",
                             "memory", "delegation", "git", "graph", "sql", "mongodb", "repo", "email",
                             "evaluation", "orchestrator", "agent_catalog", "workflows", "webdav", "canvas", "image_generation")}
LOCK = threading.Lock()


def llm(args) -> dict:
    """Sent with every call: temperature 0.2, top-p 1.0, reasoning effort high. Thinking is the
    station's default and is not asked for separately."""
    out = {"model": MODEL, "reasoning_level": "high", "temperature": 0.2, "top_p": 1.0}
    if args.max_output_tokens:
        out["max_output_tokens"] = args.max_output_tokens
    return out


def everything_else_off() -> dict:
    return {
        "auxiliary": {"enabled": False},
        "autonomy": "full",
        "memory": {"enabled": False, "required": False, "manager": {"enabled": False}, "ingestion": {"enabled": False}},
        "verification": {"enabled": False},
        "curator": {"enabled": False},
        "scholar": {"enabled": False},
        "delegation": {"enabled": False},
        "shell": {"sudo_action": "block"},
    }


def limits(args) -> dict:
    out = {}
    if args.max_tool_calls:
        out["max_tool_calls_per_job"] = args.max_tool_calls
    if args.max_model_calls:
        out["max_llm_calls_per_job"] = args.max_model_calls
    if args.max_plain_replies:
        out["max_plain_replies_in_a_row"] = args.max_plain_replies
    return out


def job_body(*, cell: dict, setup: dict, unit: dict, run_id: str, index: int, texts: dict, args) -> dict:
    """The SRW job of one unit: a job without phases (the `agent` variant of the proof of concept).
    The fixed part of the prompt is the job's instructions, the task part its description. Everything
    that makes it a thesis job is in this body: SRW stores no thesis expert, prompt or tool."""
    return {
        "project_id": setup["project_id"],
        "datasource_ids": [setup["mcp_id"], setup["env_id"]],     # the tool and the variables; no folder
        "execution_lane": "stateless",
        "workspace": {"template": {"ref": {"name": setup["template"], "scope": {"kind": "Account", "name": "me"}}}},
        "priority": 5,
        "context": {
            "experiment": {"run_id": run_id, "cell_id": cell["id"], "unit_id": unit["unit_id"], "unit_index": index,
                           "label": args.label},
            "worker_batch_target_wall_seconds": 3600,
        },
        "description": texts["user"],
        "instructions": texts["system"],
        "expert": {"inline": {"runtime": {"adapter": "srw/v1", "config": {
            "config_name": "worker_base",
            "config": {
                "display_name": "Graph builder",
                "tools": {**OFF, "workspace": FILE_TOOLS, "shell": SHELL_TOOLS,
                          "core": ["next_phase_todos", "todo_complete", "todo_list", "request_replan", "job_complete"]},
                "instruction_files": [],
            },
            "prompts": {},
        }}}},
        "config_override": {"llm": llm(args), **everything_else_off(), "limits": limits(args)},
        "phases": False,
    }


def fill(cell: dict, unit_id: str, out: Path) -> dict[str, str]:
    values = {
        "schema_description": (SCHEMAS / cell["realisation"] / "description.construction.txt").read_text(encoding="utf-8"),
        "max_self_corrections": 2,
        "max_rows_shown": 200,
        "unit": (common.UNITS_DIR / f"{unit_id}.txt").read_text(encoding="utf-8"),
    }
    texts = prompts.assemble("construction", cell["arm"], values)
    out.mkdir(parents=True, exist_ok=True)
    for part, text in texts.items():
        (out / f"construction.{cell['arm']}.{part}.txt").write_text(text, encoding="utf-8")
    return texts


def stop_job(client: SrwClient, job_id: str) -> tuple[dict, str]:
    """Cancel a job and wait until it has really stopped (the unit stays open until then)."""
    client.call("PUT", f"/jobs/{job_id}/cancel")
    job, status = {}, "unknown"
    for _ in range(60):
        time.sleep(5)
        code, answer = client.call("GET", f"/jobs/{job_id}")
        if code == 200:
            job, status = answer, answer.get("status")
            if status in TERMINAL:
                break
    return job, status


def wait_job(client: SrwClient, job_id: str, args, summary: dict, log) -> dict:
    started, last, job, status = time.monotonic(), None, {}, "unknown"
    last_token_check = time.monotonic()
    while True:
        time.sleep(5)
        code, answer = client.call("GET", f"/jobs/{job_id}")
        if code == 200:
            job, status = answer, answer.get("status")
        else:
            status = f"http {code}"
        if status != last:
            log(f"{int(time.monotonic() - started):5d} s  {status}")
            last = status
        if status in TERMINAL or status in PARKED:
            break
        if args.max_tokens and time.monotonic() - last_token_check >= args.token_check_seconds:
            last_token_check = time.monotonic()
            try:
                used = tokens_so_far(job_id)
            except Exception as error:               # a failed count must not stop the job; try again later
                used = None
                log(f"token count failed: {error}")
            if used is not None and used > args.max_tokens:
                summary.update(ended_by="driver: token limit", stopped_at="token_limit", tokens_at_stop=used)
                log(f"token limit: {used:,} tokens > {args.max_tokens:,}; stopping the job")
                stopped, stopped_status = stop_job(client, job_id)
                job, status = stopped or job, stopped_status
                break
        if time.monotonic() - started > args.deadline:
            summary.update(ended_by="driver: deadline", stopped_at="deadline")
            client.call("PUT", f"/jobs/{job_id}/cancel")
            # the unit stays open until the job has really stopped
            for _ in range(60):
                time.sleep(5)
                code, answer = client.call("GET", f"/jobs/{job_id}")
                if code == 200 and answer.get("status") in TERMINAL:
                    job, status = answer, answer.get("status")
                    break
            break
    freeze = job.get("freeze_data")
    if isinstance(freeze, str):                       # SRW answers with the freeze record as a JSON string
        try:
            freeze = json.loads(freeze)
        except ValueError:
            freeze = {}
    freeze = freeze if isinstance(freeze, dict) else {}
    summary.update(status=status, wall_seconds=round(time.monotonic() - started, 1), ended_at=now(),
                   error_message=job.get("error_message"), freeze_type=freeze.get("freeze_type"),
                   freeze_reason=freeze.get("reason"))
    message = job.get("error_message") or ""
    if freeze.get("freeze_type") == "budget_exceeded":
        summary.setdefault("stopped_at", "tool_call_limit")
    elif "max_llm_calls_per_job" in message:
        summary.setdefault("stopped_at", "model_call_limit")
    elif "max_plain_replies_in_a_row" in message:
        summary.setdefault("stopped_at", "plain_reply_limit")
    if status in PARKED:
        # SRW stopped the job and waits for a person, or parked it (outcome unknown): the unit is not done
        summary.setdefault("ended_by", f"srw: parked as {status}")
        code, _ = client.call("PUT", f"/jobs/{job_id}/cancel")
        summary["cancel_after_park_http"] = code
        for _ in range(60):
            time.sleep(5)
            code, answer = client.call("GET", f"/jobs/{job_id}")
            if code == 200 and answer.get("status") in TERMINAL:
                break
    else:
        summary.setdefault("ended_by", f"srw: {status}")
    return job


def run_unit(cell: dict, setup: dict, unit: dict, index: int, run_id: str, pass_dir: Path, args, log) -> dict:
    client = SrwClient(token_of_cell(cell["id"]))
    texts = fill(cell, unit["unit_id"], pass_dir / "prompts" / cell["id"] / unit["unit_id"])
    body = job_body(cell=cell, setup=setup, unit=unit, run_id=run_id, index=index, texts=texts, args=args)
    summary = {"run_id": run_id, "label": args.label, "cell": cell["id"], "unit_id": unit["unit_id"], "unit_index": index,
               "limits": limits(args), "llm": llm(args)}
    tool_control(cell["id"], "POST", {"unit_id": unit["unit_id"], "run_id": run_id, "source_id": unit["source_id"],
                                      "stratum": unit["stratum"]})
    opened, job_id, job = now(), None, {}
    try:
        for attempt in range(6):                      # SRW's API may be restarting: try again for a minute
            code, created = client.call("POST", "/jobs", body)
            if code in (200, 201) or code < 500:
                break
            time.sleep(10)
        if code not in (200, 201):
            summary.update(status="not_created", http=code, answer=created)
            log(f"not created: {code} {json.dumps(created)[:600]}")
            return summary
        job_id = created["id"]
        summary.update(job_id=job_id, created_at=now())
        log(f"job {job_id} created")
        job = wait_job(client, job_id, args, summary, log)
    finally:
        try:
            tool_control(cell["id"], "DELETE")
        except Exception as error:
            summary["close_error"] = str(error)
        summary.update(unit_opened_at=opened, unit_closed_at=now())
    time.sleep(8)                                     # the last audit rows are written after the end
    try:
        summary["records"] = export(job_id, opened, pass_dir / "srw")
    except Exception as error:                        # a failed export must not hide how the unit ended
        summary["records_error"] = f"{type(error).__name__}: {error}"
    (pass_dir / "srw" / f"{job_id}.job.json").write_text(json.dumps(job, ensure_ascii=False, indent=1, default=str), encoding="utf-8")
    tokens = (summary.get("records") or {}).get("tokens") or {}
    summary["over_token_report"] = (tokens.get("input") or 0) + (tokens.get("output") or 0) > args.token_report
    return summary


def run_cell(cell_id: str, units: list[dict], run_id: str, pass_dir: Path, args, results: dict) -> None:
    cell, setup = cell_of(cell_id), setup_of(cell_id)
    for index, unit in enumerate(units, 1):
        def log(text: str, _index=index, _unit=unit["unit_id"]) -> None:
            with LOCK:
                print(f"{datetime.datetime.now():%H:%M:%S} {cell_id} [{_index}/{len(units)}] {_unit}  {text}", flush=True)
        log("start")
        try:
            summary = run_unit(cell, setup, unit, index, run_id, pass_dir, args, log)
        except Exception as error:
            summary = {"run_id": run_id, "cell": cell_id, "unit_id": unit["unit_id"], "unit_index": index,
                       "status": "driver_error", "error": f"{type(error).__name__}: {error}", "trace": traceback.format_exc()[-2000:]}
        records = summary.get("records") or {}
        log(f"ended: {summary.get('status')} ({summary.get('ended_by')}), {summary.get('wall_seconds')} s, "
            f"{records.get('model_calls')} model calls, tools {records.get('tool_calls_by_name')}, tokens {records.get('tokens')}")
        with LOCK:
            with (pass_dir / "jobs.jsonl").open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(summary, ensure_ascii=False) + "\n")
        results.setdefault(cell_id, []).append(summary)
        if summary.get("status") == "driver_error" and args.stop_on_error:
            log("driver error: this cell stops")
            return


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--label", required=True, help="a name for this pass; part of the run id")
    parser.add_argument("--cells", nargs="+", required=True, help="cell ids, or 'all' for every enabled cell")
    parser.add_argument("--units", nargs="+", default=["pilot"], help="'pilot' (units.txt), 'dev5', or unit ids")
    parser.add_argument("--max-tool-calls", type=int, default=200)
    parser.add_argument("--max-model-calls", type=int, default=250)
    parser.add_argument("--max-plain-replies", type=int, default=3)
    parser.add_argument("--max-output-tokens", type=int, default=0, help="per call (0: SRW's family default)")
    parser.add_argument("--deadline", type=int, default=3600, help="seconds per unit before the driver cancels the job")
    parser.add_argument("--max-tokens", type=int, default=40_000_000,
                        help="the driver stops a job above this many tokens, input plus output (0: no stop)")
    parser.add_argument("--token-report", type=int, default=12_000_000, help="the driver reports a job above this")
    parser.add_argument("--token-check-seconds", type=int, default=30, help="how often the driver adds up a running job's tokens")
    parser.add_argument("--stop-on-error", action="store_true")
    args = parser.parse_args()

    if args.cells == ["all"]:
        args.cells = [cell["id"] for cell in common.cells_config()["cells"] if cell.get("enabled")]
    if args.units == ["pilot"]:
        units = pilot_units()
    elif args.units == ["dev5"]:
        units = [development_unit(unit_id) for unit_id in DEV5]
    else:
        units = [development_unit(unit_id) for unit_id in args.units]
    health = tool_health()
    if health.get("status") != "ok":
        raise SystemExit(f"the database tool is not ready: {health}")
    missing = [cell for cell in args.cells if cell not in (health.get("cells") or [])] if isinstance(health.get("cells"), list) else []
    if missing:
        raise SystemExit(f"the database tool does not serve {missing}: {health}")
    run_id = f"pilot-{args.label}-{datetime.datetime.now(datetime.timezone.utc):%Y%m%dT%H%M%SZ}"
    pass_dir = OUT / run_id
    (pass_dir / "srw").mkdir(parents=True)
    meta = {"run_id": run_id, "label": args.label, "cells": args.cells, "units": [u["unit_id"] for u in units],
            "limits": limits(args), "llm": llm(args), "deadline": args.deadline, "started_at": now(),
            "driver_token_stop": args.max_tokens, "driver_token_report": args.token_report,
            "stack_at_start": stack_id(), "tool_health_at_start": health}
    (pass_dir / "pass.json").write_text(json.dumps(meta, indent=1, ensure_ascii=False), encoding="utf-8")
    print(f"run {run_id}: {len(args.cells)} cells side by side, {len(units)} units each; stack "
          f"{meta['stack_at_start'].get('stack_id')}", flush=True)
    results: dict = {}
    threads = [threading.Thread(target=run_cell, args=(cell_id, units, run_id, pass_dir, args, results), name=cell_id)
               for cell_id in args.cells]
    for thread in threads:
        thread.start()
        time.sleep(2)
    for thread in threads:
        thread.join()
    meta.update(ended_at=now(), stack_at_end=stack_id(), tool_health_at_end=tool_health())
    (pass_dir / "pass.json").write_text(json.dumps(meta, indent=1, ensure_ascii=False), encoding="utf-8")
    print(f"run {run_id} ended; stack {meta['stack_at_end'].get('stack_id')}; folder {pass_dir}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
