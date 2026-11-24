#!/usr/bin/env python3
"""Do SRW's records and the database tool's record agree? One line per unit and cell of a pass.

    python3 check_records.py <run id> [--records <folder with <cell>.jsonl and warnings.jsonl>]

Default folder: deploy/pilot/run/records/ (the current pass, before reset.sh files it away).

For every unit: the calls of the cell's tool that SRW recorded in the model's replies (tool calls named
*cypher or *sparql) against the tool's entries between unit_open and unit_close. Calls made from the
workspace with run_query reach the tool's record only (they carry no model call); they are counted
apart. Carried over from the proof of concept's check_records.py, for several cells and both arms.
"""

from __future__ import annotations

import argparse
import collections
import gzip
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from common import OUT, PILOT_RUN  # noqa: E402

TOOL_ENTRIES = ("graph_read", "insert_attempt", "statement_error", "refused", "hook_missing")


def lines(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()] if path.exists() else []


def shell_calls_naming_run_query(path: Path) -> int:
    """The tool's record does not say whether a call came through the wrapper or from run_query in the
    workspace. A difference between the two counts is explained if the agent ran run_query from the
    shell: count the shell calls whose arguments name it (a lower bound: a script may call it often)."""
    if not path.exists():
        return 0
    count = 0
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        for line in handle:
            row = json.loads(line)
            for call in ((row.get("response") or {}).get("tool_calls") or []):
                if call.get("name") in ("run_command", "shell_execute") and "run_query" in json.dumps(call.get("args")):
                    count += 1
    return count


def wrapper_refusals(path: Path) -> int:
    """Calls the MCP wrapper refused before they reached the tool (an argument of the wrong type, e.g.
    `passage` sent as an object): SRW has them as tool calls, the tool's record does not. Counted from
    the tool answers SRW sent back to the model in the job's last request."""
    if not path.exists():
        return 0
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        rows = [json.loads(line) for line in handle]
    count = 0
    for row in rows[-1:]:
        for message in (row.get("request") or {}).get("messages") or []:
            content = str(message.get("content"))
            if (message.get("role") or message.get("type")) == "tool" and content.startswith("Error: MCP tool graph/") \
                    and "validation error for" in content:
                count += 1
    return count


def check(run_id: str, records: Path) -> tuple[bool, list[dict]]:
    pass_dir = OUT / run_id
    jobs = [job for job in lines(pass_dir / "jobs.jsonl") if job.get("job_id")]
    ok, out = True, []
    for job in sorted(jobs, key=lambda j: (j["cell"], j["unit_index"])):
        rows = lines(records / f"{job['cell']}.jsonl")
        mine = [row for row in rows if row.get("run_id") == run_id and row.get("unit_id") == job["unit_id"]]
        kinds = collections.Counter(row["record_type"] for row in mine)
        order = [row["record_type"] for row in mine]
        framed = bool(order) and order[0] == "unit_open" and order[-1] == "unit_close" and kinds["unit_open"] == 1 == kinds["unit_close"]
        entries = [row for row in mine if row["record_type"] in TOOL_ENTRIES]
        calls = lines(pass_dir / "srw" / f"{job['job_id']}.model_calls.jsonl")
        model_tool = sum(name.endswith(("cypher", "sparql")) for call in calls for name in call["tool_calls"])
        attempts = [row for row in mine if row["record_type"] == "insert_attempt"]
        stored = sum(1 for row in attempts if row.get("accepted") and row.get("engine_result") == "ok")
        present = sum(1 for row in attempts if row.get("engine_result") == "already_present")
        rejected = sum(1 for row in attempts if row.get("validator_result") == "reject")
        not_checkable = sum(1 for row in attempts if row.get("validator_not_checkable"))
        errors = sum(1 for row in attempts if row.get("engine_result") == "error") + kinds["statement_error"]
        retries = collections.Counter(row.get("item_id") for row in attempts)
        most = max(max(retries.values(), default=0) - 1, 0)
        run_query = shell_calls_naming_run_query(pass_dir / "srw" / f"{job['job_id']}.llm_requests.full.jsonl.gz")
        refused_by_wrapper = wrapper_refusals(pass_dir / "srw" / f"{job['job_id']}.llm_requests.full.jsonl.gz")
        agree = framed and model_tool == len(entries)
        explained = framed and model_tool - refused_by_wrapper == len(entries)
        ok = ok and agree
        out.append({"cell": job["cell"], "unit_id": job["unit_id"], "agree": agree, "framed": framed,
                    "agree_after_wrapper_refusals": explained, "wrapper_refusals": refused_by_wrapper,
                    "model_tool_calls": model_tool, "tool_entries": len(entries), "shell_calls_naming_run_query": run_query,
                    "reads": kinds["graph_read"], "stored": stored, "already_present": present, "rejected": rejected,
                    "not_checkable": not_checkable, "errors": errors, "refused": kinds["refused"],
                    "most_corrections_under_one_id": most})
    return ok, out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("run_id")
    parser.add_argument("--records", type=Path, default=PILOT_RUN / "records")
    args = parser.parse_args()
    ok, rows = check(args.run_id, args.records)
    for row in rows:
        print(f"{row['cell']:13s} {row['unit_id']}  {'agree ' if row['agree'] else ('DIFFER (wrapper refused ' + str(row['wrapper_refusals']) + '; then ' + ('agree' if row['agree_after_wrapper_refusals'] else 'still differ') + ')')}: model {row['model_tool_calls']}, "
              f"tool record {row['tool_entries']} (shell calls naming run_query {row['shell_calls_naming_run_query']}; reads {row['reads']}, "
              f"stored {row['stored']}, already present {row['already_present']}, rejected {row['rejected']}, "
              f"not checkable {row['not_checkable']}, errors {row['errors']}, refused {row['refused']}); "
              f"most corrections under one id {row['most_corrections_under_one_id']}; framed {row['framed']}")
    warnings = lines(args.records / "warnings.jsonl")
    kinds = collections.Counter(warning.get("kind") for warning in warnings)
    print(f"warnings: {dict(kinds) or 'none'}")
    for warning in warnings:
        print("  " + json.dumps(warning, ensure_ascii=False)[:400])
    return 0 if ok and not (set(kinds) - {"bulk_delete"}) else 1


if __name__ == "__main__":
    sys.exit(main())
