#!/usr/bin/env python3
"""What to watch for, per pass: loops, failed or long model calls, refused statements, not-checkable
writes, deletes, the RDF annotation form, and what a raw-triples cell built.

    python3 inspect_pass.py <run id> [--records <folder of the tool's records>]

Writes deploy/pilot/run/lane/<run id>/inspect.md and inspect.json and prints the Markdown.
"""

from __future__ import annotations

import argparse
import collections
import gzip
import json
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from check_records import lines  # noqa: E402
from common import OUT, PILOT_RUN  # noqa: E402

GRAPH_NAMING = re.compile(r"\b(GRAPH|FROM\s+NAMED|FROM|LOAD|SERVICE|INTO|WITH\s*<)\b", re.I)


def full_rows(pass_dir: Path, job_id: str) -> list[dict]:
    path = pass_dir / "srw" / f"{job_id}.llm_requests.full.jsonl.gz"
    if not path.exists():
        return []
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        return [json.loads(line) for line in handle]


def job_findings(pass_dir: Path, job: dict) -> dict:
    rows = full_rows(pass_dir, job["job_id"])
    calls = lines(pass_dir / "srw" / f"{job['job_id']}.model_calls.jsonl")
    seen = collections.Counter()
    plain_in_a_row = best_plain = 0
    for row in rows:
        tool_calls = (row.get("response") or {}).get("tool_calls") or []
        if not tool_calls and row.get("call_type") == "main":
            plain_in_a_row += 1
            best_plain = max(best_plain, plain_in_a_row)
        else:
            plain_in_a_row = 0
        for call in tool_calls:
            seen[(call.get("name"), json.dumps(call.get("args"), sort_keys=True))] += 1
    repeated = {f"{name} x{count}": args[:160] for (name, args), count in seen.items() if count > 1}
    failed = [{"call": c["call_id"], "status": c["status"], "error": str(c.get("error"))[:200],
               "wire": [w.get("status") for w in c["requests_on_the_wire"]]}
              for c in calls if c["status"] != "ok" or any((w.get("status") or 200) >= 400 for w in c["requests_on_the_wire"])]
    long_calls = [{"call": c["call_id"], "seconds": round((c["latency_ms"] or 0) / 1000), "output": c["tokens"]["output"],
                   "stream": (c["requests_on_the_wire"][0].get("params") or {}).get("stream") if c["requests_on_the_wire"] else None}
                  for c in calls if (c["latency_ms"] or 0) > 60_000]
    no_reasoning = sum(1 for c in calls if not c.get("reasoning_chars"))
    # reasoning sent back: every request after the first carries reasoning_content on all its assistant turns
    # (a call that came back with no reasoning, reasoning_tokens 0, has nothing to send back)
    replay_gaps, with_reasoning = 0, 0
    for row in rows:
        shape = ((row.get("request") or {}).get("wire") or [{}])[0].get("shape") or {}
        rc = shape.get("reasoning_content") or {}
        if (rc.get("non_empty") or 0) < with_reasoning:
            replay_gaps += 1
        extra = (row.get("response") or {}).get("additional_kwargs") or {}
        if row.get("call_type") == "main" and (extra.get("reasoning_content") or extra.get("reasoning")):
            with_reasoning += 1
    returned = sorted({c["returned_model"] for c in calls if c["returned_model"]})
    return {"repeated_identical_tool_calls": repeated, "most_plain_replies_in_a_row": best_plain,
            "failed_model_calls": failed, "long_model_calls": long_calls, "calls_without_reasoning": no_reasoning,
            "requests_missing_replayed_reasoning": replay_gaps, "returned_models": returned}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("run_id")
    parser.add_argument("--records", type=Path, default=PILOT_RUN / "records")
    args = parser.parse_args()
    pass_dir = OUT / args.run_id
    jobs = sorted([j for j in lines(pass_dir / "jobs.jsonl") if j.get("job_id")], key=lambda j: (j["cell"], j["unit_index"]))
    out = [f"## Watch list, pass `{args.run_id}`", ""]
    data = {"jobs": {}, "cells": {}}
    for job in jobs:
        data["jobs"][f"{job['cell']}/{job['unit_id']}"] = job_findings(pass_dir, job)
    out.append("| cell | unit | repeated identical tool calls | most plain replies in a row | failed model calls | calls > 60 s | calls without reasoning (model reported 0 reasoning tokens) | requests missing replayed reasoning |")
    out.append("|---|---|---|---|---|---|---|---|")
    for key, f in data["jobs"].items():
        cell, unit = key.split("/")
        out.append(f"| {cell} | `{unit}` | {len(f['repeated_identical_tool_calls'])} | {f['most_plain_replies_in_a_row']} | "
                   f"{len(f['failed_model_calls'])} | {len(f['long_model_calls'])} | {f['calls_without_reasoning']} | {f['requests_missing_replayed_reasoning']} |")
    for key, f in data["jobs"].items():
        if f["repeated_identical_tool_calls"]:
            out.append(f"- {key}: repeated calls {json.dumps(f['repeated_identical_tool_calls'], ensure_ascii=False)[:500]}")
        if f["failed_model_calls"]:
            out.append(f"- {key}: failed model calls {json.dumps(f['failed_model_calls'])[:500]}")
        if f["long_model_calls"]:
            out.append(f"- {key}: long calls {json.dumps(f['long_model_calls'])}")
    out.append("")
    for cell in sorted({j["cell"] for j in jobs}):
        rows = [r for r in lines(args.records / f"{cell}.jsonl") if r.get("run_id") == args.run_id]
        attempts = [r for r in rows if r["record_type"] == "insert_attempt"]
        refused = [r for r in rows if r["record_type"] == "refused"]
        statements = [r.get("statement") or "" for r in rows if r["record_type"] in ("insert_attempt", "graph_read", "statement_error", "refused")]
        rejected_payloads = collections.Counter(r.get("payload_sha256") for r in attempts if r.get("validator_result") == "reject")
        info = {
            "refused": len(refused),
            "refused_reasons": collections.Counter(str(r.get("reason") or r.get("error") or r.get("text"))[:120] for r in refused),
            "statements_naming_a_graph_or_source": sum(1 for s in statements if GRAPH_NAMING.search(s)) if "rdf" in cell else None,
            "not_checkable_writes": sum(1 for r in attempts if r.get("validator_not_checkable")),
            "not_checkable_items": sum(len(r.get("validator_not_checkable") or []) if isinstance(r.get("validator_not_checkable"), list) else int(bool(r.get("validator_not_checkable"))) for r in attempts),
            "annotation_form_statements": sum(1 for s in statements if "{|" in s) if "rdf" in cell else None,
            "quoted_triple_statements": sum(1 for s in statements if "<<" in s) if "rdf" in cell else None,
            "same_rejected_content_sent_again": sum(count - 1 for count in rejected_payloads.values() if count > 1),
            "deletes": sum(1 for s in statements if re.search(r"\b(DELETE|DETACH|REMOVE)\b", s, re.I)),
            "warnings": [{k: w.get(k) for k in ("kind", "unit_id", "text")} for w in rows if w["record_type"] == "warning"],
        }
        info["refused_reasons"] = dict(info["refused_reasons"])
        data["cells"][cell] = info
        out.append(f"- **{cell}**: refused {info['refused']} {info['refused_reasons'] or ''}; statements naming a graph or a source "
                   f"{info['statements_naming_a_graph_or_source']}; writes with not-checkable parts {info['not_checkable_writes']}; "
                   f"annotation form {{| |}} {info['annotation_form_statements']}, quoted triples << >> {info['quoted_triple_statements']}; "
                   f"same rejected content sent again {info['same_rejected_content_sent_again']}; statements that delete {info['deletes']}; "
                   f"warnings {len(info['warnings'])}")
        for warning in info["warnings"]:
            out.append(f"  - warning {warning['kind']} in unit `{warning['unit_id']}`: {str(warning['text'])[:300]}")
    text = "\n".join(out) + "\n"
    (pass_dir / "inspect.md").write_text(text, encoding="utf-8")
    (pass_dir / "inspect.json").write_text(json.dumps(data, indent=1, ensure_ascii=False, default=str), encoding="utf-8")
    print(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
