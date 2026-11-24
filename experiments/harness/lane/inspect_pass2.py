#!/usr/bin/env python3
"""What the second part of the pilot asks for beyond the tables of the first (KICKOFF-2.md).

    python3 inspect_pass2.py <run id> [--records <folder of the tool's records>]

Per cell: rejections by rule, refused reads that name a graph, not-checkable writes, deleting
statements, wrapper refusals, reads before the first write, compaction. On the prompt's three changed
sentences: what the agent puts into job_complete and whether its reasoning still argues about the
ending, white space in passages, named graphs. On the induced cells: whole numbers refused for
decimals, how quantities are written. Plus the tools a job is offered and how many jobs ran at once.

Writes deploy/pilot/run/lane/<run id>/inspect2.json and inspect2.md.
"""

from __future__ import annotations

import argparse
import collections
import datetime
import gzip
import json
import re
import statistics
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from check_records import lines, wrapper_refusals  # noqa: E402
from common import OUT, PILOT_RUN  # noqa: E402

# the old ending word (upper case only), and the new sentence weighed against job_complete's required summary
ENDING = re.compile(r"\bDONE\b|no report|no summary|write no")
WHITESPACE = re.compile(r"white ?space|line break|runs? of spaces", re.I)
LEFT_OUT = re.compile(r"no place|not in the schema|schema (has|offers|declares) no|cannot (be )?(store|represent)|"
                      r"leave (it |them )?out|left out|omit|not representable|no (node|class|property|relation) (type )?for", re.I)
QUANTITY_PROPS = re.compile(r"\b(quantity|capacity|amount)\b")


def ts(value: str | None):
    return datetime.datetime.fromisoformat(value.replace("Z", "+00:00")) if value else None


def full_rows(pass_dir: Path, job_id: str) -> list[dict]:
    path = pass_dir / "srw" / f"{job_id}.llm_requests.full.jsonl.gz"
    if not path.exists():
        return []
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        return [json.loads(line) for line in handle]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("run_id")
    parser.add_argument("--records", type=Path, default=PILOT_RUN / "records")
    args = parser.parse_args()
    pass_dir = OUT / args.run_id
    jobs = [j for j in lines(pass_dir / "jobs.jsonl") if j.get("job_id")]
    cells = sorted({j["cell"] for j in jobs})
    data: dict = {"cells": {}, "jobs": {}, "prompt": {}, "tools_offered": {}}

    # ---- per job: SRW's side
    intervals, ending_jobs, whitespace_jobs, left_out = [], [], [], collections.defaultdict(list)
    complete_args = []
    for job in jobs:
        rows = full_rows(pass_dir, job["job_id"])
        if rows and job["cell"] not in data["tools_offered"]:
            tools = (rows[0].get("request") or {}).get("tools") or []
            data["tools_offered"][job["cell"]] = [t.get("name") or (t.get("function") or {}).get("name") for t in tools]
        starts = [ts((r.get("metrics") or {}).get("started_at")) for r in rows if (r.get("metrics") or {}).get("started_at")]
        ends = [ts(r.get("timestamp")) for r in rows if r.get("timestamp")]
        if starts and ends:
            intervals.append((min(starts), max(ends)))
        ending_hits, ws_hits = [], []
        for row in rows:
            response = row.get("response") or {}
            extra = response.get("additional_kwargs") or {}
            text = (extra.get("reasoning_content") or "") + "\n" + str(response.get("content") or "")
            for match in ENDING.finditer(text):
                ending_hits.append(text[max(0, match.start() - 140): match.end() + 100].replace("\n", " "))
            for match in WHITESPACE.finditer(text):
                ws_hits.append(text[max(0, match.start() - 140): match.end() + 100].replace("\n", " "))
            for match in LEFT_OUT.finditer(text):
                left_out[job["cell"]].append(text[max(0, match.start() - 160): match.end() + 120].replace("\n", " "))
            for call in response.get("tool_calls") or []:
                if call.get("name") == "job_complete":
                    a = call.get("args") or {}
                    complete_args.append({"cell": job["cell"], "unit": job["unit_id"], "summary": a.get("summary"),
                                          "notes_chars": len(str(a.get("notes") or "")), "keys": sorted(a)})
        if ending_hits:
            ending_jobs.append({"cell": job["cell"], "unit": job["unit_id"], "hits": len(ending_hits), "example": ending_hits[0][:300]})
        if ws_hits:
            whitespace_jobs.append({"cell": job["cell"], "unit": job["unit_id"], "hits": len(ws_hits), "example": ws_hits[0][:300]})
        rec = job.get("records") or {}
        data["jobs"][f"{job['cell']}/{job['unit_id']}"] = {
            "compacted": rec.get("compacted"), "largest_context": rec.get("largest_context"),
            "wrapper_refusals": wrapper_refusals(pass_dir / "srw" / f"{job['job_id']}.llm_requests.full.jsonl.gz"),
            "stopped_at": job.get("stopped_at"), "status": job.get("status")}

    # ---- how many jobs ran at once (from the first model call to the last row of each job)
    events = sorted([(a, 1) for a, _ in intervals] + [(b, -1) for _, b in intervals])
    running = most = 0
    for _, step in events:
        running += step
        most = max(most, running)
    data["most_jobs_at_once"] = most

    # ---- per cell: the tool's side
    for cell in cells:
        rows = [r for r in lines(args.records / f"{cell}.jsonl") if r.get("run_id") == args.run_id]
        attempts = [r for r in rows if r["record_type"] == "insert_attempt"]
        rules = collections.Counter()
        int_for_decimal, quantity_writes = [], []
        for r in attempts:
            for rej in r.get("rejections") or []:
                rules[rej.get("rule_id")] += 1
                text = json.dumps(rej, ensure_ascii=False)
                if "decimal" in text and re.search(r"integer|whole|\"found\": \"?-?\d+\"?[,}]", text):
                    int_for_decimal.append({"unit": r["unit_id"], "rejection": text[:300]})
            statement = r.get("statement") or ""
            if QUANTITY_PROPS.search(statement) and "s2" in cell:
                for m in re.finditer(r"(quantity|capacity|amount)\s*[:=]?\s*([^,;\n}]{1,80})", statement):
                    quantity_writes.append({"unit": r["unit_id"], "result": r.get("validator_result"), "text": m.group(0)[:100]})
            for key, value in (r.get("parameters") or {}).items() if isinstance(r.get("parameters"), dict) else []:
                if "s2" in cell and QUANTITY_PROPS.search(key):
                    quantity_writes.append({"unit": r["unit_id"], "result": r.get("validator_result"), "text": f"${key} = {json.dumps(value)[:80]}"})
        # reads before the first write, per unit
        before = []
        for unit in sorted({r["unit_id"] for r in rows}):
            seq = [r["record_type"] for r in rows if r["unit_id"] == unit and r["record_type"] in ("graph_read", "insert_attempt", "refused", "statement_error")]
            first = seq.index("insert_attempt") if "insert_attempt" in seq else len(seq)
            before.append(sum(1 for kind in seq[:first] if kind in ("graph_read", "refused")))
        statements = [r.get("statement") or "" for r in rows if r["record_type"] in ("insert_attempt", "graph_read", "refused", "statement_error")]
        data["cells"][cell] = {
            "rejections_by_rule": dict(rules.most_common()),
            "refused_named_graph": sum(1 for r in rows if r["record_type"] == "refused"),
            "refused_reasons": dict(collections.Counter(str(r.get("reason"))[:80] for r in rows if r["record_type"] == "refused")),
            "not_checkable_writes": sum(1 for r in attempts if r.get("validator_not_checkable")),
            "deleting_statements": sum(1 for s in statements if re.search(r"\b(DELETE|DETACH|REMOVE)\b", s, re.I)),
            "wrapper_refusals": sum(v["wrapper_refusals"] for k, v in data["jobs"].items() if k.startswith(cell + "/")),
            "reads_before_first_write": {"per_unit": before, "median": statistics.median(before) if before else None,
                                         "total": sum(before)},
            "jobs_compacted": sum(1 for k, v in data["jobs"].items() if k.startswith(cell + "/") and v["compacted"]),
            "largest_context": max((v["largest_context"] or 0) for k, v in data["jobs"].items() if k.startswith(cell + "/")),
            "integer_for_decimal_rejections": int_for_decimal,
            "quantity_writes": quantity_writes[:40],
            "quantity_writes_total": len(quantity_writes),
            "left_out_mentions": len(left_out[cell]),
            "left_out_examples": left_out[cell][:6],
        }
    data["prompt"] = {
        "job_complete_calls": len(complete_args),
        "job_complete_summaries": dict(collections.Counter(str(a["summary"])[:40] for a in complete_args).most_common(10)),
        "job_complete_with_notes": sum(1 for a in complete_args if a["notes_chars"]),
        "job_complete_notes_chars_median": statistics.median([a["notes_chars"] for a in complete_args]) if complete_args else None,
        "job_complete_arg_keys": dict(collections.Counter(",".join(a["keys"]) for a in complete_args)),
        "jobs_whose_reasoning_mentions_the_ending": len(ending_jobs), "ending_examples": ending_jobs[:8],
        "jobs_whose_reasoning_mentions_white_space": len(whitespace_jobs), "white_space_examples": whitespace_jobs[:8],
        "wrapper_refusals_total": sum(v["wrapper_refusals"] for v in data["jobs"].values()),
        "refused_named_graph_total": sum(c["refused_named_graph"] for c in data["cells"].values()),
    }
    (pass_dir / "inspect2.json").write_text(json.dumps(data, indent=1, ensure_ascii=False, default=str), encoding="utf-8")
    out = [f"## Second part, pass `{args.run_id}`", "", f"- Most jobs running at once: {most}.",
           f"- Tools offered (first job of each cell): {json.dumps(data['tools_offered'], ensure_ascii=False)}", ""]
    out.append("| cell | rejections by rule | refused (named graph) | not-checkable writes | deleting statements | wrapper refusals | reads before first write (median, total) | jobs compacted | largest context |")
    out.append("|---|---|---|---|---|---|---|---|---|")
    for cell, c in data["cells"].items():
        out.append(f"| {cell} | {c['rejections_by_rule'] or '-'} | {c['refused_named_graph']} | {c['not_checkable_writes']} | {c['deleting_statements']} | "
                   f"{c['wrapper_refusals']} | {c['reads_before_first_write']['median']}, {c['reads_before_first_write']['total']} | "
                   f"{c['jobs_compacted']} | {c['largest_context']:,} |")
    out += ["", "```json", json.dumps(data["prompt"], indent=1, ensure_ascii=False)[:6000], "```"]
    (pass_dir / "inspect2.md").write_text("\n".join(out) + "\n", encoding="utf-8")
    print("\n".join(out))
    return 0


if __name__ == "__main__":
    sys.exit(main())
