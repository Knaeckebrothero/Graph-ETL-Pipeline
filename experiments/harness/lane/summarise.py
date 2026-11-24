#!/usr/bin/env python3
"""The tables of a pass: per unit and cell, and per pass.

    python3 summarise.py <run id> [--records <folder of the tool's records>]

Writes deploy/pilot/run/lane/<run id>/summary.md and summary.json and prints the Markdown.
"""

from __future__ import annotations

import argparse
import collections
import datetime
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from check_records import check, lines  # noqa: E402
from common import OUT, PILOT_RUN  # noqa: E402


def ts(value: str) -> datetime.datetime:
    return datetime.datetime.fromisoformat(value.replace("Z", "+00:00"))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("run_id")
    parser.add_argument("--records", type=Path, default=PILOT_RUN / "records")
    args = parser.parse_args()
    pass_dir = OUT / args.run_id
    meta = json.loads((pass_dir / "pass.json").read_text(encoding="utf-8"))
    jobs = sorted(lines(pass_dir / "jobs.jsonl"), key=lambda job: (job["cell"], job["unit_index"]))
    agree_all, checks = check(args.run_id, args.records)
    by_unit = {(row["cell"], row["unit_id"]): row for row in checks}
    warnings = lines(args.records / "warnings.jsonl")
    warnings = [w for w in warnings if w.get("run_id") in (None, args.run_id)]

    out = [f"## Pass `{args.run_id}`", "",
           f"Cells side by side: {len(meta['cells'])} ({', '.join(meta['cells'])}); units per cell: {len(meta['units'])}. "
           f"Limits: {meta['limits']}. Model settings: {meta['llm']}.", "",
           "| cell | # | unit | ended | model calls | tool calls | stored | rejected | not checkable | most corr. | input (cached) | output (reasoning) | largest output | s total | s in model | records |",
           "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|"]
    totals = collections.Counter()
    per_cell = collections.defaultdict(collections.Counter)
    for job in jobs:
        rec = job.get("records") or {}
        tok = rec.get("tokens") or {}
        chk = by_unit.get((job["cell"], job["unit_id"]), {})
        ended = job.get("status")
        if job.get("error_message"):
            ended += f": {job['error_message'][:90]}"
        row_file = pass_dir / "srw" / f"{job.get('job_id')}.job.json"
        if ended != "completed" and row_file.exists():
            freeze = json.loads(row_file.read_text(encoding="utf-8")).get("freeze_data")
            freeze = json.loads(freeze) if isinstance(freeze, str) else (freeze or {})
            if freeze.get("reason"):
                ended += f" ({freeze.get('freeze_type')}: {freeze['reason']}; the driver cancelled it)"
        out.append(f"| {job['cell']} | {job['unit_index']} | `{job['unit_id']}` | {ended} | {rec.get('model_calls')} | "
                   f"{rec.get('tool_calls')} | {chk.get('stored')} | {chk.get('rejected')} | {chk.get('not_checkable')} | "
                   f"{chk.get('most_corrections_under_one_id')} | {tok.get('input', 0):,} ({tok.get('input_cached', 0):,}) | "
                   f"{tok.get('output', 0):,} ({tok.get('reasoning', 0):,}) | {rec.get('largest_output', 0):,} | "
                   f"{job.get('wall_seconds')} | {rec.get('model_seconds')} | {'agree' if chk.get('agree') else (f"agree after {chk.get('wrapper_refusals')} wrapper refusal(s)" if chk.get('agree_after_wrapper_refusals') else 'DIFFER')} |")
        for counter in (totals, per_cell[job["cell"]]):
            counter["units"] += 1
            counter["completed"] += ended == "completed"
            counter["model_calls"] += rec.get("model_calls") or 0
            counter["tool_calls"] += rec.get("tool_calls") or 0
            for kind in ("input", "input_cached", "output", "reasoning"):
                counter[kind] += tok.get(kind) or 0
            counter["wall"] += job.get("wall_seconds") or 0
            counter["model_seconds"] += rec.get("model_seconds") or 0
            counter["stored"] += chk.get("stored") or 0
            counter["rejected"] += chk.get("rejected") or 0
            counter["largest_output"] = max(counter["largest_output"], rec.get("largest_output") or 0)
            counter["max_model_calls"] = max(counter["max_model_calls"], rec.get("model_calls") or 0)
            counter["max_tool_calls"] = max(counter["max_tool_calls"], rec.get("tool_calls") or 0)
            counter["max_tokens"] = max(counter["max_tokens"], (tok.get("input") or 0) + (tok.get("output") or 0))
    out += ["", "| cell | units completed | model calls | tool calls | stored | rejected | input tokens | output tokens | s total | s in model |",
            "|---|---|---|---|---|---|---|---|---|---|"]
    for cell, c in sorted(per_cell.items()):
        out.append(f"| {cell} | {c['completed']}/{c['units']} | {c['model_calls']} | {c['tool_calls']} | {c['stored']} | {c['rejected']} | "
                   f"{c['input']:,} | {c['output']:,} | {round(c['wall'])} | {round(c['model_seconds'])} |")
    start, end = ts(meta["started_at"]), ts(meta.get("ended_at") or meta["started_at"])
    span = max((end - start).total_seconds(), 1)
    tokens_all = totals["input"] + totals["output"]
    outside = 1 - (totals["model_seconds"] / totals["wall"]) if totals["wall"] else None
    stack_start, stack_end = (meta.get("stack_at_start") or {}).get("stack_id"), (meta.get("stack_at_end") or {}).get("stack_id")
    warn_kinds = collections.Counter(w.get("kind") for w in warnings)
    summary = {
        "run_id": args.run_id, "cells_side_by_side": len(meta["cells"]), "units": totals["units"], "completed": totals["completed"],
        "pass_seconds": round(span), "tokens_total": tokens_all, "tokens_per_second": round(tokens_all / span, 1),
        "output_tokens_per_second": round(totals["output"] / span, 1),
        "share_of_unit_time_outside_model": round(outside, 3) if outside is not None else None,
        "records_agree_for_every_unit": agree_all, "warnings": dict(warn_kinds),
        "stack_id_start": stack_start, "stack_id_end": stack_end, "stack_same": bool(stack_start) and stack_start == stack_end,
        "largest_output_of_one_call": totals["largest_output"], "most_model_calls_in_a_unit": totals["max_model_calls"],
        "most_tool_calls_in_a_unit": totals["max_tool_calls"], "most_tokens_in_a_unit": totals["max_tokens"],
        "totals": dict(totals),
    }
    out += ["", f"- Pass: {summary['pass_seconds']} s from start to end; {summary['completed']}/{summary['units']} jobs completed.",
            f"- Tokens per second over the pass (input + output): {summary['tokens_per_second']:,}; output only: {summary['output_tokens_per_second']:,}.",
            f"- Share of a unit's time spent outside the model: {summary['share_of_unit_time_outside_model']}.",
            f"- SRW's records and the tool's record agree for every unit: {agree_all}; units that agree only after counting the wrapper's "
            f"refusals apart: {sum(1 for r in checks if not r['agree'] and r['agree_after_wrapper_refusals'])}; units that still differ: "
            f"{sum(1 for r in checks if not r['agree_after_wrapper_refusals'] and not r['agree'])}.",
            f"- Stack id at start {stack_start}, at end {stack_end}: {'the same' if summary['stack_same'] else 'NOT the same or missing'}.",
            f"- Largest output of one call: {summary['largest_output_of_one_call']:,} tokens; most model calls in a unit "
            f"{summary['most_model_calls_in_a_unit']}, most tool calls {summary['most_tool_calls_in_a_unit']}, most tokens "
            f"{summary['most_tokens_in_a_unit']:,}.",
            f"- Warnings: {dict(warn_kinds) or 'none'}."]
    for warning in warnings:
        out.append(f"  - `{json.dumps(warning, ensure_ascii=False)[:300]}`")
    text = "\n".join(out) + "\n"
    (pass_dir / "summary.md").write_text(text, encoding="utf-8")
    (pass_dir / "summary.json").write_text(json.dumps(summary, indent=1), encoding="utf-8")
    print(text)
    return 0


if __name__ == "__main__":
    sys.exit(main())
