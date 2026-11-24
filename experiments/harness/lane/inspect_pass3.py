#!/usr/bin/env python3
"""The third part of the pilot (KICKOFF-3.md): the runs of one RDF cell side by side.

    python3 inspect_pass3.py <run id> [<run id> ...] [--cell pilot-s1-rdf]

Per run: triples, distinct properties and triples whose object is another entity (from the exported
graph, run/graphs/<run id>/<cell>.nq); the first unit's first two writes and how its reasoning reads
the schema text; per unit, the properties it used and how many of them the first unit had already
used; not-checkable writes, rejections by rule, refused reads; the summaries in job_complete and the
reasoning about a summary. Records come from run/records/<run id>/ (filed by reset.sh, or the copy of
the last run). Writes run/lane/pass3-<first run id>.json and prints Markdown.
"""

from __future__ import annotations

import argparse
import collections
import gzip
import json
import re
import statistics
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from check_records import lines  # noqa: E402
from common import OUT, PILOT_RUN  # noqa: E402

RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
QUAD = re.compile(r"(\S+) <([^>]+)> (.+?) (<[^>]+> )?\.\s*$")
SCHEMA_READING = re.compile(r"(schema|other properties|any property|nothing is declared|none declared|only (the )?label|not allowed|accepted)", re.I)
SUMMARY = re.compile(r"\bsummary\b", re.I)                  # any mention (planning the job_complete call included)
ARGUES = re.compile(r"no report|no summary|write no|requires a summary|summary (is )?required|must (give|write) a summary", re.I)


def graph_numbers(path: Path) -> dict:
    triples = entity_objects = 0
    preds = collections.Counter()
    for line in path.read_text(encoding="utf-8").splitlines():
        m = QUAD.match(line)
        if not m:
            continue
        triples += 1
        preds[m.group(2)] += 1
        if m.group(3).startswith("<") and m.group(2) != RDF_TYPE:
            entity_objects += 1
    return {"triples": triples, "distinct_properties": len(preds), "entity_object_triples": entity_objects,
            "top_properties": [(p.rsplit("/", 1)[-1].rsplit("#", 1)[-1], n) for p, n in preds.most_common(8)]}


def uses_of(change: dict) -> set[str]:
    out = set()
    for node in (change or {}).get("nodes") or []:
        out |= set((node.get("properties") or {}).keys()) | set((node.get("changed_properties") or {}).keys())
    for rel in (change or {}).get("relationships") or []:
        if rel.get("type"):
            out.add("->" + rel["type"])
    return out


def run_findings(run_id: str, cell: str) -> dict:
    pass_dir = OUT / run_id
    records = PILOT_RUN / "records" / run_id / f"{cell}.jsonl"
    rows = [r for r in lines(records) if r.get("run_id") == run_id]
    jobs = sorted([j for j in lines(pass_dir / "jobs.jsonl") if j.get("job_id") and j.get("cell") == cell], key=lambda j: j["unit_index"])
    summary = json.loads((pass_dir / "summary.json").read_text(encoding="utf-8"))
    out = {"run_id": run_id, "graph": graph_numbers(PILOT_RUN / "graphs" / run_id / f"{cell}.nq"),
           "stack": (summary.get("stack_id_start"), summary.get("stack_id_end")), "completed": summary.get("completed"),
           "units": summary.get("units"), "pass_seconds": summary.get("pass_seconds"), "totals": summary.get("totals")}
    # first unit: first two writes, and how its reasoning reads the schema text
    first_unit = jobs[0]["unit_id"]
    writes = [r for r in rows if r["unit_id"] == first_unit and r["record_type"] == "insert_attempt"]
    out["first_writes"] = [{"result": f"{w.get('validator_result')}/{w.get('engine_result')}", "statement": (w.get("statement") or "")[:600]}
                           for w in writes[:2]]
    snippets = []
    path = pass_dir / "srw" / f"{jobs[0]['job_id']}.llm_requests.full.jsonl.gz"
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        for line in handle:
            text = ((json.loads(line).get("response") or {}).get("additional_kwargs") or {}).get("reasoning_content") or ""
            for m in SCHEMA_READING.finditer(text):
                snippets.append(text[max(0, m.start() - 200): m.end() + 200].replace("\n", " "))
            if len(snippets) > 12:
                break
    out["first_unit_reasoning_on_the_schema"] = snippets[:6]
    # per unit: what it used, and how much of it the first unit had used already
    seen: set[str] = set()
    first_set: set[str] = set()
    per_unit = []
    for job in jobs:
        used = set()
        for r in rows:
            if r["unit_id"] == job["unit_id"] and r["record_type"] == "insert_attempt" and r.get("engine_result") == "ok":
                used |= uses_of(r.get("changes"))
        if not first_set:
            first_set = set(used)
        per_unit.append({"unit": job["unit_id"], "used": len(used), "new": len(used - seen),
                         "from_unit_1": len(used & first_set), "only_label": used <= {"label"}})
        seen |= used
    out["per_unit"] = per_unit
    out["first_unit_used"] = sorted(first_set)
    attempts = [r for r in rows if r["record_type"] == "insert_attempt"]
    out["not_checkable_writes"] = sum(1 for r in attempts if r.get("validator_not_checkable"))
    out["rejections_by_rule"] = dict(collections.Counter(x.get("rule_id") for r in attempts for x in r.get("rejections") or []))
    out["refused_named_graph"] = sum(1 for r in rows if r["record_type"] == "refused")
    # the ending
    summaries, notes, summary_jobs, argue_jobs = [], [], 0, 0
    for job in jobs:
        hit = argue = False
        with gzip.open(pass_dir / "srw" / f"{job['job_id']}.llm_requests.full.jsonl.gz", "rt", encoding="utf-8") as handle:
            for line in handle:
                response = json.loads(line).get("response") or {}
                text = (response.get("additional_kwargs") or {}).get("reasoning_content") or ""
                hit = hit or bool(SUMMARY.search(text))
                argue = argue or bool(ARGUES.search(text))
                for call in response.get("tool_calls") or []:
                    if call.get("name") == "job_complete":
                        summaries.append(len(str((call.get("args") or {}).get("summary") or "")))
                        notes.append(len(str((call.get("args") or {}).get("notes") or "")))
        summary_jobs += hit
        argue_jobs += argue
    out["ending"] = {"job_complete_calls": len(summaries), "summary_chars_median": statistics.median(summaries) if summaries else None,
                     "with_notes": sum(1 for n in notes if n), "notes_chars_median": statistics.median(notes) if notes else None,
                     "jobs_whose_reasoning_mentions_a_summary": summary_jobs,
                     "jobs_whose_reasoning_argues_about_a_summary": argue_jobs}
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("run_ids", nargs="+")
    parser.add_argument("--cell", default="pilot-s1-rdf")
    args = parser.parse_args()
    runs = [run_findings(run_id, args.cell) for run_id in args.run_ids]
    (OUT / f"pass3-{args.run_ids[0]}.json").write_text(json.dumps(runs, indent=1, ensure_ascii=False), encoding="utf-8")
    for run in runs:
        g, t = run["graph"], run["totals"] or {}
        print(f"## {run['run_id']}\n")
        print(f"- graph: {g['triples']} triples, {g['distinct_properties']} distinct properties, {g['entity_object_triples']} with another entity as object; top {g['top_properties']}")
        print(f"- jobs completed {run['completed']}/{run['units']}; {t.get('model_calls')} model calls, {t.get('tool_calls')} tool calls, stored {t.get('stored')}, rejected {t.get('rejected')}; "
              f"input {t.get('input'):,}, output {t.get('output'):,}; pass {run['pass_seconds']} s; stack {run['stack']}")
        print(f"- not-checkable writes {run['not_checkable_writes']}; rejections {run['rejections_by_rule'] or 'none'}; refused reads naming a graph {run['refused_named_graph']}")
        print(f"- first unit used: {run['first_unit_used']}")
        for w in run["first_writes"]:
            print(f"  - first write ({w['result']}): {w['statement'][:400]!r}")
        for s in run["first_unit_reasoning_on_the_schema"][:4]:
            print(f"  - reasoning: …{s[:380]}…")
        print("- per unit (used / new / also used by unit 1 / label only): " + "; ".join(
            f"{u['unit'][:7]} {u['used']}/{u['new']}/{u['from_unit_1']}{'/L' if u['only_label'] else ''}" for u in run["per_unit"]))
        print(f"- ending: {run['ending']}\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
