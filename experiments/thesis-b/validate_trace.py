#!/usr/bin/env python3
"""Check a kgc-trace/1 version 1.1 file (JSON Lines) against the schema and the cross-record rules.

Needs:  pip install jsonschema

Checks:
  1. every record against kgc-trace-v1.1.schema.json (next to this file),
  2. rules a schema cannot express (ordering, uniqueness, derived counts, the three harness
     commitments: stable item_id, Thesis A's attempt_id form next to item_id, rejections present
     exactly on a reject),
  3. optionally, the same file against Thesis A's shared-minimum-log/1 schema and its cross-record
     rules (--shared-minimum PATH). Version 1.1 carries the shared minimum under Thesis A's names,
     so a trace whose cell values were supplied passes both; the report shows where it does not.

Exit code 0 when every requested check passes, 1 otherwise.

Usage (from the repository root):
  python3 experiments/thesis-b/validate_trace.py experiments/thesis-b/sample.kgc.jsonl
  python3 experiments/thesis-b/validate_trace.py RUN.jsonl \
      --shared-minimum experiments/log-schema/shared-minimum-log.v1.schema.json
"""
from __future__ import annotations

import argparse
import json
import pathlib
import re
import sys
from collections import Counter, defaultdict

try:
    from jsonschema import Draft202012Validator
except ImportError:  # pragma: no cover
    sys.exit("jsonschema is not installed: pip install jsonschema")

HERE = pathlib.Path(__file__).parent
SCHEMA_PATH = HERE / "kgc-trace-v1.1.schema.json"
ATTEMPT_ID = re.compile(r"^(?P<run>.+)-s(?P<step>\d+)-a(?P<n>\d+)$")


def read_jsonl(path: pathlib.Path) -> list[dict]:
    records = []
    for n, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError as exc:
            sys.exit(f"{path}: line {n} is not JSON: {exc}")
    return records


def schema_errors(records: list[dict], schema: dict) -> list[str]:
    validator = Draft202012Validator(schema)
    out = []
    for i, rec in enumerate(records, 1):
        for err in sorted(validator.iter_errors(rec), key=lambda e: list(e.path)):
            where = "/".join(str(p) for p in err.path) or "record"
            out.append(f"line {i} [{rec.get('record_type')}] ({where}): {err.message[:200]}")
    return out


def consistency(records: list[dict]) -> list[str]:
    """Rules across records. Field names are those of kgc-trace/1.1 (= Thesis A's for the shared fields)."""
    p: list[str] = []
    if not records:
        return ["empty trace"]
    types = [r.get("record_type") for r in records]
    if types[0] != "run_start":
        p.append("first record must be run_start")
    if types[-1] != "run_end":
        p.append("last record must be run_end")
    if [r.get("seq") for r in records] != list(range(1, len(records) + 1)):
        p.append("seq must be 1..n without gaps")
    ids = [r.get("record_id") for r in records]
    if len(set(ids)) != len(ids):
        p.append("record_id not unique")
    run_ids = {r.get("run_id") for r in records}
    if len(run_ids) != 1:
        p.append(f"all records must share one run_id, found {sorted(map(str, run_ids))}")
    run_id = records[0].get("run_id")

    calls = [r for r in records if r.get("record_type") == "model_call"]
    call_ids = [c.get("call_id") for c in calls]
    if len(set(call_ids)) != len(call_ids):
        p.append("call_id not unique")
    attempts = [r for r in records if r.get("record_type") == "insert_attempt"]
    aids = [r.get("attempt_id") for r in attempts]
    if len(set(aids)) != len(aids):
        p.append("attempt_id not unique")

    # Harness commitment 2 (as agreed 13/14 Nov): attempt_id = {run_id}-s{step}-a{n}, item_id as its own field
    for a in attempts:
        m = ATTEMPT_ID.match(str(a.get("attempt_id")))
        if not m or m["run"] != run_id:
            p.append(f"attempt {a.get('attempt_id')}: not of the form {{run_id}}-s{{step}}-a{{n}}")
            continue
        if int(m["step"]) != a.get("step_index"):
            p.append(f"attempt {a['attempt_id']}: step in id != step_index {a.get('step_index')}")
        if a.get("call_id") is not None and a["call_id"] not in call_ids:
            p.append(f"attempt {a['attempt_id']}: call_id {a['call_id']!r} names no model_call")

    # Harness commitment 3: rejections listed exactly when the validator rejected; a reject in reject mode is not sent
    for a in attempts:
        rejected = a.get("validator_result") == "reject"
        if rejected != bool(a.get("rejections")):
            p.append(f"attempt {a.get('attempt_id')}: rejections must be non-empty exactly on a reject (rule_id, element, expected, found)")
        if rejected and a.get("validator_mode") == "reject" and (a.get("engine_result") != "not_sent" or a.get("accepted")):
            p.append(f"attempt {a.get('attempt_id')}: rejected in reject mode but sent or accepted")
        if a.get("accepted") != (a.get("engine_result") == "ok"):
            p.append(f"attempt {a.get('attempt_id')}: accepted must mirror engine_result ok")

    # Harness commitment 1: a stable item_id; every attempt belongs to a known item, retries of one item are 0..n-1
    item_events: dict[str, list[str]] = defaultdict(list)
    for r in records:
        if r.get("record_type") == "item":
            item_events[str(r.get("item_id"))].append(str(r.get("event")))
    per_item: dict[str, list[dict]] = defaultdict(list)
    for a in attempts:
        if a.get("item_id") is None:
            p.append(f"attempt {a.get('attempt_id')}: item_id is null (allowed by Thesis A for a harness without items; kgc-trace/1.1 expects one)")
            continue
        per_item[str(a["item_id"])].append(a)
        if item_events and str(a["item_id"]) not in item_events:
            p.append(f"attempt {a.get('attempt_id')}: item_id {a['item_id']} has no item record")
    for iid, rows in per_item.items():
        got = sorted(a.get("retry_index") for a in rows)
        if got != list(range(len(rows))):
            p.append(f"item {iid}: retry_index values {got} are not 0..{len(rows) - 1}")
        if len({a.get("unit_id") for a in rows}) > 1:
            p.append(f"item {iid}: attempts in more than one unit")
    for iid, evs in item_events.items():
        if evs[0] != "created" and "validated" not in evs:
            p.append(f"item {iid}: first event {evs[0]!r} is neither created nor validated")
        if "persisted" in evs and "abandoned" in evs:
            p.append(f"item {iid}: both persisted and abandoned")

    # per-unit derived counts
    by_unit: dict[str, list[dict]] = defaultdict(list)
    for a in attempts:
        by_unit[str(a.get("unit_id"))].append(a)
    started = set()
    for r in records:
        if r.get("record_type") == "source_start":
            started.add(r.get("unit_id"))
        if r.get("record_type") != "source_end":
            continue
        if r.get("unit_id") not in started:
            p.append(f"source_end for unit {r.get('unit_id')!r} without a source_start before it")
        att = by_unit.get(str(r.get("unit_id")), [])
        if r.get("attempts") != len(att):
            p.append(f"source_end.attempts {r.get('attempts')} != insert_attempt records {len(att)} for {r.get('unit_id')}")
        rejects = sum(1 for a in att if a.get("validator_result") == "reject")
        if r.get("rejects") != rejects:
            p.append(f"source_end.rejects {r.get('rejects')} != {rejects} for {r.get('unit_id')}")
        accepted = sum(1 for a in att if a.get("accepted"))
        if r.get("accepted_writes") != accepted:
            p.append(f"source_end.accepted_writes {r.get('accepted_writes')} != {accepted} for {r.get('unit_id')}")
        abandoned = r.get("final_status") == "abandoned"
        if r.get("fatal") != abandoned or (r.get("abandon_reason") is not None) != abandoned:
            p.append(f"source_end {r.get('unit_id')}: fatal and abandon_reason are set exactly when final_status is abandoned")

    end = records[-1]
    if end.get("record_type") == "run_end":
        c = end.get("counts") or {}
        if item_events and c.get("items") != len(item_events):
            p.append(f"run_end.counts.items {c.get('items')} != item ids {len(item_events)}")
        abandoned = sum(1 for evs in item_events.values() if "abandoned" in evs)
        if item_events and c.get("items_abandoned") != abandoned:
            p.append(f"run_end.counts.items_abandoned {c.get('items_abandoned')} != {abandoned}")
        if c.get("insert_attempts") != len(attempts):
            p.append(f"run_end.counts.insert_attempts {c.get('insert_attempts')} != {len(attempts)}")
        if (end.get("status") == "completed") != (end.get("void_reason") is None):
            p.append("run_end.void_reason must be set exactly when status is void")
    return p


def shared_minimum_report(records: list[dict], schema_path: pathlib.Path) -> tuple[list[str], list[str]]:
    """The same file against Thesis A's shared-minimum-log/1 schema and the cross-record rules of its README."""
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    errs = schema_errors(records, schema)
    calls = {r.get("call_id") for r in records if r.get("record_type") == "model_call"}
    for i, r in enumerate(records, 1):
        rt = r.get("record_type")
        if rt == "model_call" and isinstance(r.get("tokens"), dict):
            t = r["tokens"]
            if t.get("cached_input") is not None and t["cached_input"] > t["input"]:
                errs.append(f"line {i} [model_call]: cached_input larger than input")
            if t.get("reasoning") is not None and t["reasoning"] > t["output"]:
                errs.append(f"line {i} [model_call]: reasoning larger than output")
        if rt == "insert_attempt":
            if r.get("call_id") not in calls:
                errs.append(f"line {i} [insert_attempt]: call_id {r.get('call_id')!r} names no model_call")
            if not str(r.get("attempt_id", "")).startswith(str(r.get("run_id")) + "-"):
                errs.append(f"line {i} [insert_attempt]: attempt_id does not start with the run_id")
    known = {e["if"]["properties"]["record_type"]["const"] for e in schema.get("allOf", [])}
    kinds = Counter(r.get("record_type") if r.get("record_type") in known else "extension" for r in records)
    head = [f"shared-minimum-log/1 ({schema_path.name}): {len(errs)} errors over {len(records)} records; "
            f"as Thesis A's checker sees them: {dict(sorted(kinds.items()))}"]
    messages = Counter(e.split("): ", 1)[-1] for e in errs)
    for msg, n in messages.most_common(10):
        head.append(f"  {n:>4} x {msg}")
    return head, errs


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("trace", type=pathlib.Path, help="kgc-trace/1.1 JSON Lines file")
    ap.add_argument("--schema", type=pathlib.Path, default=SCHEMA_PATH, help="kgc-trace/1.1 schema (default: next to this script)")
    ap.add_argument("--shared-minimum", type=pathlib.Path, default=None,
                    help="also check against Thesis A's shared-minimum-log/1 schema and its cross-record rules")
    ap.add_argument("--max", type=int, default=40, help="print at most this many errors per check")
    args = ap.parse_args(argv)

    records = read_jsonl(args.trace)
    schema = json.loads(args.schema.read_text(encoding="utf-8"))
    kinds = Counter(r.get("record_type") for r in records)
    errors = schema_errors(records, schema) + consistency(records)

    print(f"{args.trace}: {len(records)} records {dict(kinds)}, {len(errors)} errors (kgc-trace/1.1)")
    for e in errors[: args.max]:
        print("  " + e)
    if len(errors) > args.max:
        print(f"  ... {len(errors) - args.max} more")

    shared_errors: list[str] = []
    if args.shared_minimum:
        print()
        head, shared_errors = shared_minimum_report(records, args.shared_minimum)
        for line in head:
            print(line)
    return 1 if (errors or shared_errors) else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
