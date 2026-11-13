#!/usr/bin/env python3
"""Check a kgc-trace/1 file (JSON Lines) against the schema and the cross-record rules.

Needs:  pip install jsonschema

Checks:
  1. every record against kgc-trace-v1.schema.json (next to this file),
  2. rules a schema cannot express (ordering, uniqueness, derived counts,
     the three harness commitments: stable item_id, attempt_id scheme,
     rejections present exactly on a reject),
  3. optionally, the same file against Thesis A's shared-minimum-log/1 schema
     (--shared-minimum PATH), reported separately so the two views stay apart.

Exit code 0 when the kgc-trace/1 checks pass, 1 otherwise. The shared-minimum
report never changes the exit code; it tells where the two schemas diverge.

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
SCHEMA_PATH = HERE / "kgc-trace-v1.schema.json"
ATTEMPT_ID = re.compile(r"^(?P<run>.+):s(?P<step>\d+):(?P<item>[^:]+):r(?P<retry>\d+)$")


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


def schema_errors(records: list[dict], schema: dict, kind_key: str) -> list[str]:
    validator = Draft202012Validator(schema)
    out = []
    for i, rec in enumerate(records, 1):
        for err in sorted(validator.iter_errors(rec), key=lambda e: list(e.path)):
            where = "/".join(str(p) for p in err.path) or "record"
            out.append(f"line {i} [{rec.get(kind_key)}] ({where}): {err.message}")
    return out


def consistency(records: list[dict]) -> list[str]:
    """Rules across records. Field names are those of kgc-trace/1."""
    p: list[str] = []
    if not records:
        return ["empty trace"]
    kinds = [r.get("kind") for r in records]
    if kinds[0] != "run_start":
        p.append("first record must be run_start")
    if kinds[-1] != "run_end":
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

    attempts = [r for r in records if r.get("kind") == "insert_attempt"]
    aids = [r.get("attempt_id") for r in attempts]
    if len(set(aids)) != len(aids):
        p.append("attempt_id not unique")

    # Harness commitment 2: attempt_id = {run_id}:s{step}:{item_id}:r{retry}
    for a in attempts:
        m = ATTEMPT_ID.match(str(a.get("attempt_id")))
        if not m:
            p.append(f"attempt {a.get('attempt_id')}: not of the form {{run_id}}:s{{step}}:{{item_id}}:r{{retry}}")
            continue
        if m["run"] != run_id:
            p.append(f"attempt {a['attempt_id']}: run part differs from run_id")
        if int(m["step"]) != a.get("step_index"):
            p.append(f"attempt {a['attempt_id']}: step in id != step_index {a.get('step_index')}")
        if m["item"] != str(a.get("item_id")):
            p.append(f"attempt {a['attempt_id']}: item in id != item_id {a.get('item_id')}")
        if int(m["retry"]) != a.get("retry_index"):
            p.append(f"attempt {a['attempt_id']}: retry in id != retry_index {a.get('retry_index')}")

    # Harness commitment 3: rejections listed exactly when the validator rejected
    for a in attempts:
        v = a.get("validator") or {}
        rejections = v.get("rejections") or []
        if v.get("result") == "reject" and not rejections:
            p.append(f"attempt {a.get('attempt_id')}: validator reject without rejections (rule_id, element, expected, found)")
        if v.get("result") != "reject" and rejections:
            p.append(f"attempt {a.get('attempt_id')}: rejections present but validator result is {v.get('result')!r}")

    # Harness commitment 1: a stable item_id; every attempt belongs to a known item,
    # and an item written more than once keeps one id across its retries
    item_events: dict[str, list[str]] = defaultdict(list)
    for r in records:
        if r.get("kind") == "item":
            item_events[str(r.get("item_id"))].append(str(r.get("event")))
    for a in attempts:
        if str(a.get("item_id")) not in item_events:
            p.append(f"attempt {a.get('attempt_id')}: item_id {a.get('item_id')} has no item record")
    retries_per_item = Counter(str(a.get("item_id")) for a in attempts)
    for iid, n in retries_per_item.items():
        got = sorted(a.get("retry_index") for a in attempts if str(a.get("item_id")) == iid)
        if got != list(range(n)):
            p.append(f"item {iid}: retry_index values {got} are not 0..{n - 1}")
    for iid, evs in item_events.items():
        if evs[0] != "created" and "validated" not in evs:
            p.append(f"item {iid}: first event {evs[0]!r} is neither created nor validated")
        if "persisted" in evs and "abandoned" in evs:
            p.append(f"item {iid}: both persisted and abandoned")

    # per-source derived counts
    by_src: dict[str, list[dict]] = defaultdict(list)
    for a in attempts:
        by_src[str(a.get("source_id"))].append(a)
    for r in records:
        if r.get("kind") != "source_end":
            continue
        att = by_src.get(str(r.get("source_id")), [])
        if r.get("attempts") != len(att):
            p.append(f"source_end.attempts {r.get('attempts')} != insert_attempt records {len(att)} for {r.get('source_id')}")
        rejects = sum(1 for a in att if (a.get("validator") or {}).get("result") == "reject")
        if r.get("rejects") != rejects:
            p.append(f"source_end.rejects {r.get('rejects')} != {rejects} for {r.get('source_id')}")
        accepted = sum(1 for a in att if (a.get("engine") or {}).get("result") in ("written", "duplicate"))
        if r.get("accepted") != accepted:
            p.append(f"source_end.accepted {r.get('accepted')} != {accepted} for {r.get('source_id')}")

    end = records[-1]
    if end.get("kind") == "run_end":
        c = end.get("counts") or {}
        if c.get("items") != len(item_events):
            p.append(f"run_end.counts.items {c.get('items')} != item ids {len(item_events)}")
        abandoned = sum(1 for evs in item_events.values() if "abandoned" in evs)
        if c.get("items_abandoned") != abandoned:
            p.append(f"run_end.counts.items_abandoned {c.get('items_abandoned')} != {abandoned}")
        if c.get("insert_attempts") != len(attempts):
            p.append(f"run_end.counts.insert_attempts {c.get('insert_attempts')} != {len(attempts)}")
    return p


def shared_minimum_report(records: list[dict], schema_path: pathlib.Path) -> list[str]:
    """Validate the same file against Thesis A's shared-minimum-log/1 and summarise."""
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    errs = schema_errors(records, schema, "record_type")
    kinds = Counter(r.get("record_type", r.get("kind")) for r in records)
    lines = [f"shared-minimum-log/1 ({schema_path.name}): {len(errs)} errors over {len(records)} records; "
             f"record types seen as Thesis A's checker sees them: {dict(kinds)}"]
    messages = Counter(e.split("): ", 1)[-1] for e in errs)
    for msg, n in messages.most_common(10):
        lines.append(f"  {n:>4} x {msg}")
    return lines


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("trace", type=pathlib.Path, help="kgc-trace/1 JSON Lines file")
    ap.add_argument("--schema", type=pathlib.Path, default=SCHEMA_PATH, help="kgc-trace/1 schema (default: next to this script)")
    ap.add_argument("--shared-minimum", type=pathlib.Path, default=None,
                    help="also check against Thesis A's shared-minimum-log/1 schema and report the differences")
    ap.add_argument("--max", type=int, default=40, help="print at most this many errors per check")
    args = ap.parse_args(argv)

    records = read_jsonl(args.trace)
    schema = json.loads(args.schema.read_text(encoding="utf-8"))
    kinds = Counter(r.get("kind") for r in records)
    errors = schema_errors(records, schema, "kind") + consistency(records)

    print(f"{args.trace}: {len(records)} records {dict(kinds)}, {len(errors)} errors")
    for e in errors[: args.max]:
        print("  " + e)
    if len(errors) > args.max:
        print(f"  ... {len(errors) - args.max} more")

    if args.shared_minimum:
        print()
        for line in shared_minimum_report(records, args.shared_minimum):
            print(line)
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
