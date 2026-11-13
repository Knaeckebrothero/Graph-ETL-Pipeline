#!/usr/bin/env python3
"""Check a JSON Lines run log against shared-minimum-log/1.

Usage:  python3 validate_log.py LOG.jsonl [LOG2.jsonl ...]
Needs:  pip install jsonschema

Two kinds of checks:
  1. every record against the JSON Schema next to this file;
  2. rules a schema cannot express (listed in README.md, "Rules across records").
Exit code 0 when no error was found, 1 otherwise.
"""
import json
import pathlib
import sys

try:
    from jsonschema import Draft202012Validator
except ImportError:
    sys.exit("jsonschema is not installed: pip install jsonschema")

SCHEMA = json.loads((pathlib.Path(__file__).parent / "shared-minimum-log.v1.schema.json").read_text(encoding="utf-8"))
KNOWN = {entry["if"]["properties"]["record_type"]["const"] for entry in SCHEMA["allOf"]}


def check(path):
    errors, counts = [], {}
    validator = Draft202012Validator(SCHEMA)
    records = []
    for number, line in enumerate(pathlib.Path(path).read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError as exc:
            errors.append(f"line {number}: not JSON ({exc.msg})")
            continue
        kind = record.get("record_type") if isinstance(record, dict) else None
        counts[kind if kind in KNOWN else "extension"] = counts.get(kind if kind in KNOWN else "extension", 0) + 1
        for err in sorted(validator.iter_errors(record), key=lambda e: list(e.absolute_path)):
            where = "/".join(str(p) for p in err.absolute_path) or "(record)"
            errors.append(f"line {number} [{kind}] {where}: {err.message[:200]}")
        if isinstance(record, dict):
            records.append((number, record))

    calls = {r["call_id"] for _, r in records if r.get("record_type") == "model_call" and "call_id" in r}
    started = set()
    for number, r in records:
        kind = r.get("record_type")
        if kind == "model_call" and isinstance(r.get("tokens"), dict):
            t = r["tokens"]
            if isinstance(t.get("cached_input"), int) and isinstance(t.get("input"), int) and t["cached_input"] > t["input"]:
                errors.append(f"line {number} [model_call] cached_input is larger than input, of which it is a part")
            if isinstance(t.get("reasoning"), int) and isinstance(t.get("output"), int) and t["reasoning"] > t["output"]:
                errors.append(f"line {number} [model_call] reasoning is larger than output, of which it is a part")
        if kind == "model_call":
            if (r.get("stage") == "querying") != (r.get("cq_id") is not None):
                errors.append(f"line {number} [model_call] cq_id is set exactly for stage querying")
            if (r.get("stage") == "querying") != (r.get("pass_id") is not None):
                errors.append(f"line {number} [model_call] pass_id is set exactly for stage querying")
        if kind in ("insert_attempt", "cq_attempt") and r.get("call_id") not in calls:
            errors.append(f"line {number} [{kind}] call_id {r.get('call_id')!r} has no model_call record in this file")
        if kind == "insert_attempt":
            if not str(r.get("attempt_id", "")).startswith(str(r.get("run_id", "")) + "-"):
                errors.append(f"line {number} [insert_attempt] attempt_id does not start with the run_id")
            rejected = r.get("validator_result") == "reject"
            if rejected != bool(r.get("rejections")):
                errors.append(f"line {number} [insert_attempt] rejections must be non-empty exactly when validator_result is reject")
            if rejected and r.get("validator_mode") == "reject" and (r.get("engine_result") != "not_sent" or r.get("accepted")):
                errors.append(f"line {number} [insert_attempt] a write rejected in reject mode is not sent and not accepted")
            if r.get("accepted") and r.get("engine_result") != "ok":
                errors.append(f"line {number} [insert_attempt] accepted needs engine_result ok")
        if kind == "source_start":
            started.add((r.get("run_id"), r.get("unit_id")))
        if kind == "source_end":
            if (r.get("run_id"), r.get("unit_id")) not in started:
                errors.append(f"line {number} [source_end] no source_start for this unit before it")
            abandoned = r.get("final_status") == "abandoned"
            if r.get("fatal") != abandoned or (r.get("abandon_reason") is not None) != abandoned:
                errors.append(f"line {number} [source_end] fatal and abandon_reason are set exactly when final_status is abandoned")
            if isinstance(r.get("attempts"), int) and isinstance(r.get("rejects"), int) and r["rejects"] > r["attempts"]:
                errors.append(f"line {number} [source_end] more rejects than attempts")
        if kind == "cq_attempt":
            if not r.get("produced") and (r.get("parses") is not None or r.get("executes") is not None):
                errors.append(f"line {number} [cq_attempt] parses and executes are null when no query was produced")
            if r.get("executes") and not r.get("parses"):
                errors.append(f"line {number} [cq_attempt] a query that executes has parsed")
    return list(dict.fromkeys(errors)), counts


def main(paths):
    failed = False
    for path in paths:
        errors, counts = check(path)
        print(f"{path}: {sum(counts.values())} records {dict(sorted(counts.items()))}, {len(errors)} errors")
        for line in errors:
            print("  " + line)
        failed = failed or bool(errors)
    return 1 if failed else 0


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit(__doc__)
    sys.exit(main(sys.argv[1:]))
