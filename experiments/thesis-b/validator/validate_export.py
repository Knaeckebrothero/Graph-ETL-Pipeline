#!/usr/bin/env python3
"""Run the shared validator over a canonical export (JSON Lines, one canonical assertion per line).

Usage (from the repository root):
  python3 -m experiments.thesis-b.validator.validate_export EXPORT.jsonl \
      [--equivalence equivalence.json] [--units units.json] [--schema] [--detail]

  --schema    also check every line against canonical.schema.json (needs jsonschema)
  --detail    print one line per assertion with a violation
Exit code 0 when no violation, 1 otherwise. Rules that could not be checked for want of a binding are listed, never
counted as violations.

The module path contains a hyphen, so run it as a file instead:
  python3 experiments/thesis-b/validator/validate_export.py EXPORT.jsonl
"""
from __future__ import annotations

import argparse
import json
import pathlib
import sys

if __package__ in (None, ""):
    sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
    from validator import CANONICAL_SCHEMA_PATH, EquivalenceTable, UnitList, Validator  # type: ignore
else:  # pragma: no cover
    from . import CANONICAL_SCHEMA_PATH, EquivalenceTable, UnitList, Validator


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("export", type=pathlib.Path)
    ap.add_argument("--equivalence", type=pathlib.Path)
    ap.add_argument("--units", type=pathlib.Path)
    ap.add_argument("--schema", action="store_true", help="also validate each line against canonical.schema.json")
    ap.add_argument("--detail", action="store_true")
    args = ap.parse_args(argv)

    rows = [json.loads(l) for l in args.export.read_text(encoding="utf-8").splitlines() if l.strip()]
    schema_errors = 0
    if args.schema:
        try:
            from jsonschema import Draft202012Validator
        except ImportError:
            sys.exit("jsonschema is not installed: pip install jsonschema")
        v = Draft202012Validator(json.loads(CANONICAL_SCHEMA_PATH.read_text(encoding="utf-8")))
        for i, r in enumerate(rows, 1):
            for err in v.iter_errors(r):
                schema_errors += 1
                if args.detail:
                    print(f"line {i} schema: {'/'.join(map(str, err.path)) or 'record'}: {err.message[:160]}")
    val = Validator(mode="reject", equivalence=EquivalenceTable.load(args.equivalence) if args.equivalence else None,
                    units=UnitList.load(args.units) if args.units else None)
    rep = val.check_export(rows)
    print(f"{args.export}: {rep['assertions']} assertions, {rep['violations']} violations in "
          f"{rep['assertions_with_violation']} assertions; by rule {rep['by_rule']}; "
          f"checked {rep['checked_rules']} of {len(val.common_rules)} rules"
          + (f"; not checkable without binding: {rep['not_checkable_rules']}" if rep["not_checkable_rules"] else "")
          + (f"; canonical schema errors: {schema_errors}" if args.schema else ""))
    if args.detail:
        for d in rep["detail"]:
            if d["result"] == "reject":
                for r in d["rejections"]:
                    print(f"  #{d['index']} {d['item_id'] or d['assertion_id'] or ''} {r['rule_id']} {r['element']}: {r['message']}")
    return 1 if rep["violations"] or schema_errors else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
