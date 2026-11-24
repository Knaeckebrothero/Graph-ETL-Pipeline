#!/usr/bin/env python3
"""Write the list of units a cell processes, in the order every cell uses.

    python3 experiments/harness/pilot/make_units.py <manifest.csv> --bucket pilot > experiments/harness/pilot/units.txt

The run design fixes "one agent task per source unit in a seeded unit order identical across all
cells" and names neither the seed nor the method. Used here until the freeze says otherwise:
ascending SHA-256 of "<seed>:<unit_id>", with the selection seed of the sample as the seed. The
order then depends on nothing but the seed and the unit ids, not on a library or its version.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import sys
from pathlib import Path

SEED = "20261113"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("manifest", type=Path, help="the frozen list of units (CSV)")
    parser.add_argument("--bucket", default="pilot")
    parser.add_argument("--seed", default=SEED)
    args = parser.parse_args()
    data = args.manifest.read_bytes()
    rows = [row for row in csv.DictReader(data.decode("utf-8").splitlines()) if row["bucket"] == args.bucket]
    if not rows:
        print(f"no unit of bucket {args.bucket!r} in {args.manifest}", file=sys.stderr)
        return 1
    rows.sort(key=lambda row: hashlib.sha256(f"{args.seed}:{row['unit_id']}".encode()).hexdigest())
    print(f"# The {len(rows)} units of bucket {args.bucket!r} in the order of a cell. Written by make_units.py; do not edit.")
    print(f"# From {args.manifest.name}, sha256 {hashlib.sha256(data).hexdigest()}.")
    print("# Order: the run design asks for a seeded order identical across all cells and names no seed and no method.")
    print(f'# Used here, until the freeze fixes it: ascending SHA-256 of "<seed>:<unit_id>", seed {args.seed}.')
    print("# Columns, tab-separated: unit_id, source_id, stratum (what the tool is told when the unit is opened).")
    for row in rows:
        print("\t".join((row["unit_id"], row["source_id"], row["stratum"])))
    return 0


if __name__ == "__main__":
    sys.exit(main())
