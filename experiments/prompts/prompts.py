"""Prompt templates of the main agent: list the slots, hash the texts, fill them.

    python experiments/prompts/prompts.py slots
    python experiments/prompts/prompts.py hashes
    python experiments/prompts/prompts.py fill construction --arm pg --values values.json
    python experiments/prompts/prompts.py induction --arm pg \
        --units-csv experiments/sample/frozen/development-pilot.csv --units-dir experiments/sample/out

A template is a text file with slots written {{like_this}}. Every role has a fixed part
(`<role>.system.txt`) and a task part (`<role>.user.txt`). The fixed part has the slot
{{arm_binding}}, which takes the text of `bindings/<role>.<arm>.txt`. A binding has no slots.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
SET_ID = "A-frozen-v1"
ROLES = ("construction", "querying", "induction")
ARMS = ("pg", "rdf")
PARTS = ("system", "user")
SLOT = re.compile(r"\{\{([a-z_]+)\}\}")

# What may appear in each text, and nothing else. `arm_binding` is filled from the binding file.
SLOTS = {
    "construction.system": {"arm_binding", "schema_description", "max_self_corrections", "max_rows_shown"},
    "construction.user": {"unit"},
    "querying.system": {"arm_binding", "schema_description", "max_rows_shown"},
    "querying.user": {"question"},
    "induction.system": {"arm_binding"},
    "induction.user": {"units"},
}


def canonical(text: str) -> str:
    """The form that is hashed: LF line ends, no white space at the end of a line, no empty
    lines at the end, one line feed after the last line."""
    lines = [line.rstrip() for line in text.replace("\r\n", "\n").replace("\r", "\n").split("\n")]
    while lines and not lines[-1]:
        lines.pop()
    return "\n".join(lines) + "\n"


def sha256(text: str) -> str:
    return hashlib.sha256(canonical(text).encode("utf-8")).hexdigest()


def template(role: str, part: str) -> str:
    return canonical((HERE / "templates" / f"{role}.{part}.txt").read_text(encoding="utf-8"))


def binding(role: str, arm: str) -> str:
    return canonical((HERE / "bindings" / f"{role}.{arm}.txt").read_text(encoding="utf-8"))


def slots(text: str) -> set[str]:
    return set(SLOT.findall(text))


def fill(text: str, values: dict[str, object]) -> str:
    """Put the values into the slots, in one pass: a value that itself contains {{...}} stays
    as it is. Refuses a slot without a value and a value without a slot."""
    wanted = slots(text)
    missing, unused = wanted - set(values), set(values) - wanted
    if missing or unused:
        raise ValueError(f"missing values for {sorted(missing)}; no slot for {sorted(unused)}")
    return SLOT.sub(lambda match: str(values[match.group(1)]).rstrip("\n"), text)


def assemble(role: str, arm: str, values: dict[str, object]) -> dict[str, str]:
    """The two messages of one task. `values` holds every slot of both parts except arm_binding."""
    out = {}
    for part in PARTS:
        text = template(role, part)
        mine = {name: values[name] for name in slots(text) - {"arm_binding"} if name in values}
        if "arm_binding" in slots(text):
            mine["arm_binding"] = binding(role, arm)
        out[part] = fill(text, mine)
    unused = set(values) - set().union(*(slots(template(role, part)) for part in PARTS))
    if unused:
        raise ValueError(f"no slot for {sorted(unused)} in the {role} templates")
    return out


def join_units(units: list[str]) -> str:
    """The units of the induction prompt, in the order given, each under a numbered line."""
    count = len(units)
    return "\n\n".join(f"=== unit {index} of {count} ===\n{unit.strip()}" for index, unit in enumerate(units, 1))


def development_units(units_csv: Path, units_dir: Path) -> list[str]:
    """The texts of the development bucket, ordered by unit id; each text is checked against its hash."""
    with units_csv.open(encoding="utf-8", newline="") as handle:
        rows = sorted((row for row in csv.DictReader(handle) if row["bucket"] == "development"),
                      key=lambda row: row["unit_id"])
    texts = []
    for row in rows:
        data = (units_dir / row["unit_file"]).read_bytes()
        if hashlib.sha256(data).hexdigest() != row["unit_sha256"]:
            raise ValueError(f"{row['unit_id']}: the unit file does not match the frozen hash")
        texts.append(data.decode("utf-8"))
    return texts


def hashes() -> dict[str, str]:
    """One hash per text, under the keys used in run_start.prompt_sha256."""
    out = {f"{role}.{part}": sha256(template(role, part)) for role in ROLES for part in PARTS}
    out.update({f"binding.{role}.{arm}": sha256(binding(role, arm)) for role in ROLES for arm in ARMS})
    return dict(sorted(out.items()))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("slots", help="list the slots of every text")
    commands.add_parser("hashes", help="print one SHA-256 per text")
    filler = commands.add_parser("fill", help="fill the two parts of one role")
    filler.add_argument("role", choices=ROLES)
    filler.add_argument("--arm", choices=ARMS, required=True)
    filler.add_argument("--values", type=Path, required=True, help="JSON object: slot name to value")
    filler.add_argument("--out", type=Path, help="folder for <role>.<arm>.system.txt and .user.txt")
    inducer = commands.add_parser("induction", help="fill the induction prompt from the frozen development units")
    inducer.add_argument("--arm", choices=ARMS, required=True)
    inducer.add_argument("--units-csv", type=Path, required=True)
    inducer.add_argument("--units-dir", type=Path, required=True)
    inducer.add_argument("--out", type=Path, help="folder for induction.<arm>.system.txt and .user.txt")
    args = parser.parse_args(argv)

    if args.command == "slots":
        for key, names in SLOTS.items():
            print(f"{key}: {', '.join(sorted(names))}")
        return 0
    if args.command == "hashes":
        print(json.dumps({"template_set": SET_ID, "sha256": hashes()}, indent=2))
        return 0
    if args.command == "fill":
        role, arm = args.role, args.arm
        messages = assemble(role, arm, json.loads(args.values.read_text(encoding="utf-8")))
    else:
        role, arm = "induction", args.arm
        units = development_units(args.units_csv, args.units_dir)
        messages = assemble(role, arm, {"units": join_units(units)})
        print(f"{len(units)} development units", file=sys.stderr)
    for part, text in messages.items():
        if args.out:
            args.out.mkdir(parents=True, exist_ok=True)
            target = args.out / f"{role}.{arm}.{part}.txt"
            target.write_text(text, encoding="utf-8")
            print(f"{target}: {len(text.encode('utf-8'))} bytes, sha256 {hashlib.sha256(text.encode('utf-8')).hexdigest()}",
                  file=sys.stderr)
        else:
            print(f"----- {part} -----\n{text}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
