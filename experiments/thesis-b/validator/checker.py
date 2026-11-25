"""The checker Thesis A's database tool loads: `check(changes, lookup) -> list of faults`.

Thesis A's tool (`experiments/harness/dbtool`, 15 Nov 2026) runs the agent's statement, gets from the store what it
changed, and hands that to a checker while the transaction is open. The checker returns one entry per fault with the
four agreed fields `rule_id`, `element`, `expected`, `found`; an empty list means pass. It changes nothing and talks to
the store only through `lookup`: `find(label, property, value)` on both cells, `entity_exists(key)` on the RDF cell
(24 Nov 2026), `relation_count(key, type)` once Thesis A adds it. The tool itself answers "already present" for a statement
that changed nothing, reports CS0 for a statement the store cannot run, assembles the message, applies the mode
(reject or log_only) and writes the record. So this object holds the rules and nothing else.

Configuration of the tool (`cells.example.json`): `"checker": "validator.checker:checker"` with `experiments/thesis-b`
on the Python path, or `"checker": "experiments.thesis-b.validator.checker:checker"` is not importable (hyphen), so the
tool is started with `PYTHONPATH=experiments/thesis-b`. The checker reads the realisation from `changes["realisation"]`
and loads `schemas/treatments/<realisation>/declaration.json` from the repository root once per realisation.

    from validator.checker import checker
    c = checker()                                   # or Checker(root=Path(...), formats=FormatTable.load(...))
    faults = c.check(changes, lookup)               # [{"rule_id": ..., "element": ..., "expected": ..., "found": ...}, ...]
    c.observations                                  # what was counted but not rejected (undeclared properties, Rule 5)
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from .rules_treatment import Declaration
from .rules_write import Observation, WriteContext
from .validator import Result, Validator
from .vocabulary import FormatTable

HERE = Path(__file__).resolve().parent
DEFAULT_ROOT = HERE.parents[2]                 # experiments/thesis-b/validator -> repository root


class Checker:
    def __init__(self, *, root: Path | str | None = None, formats: FormatTable | None = None, mode: str = "reject"):
        self.root = Path(root or os.environ.get("KG_VALIDATOR_ROOT") or DEFAULT_ROOT)
        self.formats = formats or (FormatTable.load(os.environ["KG_VALIDATOR_FORMATS"]) if os.environ.get("KG_VALIDATOR_FORMATS") else FormatTable())
        self.mode = mode
        self._declarations: dict[str, Declaration | None] = {}
        self.observations: list[dict] = []          # Rule 5 counts, per statement: {"realisation", "unit_id", "item_id", "kind", "element", "found", "note"}
        self.last: Result | None = None
        self.warnings: list[str] = []

    # ---------------------------------------------------------------- declarations
    def declaration(self, realisation: str | None) -> Declaration | None:
        if not realisation:
            return None
        if realisation not in self._declarations:
            path = self.root / "schemas" / "treatments" / realisation / "declaration.json"
            if not path.exists():
                self.warnings.append(f"no declaration.json for realisation {realisation!r} under {self.root}; treatment scope not checked")
                self._declarations[realisation] = None
            else:
                d = Declaration.load(path, allow_placeholder=True)
                if d.status == "placeholder":
                    self.warnings.append(f"realisation {realisation!r} is a placeholder (S2 before induction); treatment scope not checked")
                self._declarations[realisation] = d
        return self._declarations[realisation]

    # ---------------------------------------------------------------- the one method the tool calls
    def check(self, changes: dict, lookup: Any = None) -> list[dict]:
        decl = self.declaration(changes.get("realisation"))
        v = Validator(mode=self.mode, formats=self.formats, declaration=decl)
        # everything the cell's lookup offers is passed on: find (both cells), entity_exists (RDF, for CS3 on an end
        # outside the write), relation_count (asked of Thesis A for TS-cardinality); a missing one leaves its rule not_checkable
        ctx = WriteContext.from_lookup(lookup, self.formats)
        res = v.check_write(changes, ctx)
        self.last = res
        for o in res.observations:
            self.observations.append({"realisation": changes.get("realisation"), "unit_id": changes.get("unit_id"),
                                      "item_id": changes.get("item_id"), **o.to_log()})
        return [r.to_log() for r in res.rejections]

    def not_checkable(self) -> list[str]:
        """Rules the last check could not decide for want of a binding (never a fault, never a pass)."""
        return sorted({r.rule_id for r in self.last.not_checkable}) if self.last else []


def checker() -> Checker:
    """Factory for Thesis A's `load_checker("validator.checker:checker")`."""
    return Checker()
