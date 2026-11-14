"""The shared harness validator (Thesis B, agreement 3.2): local per-insert rules, global end-of-run rules,
reject and log-only mode, uniform messages carrying rule id, element, expected and found.

    v = Validator(mode="reject", equivalence=EquivalenceTable.load(...), units=UnitList.load(...),
                  declaration=Declaration.load("schemas/treatments/S4-curated/pg"))

    r = v.check_insert(assertions, graph)          # local: common scope on the canonical candidates of one write
    r.blocks_write                                 # True in reject mode when something was rejected
    r.to_log(item_id=...)                          # the insert_attempt fields of shared-minimum-log/1 / kgc-trace/1.1
    r.message()                                    # the text returned to the agent, item ids included

    w = v.check_write(schema_write, graph)         # treatment scope on the write as rendered into the schema
    g = v.check_export(assertions)                 # global: common scope over the canonical export (run_end.global_validator)

`graph` is a GraphView: what the validator may know about the graph so far (entity ids, assertion identities,
unique property values). The harness fills it; the validator never queries a database itself, so engine-native
constraints are never the measuring instrument (Eval 2, BU-15, EX-02).
"""
from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

from .rules_common import COMMON_RULES, Context, Rejection, assertion_key, run_common
from .rules_treatment import Declaration, WriteContext, run_treatment
from .vocabulary import EquivalenceTable, UnitList

VALIDATOR_VERSION = "0.1.0"
RULES_VERSION = "common-scope draft-0.1 (Eval 2 package v0.1, card P.3) + treatment-scope from declaration.json draft-0.1"
HERE = Path(__file__).parent
CANONICAL_SCHEMA_PATH = HERE / "canonical.schema.json"


@dataclass
class GraphView:
    """What the harness tells the validator about the graph before this write."""
    entity_ids: set[str] = field(default_factory=set)
    assertion_keys: set[tuple] = field(default_factory=set)
    node_labels: dict[str, str] = field(default_factory=dict)                     # element id -> label (treatment scope)
    unique_values: dict[tuple[str, str], set[Any]] = field(default_factory=dict)   # (label, property) -> values

    def remember(self, assertions: Iterable[dict], ctx: Context) -> None:
        """After an accepted write: record identities so that CS-07 sees them next time."""
        for a in assertions:
            self.assertion_keys.add(assertion_key(a, ctx))
            for role in ("subject", "object"):
                ref = a.get(role)
                if isinstance(ref, dict) and ref.get("kind", "entity") == "entity" and ref.get("id"):
                    self.entity_ids.add(ref["id"])


@dataclass
class ItemResult:
    item_id: str | None
    index: int
    rejections: list[Rejection]
    not_checkable: list[Rejection]

    @property
    def result(self) -> str:
        return "reject" if self.rejections else "pass"


@dataclass
class Result:
    mode: str
    scope: str                       # local-common | local-treatment | global-common
    items: list[ItemResult]
    rules_run: list[str]

    @property
    def result(self) -> str:
        return "reject" if any(i.rejections for i in self.items) else "pass"

    @property
    def blocks_write(self) -> bool:
        return self.mode == "reject" and self.result == "reject"

    @property
    def rejections(self) -> list[Rejection]:
        return [r for i in self.items for r in i.rejections]

    @property
    def not_checkable(self) -> list[Rejection]:
        return [r for i in self.items for r in i.not_checkable]

    def to_log(self, item_id: str | None = None) -> dict:
        """Fields of one insert_attempt record (shared-minimum-log/1 names). With item_id: that candidate only."""
        sel = [i for i in self.items if item_id is None or i.item_id == item_id]
        rej = [r.to_log() for i in sel for r in i.rejections]
        return {"validator_mode": self.mode, "validator_result": "reject" if rej else "pass", "rejections": rej,
                "validator_not_checkable": [r.rule_id for i in sel for r in i.not_checkable],
                "validator_version": VALIDATOR_VERSION}

    def message(self) -> str:
        """Uniform text returned to the agent: one line per rejection, item id first so the model can resend it."""
        if self.result == "pass":
            return "accepted" if self.mode == "reject" else "accepted (validator in log-only mode)"
        lines = ["rejected" if self.mode == "reject" else "would be rejected (log-only mode, write goes through)"]
        for i in self.items:
            for r in i.rejections:
                tag = f"item {i.item_id}" if i.item_id else f"candidate #{i.index}"
                lines.append(f"{tag}: {r.rule_id} at {r.element}: expected {_short(r.expected)}, found {_short(r.found)}. {r.message}")
        if self.mode == "reject":
            lines.append("Resend the corrected candidates with the same item ids.")
        return "\n".join(lines)


def _short(x: Any, n: int = 120) -> str:
    s = json.dumps(x, ensure_ascii=False, default=str) if not isinstance(x, str) else x
    return s if len(s) <= n else s[: n - 3] + "..."


class Validator:
    def __init__(self, *, mode: str = "reject", equivalence: EquivalenceTable | None = None, units: UnitList | None = None,
                 declaration: Declaration | None = None, common_rules: Iterable[str] | None = None):
        if mode not in ("reject", "log_only"):
            raise ValueError("mode must be reject or log_only")
        self.mode = mode
        self.equivalence = equivalence or EquivalenceTable()
        self.units = units or UnitList()
        self.declaration = declaration
        self.common_rules = list(common_rules or COMMON_RULES)

    # ---------------------------------------------------------------- local
    def check_insert(self, assertions: list[dict], graph: GraphView | None = None) -> Result:
        """Common scope on the canonical candidates of one write-tool call (local rules)."""
        graph = graph or GraphView()
        ctx = Context(equivalence=self.equivalence, units=self.units, known_entity_ids=set(graph.entity_ids),
                      batch=list(assertions), known_assertion_keys=set(graph.assertion_keys))
        items = []
        for n, a in enumerate(assertions):
            rej = run_common(a, ctx, self.common_rules)
            items.append(ItemResult(item_id=a.get("item_id") if isinstance(a, dict) else None, index=n,
                                    rejections=[r for r in rej if r.status == "reject"],
                                    not_checkable=[r for r in rej if r.status == "not_checkable"]))
        return Result(mode=self.mode, scope="local-common", items=items, rules_run=list(self.common_rules))

    def check_write(self, write: dict, graph: GraphView | None = None) -> Result:
        """Treatment scope on the write as rendered into the realisation's schema."""
        if self.declaration is None:
            raise ValueError("no declaration loaded; treatment scope needs the realisation's declaration.json")
        graph = graph or GraphView()
        rej = run_treatment(write, self.declaration, WriteContext(known_nodes=dict(graph.node_labels), unique_values=graph.unique_values))
        return Result(mode=self.mode, scope="local-treatment",
                      items=[ItemResult(item_id=write.get("item_id"), index=0, rejections=rej, not_checkable=[])],
                      rules_run=["TS-01", "TS-02", "TS-03", "TS-04", "TS-05"])

    # ---------------------------------------------------------------- global
    def check_export(self, assertions: list[dict]) -> dict:
        """End-of-run rules over the canonical export: the whole file is one batch, so CS-07 finds duplicates across
        units and CS-03 sees every introduced entity. Returns run_end.global_validator plus the per-assertion detail."""
        ctx = Context(equivalence=self.equivalence, units=self.units, batch=list(assertions))
        by_rule: Counter = Counter()
        not_checkable: Counter = Counter()
        detail = []
        seen_keys: set[tuple] = set()
        for n, a in enumerate(assertions):
            rej = run_common(a, ctx, self.common_rules)
            # CS-07 over the file: report each duplicate once per extra occurrence, not twice per pair
            key = assertion_key(a, ctx)
            rej = [r for r in rej if r.rule_id != "CS-07"]
            if key in seen_keys:
                rej.append(Rejection("CS-07", "assertion", "one occurrence per proposition in the export", key,
                                     "duplicate of an earlier assertion in the export"))
            seen_keys.add(key)
            hard = [r for r in rej if r.status == "reject"]
            for r in hard:
                by_rule[r.rule_id] += 1
            for r in rej:
                if r.status == "not_checkable":
                    not_checkable[r.rule_id] += 1
            detail.append({"index": n, "assertion_id": a.get("assertion_id"), "item_id": a.get("item_id"),
                           "result": "reject" if hard else "pass", "rejections": [r.to_log() for r in hard]})
        checked = len(self.common_rules) - len(not_checkable)
        return {"checked_rules": checked, "violations": sum(by_rule.values()), "by_rule": dict(sorted(by_rule.items())),
                "assertions": len(assertions), "assertions_with_violation": sum(1 for d in detail if d["result"] == "reject"),
                "not_checkable_rules": sorted(not_checkable), "validator_version": VALIDATOR_VERSION,
                "rules_version": RULES_VERSION, "detail": detail}

    # ---------------------------------------------------------------- introspection
    def describe(self) -> dict:
        return {"validator_version": VALIDATOR_VERSION, "rules_version": RULES_VERSION, "mode": self.mode,
                "common_rules": {rid: COMMON_RULES[rid][0] for rid in self.common_rules},
                "bindings": {"equivalence_table": self.equivalence.loaded, "unit_list": self.units.loaded},
                "treatment": None if self.declaration is None else
                {"realisation": self.declaration.realisation, "status": self.declaration.status,
                 "constraint_inventory": self.declaration.constraint_inventory()}}
