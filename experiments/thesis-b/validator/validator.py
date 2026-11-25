"""The shared harness validator (Thesis B, agreement 3.2): a library with two entry points and the same rule code,
as Thesis A's common-scope rule list v0.2 (section 9) asks for.

    v = Validator(mode="reject", declaration=Declaration.load("schemas/treatments/S4-curated/pg"))

    # 1. before the database: the diff the agent's query made, handed over by Thesis A's query tool
    r = v.check_write(diff_from_neo4j(nodes, rels, query=cypher, unit_id=unit))     # or diff_from_ntriples(...)
    r.outcome          # pass | reject | already_present | nothing_written
    r.blocks_write     # True in reject mode when something was rejected -> the tool rolls back
    r.message()        # the text returned to the agent: every fault of a readable write in one message
    r.to_log()         # validator_mode, validator_result, rejections[...] for the insert_attempt record
    r.element_ids      # what the write produced (Thesis A's element_ids), or what was already there

    # a write that cannot be read at all (query did not parse, diff unreadable): CS0, nothing else is tested
    r = v.unreadable("syntax error at line 1")

    # 2. after the run: the canonical export (JSON Lines), one assertion per line
    g = v.check_export(assertions, write_log={element_id: unit_id, ...})          # run_end.global_validator

The validator never queries a database itself; what it may know about the graph comes from the tool (`WriteContext`).
Engine-native constraints are never the measuring instrument (Eval 2, BU-15, EX-02).
"""
from __future__ import annotations

import json
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable

from .items import Diff, EntityItem, RelationItem, diff_from_changes, diff_from_neo4j, diff_from_ntriples, is_changes
from .rules_common import COMMON_RULES, Context, Rejection, assertion_key, run_common, _has_identity, _is_entity, _nonempty
from .rules_treatment import Declaration
from .rules_write import TREATMENT_RULE_IDS, WRITE_COMMON_RULES, Observation, WriteContext, diff_level, item_key, run_treatment_items, run_write_common
from .vocabulary import EquivalenceTable, FormatTable

VALIDATOR_VERSION = "0.3.0"
RULES_VERSION = "common-scope-rules v0.2 (15 Nov 2026) + treatment scope from declaration.json v0.2 with Rule 6 (induced schema, 23 Nov 2026)"
HERE = Path(__file__).parent
CANONICAL_SCHEMA_PATH = HERE / "canonical.schema.json"
CHANGES_SCHEMA_PATH = HERE / "changes.schema.json"


@dataclass
class ItemResult:
    ref: str | None
    kind: str                         # entity | relation | write
    rejections: list[Rejection]
    not_checkable: list[Rejection]

    @property
    def result(self) -> str:
        return "reject" if self.rejections else "pass"


@dataclass
class Result:
    mode: str
    scope: str                        # write | export
    items: list[ItemResult]
    rules_run: list[str]
    outcome: str = "pass"             # pass | reject | already_present | nothing_written
    element_ids: list[str] = field(default_factory=list)
    observations: list[Observation] = field(default_factory=list)   # counted, not rejected (Rule 5)

    @property
    def result(self) -> str:
        """Thesis A's validator_result: pass or reject."""
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

    def to_log(self) -> dict:
        """Validator fields of one insert_attempt record (shared-minimum-log/1 names) plus Thesis B's extensions."""
        rej = [r.to_log() for r in self.rejections]
        return {"validator_mode": self.mode, "validator_result": "reject" if rej else "pass", "rejections": rej,
                "validator_not_checkable": sorted({r.rule_id for r in self.not_checkable}),
                "validator_outcome": self.outcome, "validator_observations": [o.to_log() for o in self.observations],
                "validator_version": VALIDATOR_VERSION}

    def engine_result(self) -> str:
        """What the tool should write as engine_result when it follows this result: not_sent on a block,
        already_present for a repeat; ok otherwise (the tool overrides with error when the engine fails)."""
        if self.blocks_write:
            return "not_sent"
        if self.outcome == "already_present":
            return "already_present"
        return "ok"

    def message(self) -> str:
        """Text returned to the agent. Nothing from the benchmark: `expected` only says what the schema description or
        the unit already shows. Every fault of a readable write in one message (list v0.2, section 5)."""
        if self.outcome == "already_present":
            return "already present: " + ", ".join(self.element_ids) if self.element_ids else "already present"
        if self.result == "pass":
            return "accepted" if self.mode == "reject" else "accepted (validator in log-only mode)"
        lines = ["rejected" if self.mode == "reject" else "would be rejected (log-only mode, write goes through)"]
        for i in self.items:
            for r in i.rejections:
                lines.append(f"{r.rule_id} at {r.element}: expected {_short(r.expected)}, found {_short(r.found)}. {r.message}")
        if self.mode == "reject":
            lines.append("Nothing was written. Correct the write and send it again.")
        return "\n".join(lines)


def _short(x: Any, n: int = 120) -> str:
    s = json.dumps(x, ensure_ascii=False, default=str) if not isinstance(x, str) else x
    return s if len(s) <= n else s[: n - 3] + "..."


class Validator:
    def __init__(self, *, mode: str = "reject", equivalence: EquivalenceTable | None = None, formats: FormatTable | None = None,
                 declaration: Declaration | None = None, units: FormatTable | None = None):
        if mode not in ("reject", "log_only"):
            raise ValueError("mode must be reject or log_only")
        self.mode = mode
        self.equivalence = equivalence or EquivalenceTable()
        self.formats = formats or units or FormatTable()
        self.declaration = declaration

    # ---------------------------------------------------------------- 1. before the database
    def unreadable(self, reason: str) -> Result:
        """CS0: the write cannot be read (the query did not parse, the diff is not readable). Only that is said; nothing
        else is tested, because what the agent meant cannot be known. Counts as friction, never on the export."""
        rej = Rejection("CS0", "write", "a query the engine can parse", _short(reason, 200), "the write could not be read")
        return Result(mode=self.mode, scope="write", items=[ItemResult(None, "write", [rej], [])], rules_run=["CS0"], outcome="reject")

    def check_write(self, diff: Diff | str | dict, ctx: WriteContext | None = None, *, query: str | None = None,
                    unit_id: str | None = None, existing_ids: Iterable[str] | None = None) -> Result:
        """Common scope at the write (CS1, CS3, CS4) and the treatment scope on every item of the diff.
        `diff` may be Thesis A's `changes` dict (database tool), a Diff, N-Triples text (rdf), or a plain dict
        {"nodes": [...], "relationships": [...]} of driver-shaped nodes (pg)."""
        ctx = ctx or WriteContext(formats=self.formats)
        if ctx.formats is None:
            ctx.formats = self.formats
        if ctx.identifying is None and self.declaration is not None:
            ctx.identifying = self.declaration.identifying
        try:
            d = self._as_diff(diff, query=query, unit_id=unit_id, existing_ids=existing_ids,
                              unique_by_type=self.declaration.unique_by_type if self.declaration else None,
                              identifying=self.declaration.identifying if self.declaration else None,
                              identifier_by_type=self.declaration.identifier_by_type if self.declaration else None)
        except (ValueError, TypeError, KeyError, AttributeError) as e:
            return self.unreadable(str(e))
        rules = list(WRITE_COMMON_RULES) + (TREATMENT_RULE_IDS if self.declaration and not self.declaration.declares_nothing else [])
        outcome, level = diff_level(d)
        items: list[ItemResult] = []
        if level:
            items.append(ItemResult(None, "write", [r for r in level if r.status == "reject"], [r for r in level if r.status == "not_checkable"]))
        failed: set[str] = set()
        for it in d.items():
            rej = run_write_common(it, ctx, d)
            key = item_key(it)
            hard = [r for r in rej if r.status == "reject"]
            if hard:
                failed.add(key)
            items.append(ItemResult(key, it.kind, hard, [r for r in rej if r.status == "not_checkable"]))
        observations: list[Observation] = []
        if self.declaration is not None:
            ts, observations = run_treatment_items(d, self.declaration, ctx, failed)
            for key, rej in ts.items():
                hit = next((i for i in items if i.ref == key), None)
                hard = [r for r in rej if r.status == "reject"]
                soft = [r for r in rej if r.status == "not_checkable"]
                if hit is None:
                    items.append(ItemResult(key, "item", hard, soft))
                else:
                    hit.rejections.extend(hard)
                    hit.not_checkable.extend(soft)
        res = Result(mode=self.mode, scope="write", items=items, rules_run=rules, outcome=outcome or "pass",
                     element_ids=list(d.existing_ids) if outcome == "already_present" else d.element_ids, observations=observations)
        if res.result == "reject":
            res.outcome = "reject"
        return res

    check = check_write

    @staticmethod
    def _as_diff(diff: Diff | str | dict, *, query: str | None, unit_id: str | None, existing_ids: Iterable[str] | None,
                 unique_by_type: dict[str, set[str]] | None = None, identifying: dict[str, str] | None = None,
                 identifier_by_type: dict[str, str] | None = None) -> Diff:
        if isinstance(diff, Diff):
            if query and not diff.query:
                diff.query = query
            if unit_id and not diff.unit_id:
                diff.unit_id = unit_id
            if existing_ids:
                diff.existing_ids = list(existing_ids)
            return diff
        if is_changes(diff):
            return diff_from_changes(diff, unique_by_type=unique_by_type, identifying=identifying, identifier_by_type=identifier_by_type)
        if isinstance(diff, str):
            return diff_from_ntriples(diff, query=query, unit_id=unit_id, existing_ids=existing_ids, unique_by_type=unique_by_type,
                                      identifier_by_type=identifier_by_type)
        if isinstance(diff, dict):
            return diff_from_neo4j(diff.get("nodes") or [], diff.get("relationships") or [], query=query or diff.get("query"),
                                   unit_id=unit_id or diff.get("unit_id"), existing_ids=existing_ids or diff.get("existing_ids"),
                                   unique_by_type=unique_by_type)
        raise TypeError(f"diff of type {type(diff).__name__} is not readable")

    # ---------------------------------------------------------------- 2. after the run
    def check_export(self, assertions: list[dict], *, write_log: dict[str, str] | None = None,
                     rule_ids: Iterable[str] | None = None) -> dict:
        """End-of-run rules over the canonical export: every occurrence is kept, the first passes CS7 and every further
        one is a violation; CS3 sees every entity that has an identifying value anywhere in the export; CS6 follows the
        chain into `write_log` (element id -> unit id) when given. Returns run_end.global_validator plus detail."""
        rules = list(rule_ids or COMMON_RULES)
        known_ids = {a[role]["id"] for a in assertions for role in ("subject", "object")
                     if _is_entity(a.get(role)) and _nonempty(a[role].get("id")) and _has_identity(a[role])}
        ctx = Context(equivalence=self.equivalence, formats=self.formats, batch=list(assertions), known_entity_ids=known_ids,
                      write_log=write_log)
        by_rule: Counter = Counter()
        not_checkable: Counter = Counter()
        detail = []
        for n, a in enumerate(assertions):
            rej = run_common(a, ctx, rules)
            ctx.known_assertion_keys.add(assertion_key(a, ctx))
            hard = [r for r in rej if r.status == "reject"]
            for r in hard:
                by_rule[r.rule_id] += 1
            for r in rej:
                if r.status == "not_checkable":
                    not_checkable[r.rule_id] += 1
            detail.append({"index": n, "assertion_id": a.get("assertion_id"), "item_id": a.get("item_id"),
                           "result": "reject" if hard else "pass", "rejections": [r.to_log() for r in hard]})
        checked = len(rules) - len(not_checkable)
        return {"checked_rules": checked, "violations": sum(by_rule.values()), "by_rule": dict(sorted(by_rule.items())),
                "assertions": len(assertions), "assertions_with_violation": sum(1 for d in detail if d["result"] == "reject"),
                "not_checkable_rules": sorted(not_checkable), "validator_version": VALIDATOR_VERSION,
                "rules_version": RULES_VERSION, "equivalence_table": "loaded" if self.equivalence.loaded else "none (figure provisional)",
                "detail": detail}

    # ---------------------------------------------------------------- introspection
    def describe(self) -> dict:
        return {"validator_version": VALIDATOR_VERSION, "rules_version": RULES_VERSION, "mode": self.mode,
                "at_write": {rid: WRITE_COMMON_RULES[rid][0] for rid in WRITE_COMMON_RULES} | {"CS0": "write cannot be read"},
                "on_export": {rid: COMMON_RULES[rid][0] for rid in COMMON_RULES},
                "bindings": {"equivalence_table": self.equivalence.loaded, "unit_list": self.formats.loaded, "formats": self.formats.version},
                "treatment": None if self.declaration is None else
                {"realisation": self.declaration.realisation, "status": self.declaration.status,
                 "constraint_inventory": self.declaration.constraint_inventory()}}
