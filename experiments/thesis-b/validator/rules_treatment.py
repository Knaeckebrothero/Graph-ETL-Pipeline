"""Treatment-scope rules TS-01 to TS-05, read from a realisation's `declaration.json`.

Thesis A writes one `declaration.json` per realisation (`schemas/treatments/<S>/<arm>/`, derive.py): node types with
their properties, relationship patterns with source and target, uniqueness constraints. The treatment-scope rules
follow from these declared constraints (agreement 3.2, Eval 2 card P.4). They are checked on the write as rendered
into the schema, not on the canonical form, because the canonical form is schema-neutral by design.

A schema write is a dict:

    {"nodes": [{"label": "WasteItem", "key": "n1", "properties": {"uid": "...", "name": "..."}}],
     "relationships": [{"type": "ROUTES_TO", "from": "n1", "to": "n2", "properties": {}}]}

`key` names a node inside the write; `from`/`to` may be such a key, or an existing graph element given to the
Context as `known_nodes` {id: label}.

What v5.0 declares and what it does not: labels, properties (from constraints, indexes and comments), relationship
patterns (from/to), uniqueness. No required properties, no datatypes, no cardinalities, no value lists (the
`level (1|2|3)` notes are kept by Thesis A as notes, not constraints). So TS rules exist for the first four
constraint kinds only; the others are "n.a." for every realisation of draft 0.1, as card P.4 foresees.

S1 (raw triples) declares nothing: every write passes the treatment scope. S2 (induced) is a placeholder until the
induction: the loader refuses it so that nothing is validated against an empty schema by accident.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .rules_common import Rejection


@dataclass
class Declaration:
    realisation: str
    treatment: str
    arm: str
    status: str
    node_types: dict[str, dict]                     # label -> {properties: set, unique: set}
    patterns: dict[tuple[str, str, str], set[str]]  # (type, from, to) -> relationship property names
    rel_types: set[str]
    source: dict | None = None

    @classmethod
    def load(cls, path: str | Path) -> "Declaration":
        p = Path(path)
        if p.is_dir():
            p = p / "declaration.json"
        d = json.loads(p.read_text(encoding="utf-8"))
        if d.get("status") == "placeholder":
            raise ValueError(f"{p}: realisation {d.get('realisation')} is a placeholder (S2 before induction); nothing to validate against")
        nodes = {n["name"]: {"properties": {pr["name"] for pr in n.get("properties", [])}, "unique": set(n.get("unique", [])),
                             "layer": n.get("layer")} for n in d.get("node_types", [])}
        patterns: dict[tuple[str, str, str], set[str]] = {}
        for pat in d.get("relationship_patterns", []):
            for to in pat.get("to", []):
                patterns[(pat["type"], pat["from"], to)] = set(pat.get("properties", []))
        return cls(realisation=d.get("realisation", p.parent.name), treatment=d.get("treatment", "?"), arm=d.get("arm", "?"),
                   status=d.get("status", "?"), node_types=nodes, patterns=patterns,
                   rel_types={k[0] for k in patterns}, source=d.get("derived_from"))

    @property
    def declares_nothing(self) -> bool:
        return not self.node_types and not self.patterns

    def constraint_inventory(self) -> dict[str, int]:
        """Card P.4: constraint count by kind. Kinds v5.0 does not declare are 0 (n.a.)."""
        return {"label_and_type": len(self.node_types), "declared_property": sum(len(n["properties"]) for n in self.node_types.values()),
                "relationship_pattern": len({(t, f) for (t, f, _) in self.patterns}),   # as declared; a pattern with several targets counts once
                "relationship_pattern_expanded": len(self.patterns),
                "uniqueness": sum(len(n["unique"]) for n in self.node_types.values()),
                "required_property": 0, "datatype": 0, "cardinality": 0, "closed_value_list": 0}


@dataclass
class WriteContext:
    known_nodes: dict[str, str] = field(default_factory=dict)              # existing graph element id -> label
    unique_values: dict[tuple[str, str], set[Any]] = field(default_factory=dict)  # (label, property) -> values already in the graph


def _label_of(ref: str, write: dict, ctx: WriteContext) -> str | None:
    for n in write.get("nodes", []):
        if n.get("key") == ref:
            return n.get("label")
    return ctx.known_nodes.get(ref)


def ts01_label_declared(write: dict, decl: Declaration, ctx: WriteContext) -> list[Rejection]:
    out = []
    for n in write.get("nodes", []):
        if n.get("label") not in decl.node_types:
            out.append(Rejection("TS-01", f"node[{n.get('key')}].label", sorted(decl.node_types), n.get("label"),
                                 f"label {n.get('label')!r} is not declared in {decl.realisation}"))
    for r in write.get("relationships", []):
        if r.get("type") not in decl.rel_types:
            out.append(Rejection("TS-01", f"relationship.type", sorted(decl.rel_types), r.get("type"),
                                 f"relationship type {r.get('type')!r} is not declared in {decl.realisation}"))
    return out


def ts02_property_declared(write: dict, decl: Declaration, ctx: WriteContext) -> list[Rejection]:
    out = []
    for n in write.get("nodes", []):
        nt = decl.node_types.get(n.get("label"))
        if nt is None:
            continue
        for prop in (n.get("properties") or {}):
            if prop not in nt["properties"]:
                out.append(Rejection("TS-02", f"node[{n.get('key')}].{prop}", sorted(nt["properties"]), prop,
                                     f"property {prop!r} is not declared for {n.get('label')}"))
    return out


def ts03_pattern_allowed(write: dict, decl: Declaration, ctx: WriteContext) -> list[Rejection]:
    out = []
    for r in write.get("relationships", []):
        if r.get("type") not in decl.rel_types:
            continue  # TS-01 reports it
        frm, to = _label_of(r.get("from"), write, ctx), _label_of(r.get("to"), write, ctx)
        if frm is None or to is None:
            out.append(Rejection("TS-03", f"relationship[{r.get('type')}]", "from and to resolve to a node of this write or of the graph",
                                 {"from": r.get("from"), "to": r.get("to")}, "relationship endpoint does not resolve"))
            continue
        if (r["type"], frm, to) not in decl.patterns:
            allowed = sorted(f"({f})-[:{t}]->({o})" for (t, f, o) in decl.patterns if t == r["type"])
            out.append(Rejection("TS-03", f"relationship[{r['type']}]", allowed, f"({frm})-[:{r['type']}]->({to})",
                                 f"pattern ({frm})-[:{r['type']}]->({to}) is not declared"))
        else:
            allowed_props = decl.patterns[(r["type"], frm, to)]
            for prop in (r.get("properties") or {}):
                if prop not in allowed_props:
                    out.append(Rejection("TS-05", f"relationship[{r['type']}].{prop}", sorted(allowed_props), prop,
                                         f"relationship property {prop!r} is not declared on {r['type']}"))
    return out


def ts04_unique(write: dict, decl: Declaration, ctx: WriteContext) -> list[Rejection]:
    out = []
    seen: dict[tuple[str, str], set[Any]] = {}
    for n in write.get("nodes", []):
        nt = decl.node_types.get(n.get("label"))
        if nt is None:
            continue
        for prop in nt["unique"]:
            v = (n.get("properties") or {}).get(prop)
            if v is None:
                continue
            k = (n["label"], prop)
            bucket = seen.setdefault(k, set())
            if v in ctx.unique_values.get(k, set()) or v in bucket:
                out.append(Rejection("TS-04", f"node[{n.get('key')}].{prop}", f"a value unique among {n['label']}.{prop}", v,
                                     f"{n['label']}.{prop} = {v!r} already exists"))
            bucket.add(v)
    return out


TREATMENT_RULES = {
    "TS-01": ("label and relationship type declared", ts01_label_declared),
    "TS-02": ("node property declared", ts02_property_declared),
    "TS-03": ("relationship pattern (from, type, to) declared; TS-05 relationship property declared", ts03_pattern_allowed),
    "TS-04": ("uniqueness constraint", ts04_unique),
}


def run_treatment(write: dict, decl: Declaration, ctx: WriteContext | None = None) -> list[Rejection]:
    if decl.declares_nothing:
        return []
    ctx = ctx or WriteContext()
    out: list[Rejection] = []
    for _, fn in TREATMENT_RULES.values():
        out.extend(fn(write, decl, ctx))
    return out
