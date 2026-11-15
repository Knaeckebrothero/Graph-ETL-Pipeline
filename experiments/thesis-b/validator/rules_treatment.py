"""The constraints a realisation declares, read from its `declaration.json` (treatment scope).

Thesis A writes one `declaration.json` per realisation (`schemas/treatments/<S>/<arm>/`, derive.py): node types with
their properties, relationship patterns with source and target, uniqueness constraints. The treatment-scope rules
follow from these declared constraints (agreement 3.2, Eval 2 card P.4); they run on items in `rules_write.py`.

What v5.0 declares and what it does not: labels, properties (from constraints, indexes and comments), relationship
patterns (from/to), uniqueness. No required properties, no datatypes, no cardinalities, no value lists (the
`level (1|2|3)` notes are kept by Thesis A as notes, not constraints). The loader reads `required`, `datatypes`
(property -> datatype) and `values` (property -> list) per node type when a later declaration carries them, so the
rules TS-required, TS-datatype and TS-valuelist exist but are "n.a." for every realisation of draft 0.1.

S1 (raw triples) declares nothing: every write passes the treatment scope. S2 (induced) is a placeholder until the
induction: the loader refuses it so that nothing is validated against an empty schema by accident.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class Declaration:
    realisation: str
    treatment: str
    arm: str
    status: str
    node_types: dict[str, dict]                     # label -> {properties: set, unique: set, required: set, datatypes: dict, values: dict}
    patterns: dict[tuple[str, str, str], set[str]]  # (type, from, to) -> relationship property names
    rel_types: set[str]
    source: dict | None = None
    enforcement: dict | None = None                 # Thesis A's Rule 5 (schema treatments v0.2): what a write that leaves the schema gets
    identifying: dict[str, str] | None = None       # node type -> property that identifies an entity (S1: Entity.name)

    DEFAULT_ENFORCEMENT = {"node_type_outside_the_list": "reject", "relationship_outside_the_patterns": "reject",
                           "uniqueness_violation": "reject", "undeclared_property": "accept_and_count"}

    @classmethod
    def load(cls, path: str | Path, *, allow_placeholder: bool = False) -> "Declaration":
        p = Path(path)
        if p.is_dir():
            p = p / "declaration.json"
        d = json.loads(p.read_text(encoding="utf-8"))
        if d.get("status") == "placeholder" and not allow_placeholder:
            raise ValueError(f"{p}: realisation {d.get('realisation')} is a placeholder (S2 before induction); nothing to validate against")
        ident = {}
        iv = d.get("identifying_value") or {}
        if iv.get("node_type") and iv.get("property"):
            ident[iv["node_type"]] = iv["property"]
        nodes = {}
        for n in d.get("node_types", []):
            nodes[n["name"]] = {"properties": {pr["name"] if isinstance(pr, dict) else pr for pr in n.get("properties", [])},
                                "unique": set(n.get("unique", [])), "required": set(n.get("required", [])),
                                "datatypes": dict(n.get("datatypes") or {}), "values": {k: set(v) for k, v in (n.get("values") or {}).items()},
                                "layer": n.get("layer")}
        patterns: dict[tuple[str, str, str], set[str]] = {}
        for pat in d.get("relationship_patterns", []):
            for to in pat.get("to", []):
                patterns[(pat["type"], pat["from"], to)] = set(pat.get("properties", []))
        return cls(realisation=d.get("realisation", p.parent.name), treatment=d.get("treatment", "?"), arm=d.get("arm", "?"),
                   status=d.get("status", "?"), node_types=nodes, patterns=patterns,
                   rel_types={k[0] for k in patterns}, source=d.get("derived_from"),
                   enforcement={**cls.DEFAULT_ENFORCEMENT, **(d.get("enforcement") or {})}, identifying=ident)

    def rejects(self, kind: str) -> bool:
        """Whether a write of this kind is rejected under the realisation's enforcement (Rule 5). Kinds:
        node_type_outside_the_list, relationship_outside_the_patterns, uniqueness_violation, undeclared_property."""
        return (self.enforcement or self.DEFAULT_ENFORCEMENT).get(kind) == "reject"

    @property
    def declares_nothing(self) -> bool:
        return not self.node_types and not self.patterns

    @property
    def unique_by_type(self) -> dict[str, set[str]]:
        """Declared unique properties per type: what identifies an entity item of that type."""
        return {t: set(n["unique"]) for t, n in self.node_types.items() if n["unique"]}

    def constraint_inventory(self) -> dict[str, int]:
        """Card P.4: constraint count by kind. Kinds v5.0 does not declare are 0 (n.a.)."""
        return {"label_and_type": len(self.node_types), "declared_property": sum(len(n["properties"]) for n in self.node_types.values()),
                "relationship_pattern": len({(t, f) for (t, f, _) in self.patterns}),   # as declared; a pattern with several targets counts once
                "relationship_pattern_expanded": len(self.patterns),
                "uniqueness": sum(len(n["unique"]) for n in self.node_types.values()),
                "required_property": sum(len(n["required"]) for n in self.node_types.values()),
                "datatype": sum(len(n["datatypes"]) for n in self.node_types.values()),
                "cardinality": 0,
                "closed_value_list": sum(len(n["values"]) for n in self.node_types.values())}
