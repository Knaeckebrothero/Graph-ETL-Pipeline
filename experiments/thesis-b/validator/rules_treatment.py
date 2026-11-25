"""The constraints a realisation declares, read from its `declaration.json` (treatment scope).

Thesis A writes one `declaration.json` per realisation (`schemas/treatments/<S>/<arm>/`, derive.py): node types with
their properties, relationship patterns with source and target, uniqueness constraints. The treatment-scope rules
follow from these declared constraints (agreement 3.2, Eval 2 card P.4); they run on items in `rules_write.py`.

What v5.0 declares and what it does not: labels, properties (from constraints, indexes and comments), relationship
patterns (from/to), uniqueness. No required properties, no datatypes, no cardinalities, no value lists (the
`level (1|2|3)` notes are kept by Thesis A as notes, not constraints). So TS-required, TS-datatype, TS-valuelist and
TS-cardinality are "n.a." for S3 and S4.

The induced schema (S2, Thesis A's Rule 6 of 23 Nov 2026) carries more, and the loader reads all of it: per node type
`identifier`, `required`, `datatypes` (property -> datatype) and `values` (property -> list); per relationship pattern
`at_most_one` and the same three keys for the pattern's properties (`properties` itself stays a list of names, or of
`{"name": ...}` objects as on a node type); and four more kinds in `enforcement` (`required_property_missing`,
`datatype_violation`, `value_outside_the_list`, `cardinality_violation`).

S1 (raw triples) declares nothing: every write passes the treatment scope. S2 (induced) is a placeholder until the
induction: the loader refuses it so that nothing is validated against an empty schema by accident.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path


def _names(items) -> set[str]:
    """A list of property names, given as strings or as `{"name": ...}` objects (both forms occur in Thesis A's files)."""
    out = set()
    for pr in items or []:
        if isinstance(pr, dict):
            if pr.get("name"):
                out.add(str(pr["name"]))
        elif pr is not None:
            out.add(str(pr))
    return out


def _constraints(d: dict) -> dict:
    """The three property constraints a node type or a pattern may carry."""
    return {"required": _names(d.get("required")),
            "datatypes": {str(k): v for k, v in (d.get("datatypes") or {}).items()},
            "values": {str(k): list(v) for k, v in (d.get("values") or {}).items()}}


@dataclass
class Declaration:
    realisation: str
    treatment: str
    arm: str
    status: str
    node_types: dict[str, dict]                     # label -> {properties: set, unique: set, required: set, datatypes: dict, values: dict, identifier, layer}
    patterns: dict[tuple[str, str, str], set[str]]  # (type, from, to) -> relationship property names
    rel_types: set[str]
    source: dict | None = None
    enforcement: dict | None = None                 # Thesis A's Rule 5 (schema treatments v0.2) and Rule 6 (S2): what a write that leaves the schema gets
    identifying: dict[str, str] | None = None       # node type -> property that identifies an entity (S1: Entity.name)
    pattern_rules: dict[tuple[str, str, str], dict] = field(default_factory=dict)  # (type, from, to) -> {at_most_one, required, datatypes, values}

    DEFAULT_ENFORCEMENT = {"node_type_outside_the_list": "reject", "relationship_outside_the_patterns": "reject",
                           "uniqueness_violation": "reject", "undeclared_property": "accept_and_count",
                           # Rule 6: a constraint the schema declares is enforced. S3 and S4 declare none of these.
                           "required_property_missing": "reject", "datatype_violation": "reject",
                           "value_outside_the_list": "reject", "cardinality_violation": "reject"}

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
            nodes[n["name"]] = {"properties": _names(n.get("properties")), "unique": _names(n.get("unique")),
                                **_constraints(n), "identifier": n.get("identifier") or None, "layer": n.get("layer")}
        patterns: dict[tuple[str, str, str], set[str]] = {}
        rules: dict[tuple[str, str, str], dict] = {}
        for pat in d.get("relationship_patterns", []):
            for to in pat.get("to", []):
                key = (pat["type"], pat["from"], to)
                patterns[key] = _names(pat.get("properties"))
                rules[key] = {"at_most_one": bool(pat.get("at_most_one", False)), **_constraints(pat)}
        return cls(realisation=d.get("realisation", p.parent.name), treatment=d.get("treatment", "?"), arm=d.get("arm", "?"),
                   status=d.get("status", "?"), node_types=nodes, patterns=patterns, pattern_rules=rules,
                   rel_types={k[0] for k in patterns}, source=d.get("derived_from"),
                   enforcement={**cls.DEFAULT_ENFORCEMENT, **(d.get("enforcement") or {})}, identifying=ident)

    def rejects(self, kind: str) -> bool:
        """Whether a write of this kind is rejected under the realisation's enforcement (Rules 5 and 6). Kinds:
        node_type_outside_the_list, relationship_outside_the_patterns, uniqueness_violation, undeclared_property,
        required_property_missing, datatype_violation, value_outside_the_list, cardinality_violation."""
        return (self.enforcement or self.DEFAULT_ENFORCEMENT).get(kind) == "reject"

    @property
    def declares_nothing(self) -> bool:
        return not self.node_types and not self.patterns

    @property
    def unique_by_type(self) -> dict[str, set[str]]:
        """Declared unique properties per type: what identifies an entity item of that type. Whether the declared
        identifier is among them is Thesis A's rule (derive.py `IDENTIFIER_IS_UNIQUE`), not this loader's."""
        return {t: set(n["unique"]) for t, n in self.node_types.items() if n["unique"]}

    @property
    def identifier_by_type(self) -> dict[str, str]:
        """The declared identifier per node type (Rule 6, S2 only): the property that must carry the identifying value."""
        return {t: n["identifier"] for t, n in self.node_types.items() if n.get("identifier")}

    def constraint_inventory(self) -> dict[str, int]:
        """Card P.4: constraint count by kind. Kinds v5.0 does not declare are 0 (n.a.). Node-type counts keep their
        names of 0.2.0; what a pattern carries (Rule 6) is counted under its own keys."""
        pr = self.pattern_rules.values()
        return {"label_and_type": len(self.node_types), "declared_property": sum(len(n["properties"]) for n in self.node_types.values()),
                "relationship_pattern": len({(t, f) for (t, f, _) in self.patterns}),   # as declared; a pattern with several targets counts once
                "relationship_pattern_expanded": len(self.patterns),
                "uniqueness": sum(len(n["unique"]) for n in self.node_types.values()),
                "identifier": sum(1 for n in self.node_types.values() if n.get("identifier")),
                "required_property": sum(len(n["required"]) for n in self.node_types.values()),
                "datatype": sum(len(n["datatypes"]) for n in self.node_types.values()),
                "cardinality": sum(1 for r in pr if r["at_most_one"]),
                "closed_value_list": sum(len(n["values"]) for n in self.node_types.values()),
                "relationship_property_required": sum(len(r["required"]) for r in pr),
                "relationship_property_datatype": sum(len(r["datatypes"]) for r in pr),
                "relationship_property_value_list": sum(len(r["values"]) for r in pr)}
