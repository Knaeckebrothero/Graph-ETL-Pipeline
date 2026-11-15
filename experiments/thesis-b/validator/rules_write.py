"""Write-time rules on items (Thesis A's common-scope rule list v0.2, sections 3 to 5, and the treatment scope).

Tested at the write: CS1, CS3, CS4 (list section 8, point 1); CS0 when the write cannot be read at all, which Thesis A's
database tool reports itself (a statement the store cannot run). CS2, CS5, CS6 and CS7 are never tested at the write:
CS2 because the message would show the agent the benchmark's vocabulary (BE-10), CS5 because without the mapping the
validator cannot know which attribute is a qualifier, CS6 because the pointer is the log chain, CS7 because a repeated
write is answered "already present" by the tool and is no rejection.

Treatment scope: the constraints the realisation declares (`declaration.json`), with Thesis A's ids `TS-type`,
`TS-required`, `TS-datatype`, `TS-unique`, `TS-cardinality`, `TS-endpoint`, `TS-valuelist`, under the realisation's
enforcement (schema treatments v0.2, Rule 5): a label outside the list, a relationship whose type and end types match
no pattern, and a uniqueness violation are rejected; a property the schema does not name is **accepted and counted**
(an observation `TS-property`, never a fault). S1 declares nothing.

Order (list section 4, CS4): common-scope rules first; an item that fails one is not tested against the schema's rules
as well. Messages (section 5): every fault of a readable item in one message; nothing from the benchmark.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable

from .items import Attribute, Diff, EntityItem, RelationItem, control_chars, query_intent
from .rules_common import Rejection
from .rules_treatment import Declaration
from .vocabulary import FormatTable


@dataclass
class WriteContext:
    """What the validator may know about the graph at the write, supplied by the tool; never queried by the validator.
    `lookup` is Thesis A's `lookup.find(label, property, value) -> [element ids]` of the nodes that were in the graph
    before the statement."""
    formats: FormatTable = field(default_factory=FormatTable)
    lookup: Any = None
    identifying: dict[str, str] | None = None               # declaration.json identifying_value: S1 names Entity.name; other S1 node types (Assertion) are reifications, not entities
    entity_exists: Callable[[str], int] | None = None       # ref -> how many entities match (rdf ends outside the diff)
    known_entity_ids: set[str] = field(default_factory=set)  # or a plain set of ids already in the graph
    unique_values: dict[tuple[str, str], set[Any]] = field(default_factory=dict)  # (type, property) -> values in the graph

    def find(self, label: str | None, prop: str, value: Any) -> list[str] | None:
        """Ids of pre-existing nodes with that label and value; None when the tool gave no way to know."""
        if self.lookup is not None:
            return list(self.lookup.find(label, prop, value))
        if self.unique_values:
            return ["?"] if value in self.unique_values.get((label, prop), set()) else []
        return None


@dataclass
class Observation:
    """Something counted, not rejected (Rule 5: a property the schema does not name)."""
    kind: str
    element: str
    found: Any
    note: str

    def to_log(self) -> dict:
        return {"kind": self.kind, "element": self.element, "found": self.found, "note": self.note}


def _blank(v: Any) -> bool:
    return v is None or (isinstance(v, str) and v.strip() == "")


def _el(item: EntityItem | RelationItem, what: str) -> str:
    """The `element` of a message: names the item by its position in the write, not by anything from the benchmark."""
    head = f"{item.kind}:{item.ref}" if item.ref else item.kind
    return f"{head}/{what}" if what else head


def item_key(it: EntityItem | RelationItem) -> str:
    return it.ref or (f"{it.start.ref}-{it.type}->{it.end.ref}" if isinstance(it, RelationItem) else "?")


# ----------------------------------------------------------------------------- CS1 well formed, at the write
def cs1_write(item: EntityItem | RelationItem, ctx: WriteContext) -> list[Rejection]:
    out: list[Rejection] = []
    texts: list[tuple[str, str]] = []
    if isinstance(item, EntityItem):
        for t in item.types:
            if _blank(t):
                out.append(Rejection("CS1", _el(item, "type"), "non-empty type", t, "empty type"))
            else:
                texts.append(("type", t))
        needs_identifier = not ctx.identifying or not item.types or any(t in ctx.identifying for t in item.types)
        if needs_identifier and _blank(item.identifier):
            out.append(Rejection("CS1", _el(item, "identifier"), "an identifying value (key or name)", item.identifier,
                                 "entity without an identifying value"))
    else:
        if _blank(item.type):
            out.append(Rejection("CS1", _el(item, "type"), "non-empty relation type", item.type, "empty relation type"))
        else:
            texts.append(("type", item.type))
        for side, end in (("from", item.start), ("to", item.end)):
            if _blank(end.ref):
                out.append(Rejection("CS1", _el(item, side), "a reference to an entity", end.ref, f"relation with a missing {side} end"))
    for a in item.attributes:
        if _blank(a.name):
            out.append(Rejection("CS1", _el(item, "attribute"), "non-empty attribute name", a.name, "empty attribute name"))
        else:
            texts.append(("attribute name", a.name))
        empty = len(a.value) == 0 if isinstance(a.value, (list, tuple)) else _blank(a.value)
        if empty:
            out.append(Rejection("CS1", _el(item, f"attribute:{a.name}"), "non-empty value", a.value, f"attribute {a.name!r} is empty or white space"))
        elif isinstance(a.value, str):
            texts.append((f"attribute:{a.name}", a.value))
    for where, s in texts:
        bad = control_chars(s)
        if bad:
            out.append(Rejection("CS1", _el(item, where), "text without control characters (tab and line break allowed)", bad[:5],
                                 f"{where} holds control or invalid characters"))
    return out


# ----------------------------------------------------------------------------- CS3 no dangling reference, at the write
def cs3_write(item: EntityItem | RelationItem, ctx: WriteContext, diff: Diff) -> list[Rejection]:
    """An end of a relation must resolve to exactly one entity, in the graph or earlier in the same write. In the
    property graph the engine resolved the ends before the diff exists (Thesis A's `changes.ends` names them), so a
    relation in the diff has its ends; what the diff cannot show, an end that matched nothing or several, is read from
    an empty or multiplied diff in `diff_level`. In RDF an IRI object may name nothing: the end must be in the diff or
    known to the graph."""
    if not isinstance(item, RelationItem):
        return []
    out = []
    for side, end in (("from", item.start), ("to", item.end)):
        if _blank(end.ref):
            continue                                     # CS1 reports the missing end
        if end.in_diff or end.types:                     # created here, or the tool named it under `ends`: it exists
            continue
        if ctx.entity_exists is not None:
            n = ctx.entity_exists(end.ref)
            if n != 1:
                out.append(Rejection("CS3", _el(item, side), "exactly one existing entity", f"{n} matches for {end.ref}",
                                     f"{side} end resolves to {n} entities"))
        elif ctx.known_entity_ids:
            if end.ref not in ctx.known_entity_ids:
                out.append(Rejection("CS3", _el(item, side), "exactly one existing entity", f"0 matches for {end.ref}",
                                     f"{side} end names no entity of the graph"))
        elif diff.arm == "rdf":
            out.append(Rejection("CS3", _el(item, side), "an entity of the graph", end.ref,
                                 "end outside this write; the tool gave no graph lookup, so the rule is not checkable", status="not_checkable"))
    return out


def diff_level(diff: Diff) -> tuple[str | None, list[Rejection]]:
    """What an empty or multiplied diff means. Returns (outcome, rejections): outcome "already_present" when the tool
    says so or a MERGE-only query produced nothing; a CS3 rejection when a CREATE/INSERT produced nothing (its MATCH
    found no entity) or one relationship pattern produced several relations (its MATCH found several). Thesis A's tool
    answers "already present" itself and never calls the checker on an empty diff; this is for the other adapters."""
    intent = query_intent(diff.query)
    if diff.empty:
        if diff.existing_ids or intent["merge_only"]:
            return "already_present", []
        if intent["writes"]:
            return None, [Rejection("CS3", "relation/to", "exactly one existing entity", "0 matches",
                                    "the write produced nothing: an end of the relation matched no entity")]
        if intent["writes"] is None:
            return None, [Rejection("CS3", "write", "a write that produced something, or the query text to read why not", "empty diff, no query",
                                    "empty diff and no query text: not checkable", status="not_checkable")]
        return "nothing_written", []
    rp = intent["relationship_patterns"]
    if rp == 1 and len(diff.relations) > 1 and len({r.type for r in diff.relations}) == 1:
        return None, [Rejection("CS3", f"relation:{diff.relations[0].type}", "exactly one existing entity per end",
                                f"{len(diff.relations)} relations from one pattern", "an end of the relation matched several entities")]
    return None, []


# ----------------------------------------------------------------------------- CS4 datatypes and units parse, at the write
def cs4_write(item: EntityItem | RelationItem, ctx: WriteContext) -> list[Rejection]:
    """A value given with a datatype must be a valid form of it under the format and unit table. A value without a
    datatype is text and passes. Key-like values are not tested here (list section 4, boundary)."""
    out = []
    for a in item.attributes:
        if a.datatype is None or _blank(a.value):
            continue
        expected = ctx.formats.check(a.value, a.datatype)
        if expected is not None:
            out.append(Rejection("CS4", _el(item, f"attribute:{a.name}"), expected, a.value, f"value is not a valid {a.datatype}"))
        if a.datatype == "quantity" or a.unit:
            status = ctx.formats.unit_status(a.unit)
            if status == "unknown":
                out.append(Rejection("CS4", _el(item, f"attribute:{a.name}/unit"), "a unit of the format and unit table", a.unit, f"unknown unit {a.unit!r}"))
            elif status == "not_checkable":
                out.append(Rejection("CS4", _el(item, f"attribute:{a.name}/unit"), "format and unit table", a.unit,
                                     "no unit table yet (Thesis A, 23 Nov); unit not checkable", status="not_checkable"))
    return out


WRITE_COMMON_RULES = {
    "CS1": ("well formed", cs1_write),
    "CS3": ("no dangling reference", cs3_write),
    "CS4": ("datatypes and units parse", cs4_write),
}


def run_write_common(item: EntityItem | RelationItem, ctx: WriteContext, diff: Diff) -> list[Rejection]:
    out = cs1_write(item, ctx)
    out += cs3_write(item, ctx, diff)
    out += cs4_write(item, ctx)
    return out


# ----------------------------------------------------------------------------- treatment scope on items
def ts_entity(item: EntityItem, decl: Declaration, ctx: WriteContext, seen_unique: dict[tuple[str, str], set[Any]],
              obs: list[Observation]) -> list[Rejection]:
    out = []
    declared = {}
    for t in item.types:
        nt = decl.node_types.get(t)
        if nt is None:
            if decl.rejects("node_type_outside_the_list"):
                out.append(Rejection("TS-type", _el(item, "type"), sorted(decl.node_types), t, f"type {t!r} is not declared by the schema"))
            else:
                obs.append(Observation("TS-type", _el(item, "type"), t, "type outside the list; accepted under the realisation's enforcement"))
        else:
            declared[t] = nt
    for a in item.attributes:
        if declared and not any(a.name in nt["properties"] for nt in declared.values()):
            allowed = sorted({p for nt in declared.values() for p in nt["properties"]})
            if decl.rejects("undeclared_property"):
                out.append(Rejection("TS-property", _el(item, f"attribute:{a.name}"), allowed, a.name, f"property {a.name!r} is not declared for {'/'.join(declared)}"))
            else:
                obs.append(Observation("TS-property", _el(item, f"attribute:{a.name}"), a.name, f"property not named by the schema for {'/'.join(declared)}; accepted and counted (Rule 5)"))
    for t, nt in declared.items():
        for prop in nt.get("required", ()):
            if not any(a.name == prop and not _blank(a.value) for a in item.attributes):
                out.append(Rejection("TS-required", _el(item, f"attribute:{prop}"), f"{t}.{prop} present", None, f"required property {prop!r} missing"))
        for prop, dt in (nt.get("datatypes") or {}).items():
            for a in item.attributes:
                if a.name == prop and a.datatype not in (None, dt):
                    out.append(Rejection("TS-datatype", _el(item, f"attribute:{prop}"), dt, a.datatype, f"{t}.{prop} declared as {dt}"))
        for prop, values in (nt.get("values") or {}).items():
            for a in item.attributes:
                if a.name == prop and a.value not in values:
                    out.append(Rejection("TS-valuelist", _el(item, f"attribute:{prop}"), sorted(values), a.value, f"{t}.{prop} must be one of the declared values"))
        if not decl.rejects("uniqueness_violation"):
            continue
        for prop in nt["unique"]:
            v = next((a.value for a in item.attributes if a.name == prop), None)
            if _blank(v):
                continue
            k = (t, prop)
            bucket = seen_unique.setdefault(k, set())
            before = ctx.find(t, prop, v)
            others = [i for i in (before or []) if i != item.ref]       # a changed node keeping its own value is no violation
            if others or v in bucket:
                out.append(Rejection("TS-unique", _el(item, f"attribute:{prop}"), f"a value unique among {t}.{prop}", v, f"{t}.{prop} = {v!r} already exists"))
            bucket.add(v)
    return out


def ts_relation(item: RelationItem, decl: Declaration, ctx: WriteContext, obs: list[Observation]) -> list[Rejection]:
    out = []
    reject_rel = decl.rejects("relationship_outside_the_patterns")
    if item.type not in decl.rel_types:
        if reject_rel:
            out.append(Rejection("TS-type", _el(item, "type"), sorted(decl.rel_types), item.type, f"relation type {item.type!r} is not declared by the schema"))
        else:
            obs.append(Observation("TS-type", _el(item, "type"), item.type, "relation type outside the list; accepted under the realisation's enforcement"))
        return out
    pairs = [(f, t) for f in (item.start.types or [None]) for t in (item.end.types or [None])]
    matched = [(item.type, f, t) for (f, t) in pairs if (item.type, f, t) in decl.patterns]
    if not matched:
        if not item.start.types or not item.end.types:
            out.append(Rejection("TS-endpoint", _el(item, "ends"), "ends with a declared type", {"from": item.start.types, "to": item.end.types},
                                 "an end has no type, so the pattern cannot be checked", status="not_checkable"))
        elif reject_rel:
            allowed = sorted(f"({f})-[:{t}]->({o})" for (t, f, o) in decl.patterns if t == item.type)
            found = f"({'/'.join(item.start.types)})-[:{item.type}]->({'/'.join(item.end.types)})"
            out.append(Rejection("TS-endpoint", _el(item, "ends"), allowed, found, f"pattern {found} is not declared"))
        else:
            obs.append(Observation("TS-endpoint", _el(item, "ends"), item.type, "pattern not declared; accepted under the realisation's enforcement"))
        return out
    allowed_props = set().union(*(decl.patterns[m] for m in matched))
    for a in item.attributes:
        if a.name not in allowed_props:
            if decl.rejects("undeclared_property"):
                out.append(Rejection("TS-property", _el(item, f"attribute:{a.name}"), sorted(allowed_props), a.name,
                                     f"relation property {a.name!r} is not declared on {item.type}"))
            else:
                obs.append(Observation("TS-property", _el(item, f"attribute:{a.name}"), a.name, f"relation property not named by the schema on {item.type}; accepted and counted (Rule 5)"))
    return out


TREATMENT_RULE_IDS = ["TS-type", "TS-required", "TS-datatype", "TS-unique", "TS-cardinality", "TS-endpoint", "TS-valuelist"]


def run_treatment_items(diff: Diff, decl: Declaration, ctx: WriteContext, skip: set[str]) -> tuple[dict[str, list[Rejection]], list[Observation]]:
    """Treatment scope per item key; items in `skip` (failed a common-scope rule) are not tested (list section 4).
    Returns (faults by item, observations)."""
    obs: list[Observation] = []
    if decl.declares_nothing:
        return {}, obs
    out: dict[str, list[Rejection]] = {}
    seen: dict[tuple[str, str], set[Any]] = {}
    for e in diff.entities:
        if e.ref in skip:
            continue
        r = ts_entity(e, decl, ctx, seen, obs)
        if r:
            out[e.ref] = r
    for rel in diff.relations:
        key = item_key(rel)
        if key in skip:
            continue
        r = ts_relation(rel, decl, ctx, obs)
        if r:
            out[key] = r
    return out, obs
