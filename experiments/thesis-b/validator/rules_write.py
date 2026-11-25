"""Write-time rules on items (Thesis A's common-scope rule list v0.2, sections 3 to 5, and the treatment scope).

Tested at the write: CS1, CS3, CS4 (list section 8, point 1); CS0 when the write cannot be read at all, which Thesis A's
database tool reports itself (a statement the store cannot run). CS2, CS5, CS6 and CS7 are never tested at the write:
CS2 because the message would show the agent the benchmark's vocabulary (BE-10), CS5 because without the mapping the
validator cannot know which attribute is a qualifier, CS6 because the pointer is the log chain, CS7 because a repeated
write is answered "already present" by the tool and is no rejection.

Treatment scope: the constraints the realisation declares (`declaration.json`), with Thesis A's ids `TS-type`,
`TS-required`, `TS-datatype`, `TS-unique`, `TS-cardinality`, `TS-endpoint`, `TS-valuelist`, under the realisation's
enforcement (schema treatments v0.2 Rule 5, and Rule 6 for the induced schema): a label outside the list, a
relationship whose type and end types match no pattern, and a uniqueness violation are rejected; a property the schema
does not name is **accepted and counted** (an observation `TS-property`, never a fault); a missing required property,
a value of another datatype, a value outside its list and a second relationship where the pattern says "at most one"
are rejected where the declaration says so. The same three property constraints hold for the properties of a
relationship (an annotation in RDF). S1 declares nothing.

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
    before the statement. `entity_exists(ref) -> 0 | 1` is the RDF cell's answer whether a reference outside the write
    names a node of the graph (counting what the write itself adds). `relation_count(ref, type) -> int` is the number
    of relationships of that type that leave the node `ref` and were in the graph before the statement (asked of
    Thesis A on 25 Nov 2026 for TS-cardinality; until it exists the rule is `not_checkable` beyond the write itself)."""
    formats: FormatTable = field(default_factory=FormatTable)
    lookup: Any = None
    identifying: dict[str, str] | None = None               # declaration.json identifying_value: S1 names Entity.name; other S1 node types (Assertion) are reifications, not entities
    entity_exists: Callable[[str], int] | None = None       # ref -> how many entities match (rdf ends outside the diff)
    relation_count: Callable[[str, str], int] | None = None  # (start ref, relationship type) -> how many such relationships leave it already
    known_entity_ids: set[str] = field(default_factory=set)  # or a plain set of ids already in the graph
    unique_values: dict[tuple[str, str], set[Any]] = field(default_factory=dict)  # (type, property) -> values in the graph

    @classmethod
    def from_lookup(cls, lookup: Any, formats: FormatTable | None = None, **more: Any) -> "WriteContext":
        """Everything Thesis A's lookup object offers, passed on: `find` always, `entity_exists` and `relation_count`
        when the cell has them (getattr, so a cell without one leaves the rule `not_checkable`)."""
        return cls(formats=formats or FormatTable(), lookup=lookup,
                   entity_exists=getattr(lookup, "entity_exists", None) if lookup is not None else None,
                   relation_count=getattr(lookup, "relation_count", None) if lookup is not None else None, **more)

    def find(self, label: str | None, prop: str, value: Any) -> list[str] | None:
        """Ids of pre-existing nodes with that label and value; None when the tool gave no way to know."""
        if self.lookup is not None:
            return list(self.lookup.find(label, prop, value))
        if self.unique_values:
            return ["?"] if value in self.unique_values.get((label, prop), set()) else []
        return None


@dataclass
class Observation:
    """Something counted, not rejected (Rule 5: a property the schema does not name; or a constraint whose
    enforcement the realisation sets to accept_and_count)."""
    kind: str
    element: str
    found: Any
    note: str

    def to_log(self) -> dict:
        return {"kind": self.kind, "element": self.element, "found": self.found, "note": self.note}


def _blank(v: Any) -> bool:
    return v is None or (isinstance(v, str) and v.strip() == "") or (isinstance(v, (list, tuple)) and len(v) == 0)


def _el(item: EntityItem | RelationItem, what: str) -> str:
    """The `element` of a message: names the item by its position in the write, not by anything from the benchmark."""
    head = f"{item.kind}:{item.ref}" if item.ref else item.kind
    return f"{head}/{what}" if what else head


def item_key(it: EntityItem | RelationItem) -> str:
    return it.ref or (f"{it.start.ref}-{it.type}->{it.end.ref}" if isinstance(it, RelationItem) else "?")


def _values(a: Attribute) -> list[Any]:
    """The values of an attribute: one, or several for a property with several values."""
    return list(a.value) if isinstance(a.value, (list, tuple)) else [a.value]


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
        if _blank(a.value):
            out.append(Rejection("CS1", _el(item, f"attribute:{a.name}"), "non-empty value", a.value, f"attribute {a.name!r} is empty or white space"))
        else:
            for v in _values(a):
                if isinstance(v, str):
                    texts.append((f"attribute:{a.name}", v))
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
    an empty or multiplied diff in `diff_level`. In RDF an IRI object may name nothing: the end must be in the diff,
    answered by the tool (`ends[..].exists`, 24 Nov 2026), or found through `entity_exists`."""
    if not isinstance(item, RelationItem):
        return []
    out = []
    for side, end in (("from", item.start), ("to", item.end)):
        if _blank(end.ref):
            continue                                     # CS1 reports the missing end
        if end.in_diff or end.types:                     # created here, or the tool named it under `ends`: it exists
            continue
        if end.exists is not None:                       # the tool's own answer
            if not end.exists:
                out.append(Rejection("CS3", _el(item, side), "exactly one existing entity", f"0 matches for {end.ref}",
                                     f"{side} end names no entity of the graph"))
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
        for v in _values(a):
            expected = ctx.formats.check(v, a.datatype)
            if expected is not None:
                out.append(Rejection("CS4", _el(item, f"attribute:{a.name}"), expected, v, f"value is not a valid {a.datatype}"))
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


# ----------------------------------------------------------------------------- treatment scope: the three property constraints
# A typed value counts as a valid form of the declared datatype when its own datatype is the declared one or a
# narrower number: a whole number is a decimal, and both are quantities (a number without unit). Decided 25 Nov 2026
# after the pilot (volume_litres: 120 rejected, 120.0 accepted where the schema said decimal).
VALID_FORMS = {"integer": {"integer"}, "decimal": {"integer", "decimal"}, "quantity": {"integer", "decimal", "quantity"},
               "boolean": {"boolean"}, "date": {"date"}, "datetime": {"datetime"}, "duration": {"duration"}}


def datatype_fault(a: Attribute, declared: str | None, formats: FormatTable) -> tuple[Any, Any] | None:
    """(expected, found) when the attribute is not a valid form of the declared datatype, else None.
    A typed value is compared by datatype (VALID_FORMS). A value that arrives as text is parsed against the declared
    datatype with the format table (in the property graph every date arrives as text; until Thesis A's format and unit
    table the formats are Thesis B's provisional ones: ISO 8601, point or comma decimals, yes/no words). `string`
    accepts text only: a number stored where the schema says text is another datatype."""
    if declared is None:
        return None
    if declared == "string":
        return (declared, a.datatype) if a.datatype not in (None, "string") else None
    if a.datatype is not None:
        return None if a.datatype in VALID_FORMS.get(declared, {declared}) else (declared, a.datatype)
    for v in _values(a):
        if _blank(v):
            continue
        hint = formats.check(v, declared)
        if hint is not None:
            return f"{declared} ({hint})", v
    return None


def _as_number(x: Any) -> float | None:
    if isinstance(x, bool):
        return None
    if isinstance(x, (int, float)):
        return float(x)
    if isinstance(x, str):
        try:
            return float(x.strip().replace(",", "."))
        except ValueError:
            return None
    return None


def same_value(v: Any, w: Any) -> bool:
    """Equality for a value list: exact, or the same number written differently (RDF hands a typed number over in its
    written form, "2", where the list holds 2), or the same boolean word."""
    if isinstance(v, bool) or isinstance(w, bool):
        return v is w or str(v).strip().lower() == str(w).strip().lower()
    if v == w:
        return True
    nv, nw = _as_number(v), _as_number(w)
    return nv is not None and nw is not None and nv == nw


def outside_list(a: Attribute, values: list[Any]) -> list[Any]:
    """The values of the attribute that are in none of the declared values. With several values every one must be in
    the list (decided 25 Nov 2026; 0.2.0 raised TypeError here)."""
    return [v for v in _values(a) if not any(same_value(v, w) for w in values)]


def _emit(decl: Declaration, kind: str, rej: Rejection, out: list[Rejection], obs: list[Observation], note: str) -> None:
    """Reject under the realisation's enforcement, else count (accept_and_count, nothing_declared)."""
    if decl.rejects(kind):
        out.append(rej)
    else:
        obs.append(Observation(rej.rule_id, rej.element, rej.found, note))


def property_rules(owner: str, item: EntityItem | RelationItem, spec: dict, decl: Declaration, ctx: WriteContext,
                   out: list[Rejection], obs: list[Observation], *, required_also: set[str] = frozenset()) -> None:
    """TS-required, TS-datatype and TS-valuelist for the properties of one owner (a node type, or the matched pattern
    of a relationship), from `spec` = {required, datatypes, values}. `required_also` adds names that must be present
    for another reason (the declared identifier)."""
    attrs = item.attributes
    for prop in sorted(set(spec.get("required") or ()) | set(required_also)):
        if not any(a.name == prop and not _blank(a.value) for a in attrs):
            why = "identifier" if prop in required_also and prop not in (spec.get("required") or ()) else "required property"
            _emit(decl, "required_property_missing",
                  Rejection("TS-required", _el(item, f"attribute:{prop}"), f"{owner}.{prop} present", None, f"{why} {prop!r} missing"),
                  out, obs, f"{why} {prop!r} missing; accepted under the realisation's enforcement")
    for prop, dt in (spec.get("datatypes") or {}).items():
        for a in attrs:
            if a.name != prop or _blank(a.value):
                continue
            fault = datatype_fault(a, dt, ctx.formats)
            if fault is not None:
                _emit(decl, "datatype_violation",
                      Rejection("TS-datatype", _el(item, f"attribute:{prop}"), fault[0], fault[1], f"{owner}.{prop} declared as {dt}"),
                      out, obs, f"{owner}.{prop} declared as {dt}; accepted under the realisation's enforcement")
    for prop, values in (spec.get("values") or {}).items():
        for a in attrs:
            if a.name != prop or _blank(a.value):
                continue
            off = outside_list(a, values)
            if off:
                found = off[0] if len(off) == 1 else off
                _emit(decl, "value_outside_the_list",
                      Rejection("TS-valuelist", _el(item, f"attribute:{prop}"), sorted(values, key=str), found, f"{owner}.{prop} must be one of the declared values"),
                      out, obs, f"{owner}.{prop} outside the declared values; accepted under the realisation's enforcement")


# ----------------------------------------------------------------------------- treatment scope on items
def ts_entity(item: EntityItem, decl: Declaration, ctx: WriteContext, seen_unique: dict[tuple[str, str], set[Any]],
              obs: list[Observation]) -> list[Rejection]:
    out: list[Rejection] = []
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
        ident = {nt["identifier"]} if nt.get("identifier") else set()
        property_rules(t, item, nt, decl, ctx, out, obs, required_also=ident)
        if not decl.rejects("uniqueness_violation"):
            continue
        for prop in sorted(nt["unique"]):
            v = next((a.value for a in item.attributes if a.name == prop), None)
            if _blank(v):
                continue
            k = (t, prop)
            bucket = seen_unique.setdefault(k, set())
            before = ctx.find(t, prop, v)
            others = [i for i in (before or []) if i != item.ref]       # a changed node keeping its own value is no violation
            marker = repr(v)                                            # a list value is not hashable; its repr is
            if others or marker in bucket:
                out.append(Rejection("TS-unique", _el(item, f"attribute:{prop}"), f"a value unique among {t}.{prop}", v, f"{t}.{prop} = {v!r} already exists"))
            bucket.add(marker)
    return out


def ts_relation(item: RelationItem, decl: Declaration, ctx: WriteContext, obs: list[Observation], diff: Diff | None = None) -> list[Rejection]:
    out: list[Rejection] = []
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
    # the constraints of the matched pattern(s) on the relationship's own properties (Rule 6)
    spec = {"required": set(), "datatypes": {}, "values": {}}
    at_most_one = False
    for m in matched:
        r = decl.pattern_rules.get(m) or {}
        spec["required"] |= set(r.get("required") or ())
        spec["datatypes"].update(r.get("datatypes") or {})
        spec["values"].update(r.get("values") or {})
        at_most_one = at_most_one or bool(r.get("at_most_one"))
    pattern_name = f"({'/'.join(item.start.types)})-[:{item.type}]->({'/'.join(item.end.types)})"
    property_rules(item.type, item, spec, decl, ctx, out, obs)
    if at_most_one:
        out += ts_cardinality(item, pattern_name, decl, ctx, obs, diff)
    return out


def ts_cardinality(item: RelationItem, pattern_name: str, decl: Declaration, ctx: WriteContext, obs: list[Observation],
                   diff: Diff | None) -> list[Rejection]:
    """"At most one" (Thesis A's induction binding: a start node may have only one relationship of this type). Counted:
    the relationships of this type leaving the start node before the statement (`relation_count`, the tool's answer) plus
    those this write creates; a changed relationship is already among the former. Without the tool's answer the write
    alone can still show two; otherwise the rule is `not_checkable`."""
    start = item.start.ref
    same = [r for r in (diff.relations if diff else [item]) if r.type == item.type and r.start.ref == start]
    changed = diff.changed if diff else set()
    created_here = [r for r in same if item_key(r) not in changed]
    before = None
    if ctx.relation_count is not None and start is not None:
        before = ctx.relation_count(start, item.type)
    el = _el(item, "cardinality")
    expected = f"at most one {item.type} from the start node ({pattern_name})"
    if before is None:
        if len(created_here) > 1:
            return _maybe(decl, Rejection("TS-cardinality", el, expected, f"{len(created_here)} in this write",
                                          f"the pattern allows at most one {item.type} per start node"), obs)
        return [Rejection("TS-cardinality", el, expected, "unknown: no relationship count from the tool",
                          "the tool gave no relation_count, so what the start node already has is not checkable", status="not_checkable")]
    total = int(before) + len(created_here)
    if total > 1:
        return _maybe(decl, Rejection("TS-cardinality", el, expected, f"{total} ({before} in the graph, {len(created_here)} in this write)",
                                      f"the pattern allows at most one {item.type} per start node"), obs)
    return []


def _maybe(decl: Declaration, rej: Rejection, obs: list[Observation]) -> list[Rejection]:
    out: list[Rejection] = []
    _emit(decl, "cardinality_violation", rej, out, obs, "second relationship where the pattern says at most one; accepted under the realisation's enforcement")
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
        r = ts_relation(rel, decl, ctx, obs, diff)
        if r:
            out[key] = r
    return out, obs
