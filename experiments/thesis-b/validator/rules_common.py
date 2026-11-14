"""Common-scope rules CS-01 to CS-07, applied to canonical assertions identically in both arms.

The rule list is Thesis A's (Eval 2 package v0.1, card P.3, "Common scope"):
  subject, predicate and object well formed; predicate and type mapped or mappable under the equivalence table;
  no dangling reference; datatypes and units parse; qualifiers well formed; provenance pointer present;
  no duplicate assertion.
Thesis B implements the rules (agreement 3.2). A rule that some realisation cannot satisfy structurally is removed by
Thesis A, not relaxed here.

Every rule returns a list of Rejection objects; an empty list is a pass. A rule that cannot be decided because a
binding is missing (equivalence table, unit list) returns one Rejection with `status = not_checkable`, which the
validator reports separately and never counts as a reject.
"""
from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field
from datetime import date, datetime
from typing import Any, Iterable

from .vocabulary import EquivalenceTable, UnitList

QUALIFIER_KEYS = {"scope", "validity_time", "condition", "exception", "modality", "priority"}
MODALITIES = {"must", "must_not", "may", "should", "is"}
DATATYPES = {"string", "integer", "decimal", "boolean", "date", "datetime", "duration"}
ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
ISO_DURATION = re.compile(r"^P(?!$)(\d+Y)?(\d+M)?(\d+W)?(\d+D)?(T(?=\d)(\d+H)?(\d+M)?(\d+S)?)?$")


@dataclass
class Rejection:
    """Uniform validator message (agreement 3.2): rule id, element, expected, found; message for the agent."""
    rule_id: str
    element: str
    expected: Any
    found: Any
    message: str
    status: str = "reject"          # reject | not_checkable

    def to_log(self) -> dict:
        """The four fields of shared-minimum-log/1 plus Thesis B's message (an extension)."""
        return {"rule_id": self.rule_id, "element": self.element, "expected": self.expected, "found": self.found,
                "message": self.message}


@dataclass
class Context:
    """What a rule may look at beyond the assertion itself."""
    equivalence: EquivalenceTable = field(default_factory=EquivalenceTable)
    units: UnitList = field(default_factory=UnitList)
    known_entity_ids: set[str] = field(default_factory=set)      # entities already in the graph
    batch: list[dict] = field(default_factory=list)              # the other assertions of the same write / export
    known_assertion_keys: set[tuple] = field(default_factory=set)  # identities already in the graph (CS-07)


# ----------------------------------------------------------------------------- helpers
def _is_entity(x: Any) -> bool:
    return isinstance(x, dict) and x.get("kind", "entity") == "entity"


def _is_literal(x: Any) -> bool:
    return isinstance(x, dict) and x.get("kind") == "literal"


def _nonempty(s: Any) -> bool:
    return isinstance(s, str) and s.strip() != ""


def entity_key(ref: dict, ctx: Context) -> str:
    """Identity of an entity reference for CS-07: id when known, else canonical type + normalised label."""
    if _nonempty(ref.get("id")):
        return f"id:{ref['id']}"
    typ = ref.get("type")
    canon = ctx.equivalence.canonical_type(typ) if typ and ctx.equivalence.loaded else typ
    return f"{canon or '?'}:{re.sub(r'\\s+', ' ', str(ref.get('label') or '').strip().lower())}"


def _qualifier_key(q: dict | None) -> tuple:
    q = q or {}
    vt = q.get("validity_time") or {}
    return (q.get("scope"), vt.get("from"), vt.get("to"), q.get("condition"), q.get("exception"), q.get("modality"),
            q.get("priority"))


def assertion_key(a: dict, ctx: Context) -> tuple:
    """Identity of an assertion (Eval 2 package, P.8: the qualifiers are part of an assertion's identity; provenance is
    not). Duplicate = same subject, canonical predicate, object and qualifiers."""
    pred = a.get("predicate")
    canon = ctx.equivalence.canonical_predicate(pred) if pred and ctx.equivalence.loaded else pred
    obj = a.get("object")
    if _is_literal(obj):
        okey = ("lit", str(obj.get("value")), obj.get("datatype"), obj.get("unit"))
    elif isinstance(obj, dict):
        okey = ("ent", entity_key(obj, ctx))
    else:
        okey = ("?", str(obj))
    subj = a.get("subject") if isinstance(a.get("subject"), dict) else {}
    return (entity_key(subj, ctx), canon or "?", okey, _qualifier_key(a.get("qualifiers")))


# ----------------------------------------------------------------------------- rules
def cs01_well_formed(a: dict, ctx: Context) -> list[Rejection]:
    out = []
    subj = a.get("subject")
    if not isinstance(subj, dict) or not (_nonempty(subj.get("id")) or _nonempty(subj.get("label"))):
        out.append(Rejection("CS-01", "subject", "entity reference with id or label", subj, "subject missing or empty"))
    if not _nonempty(a.get("predicate")):
        out.append(Rejection("CS-01", "predicate", "non-empty string", a.get("predicate"), "predicate missing or empty"))
    obj = a.get("object")
    if _is_literal(obj):
        if obj.get("value") is None or (isinstance(obj.get("value"), str) and obj["value"].strip() == ""):
            out.append(Rejection("CS-01", "object.value", "non-null literal value", obj.get("value"), "literal object without a value"))
    elif _is_entity(obj):
        if not (_nonempty(obj.get("id")) or _nonempty(obj.get("label"))):
            out.append(Rejection("CS-01", "object", "entity reference with id or label", obj, "object entity missing id and label"))
    else:
        out.append(Rejection("CS-01", "object", "entity reference or literal", obj, "object missing"))
    return out


def cs02_vocabulary(a: dict, ctx: Context) -> list[Rejection]:
    if not ctx.equivalence.loaded:
        return [Rejection("CS-02", "predicate", "equivalence table", None,
                          "no equivalence table loaded (Thesis A's Domain Binding); rule not checkable", status="not_checkable")]
    out = []
    pred = a.get("predicate")
    if _nonempty(pred) and ctx.equivalence.canonical_predicate(pred) is None:
        out.append(Rejection("CS-02", "predicate", "a predicate of the canonical vocabulary or an alias in the equivalence table",
                             pred, f"predicate {pred!r} is not mapped and not mappable"))
    for role in ("subject", "object"):
        ref = a.get(role)
        if _is_entity(ref) and _nonempty(ref.get("type")) and ctx.equivalence.canonical_type(ref["type"]) is None:
            out.append(Rejection("CS-02", f"{role}.type", "a type of the canonical vocabulary or an alias", ref["type"],
                                 f"type {ref['type']!r} is not mapped and not mappable"))
    return out


def cs03_no_dangling(a: dict, ctx: Context) -> list[Rejection]:
    """An entity referenced by id must exist: in the graph, or be introduced in the same batch (as a subject, or as an
    object with a label that names it)."""
    out = []
    introduced = set()
    for b in ctx.batch:
        for role in ("subject", "object"):
            ref = b.get(role)
            if _is_entity(ref) and _nonempty(ref.get("id")) and _nonempty(ref.get("label")):
                introduced.add(ref["id"])
    for role in ("subject", "object"):
        ref = a.get(role)
        if _is_entity(ref) and _nonempty(ref.get("id")) and not _nonempty(ref.get("label")):
            if ref["id"] not in ctx.known_entity_ids and ref["id"] not in introduced:
                out.append(Rejection("CS-03", f"{role}.id", "an entity that exists in the graph or is introduced in this write",
                                     ref["id"], f"{role} refers to unknown entity {ref['id']!r}"))
    return out


def _parse_literal(value: Any, datatype: str | None) -> str | None:
    """None when the value parses as the datatype, else what was expected."""
    if datatype in (None, "string"):
        return None if isinstance(value, str) or value is not None else "string"
    if datatype == "integer":
        if isinstance(value, bool) or not (isinstance(value, int) or (isinstance(value, str) and re.fullmatch(r"[+-]?\d+", value.strip()))):
            return "integer"
    elif datatype == "decimal":
        if isinstance(value, bool):
            return "decimal"
        try:
            float(str(value).replace(",", ".")) if not isinstance(value, (int, float)) else None
        except ValueError:
            return "decimal number"
    elif datatype == "boolean":
        if not (isinstance(value, bool) or str(value).lower() in ("true", "false", "yes", "no", "ja", "nein")):
            return "boolean"
    elif datatype == "date":
        if not (isinstance(value, str) and ISO_DATE.match(value)):
            return "ISO 8601 date"
        try:
            date.fromisoformat(value)
        except ValueError:
            return "ISO 8601 date"
    elif datatype == "datetime":
        try:
            datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except ValueError:
            return "ISO 8601 datetime"
    elif datatype == "duration":
        if not (isinstance(value, str) and ISO_DURATION.match(value)):
            return "ISO 8601 duration"
    return None


def cs04_datatypes_units(a: dict, ctx: Context) -> list[Rejection]:
    obj = a.get("object")
    if not _is_literal(obj):
        return []
    out = []
    dt = obj.get("datatype")
    if dt is not None and dt not in DATATYPES:
        out.append(Rejection("CS-04", "object.datatype", sorted(DATATYPES), dt, f"unknown datatype {dt!r}"))
    else:
        expected = _parse_literal(obj.get("value"), dt)
        if expected:
            out.append(Rejection("CS-04", "object.value", expected, obj.get("value"), f"value does not parse as {dt}"))
    unit = obj.get("unit")
    if _nonempty(unit):
        if not ctx.units.loaded:
            out.append(Rejection("CS-04", "object.unit", "unit list", unit,
                                 "no unit list loaded (Thesis A's Domain Binding); unit not checkable", status="not_checkable"))
        elif not ctx.units.known(unit):
            out.append(Rejection("CS-04", "object.unit", "a unit of the unit list", unit, f"unknown unit {unit!r}"))
    return out


def cs05_qualifiers(a: dict, ctx: Context) -> list[Rejection]:
    q = a.get("qualifiers")
    if q is None:
        return [Rejection("CS-05", "qualifiers", "object (may be empty)", None, "qualifiers missing; use {} when the source documents none")]
    if not isinstance(q, dict):
        return [Rejection("CS-05", "qualifiers", "object", q, "qualifiers is not an object")]
    out = []
    for k in q:
        if k not in QUALIFIER_KEYS:
            out.append(Rejection("CS-05", f"qualifiers.{k}", sorted(QUALIFIER_KEYS), k, f"unknown qualifier {k!r}"))
    vt = q.get("validity_time")
    if vt is not None:
        if not isinstance(vt, dict):
            out.append(Rejection("CS-05", "qualifiers.validity_time", "object with from/to", vt, "validity_time is not an object"))
        else:
            for k in ("from", "to"):
                v = vt.get(k)
                if v is not None and _parse_literal(v, "date"):
                    out.append(Rejection("CS-05", f"qualifiers.validity_time.{k}", "ISO 8601 date", v, f"validity_time.{k} is not a date"))
            if vt.get("from") and vt.get("to") and ISO_DATE.match(str(vt["from"])) and ISO_DATE.match(str(vt["to"])) and vt["from"] > vt["to"]:
                out.append(Rejection("CS-05", "qualifiers.validity_time", "from <= to", vt, "validity_time ends before it starts"))
    if q.get("modality") is not None and q["modality"] not in MODALITIES:
        out.append(Rejection("CS-05", "qualifiers.modality", sorted(MODALITIES), q["modality"], "unknown modality"))
    if q.get("priority") is not None and (isinstance(q["priority"], bool) or not isinstance(q["priority"], int)):
        out.append(Rejection("CS-05", "qualifiers.priority", "integer", q["priority"], "priority is not an integer"))
    for k in ("scope", "condition", "exception"):
        if q.get(k) is not None and not _nonempty(q[k]):
            out.append(Rejection("CS-05", f"qualifiers.{k}", "non-empty string or null", q[k], f"{k} is empty"))
    return out


def cs06_provenance(a: dict, ctx: Context) -> list[Rejection]:
    p = a.get("provenance")
    if not isinstance(p, dict) or not _nonempty(p.get("unit_id")):
        return [Rejection("CS-06", "provenance.unit_id", "the source unit the agent was processing", (p or {}).get("unit_id") if isinstance(p, dict) else p,
                          "provenance pointer missing: unit_id required")]
    if not (_nonempty(p.get("locator")) or _nonempty(p.get("quote"))):
        return [Rejection("CS-06", "provenance", "locator or quote inside the unit", {"locator": p.get("locator"), "quote": p.get("quote")},
                          "provenance has no anchor inside the unit (locator or quote)")]
    return []


def cs07_no_duplicate(a: dict, ctx: Context) -> list[Rejection]:
    key = assertion_key(a, ctx)
    if key in ctx.known_assertion_keys:
        return [Rejection("CS-07", "assertion", "a proposition not yet in the graph", key, "duplicate of an assertion already in the graph")]
    same = [b for b in ctx.batch if b is not a and assertion_key(b, ctx) == key]
    if same:
        return [Rejection("CS-07", "assertion", "one occurrence per proposition in a write", key, "duplicate inside the same write")]
    return []


COMMON_RULES = {
    "CS-01": ("subject, predicate and object well formed", cs01_well_formed),
    "CS-02": ("predicate and type mapped or mappable under the equivalence table", cs02_vocabulary),
    "CS-03": ("no dangling reference", cs03_no_dangling),
    "CS-04": ("datatypes and units parse", cs04_datatypes_units),
    "CS-05": ("qualifiers well formed", cs05_qualifiers),
    "CS-06": ("provenance pointer present", cs06_provenance),
    "CS-07": ("no duplicate assertion", cs07_no_duplicate),
}


def run_common(a: dict, ctx: Context, rule_ids: Iterable[str] | None = None) -> list[Rejection]:
    out: list[Rejection] = []
    for rid in (rule_ids or COMMON_RULES):
        out.extend(COMMON_RULES[rid][1](a, ctx))
    return out
