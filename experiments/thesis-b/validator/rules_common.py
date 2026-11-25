"""Common-scope rules CS1 to CS7 on the export: the stored graph turned back into schema-neutral assertions after the
run (Thesis A's common-scope rule list v0.2, section 4, "On the export, counts"). Identical in both arms.

The write-time statements of CS1, CS3 and CS4 are in `rules_write.py`, on items. CS0 (a write that cannot be read)
exists only at the write.

Every rule returns a list of Rejection objects; an empty list is a pass. A rule that cannot be decided because a
binding is missing (equivalence table, unit list, write log) returns one Rejection with `status = not_checkable`, which
the validator reports separately and never counts as a reject.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Iterable

from .vocabulary import DATATYPES, ISO_DATE, EquivalenceTable, FormatTable

QUALIFIER_KEYS = {"scope", "validity_time", "condition", "exception", "modality", "priority"}
# BE-08: must, may, must not. "must_not" and "must-not" are read as "must not".
MODALITIES = {"must", "may", "must not"}
SCOPE_STATE = re.compile(r"^DE-[A-Z]{2}$")
SCOPE_KEY = re.compile(r"^\d{8}$")


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
    """What an export rule may look at beyond the assertion itself."""
    equivalence: EquivalenceTable = field(default_factory=EquivalenceTable)
    formats: FormatTable = field(default_factory=FormatTable)
    batch: list[dict] = field(default_factory=list)              # the other assertions of the same export
    known_entity_ids: set[str] = field(default_factory=set)      # entities with an identifying value anywhere in the export
    known_assertion_keys: set[tuple] = field(default_factory=set)
    write_log: dict[str, str] | None = None                      # element id -> unit_id of the accepted write (CS6 chain)


# ----------------------------------------------------------------------------- helpers
def _is_entity(x: Any) -> bool:
    return isinstance(x, dict) and x.get("kind", "entity") == "entity"


def _is_literal(x: Any) -> bool:
    return isinstance(x, dict) and x.get("kind") == "literal"


def _nonempty(s: Any) -> bool:
    return isinstance(s, str) and s.strip() != ""


def normalise_modality(m: Any) -> Any:
    return re.sub(r"[_\-]+", " ", m.strip().lower()) if isinstance(m, str) else m


def entity_key(ref: dict, ctx: Context) -> str:
    """Identity of an entity reference for CS7: id when known, else canonical type + normalised label."""
    if _nonempty(ref.get("id")):
        return f"id:{ref['id']}"
    typ = ref.get("type")
    canon = ctx.equivalence.canonical_type(typ) if typ and ctx.equivalence.loaded else typ
    return f"{canon or '?'}:{re.sub(r'\\s+', ' ', str(ref.get('label') or '').strip().lower())}"


def _qualifier_key(q: dict | None) -> tuple:
    q = q or {}
    vt = q.get("validity_time") or {}
    return (q.get("scope"), vt.get("from"), vt.get("to"), q.get("condition"), q.get("exception"),
            normalise_modality(q.get("modality")), q.get("priority"))


def assertion_key(a: dict, ctx: Context) -> tuple:
    """Identity of an assertion (list v0.2, CS7): subject, predicate, object and every qualifier after normalisation.
    Provenance is not part of the comparison."""
    pred = a.get("predicate")
    canon = ctx.equivalence.canonical_predicate(pred) if pred and ctx.equivalence.loaded else pred
    obj = a.get("object")
    if _is_literal(obj):
        okey = ("lit", str(obj.get("value")).strip().lower() if obj.get("value") is not None else None, obj.get("datatype"), obj.get("unit"))
    elif isinstance(obj, dict):
        okey = ("ent", entity_key(obj, ctx))
    else:
        okey = ("?", str(obj))
    subj = a.get("subject") if isinstance(a.get("subject"), dict) else {}
    return (entity_key(subj, ctx), canon or "?", okey, _qualifier_key(a.get("qualifiers")))


def _has_identity(ref: dict) -> bool:
    return _nonempty(ref.get("label")) or _nonempty(ref.get("key")) or _nonempty(ref.get("type"))


# ----------------------------------------------------------------------------- rules
def cs1_well_formed(a: dict, ctx: Context) -> list[Rejection]:
    """Subject, predicate or object missing or empty after spelling normalisation."""
    out = []
    subj = a.get("subject")
    if not isinstance(subj, dict) or not (_nonempty(subj.get("id")) or _nonempty(subj.get("label"))):
        out.append(Rejection("CS1", "subject", "entity reference with id or label", subj, "subject missing or empty"))
    if not _nonempty(a.get("predicate")):
        out.append(Rejection("CS1", "predicate", "non-empty string", a.get("predicate"), "predicate missing or empty"))
    obj = a.get("object")
    if _is_literal(obj):
        if obj.get("value") is None or (isinstance(obj.get("value"), str) and obj["value"].strip() == ""):
            out.append(Rejection("CS1", "object.value", "non-null literal value", obj.get("value"), "literal object without a value"))
    elif _is_entity(obj):
        if not (_nonempty(obj.get("id")) or _nonempty(obj.get("label"))):
            out.append(Rejection("CS1", "object", "entity reference with id or label", obj, "object entity missing id and label"))
    else:
        out.append(Rejection("CS1", "object", "entity reference or literal", obj, "object missing"))
    return out


def cs2_vocabulary(a: dict, ctx: Context) -> list[Rejection]:
    """Predicate or type resolves neither through the mapping nor the equivalence table nor an adjudication."""
    if not ctx.equivalence.loaded:
        return [Rejection("CS2", "predicate", "equivalence table", None,
                          "no equivalence table loaded (Thesis A, 28 Dec); rule not checkable", status="not_checkable")]
    out = []
    pred = a.get("predicate")
    if _nonempty(pred) and ctx.equivalence.canonical_predicate(pred) is None:
        out.append(Rejection("CS2", "predicate", "a predicate of the canonical vocabulary or an alias in the equivalence table",
                             pred, f"predicate {pred!r} is not mapped and not mappable"))
    for role in ("subject", "object"):
        ref = a.get(role)
        if _is_entity(ref) and _nonempty(ref.get("type")) and ctx.equivalence.canonical_type(ref["type"]) is None:
            out.append(Rejection("CS2", f"{role}.type", "a type of the canonical vocabulary or an alias", ref["type"],
                                 f"type {ref['type']!r} is not mapped and not mappable"))
    return out


def cs3_no_dangling(a: dict, ctx: Context) -> list[Rejection]:
    """Subject or entity-valued object has no identifying value anywhere in the export (an empty node; a bare resource
    without name, key or type)."""
    out = []
    for role in ("subject", "object"):
        ref = a.get(role)
        if not _is_entity(ref):
            continue
        if _has_identity(ref):
            continue
        rid = ref.get("id")
        if _nonempty(rid) and rid in ctx.known_entity_ids:
            continue
        out.append(Rejection("CS3", f"{role}", "an entity with an identifying value (name, key or type) somewhere in the export",
                             ref, f"{role} refers to an entity without an identifying value"))
    return out


def cs4_datatypes_units(a: dict, ctx: Context) -> list[Rejection]:
    """An object value that does not normalise to the value type its predicate expects, under the format table."""
    obj = a.get("object")
    if not _is_literal(obj):
        return []
    out = []
    dt = obj.get("datatype")
    if dt is not None and dt not in DATATYPES:
        out.append(Rejection("CS4", "object.datatype", sorted(DATATYPES), dt, f"unknown datatype {dt!r}"))
    else:
        expected = ctx.formats.check(obj.get("value"), dt)
        if expected:
            out.append(Rejection("CS4", "object.value", expected, obj.get("value"), f"value does not parse as {dt}"))
    unit = obj.get("unit")
    if _nonempty(unit):
        status = ctx.formats.unit_status(unit)
        if status == "not_checkable":
            out.append(Rejection("CS4", "object.unit", "format and unit table", unit,
                                 "no unit table yet (Thesis A, 23 Nov); unit not checkable", status="not_checkable"))
        elif status == "unknown":
            out.append(Rejection("CS4", "object.unit", "a unit of the format and unit table", unit, f"unknown unit {unit!r}"))
    return out


def cs5_qualifiers(a: dict, ctx: Context) -> list[Rejection]:
    """A qualifier that is present and not in the form BE-08 fixes. A missing qualifier is no violation."""
    q = a.get("qualifiers")
    if q is None:
        return []
    if not isinstance(q, dict):
        return [Rejection("CS5", "qualifiers", "object", q, "qualifiers is not an object")]
    out = []
    for k in q:
        if k not in QUALIFIER_KEYS:
            out.append(Rejection("CS5", f"qualifiers.{k}", sorted(QUALIFIER_KEYS), k, f"unknown qualifier {k!r}"))
    scope = q.get("scope")
    if scope is not None:
        if not _nonempty(scope):
            out.append(Rejection("CS5", "qualifiers.scope", "DE, a federal-state code, an 8-digit municipality key or an institution id", scope, "scope is empty"))
        elif scope.strip().isdigit() and not SCOPE_KEY.match(scope.strip()):
            out.append(Rejection("CS5", "qualifiers.scope", "8-digit municipality key as text", scope, "numeric scope is not an 8-digit key"))
    vt = q.get("validity_time")
    if vt is not None:
        if not isinstance(vt, dict):
            out.append(Rejection("CS5", "qualifiers.validity_time", "closed-open ISO 8601 interval {from, to}", vt, "validity_time is not an object"))
        else:
            for k in ("from", "to"):
                v = vt.get(k)
                if v is not None and ctx.formats.check(v, "date"):
                    out.append(Rejection("CS5", f"qualifiers.validity_time.{k}", "ISO 8601 date", v, f"validity_time.{k} is not a date"))
            f, t = vt.get("from"), vt.get("to")
            if f and t and ISO_DATE.match(str(f)) and ISO_DATE.match(str(t)) and f >= t:
                out.append(Rejection("CS5", "qualifiers.validity_time", "start before end (closed-open interval)", vt, "validity_time does not start before it ends"))
    if q.get("modality") is not None and normalise_modality(q["modality"]) not in MODALITIES:
        out.append(Rejection("CS5", "qualifiers.modality", sorted(MODALITIES), q["modality"], "modality is not must, may or must not"))
    if q.get("priority") is not None and (isinstance(q["priority"], bool) or not isinstance(q["priority"], int)):
        out.append(Rejection("CS5", "qualifiers.priority", "integer", q["priority"], "priority is not an integer"))
    for k in ("condition", "exception"):
        if q.get(k) is not None and not _nonempty(q[k]):
            out.append(Rejection("CS5", f"qualifiers.{k}", "non-empty text", q[k], f"{k} is empty"))
    return out


def cs6_provenance(a: dict, ctx: Context) -> list[Rejection]:
    """The chain breaks: no pointer to a graph element, or an element the write log does not show as the product of an
    accepted write with a unit id. With a write log (element id -> unit id) the chain is followed; without one the
    assertion's own `provenance.unit_id` (what the exporter copied from the log) stands in."""
    elements = a.get("elements") or a.get("element_ids") or []
    if ctx.write_log is not None:
        if not elements:
            return [Rejection("CS6", "elements", "pointers to the graph elements the assertion was built from", elements,
                              "no pointer to a graph element: unanchored")]
        missing = [e for e in elements if e not in ctx.write_log]
        if missing:
            return [Rejection("CS6", "elements", "elements produced by an accepted write with a unit id", missing,
                              "element not found in the write log: unanchored")]
        return []
    p = a.get("provenance")
    if not isinstance(p, dict) or not _nonempty(p.get("unit_id")):
        return [Rejection("CS6", "provenance.unit_id", "the source unit of the accepted write", (p or {}).get("unit_id") if isinstance(p, dict) else p,
                          "no source unit: unanchored")]
    return []


def cs7_no_duplicate(a: dict, ctx: Context) -> list[Rejection]:
    key = assertion_key(a, ctx)
    if key in ctx.known_assertion_keys:
        return [Rejection("CS7", "assertion", "one occurrence per fact", key, "duplicate of an earlier assertion")]
    return []


COMMON_RULES = {
    "CS1": ("subject, predicate and object well formed", cs1_well_formed),
    "CS2": ("predicate and type mapped or mappable under the equivalence table", cs2_vocabulary),
    "CS3": ("no dangling reference", cs3_no_dangling),
    "CS4": ("datatypes and units parse", cs4_datatypes_units),
    "CS5": ("qualifiers well formed", cs5_qualifiers),
    "CS6": ("provenance pointer present", cs6_provenance),
    "CS7": ("no duplicate assertion", cs7_no_duplicate),
}


def run_common(a: dict, ctx: Context, rule_ids: Iterable[str] | None = None) -> list[Rejection]:
    out: list[Rejection] = []
    for rid in (rule_ids or COMMON_RULES):
        out.extend(COMMON_RULES[rid][1](a, ctx))
    return out
