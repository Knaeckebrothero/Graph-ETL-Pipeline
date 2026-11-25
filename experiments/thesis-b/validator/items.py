"""The item model of the write-time check, and the adapters that build it from a database diff.

Thesis A's rule list (common-scope-rules.md v0.2, section 3) states the write-time rules on an *item*:

- an **entity item**: its types (labels in the property graph, classes in RDF; may be none), an identifying value (key or
  name) and attributes (name, value, datatype where one is given);
- a **relation item**: a type (relationship type or predicate), two ends that each refer to an entity, and attributes of
  its own where the realisation allows them;
- with every item the harness, not the model, adds the unit being processed.

Thesis A's write tool (decision of 15 Nov 2026) takes a raw Cypher or SPARQL query from the agent, runs it, and hands the
validator the **diff** the query made: what was inserted. The validator never queries a database itself. The two
adapters below turn the natural diff of each engine into items:

- `diff_from_neo4j(nodes, relationships, ...)`: the created nodes and relationships as the Neo4j driver returns them
  (objects with `.element_id`, `.labels`/`.type`, `.items()`, `.start_node`/`.end_node`) or as plain dicts of the same
  shape: `{"element_id", "labels", "properties"}` and `{"element_id", "type", "start": {"element_id", "labels"},
  "end": {...}, "properties"}`.
- `diff_from_ntriples(text, ...)`: the inserted triples as N-Triples lines (what Fuseki's diff gives, and what Thesis A
  hashes into `element_ids`). RDF 1.2 triple terms `<<( s p o )>>` in subject position are read as attributes of that
  relation (the edge-property form Thesis A's RDF realisations use).

A `Diff` also carries what the validator needs around the items: the query text (so that an empty diff can be told
apart: MERGE found it = already present, CREATE after a MATCH that matched nothing = dangling end, CS3), the arm, and
the ids of elements that were already there when the tool answers "already present".
"""
from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field
from typing import Any, Iterable

RDF_TYPE = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
LABEL_PREDICATES = ("http://www.w3.org/2000/01/rdf-schema#label", "http://schema.org/name", "http://xmlns.com/foaf/0.1/name",
                    "http://www.w3.org/2004/02/skos/core#prefLabel", "http://purl.org/dc/terms/title")
XSD = "http://www.w3.org/2001/XMLSchema#"
XSD_TO_DATATYPE = {"date": "date", "dateTime": "datetime", "dateTimeStamp": "datetime", "integer": "integer", "int": "integer",
                   "long": "integer", "short": "integer", "nonNegativeInteger": "integer", "positiveInteger": "integer",
                   "decimal": "decimal", "float": "decimal", "double": "decimal", "boolean": "boolean", "duration": "duration",
                   "string": None, "langString": None}
# Property names that identify an entity when the realisation declares no uniqueness constraint (S1 raw triples).
IDENTIFYING_PROPERTIES = ("uid", "id", "key", "name", "title", "label", "code", "ean", "avv_code", "iri")


@dataclass
class Attribute:
    name: str
    value: Any
    datatype: str | None = None          # date | datetime | integer | decimal | boolean | duration | quantity | None (text)
    unit: str | None = None              # for quantity


@dataclass
class EndRef:
    """One end of a relation item: which entity, and how it was found."""
    ref: str | None                      # element id (pg) or IRI (rdf); None when missing
    types: list[str] = field(default_factory=list)
    in_diff: bool = False                # the entity is created in the same write
    exists: bool | None = None           # the tool's own answer (`ends[..].exists`, RDF cells); None when it gave none


@dataclass
class EntityItem:
    ref: str                             # element id (pg) or IRI (rdf)
    types: list[str]
    identifier: Any                      # key or name; None when the entity has none
    attributes: list[Attribute]
    identifier_from: str | None = None   # which attribute gave the identifier

    @property
    def kind(self) -> str:
        return "entity"


@dataclass
class RelationItem:
    ref: str | None                      # element id (pg); for rdf the triple as one N-Triples line
    type: str
    start: EndRef
    end: EndRef
    attributes: list[Attribute]

    @property
    def kind(self) -> str:
        return "relation"


@dataclass
class Diff:
    arm: str                                       # pg | rdf
    entities: list[EntityItem]
    relations: list[RelationItem]
    query: str | None = None                       # the agent's query text, if the tool passes it on
    existing_ids: list[str] = field(default_factory=list)   # ids of what was already there (tool answers "already present")
    unit_id: str | None = None                     # added by the harness, never by the model
    raw: Any = None
    deleted: dict = field(default_factory=lambda: {"nodes": [], "relationships": []})
    changed: set[str] = field(default_factory=set)  # refs of items that existed before and were changed, not created
    realisation: str | None = None

    @property
    def empty(self) -> bool:
        return not self.entities and not self.relations

    @property
    def element_ids(self) -> list[str]:
        """What the write produced, in the form of Thesis A's `element_ids`: pg element ids; rdf the N-Triples lines
        (Thesis A hashes them; the hash is the tool's job, the line is what the validator saw)."""
        out = [e.ref for e in self.entities]
        out += [r.ref for r in self.relations if r.ref]
        return out

    def items(self) -> list[EntityItem | RelationItem]:
        return [*self.entities, *self.relations]


# ----------------------------------------------------------------------------- shared helpers
def value_datatype(v: Any) -> str | None:
    """Datatype of a driver-typed value (Neo4j returns typed Python objects). Text stays None."""
    if isinstance(v, bool):
        return "boolean"
    if isinstance(v, int):
        return "integer"
    if isinstance(v, float):
        return "decimal"
    tn = type(v).__name__
    if tn in ("date", "Date"):
        return "date"
    if tn in ("datetime", "DateTime"):
        return "datetime"
    if tn in ("timedelta", "Duration"):
        return "duration"
    return None


def _attribute(name: str, v: Any) -> Attribute:
    """One attribute from a stored value: a driver-typed value, the explicit form `{"value", "datatype", "unit"}` (a typed
    RDF literal as Thesis A's tool hands it over), or a list of either (a property with several values). A list keeps
    its values unwrapped; its datatype is the one every value shares, else none (text)."""
    if isinstance(v, dict) and "value" in v:                 # explicit form {"value": ..., "datatype": ..., "unit": ...}
        return Attribute(name, v.get("value"), v.get("datatype"), v.get("unit"))
    if isinstance(v, (list, tuple)):
        parts = [_attribute(name, x) for x in v]
        dts = {p.datatype for p in parts}
        units = {p.unit for p in parts}
        return Attribute(name, [p.value for p in parts], dts.pop() if len(dts) == 1 else None, units.pop() if len(units) == 1 else None)
    return Attribute(name, v, value_datatype(v))


def pick_identifier(types: list[str], attributes: list[Attribute], unique_by_type: dict[str, set[str]] | None,
                    identifier_by_type: dict[str, str] | None = None) -> tuple[Any, str | None]:
    """Identifying value: the declared identifier of one of the item's types (S2, Rule 6), else a declared unique property
    of one of them (treatment scope), else the first of IDENTIFYING_PROPERTIES that is present, else None."""
    by_name = {a.name: a for a in attributes}

    def usable(a: Attribute | None) -> bool:
        return a is not None and a.value is not None and not (isinstance(a.value, str) and a.value.strip() == "")

    for t in types:
        prop = (identifier_by_type or {}).get(t)
        if prop and usable(by_name.get(prop)):
            return by_name[prop].value, prop
    for t in types:
        for prop in sorted((unique_by_type or {}).get(t, ())):
            if usable(by_name.get(prop)):
                return by_name[prop].value, prop
    for prop in IDENTIFYING_PROPERTIES:
        if usable(by_name.get(prop)):
            return by_name[prop].value, prop
    return None, None


# ----------------------------------------------------------------------------- Neo4j
def _get(o: Any, key: str, default: Any = None) -> Any:
    if isinstance(o, dict):
        return o.get(key, default)
    return getattr(o, key, default)


def _props(o: Any) -> dict:
    if isinstance(o, dict):
        return dict(o.get("properties") or {})
    if hasattr(o, "items"):
        return dict(o.items())
    return {}


def _node_ref(o: Any) -> tuple[str | None, list[str]]:
    if o is None:
        return None, []
    return _get(o, "element_id"), list(_get(o, "labels", []) or [])


def diff_from_neo4j(nodes: Iterable[Any], relationships: Iterable[Any], *, query: str | None = None,
                    existing_ids: Iterable[str] | None = None, unit_id: str | None = None,
                    unique_by_type: dict[str, set[str]] | None = None) -> Diff:
    """Items from the nodes and relationships a Cypher write created (driver objects or dicts, see module docstring)."""
    ents: list[EntityItem] = []
    for n in nodes:
        eid, labels = _node_ref(n)
        attrs = [_attribute(k, v) for k, v in _props(n).items()]
        ident, src = pick_identifier(labels, attrs, unique_by_type)
        ents.append(EntityItem(ref=str(eid) if eid is not None else "", types=labels, identifier=ident, attributes=attrs, identifier_from=src))
    in_diff = {e.ref for e in ents}
    rels: list[RelationItem] = []
    for r in relationships:
        s = _get(r, "start_node", None) if not isinstance(r, dict) else r.get("start") or r.get("start_node")
        t = _get(r, "end_node", None) if not isinstance(r, dict) else r.get("end") or r.get("end_node")
        sid, sl = _node_ref(s)
        tid, tl = _node_ref(t)
        rels.append(RelationItem(ref=str(_get(r, "element_id")) if _get(r, "element_id") is not None else None,
                                 type=str(_get(r, "type") or ""),
                                 start=EndRef(ref=str(sid) if sid is not None else None, types=sl, in_diff=str(sid) in in_diff),
                                 end=EndRef(ref=str(tid) if tid is not None else None, types=tl, in_diff=str(tid) in in_diff),
                                 attributes=[_attribute(k, v) for k, v in _props(r).items()]))
    return Diff(arm="pg", entities=ents, relations=rels, query=query, existing_ids=list(existing_ids or []), unit_id=unit_id,
                raw={"nodes": list(nodes) if not isinstance(nodes, list) else nodes, "relationships": relationships})


# ----------------------------------------------------------------------------- Thesis A's database tool: `changes`
def is_changes(obj: Any) -> bool:
    """True for the dict Thesis A's database tool hands a checker (experiments/harness/dbtool/checker.py)."""
    return isinstance(obj, dict) and "ends" in obj and "nodes" in obj and "relationships" in obj


def diff_from_changes(changes: dict, *, unique_by_type: dict[str, set[str]] | None = None,
                      identifying: dict[str, str] | None = None, identifier_by_type: dict[str, str] | None = None) -> Diff:
    """Items from the `changes` of Thesis A's database tool (checker.py, 15 Nov 2026): the nodes and relationships
    the statement created or changed, in their state after the statement; every node a relationship touches under
    `ends` (with `exists` on the RDF side, 24 Nov 2026); the ids of what was deleted. `identifying` maps a node type to
    the property that identifies it when the realisation says so (declaration.json `identifying_value`, S1:
    Entity.name); `identifier_by_type` is the declared identifier of an induced type (Rule 6)."""
    ents: list[EntityItem] = []
    changed: set[str] = set()
    for n in changes.get("nodes") or []:
        labels = list(n.get("labels") or [])
        attrs = [_attribute(k, v) for k, v in (n.get("properties") or {}).items()]
        ident, src = pick_identifier(labels, attrs, unique_by_type, identifier_by_type)
        if ident is None and identifying:
            for t in labels:
                prop = identifying.get(t)
                a = next((a for a in attrs if a.name == prop), None) if prop else None
                if a is not None and a.value not in (None, ""):
                    ident, src = a.value, prop
                    break
        ref = str(n.get("key"))
        ents.append(EntityItem(ref=ref, types=labels, identifier=ident, attributes=attrs, identifier_from=src))
        if n.get("change") == "changed":
            changed.add(ref)
    in_diff = {e.ref for e in ents}
    ends = changes.get("ends") or {}
    rels: list[RelationItem] = []
    def end(ref: Any) -> EndRef:
        e = ends.get(ref) or {}
        ex = e.get("exists")
        return EndRef(ref=str(ref) if ref is not None else None, types=list(e.get("labels") or []), in_diff=str(ref) in in_diff,
                      exists=bool(ex) if isinstance(ex, (bool, int)) else None)

    for r in changes.get("relationships") or []:
        rels.append(RelationItem(ref=str(r.get("key")) if r.get("key") is not None else None, type=str(r.get("type") or ""),
                                 start=end(r.get("from")), end=end(r.get("to")),
                                 attributes=[_attribute(k, v) for k, v in (r.get("properties") or {}).items()]))
        if r.get("change") == "changed" and r.get("key") is not None:
            changed.add(str(r["key"]))
    return Diff(arm=changes.get("arm") or "pg", entities=ents, relations=rels, query=changes.get("statement"),
                unit_id=changes.get("unit_id"), raw=changes, deleted=dict(changes.get("deleted") or {"nodes": [], "relationships": []}),
                changed=changed, realisation=changes.get("realisation"))


# ----------------------------------------------------------------------------- N-Triples (Fuseki, RDF 1.2)
_IRI = r"<([^>]*)>"
_BNODE = r"(_:[A-Za-z0-9_.-]+)"
_LIT = r'"((?:[^"\\]|\\.)*)"(?:@([A-Za-z0-9-]+)|\^\^<([^>]*)>)?'
_TERM = rf"(?:{_IRI}|{_BNODE}|{_LIT})"
_TRIPLE_TERM = r"<<\(\s*" + _TERM + r"\s+" + _IRI + r"\s+" + _TERM + r"\s*\)>>"
LINE = re.compile(rf"^\s*(?:{_TRIPLE_TERM}|{_TERM})\s+{_IRI}\s+{_TERM}\s*\.\s*$")


def _unescape(s: str) -> str:
    return s.encode("utf-8").decode("unicode_escape").encode("latin-1").decode("utf-8") if "\\" in s else s


def _term(iri: str | None, bnode: str | None, lit: str | None, lang: str | None, dt: str | None) -> tuple[str, Any, str | None, str | None]:
    """(kind, value, datatype, raw_datatype_iri)"""
    if iri is not None:
        return "iri", iri, None, None
    if bnode is not None:
        return "bnode", bnode, None, None
    value = _unescape(lit or "")
    if dt:
        local = dt[len(XSD):] if dt.startswith(XSD) else dt
        return "literal", value, XSD_TO_DATATYPE.get(local, local), dt
    return "literal", value, None, None


def parse_ntriples(text: str) -> list[dict]:
    """Each line -> {"s": (kind, value), "p": iri, "o": (kind, value, datatype, dt_iri), "about": inner triple or None, "line": str}.
    A line that does not parse raises ValueError (the validator turns that into CS0)."""
    out = []
    for n, line in enumerate(text.splitlines(), 1):
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        m = LINE.match(line)
        if not m:
            raise ValueError(f"line {n} is not an N-Triples statement: {line.strip()[:80]}")
        g = m.groups()
        # groups: triple term (inner s: 5 groups, inner p: 1, inner o: 5) = 11, outer s: 5, outer p: 1, outer o: 5
        inner_s, inner_p, inner_o = g[0:5], g[5], g[6:11]
        outer_s, outer_p, outer_o = g[11:16], g[16], g[17:22]
        if inner_p is not None:
            about = {"s": _term(*inner_s)[:2], "p": inner_p, "o": _term(*inner_o)}
            subj = None
        else:
            about = None
            subj = _term(*outer_s)[:2]
        out.append({"s": subj, "p": outer_p, "o": _term(*outer_o), "about": about, "line": line.strip()})
    return out


def _local(iri: str) -> str:
    return re.split(r"[#/]", iri.rstrip("/#"))[-1] or iri


def diff_from_ntriples(text: str, *, query: str | None = None, existing_ids: Iterable[str] | None = None,
                       unit_id: str | None = None, unique_by_type: dict[str, set[str]] | None = None,
                       type_names: str = "local", identifier_by_type: dict[str, str] | None = None) -> Diff:
    """Items from inserted triples. Subjects with literal objects or rdf:type become entity items (types = classes,
    attributes = literal-valued predicates); triples with an IRI object other than rdf:type become relation items;
    triple-term statements become attributes of the relation they are about. `type_names`: "local" shortens class and
    predicate IRIs to their local name (what declaration.json names), "iri" keeps them whole."""
    rows = parse_ntriples(text)
    name = _local if type_names == "local" else (lambda x: x)
    types: dict[str, list[str]] = {}
    attrs: dict[str, list[Attribute]] = {}
    rel_rows: list[dict] = []
    edge_attrs: dict[tuple, list[Attribute]] = {}
    for r in rows:
        if r["about"] is not None:
            key = (r["about"]["s"][1], r["about"]["p"], r["about"]["o"][1])
            k, v, dt, _ = r["o"]
            edge_attrs.setdefault(key, []).append(Attribute(name(r["p"]), v, dt if k == "literal" else None))
            continue
        skind, s = r["s"]
        okind, o, dt, _ = r["o"]
        if r["p"] == RDF_TYPE and okind != "literal":
            types.setdefault(s, []).append(name(o))
            attrs.setdefault(s, [])
        elif okind == "literal":
            attrs.setdefault(s, []).append(Attribute(name(r["p"]), o, dt))
            types.setdefault(s, [])
        else:
            rel_rows.append(r)
    ents: list[EntityItem] = []
    for s in list(types):
        ident, src = pick_identifier(types[s], attrs.get(s, []), unique_by_type, identifier_by_type)
        if ident is None:
            for a in attrs.get(s, []):
                if a.name in [_local(p) for p in LABEL_PREDICATES] and a.value not in (None, ""):
                    ident, src = a.value, a.name
                    break
        ents.append(EntityItem(ref=s, types=types[s], identifier=ident, attributes=attrs.get(s, []), identifier_from=src))
    in_diff = {e.ref for e in ents}
    rels: list[RelationItem] = []
    for r in rel_rows:
        skind, s = r["s"]
        okind, o, _, _ = r["o"]
        key = (s, r["p"], o)
        rels.append(RelationItem(ref=r["line"], type=name(r["p"]),
                                 start=EndRef(ref=s, types=types.get(s, []), in_diff=s in in_diff),
                                 end=EndRef(ref=o, types=types.get(o, []), in_diff=o in in_diff),
                                 attributes=edge_attrs.get(key, [])))
    return Diff(arm="rdf", entities=ents, relations=rels, query=query, existing_ids=list(existing_ids or []), unit_id=unit_id, raw=text)


# ----------------------------------------------------------------------------- query text (only to read an empty diff)
_WRITE_CLAUSE = re.compile(r"\b(CREATE|MERGE|INSERT(?:\s+DATA)?|DELETE|SET|REMOVE|DROP)\b", re.IGNORECASE)
_REL_PATTERN = re.compile(r"-\s*\[[^\]]*\]\s*->|<-\s*\[[^\]]*\]\s*-|-\s*\[[^\]]*\]\s*-")


def query_intent(query: str | None) -> dict:
    """What the query text says it wants: write clauses present, whether a MERGE alone could explain an empty diff,
    how many relationship patterns the write names. Heuristic, used only to read an empty diff; never to judge content."""
    if not query:
        return {"clauses": [], "merge_only": False, "writes": None, "relationship_patterns": None}
    clauses = [m.group(1).upper() for m in _WRITE_CLAUSE.finditer(query)]
    inserts = [c for c in clauses if c in ("CREATE", "MERGE") or c.startswith("INSERT")]
    merge_only = bool(inserts) and all(c == "MERGE" for c in inserts)
    return {"clauses": clauses, "merge_only": merge_only, "writes": bool(inserts),
            "relationship_patterns": len(_REL_PATTERN.findall(query))}


def control_chars(s: str) -> list[str]:
    """Characters that CS1 (e) forbids: control characters other than tab and line break, and unassigned code points."""
    bad = []
    for ch in s:
        cat = unicodedata.category(ch)
        if (cat == "Cc" and ch not in "\t\n\r") or cat == "Cn" or cat == "Cs":
            bad.append(repr(ch))
    return bad
