#!/usr/bin/env python3
"""Derive the schema realisations of the four treatments.

Input:  schemas/v5.0/schema.cql (the curated fessi schema, never changed here), and for the
        induced schema (S2) the reply of the induction, where one exists:
        schemas/treatments/S2-induced/<arm>/induction.reply.json (the model's reply, unchanged).
Output: one folder per treatment under schemas/treatments/, with one sub-folder
        per arm (pg, rdf). Every file in those folders is generated, except the reply.

    python schemas/treatments/derive.py           # write all files
    python schemas/treatments/derive.py --check   # fail if a file on disk differs

The rules applied here (flat derivation, property graph to RDF, raw-triple
pattern, what is binding, the induced schema) are described in
schemas/treatments/README.md. Standard library only.

Without a reply the two S2 realisations stay placeholders. A reply that cannot be
used (induced.py lists why) is refused: nothing of that arm is written or checked,
the other files are, and the script ends with an error.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import induced  # noqa: E402
REPO = HERE.parent.parent
CURATED = REPO / "schemas" / "v5.0" / "schema.cql"

RULES_VERSION = "0.2"
STATUS_LINE = "Status: rules decided on 15 November 2026; not frozen (freeze: 23 November)."
NAMESPACE = "https://fessi.example/schema/{slug}#"
MAPPING_NS = "https://fessi.example/mapping#"
# Where an agent creates the IRIs of data in an RDF realisation. One namespace for every treatment, so
# that an entity's IRI does not carry the variant. A default set on 22 November 2026; the author has
# still to confirm it.
DATA_NAMESPACE = "https://fessi.example/data/"

# Rule "flat": node types that reify a rule or a decision step, and the node
# types that could own a rule in the curated schema (the sources of HAS_RULE).
FLAT_REMOVED = ("DisposalRule", "Condition")
FLAT_RULE_OWNER_VIA = "HAS_RULE"

# Rule "binding": what the harness validator does with a write that leaves the
# declared schema. The stores enforce nothing of this themselves.
ENFORCEMENT_DECLARED = {
    "node_type_outside_the_list": "reject",
    "relationship_outside_the_patterns": "reject",
    "uniqueness_violation": "reject",
    "undeclared_property": "accept_and_count",
}
ENFORCEMENT_NONE = {key: "nothing_declared" for key in ENFORCEMENT_DECLARED}
# Rule "induced": the induction prompt tells the model that a constraint it declares is enforced,
# so the four kinds of constraint only an induced schema can declare are binding as well.
ENFORCEMENT_INDUCED = {
    **ENFORCEMENT_DECLARED,
    "required_property_missing": "reject",
    "datatype_violation": "reject",
    "value_outside_the_list": "reject",
    "cardinality_violation": "reject",
}
REPLY = "induction.reply.json"
INDUCED_SLUG = "s2"
XSD_TERM = {"string": "xsd:string", "integer": "xsd:integer", "decimal": "xsd:decimal", "boolean": "xsd:boolean",
            "date": "xsd:date", "datetime": "xsd:dateTime", "quantity": "map:quantity"}
# What a declaration.json carries of a node type and of a relationship pattern, in this order. The keys
# after the first five and four exist only where a schema declares them (an induced schema).
NODE_OUT = ("name", "layer", "identifier", "properties", "unique", "indexed", "required", "datatypes", "values")
PATTERN_OUT = ("type", "from", "to", "properties", "at_most_one", "required", "datatypes", "values")

# Rule "raw triples": how an entity carries its identifying value
RAW_NAME_PG = "name"
RAW_NAME_RDF = {"property": "rdfs:label", "iri": "http://www.w3.org/2000/01/rdf-schema#label"}

# Rule "pg-to-rdf": how the data carries a property on a relationship
EDGE_PROPERTY_FORM = {
    "form": "rdf12-annotation",
    "written_as": "s p o {| q v |} .",
    "note": "the statement s p o itself is always asserted",
}

STATEMENT = re.compile(
    r"^CREATE (CONSTRAINT|INDEX) (\w+) IF NOT EXISTS FOR \((\w+):(\w+)\) "
    r"(?:REQUIRE \3\.(\w+) IS UNIQUE|ON \(\3\.(\w+)\));$"
)
PATTERN = re.compile(r"^//\s+\((\w+)\)-\[:(\w+)(?:\s*\{([^}]*)\})?\]->\(([\w|]+)\)")
SECTION = re.compile(r"^// (\d)\. (.+?) \(")
HEADER_LAYER = re.compile(r"^//\s+(\d)\. .+? - (.+)$")


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def split_properties(text: str) -> list[dict]:
    """'uid, shape (pot|lid|...), weight_g' -> [{'name': 'uid'}, {'name': 'shape', 'note': ...}, ...]"""
    parts, depth, current = [], 0, ""
    for char in text:
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        if char == "," and depth == 0:
            parts.append(current)
            current = ""
        else:
            current += char
    parts.append(current)
    properties = []
    for part in parts:
        part = part.strip()
        if not part:
            continue
        match = re.match(r"^(\w+)\s*(?:\((.*)\))?$", part)
        if not match:
            raise ValueError(f"cannot read property spec: {part!r}")
        entry = {"name": match.group(1)}
        if match.group(2):
            entry["note"] = match.group(2)
        properties.append(entry)
    return properties


def parse_curated(path: Path) -> dict:
    """Read the curated Cypher file into node types and relationship patterns."""
    text = path.read_text(encoding="utf-8")
    head, _, reference = text.partition("// RELATIONSHIP PATTERNS (Reference)")

    header_layer: dict[str, int] = {}
    for line in head.splitlines():
        match = HEADER_LAYER.match(line)
        if match:
            for label in match.group(2).split(","):
                header_layer[label.strip()] = int(match.group(1))

    node_types: list[dict] = []
    by_name: dict[str, dict] = {}
    section_layer = None
    for paragraph in re.split(r"\n\s*\n", head):
        lines = [line for line in paragraph.splitlines() if line.strip()]
        statements = [line for line in lines if line.startswith("CREATE ")]
        comments = [line for line in lines if not line.startswith("CREATE ")]
        if not statements:
            for line in comments:
                match = SECTION.match(line)
                if match:
                    section_layer = int(match.group(1))
            continue
        labels = set()
        for statement in statements:
            match = STATEMENT.match(statement)
            if not match:
                raise ValueError(f"cannot read statement: {statement}")
            labels.add(match.group(4))
        if len(labels) != 1:
            raise ValueError(f"one paragraph, several labels: {sorted(labels)}")
        label = labels.pop()
        node = by_name.get(label)
        if node is None:
            node = {
                "name": label,
                "layer": header_layer.get(label, section_layer),
                "section_layer": section_layer,
                "properties": [],
                "unique": [],
                "indexed": [],
                "comment_lines": [],
                "statements": [],
            }
            by_name[label] = node
            node_types.append(node)
        node["comment_lines"] += comments
        node["statements"] += statements

        def add_property(name: str, origin: str, note: str | None = None) -> None:
            for existing in node["properties"]:
                if existing["name"] == name:
                    if note and "note" not in existing:
                        existing["note"] = note
                    return
            entry = {"name": name, "declared_by": origin}
            if note:
                entry["note"] = note
            node["properties"].append(entry)

        for statement in statements:
            kind, _, _, _, unique_prop, index_prop = STATEMENT.match(statement).groups()
            if kind == "CONSTRAINT":
                node["unique"].append(unique_prop)
                add_property(unique_prop, "constraint")
            else:
                node["indexed"].append(index_prop)
                add_property(index_prop, "index")
        # "// Properties: a, b (note)," may continue on the following comment lines
        spec, collecting = "", False
        for line in comments:
            body = line[2:].strip()
            if body.startswith("Properties:"):
                spec, collecting = body[len("Properties:"):], True
            elif collecting and spec.rstrip().endswith(","):
                spec += " " + body
            else:
                collecting = False
        for entry in split_properties(spec):
            add_property(entry["name"], "comment", entry.get("note"))

    patterns = []
    group = None
    for line in reference.splitlines():
        match = PATTERN.match(line)
        if match:
            source, rel_type, props, targets = match.groups()
            patterns.append({
                "type": rel_type,
                "from": source,
                "to": targets.split("|"),
                "properties": [p.strip() for p in props.split(",")] if props else [],
                "group": group,
            })
        elif re.match(r"^// \S.*:$", line.strip()):
            group = line.strip()[3:-1]
    return {"node_types": node_types, "relationship_patterns": patterns}


def counts(schema: dict) -> dict:
    return {
        "node_types": len(schema["node_types"]),
        "relationship_types": len({p["type"] for p in schema["relationship_patterns"]}),
        "relationship_patterns": len(schema["relationship_patterns"]),
        "uniqueness_constraints": sum(len(n["unique"]) for n in schema["node_types"]),
        "indexes": sum(len(n["indexed"]) for n in schema["node_types"]),
    }


def check_references(schema: dict) -> None:
    names = {n["name"] for n in schema["node_types"]}
    for pattern in schema["relationship_patterns"]:
        for label in [pattern["from"], *pattern["to"]]:
            if label not in names:
                raise ValueError(f"pattern names an undeclared node type: {label}")


# --------------------------------------------------------------------------- flat rule

def derive_flat(curated: dict) -> dict:
    """Rule 'flat': remove the node types that reify a rule; re-attach what hung on them."""
    owners = [p["from"] for p in curated["relationship_patterns"]
              if p["type"] == FLAT_RULE_OWNER_VIA and set(p["to"]) & set(FLAT_REMOVED)]
    nodes = [copy.deepcopy(n) for n in curated["node_types"] if n["name"] not in FLAT_REMOVED]
    for node in nodes:  # no layers in the flat variant
        node["layer"] = None
        node["section_layer"] = None
    patterns: list[dict] = []

    def add(pattern: dict) -> None:
        key = (pattern["from"], pattern["type"], tuple(pattern["to"]))
        if key not in {(p["from"], p["type"], tuple(p["to"])) for p in patterns}:
            patterns.append(pattern)

    for pattern in curated["relationship_patterns"]:
        targets = [t for t in pattern["to"] if t not in FLAT_REMOVED]
        source_removed = pattern["from"] in FLAT_REMOVED
        if not targets:
            continue  # leads only to a removed node type: HAS_RULE, HAS_CONDITION
        if not source_removed:
            add({**pattern, "to": targets, "group": None})
        elif pattern["from"] == "DisposalRule":
            for owner in owners:  # what a rule pointed to now hangs on the rule's owner
                add({**pattern, "from": owner, "to": targets, "group": None})
        # patterns that start at Condition are dropped with it: IF_TRUE, IF_FALSE
    nodes.sort(key=lambda n: n["name"])
    patterns.sort(key=lambda p: (p["from"], p["type"], p["to"]))
    flat = {"node_types": nodes, "relationship_patterns": patterns}
    check_references(flat)
    return flat


# --------------------------------------------------------------------------- raw triples

def raw_triples_pg() -> dict:
    """Generic entity/assertion pattern: nothing typed, nothing required."""
    def node(name: str, properties: list[str]) -> dict:
        return {
            "name": name, "layer": None, "section_layer": None,
            "properties": [{"name": p, "declared_by": "pattern"} for p in properties],
            "unique": [], "indexed": [], "comment_lines": [], "statements": [],
        }
    return {
        "node_types": [node("Entity", [RAW_NAME_PG]), node("Assertion", ["predicate", "value"])],
        "relationship_patterns": [
            {"type": "SUBJECT", "from": "Assertion", "to": ["Entity"], "properties": [], "group": None},
            {"type": "OBJECT", "from": "Assertion", "to": ["Entity"], "properties": [], "group": None},
        ],
    }


# --------------------------------------------------------------------------- writers

RULE = "// " + "=" * 77


def pattern_line(pattern: dict) -> str:
    props = " {" + ", ".join(pattern["properties"]) + "}" if pattern["properties"] else ""
    line = f"//   ({pattern['from']})-[:{pattern['type']}{props}]->({'|'.join(pattern['to'])})"
    notes = ["at most one per start node"] if pattern.get("at_most_one") else []
    notes += [f"{name}: {note}" for name in pattern["properties"]
              if (note := property_note(pattern, name))]
    return line + ("   // " + "; ".join(notes) if notes else "")


def property_note(owner: dict, name: str) -> str:
    """What an induced schema declares for one property of a node type or a pattern, for a comment."""
    parts = ["identifier"] if owner.get("identifier") == name else []
    if name in owner.get("datatypes", {}):
        parts.append(owner["datatypes"][name])
    if name in owner.get("required", []):
        parts.append("required")
    if name in owner.get("values", {}):
        parts.append("one of " + "|".join(json.dumps(value, ensure_ascii=False) for value in owner["values"][name]))
    return ", ".join(parts)


def turtle_literal(value) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return json.dumps(value, ensure_ascii=False)        # text in double quotes; a number as written


def write_cql(title: str, origin: list[str], schema: dict) -> str:
    out = [RULE, f"// {title}", RULE, *[f"// {line}" for line in origin], RULE, ""]
    for node in schema["node_types"]:
        if node["comment_lines"]:
            out += node["comment_lines"]
        else:
            out.append(f"// {node['name']}")
            if node["properties"]:
                out.append("// Properties: " + ", ".join(p["name"] for p in node["properties"]))
        out += node["statements"]
        out.append("")
    out += [
        "", RULE, "// RELATIONSHIP PATTERNS (Reference)", RULE,
        "// These are not executable - just documentation of expected relationships:", "//",
        *[pattern_line(p) for p in schema["relationship_patterns"]],
        "//", RULE, "",
    ]
    return "\n".join(out)


def union(prefix: str, names: list[str]) -> str:
    if len(names) == 1:
        return f"{prefix}:{names[0]}"
    return "[ a owl:Class ; owl:unionOf ( " + " ".join(f"{prefix}:{n}" for n in names) + " ) ]"


def ordered(values) -> list[str]:
    seen: list[str] = []
    for value in values:
        if value not in seen:
            seen.append(value)
    return seen


def write_ttl(slug: str, title: str, origin: list[str], schema: dict) -> str:
    """Rule 'pg-to-rdf': labels to classes, properties and relationship types to
    properties, uniqueness to keys, patterns to shapes."""
    namespace = NAMESPACE.format(slug=slug)
    nodes, patterns = schema["node_types"], schema["relationship_patterns"]
    labels = [n["name"] for n in nodes]
    node_props = ordered(p["name"] for n in nodes for p in n["properties"])
    edge_props = ordered(p for pattern in patterns for p in pattern["properties"])
    rel_types = ordered(p["type"] for p in patterns)
    overlap = (set(labels) & set(rel_types)) | (set(labels) & set(node_props + edge_props)) \
        | (set(rel_types) & set(node_props + edge_props))
    if overlap:
        raise ValueError(f"one name used for two kinds of term: {sorted(overlap)}")

    # What only an induced schema declares: datatypes, required properties, value lists, "at most one".
    datatypes = {dt for owner in [*nodes, *patterns] for dt in owner.get("datatypes", {}).values()}
    value_shapes = any(node.get("datatypes") or node.get("required") or node.get("values") for node in nodes)
    edge_constraints = any(p.get("datatypes") or p.get("required") or p.get("values") for p in patterns)

    out = [f"# {title}", *[f"# {line}" for line in origin],
           "# Enforced by the harness validator, not by the store: see declaration.json.", ""]
    out += [
        f"@prefix fessi: <{namespace}> .",
        f"@prefix map:   <{MAPPING_NS}> .",
        "@prefix owl:   <http://www.w3.org/2002/07/owl#> .",
        "@prefix rdfs:  <http://www.w3.org/2000/01/rdf-schema#> .",
        "@prefix sh:    <http://www.w3.org/ns/shacl#> .",
        *(["@prefix xsd:   <http://www.w3.org/2001/XMLSchema#> ."] if datatypes - {"quantity"} else []),
        "",
        f"<{namespace[:-1]}> a owl:Ontology .",
        "map:edgeProperty a owl:AnnotationProperty .",
        *(["map:quantity a rdfs:Datatype .        # a number with a unit of measure"] if "quantity" in datatypes else []),
        "",
        "# Classes: one per node label",
    ]
    out += [f"fessi:{label} a owl:Class ." for label in labels]

    out += ["", "# Datatype properties: one per property name; the domain lists the classes that carry it"]
    for prop in node_props + [p for p in edge_props if p not in node_props]:
        carriers = [n["name"] for n in nodes if prop in [p["name"] for p in n["properties"]]]
        domain = f" ;\n    rdfs:domain {union('fessi', carriers)}" if carriers else ""
        out.append(f"fessi:{prop} a owl:DatatypeProperty{domain} .")

    out += ["", "# Object properties: one per relationship type; domain and range are the union over its patterns"]
    for rel in rel_types:
        own = [p for p in patterns if p["type"] == rel]
        domain = union("fessi", ordered(p["from"] for p in own))
        range_ = union("fessi", ordered(t for p in own for t in p["to"]))
        out.append(f"fessi:{rel} a owl:ObjectProperty ;\n    rdfs:domain {domain} ;\n    rdfs:range {range_} .")

    with_edge_props = [(rel, ordered(p for pattern in patterns if pattern["type"] == rel
                                     for p in pattern["properties"])) for rel in rel_types]
    with_edge_props = [(rel, props) for rel, props in with_edge_props if props]
    if with_edge_props:
        out += ["", "# Relationship properties: carried by statements of the object property.",
                "# In the data: an RDF 1.2 annotation on the statement, s p o {| q v |} .",
                "# The statement s p o itself is always asserted."]
        for rel, props in with_edge_props:
            out.append(f"fessi:{rel} map:edgeProperty " + " , ".join(f"fessi:{p}" for p in props) + " .")
        if edge_constraints:
            out.append("# What is declared for such a property (datatype, required, allowed values), per pattern:")
            for pattern in patterns:
                notes = [f"{name}: {note}" for name in pattern["properties"] if (note := property_note(pattern, name))]
                if notes:
                    out.append(f"#   fessi:{pattern['from']} fessi:{pattern['type']} "
                               f"{' | '.join('fessi:' + t for t in pattern['to'])}   {'; '.join(notes)}")

    keyed = [n for n in nodes if n["unique"]]
    if keyed:
        out += ["", "# Keys: one per uniqueness constraint"]
        for node in keyed:
            keys = " , ".join(f"( fessi:{p} )" for p in node["unique"])
            out.append(f"fessi:{node['name']} owl:hasKey {keys} .")

    out += ["", "# Shapes: the exact relationship patterns, per source class"
            + ("; and what is declared for a property with values" if value_shapes else "")]
    for node in nodes:
        label = node["name"]
        own = [p for p in patterns if p["from"] == label]
        entries = []
        for prop in node["properties"]:                 # only an induced schema declares anything here
            name, parts = prop["name"], []
            if name in node.get("datatypes", {}):
                parts.append(f"sh:datatype {XSD_TERM[node['datatypes'][name]]}")
            if name in node.get("required", []):
                parts.append("sh:minCount 1")
            if name in node.get("values", {}):
                parts.append("sh:in ( " + " ".join(turtle_literal(v) for v in node["values"][name]) + " )")
            if parts:
                entries.append(f"    sh:property [ sh:path fessi:{name} ; " + " ; ".join(parts) + " ]")
        if not own and not entries:
            continue
        lines = [f"fessi:{label}Shape a sh:NodeShape ;", f"    sh:targetClass fessi:{label} ;"]
        for rel in ordered(p["type"] for p in own):
            of_rel = [p for p in own if p["type"] == rel]
            targets = ordered(t for p in of_rel for t in p["to"])
            # "at most one" is declared per pattern; the shape can say it where every pattern of this class
            # and property does
            at_most_one = " ; sh:maxCount 1" if all(p.get("at_most_one") for p in of_rel) else ""
            if len(targets) == 1:
                entries.append(f"    sh:property [ sh:path fessi:{rel} ; sh:class fessi:{targets[0]}{at_most_one} ]")
            else:
                alternatives = " ".join(f"[ sh:class fessi:{t} ]" for t in targets)
                entries.append(f"    sh:property [ sh:path fessi:{rel} ; sh:or ( {alternatives} ){at_most_one} ]")
        lines.append(" ;\n".join(entries) + " .")
        out += lines
    return "\n".join(out) + "\n"


def declaration(realisation: str, status: str, source: dict | None, schema: dict | None,
                rdf_slug: str | None = None, schema_file: str | None = None,
                enforcement: dict | None = None, identifying_value: dict | None = None,
                induction: dict | None = None) -> str:
    treatment, arm = realisation.split("/")
    body: dict = {
        "realisation": realisation,
        "treatment": treatment.split("-")[0],
        "arm": arm,
        "status": status,
        "rules_version": RULES_VERSION,
        "schema_file": schema_file,
        "derived_from": source,
    }
    if induction is not None:
        body["induction"] = induction
    if arm == "rdf" and rdf_slug:
        body["namespace"] = NAMESPACE.format(slug=rdf_slug)
    if arm == "rdf":
        body["data_namespace"] = DATA_NAMESPACE
    if enforcement is not None:
        body["enforcement"] = enforcement
    if identifying_value is not None:
        body["identifying_value"] = identifying_value
    if arm == "rdf" and schema is not None and any(p["properties"] for p in schema["relationship_patterns"]):
        body["relationship_property_form"] = EDGE_PROPERTY_FORM
    if schema is not None:
        body["counts"] = counts(schema)
        body["node_types"] = [
            {key: node[key] for key in NODE_OUT if key in node}
            for node in schema["node_types"]
        ]
        body["relationship_patterns"] = [
            {key: pattern[key] for key in PATTERN_OUT if key in pattern}
            for pattern in schema["relationship_patterns"]
        ]
    return json.dumps(body, indent=2, ensure_ascii=False) + "\n"


def placeholder(comment: str, treatment: str, arm: str) -> str:
    return "\n".join([
        f"{comment} PLACEHOLDER. {treatment}, {arm} realisation.",
        f"{comment} The induced schema does not exist yet. The main agent model produces it once per",
        f"{comment} arm from the development units; the result replaces this file and is frozen.",
        f"{comment} Do not build on the content of this file.",
        "",
    ])


def induced_files(folder: Path, arm: str) -> dict[str, str] | None:
    """The generated files of one S2 realisation, by file name, from the reply in its folder.

    None when there is no reply: the realisation stays a placeholder. A reply that cannot be used raises
    induced.Refused with every fault; nothing is made of such a reply."""
    reply = folder / REPLY
    if not reply.is_file():
        return None
    realisation = f"S2-induced/{arm}"
    reading = induced.read(reply.read_text(encoding="utf-8"), arm)
    if not reading.usable:
        raise induced.Refused(f"{realisation}/{REPLY}", reading.faults)
    schema, done = induced.to_schema(reading)
    check_references(schema)
    source = {"file": f"schemas/treatments/{realisation}/{REPLY}", "sha256": sha256(reply)}
    origin = [
        f"Written from the reply of the schema induction, {source['file']}",
        f"(sha256 {source['sha256'][:16]}...), by schemas/treatments/derive.py, rules {RULES_VERSION}.",
        "Generated; do not edit by hand. The schema is the model's: nothing was renamed, dropped or added,",
        "except that the identifier of a type counts as unique (see declaration.json, \"induction\").",
        STATUS_LINE,
    ]
    if arm == "pg":
        for node in schema["node_types"]:
            described = [f"{prop['name']} ({note})" if (note := property_note(node, prop["name"])) else prop["name"]
                         for prop in node["properties"]]
            node["comment_lines"] = [f"// {node['name']}", "// Properties: " + ", ".join(described)]
            node["statements"] = [
                f"CREATE CONSTRAINT {node['name'].lower()}_{prop}_unique IF NOT EXISTS "
                f"FOR (n:{node['name']}) REQUIRE n.{prop} IS UNIQUE;" for prop in node["unique"]]
        schema_name = "schema.cql"
        text = write_cql("Induced schema (S2), property-graph realisation", origin, schema)
        slug = None
    else:
        schema_name = "schema.ttl"
        text = write_ttl(INDUCED_SLUG, "Induced schema (S2), RDF realisation", origin, schema)
        slug = INDUCED_SLUG
    return {
        schema_name: text,
        "declaration.json": declaration(realisation, "draft", source, schema, rdf_slug=slug, schema_file=schema_name,
                                        enforcement=ENFORCEMENT_INDUCED, induction=done),
    }


def build(here: Path = HERE, refused: dict[str, list[str]] | None = None) -> dict[Path, str | bytes]:
    """Every generated file with its content. `here` is the folder the treatments stand in.

    A reply of the induction that cannot be used raises induced.Refused, unless `refused` is given: then
    the faults are put there under the realisation's name and no file of that arm is in the result."""
    curated = parse_curated(CURATED)
    check_references(curated)
    expected = {"node_types": 17, "relationship_types": 21, "relationship_patterns": 33,
                "uniqueness_constraints": 21, "indexes": 11}
    if counts(curated) != expected:
        raise ValueError(f"curated schema read as {counts(curated)}, expected {expected}")
    flat = derive_flat(curated)
    raw = raw_triples_pg()
    source = {"file": str(CURATED.relative_to(REPO)), "sha256": sha256(CURATED)}
    origin = [
        f"Derived from {source['file']} (sha256 {source['sha256'][:16]}...)",
        f"by schemas/treatments/derive.py, rules {RULES_VERSION}. Generated; do not edit by hand.",
        STATUS_LINE,
    ]
    files: dict[Path, str | bytes] = {}

    # S4, curated: the Cypher file is copied byte for byte
    files[here / "S4-curated/pg/schema.cql"] = CURATED.read_bytes()
    files[here / "S4-curated/pg/declaration.json"] = declaration(
        "S4-curated/pg", "draft", source, curated, schema_file="schema.cql",
        enforcement=ENFORCEMENT_DECLARED)
    files[here / "S4-curated/rdf/schema.ttl"] = write_ttl(
        "s4", "Fessi curated v5.0 (S4), RDF realisation", origin, curated)
    files[here / "S4-curated/rdf/declaration.json"] = declaration(
        "S4-curated/rdf", "draft", source, curated, rdf_slug="s4", schema_file="schema.ttl",
        enforcement=ENFORCEMENT_DECLARED)

    # S3, flat
    files[here / "S3-flat/pg/schema.cql"] = write_cql(
        "Fessi flat (S3), property-graph realisation", origin, flat)
    files[here / "S3-flat/pg/declaration.json"] = declaration(
        "S3-flat/pg", "draft", source, flat, schema_file="schema.cql",
        enforcement=ENFORCEMENT_DECLARED)
    files[here / "S3-flat/rdf/schema.ttl"] = write_ttl(
        "s3", "Fessi flat (S3), RDF realisation", origin, flat)
    files[here / "S3-flat/rdf/declaration.json"] = declaration(
        "S3-flat/rdf", "draft", source, flat, rdf_slug="s3", schema_file="schema.ttl",
        enforcement=ENFORCEMENT_DECLARED)

    # S2, induced: from the reply of the induction where there is one, else placeholders
    for arm, comment, schema_name, arm_name in (("pg", "//", "schema.cql", "property-graph"),
                                                 ("rdf", "#", "schema.ttl", "RDF")):
        folder = here / "S2-induced" / arm
        try:
            made = induced_files(folder, arm)
        except induced.Refused as refusal:
            if refused is None:
                raise
            refused[f"S2-induced/{arm}"] = refusal.faults
            continue
        if made is None:
            made = {schema_name: placeholder(comment, "S2 (induced)", arm_name),
                    "declaration.json": declaration(f"S2-induced/{arm}", "placeholder", None, None,
                                                    schema_file=schema_name)}
        for name, content in made.items():
            files[folder / name] = content

    # S1, raw triples: a generic pattern in the property graph, no schema file in RDF
    raw_origin = [
        f"Written by schemas/treatments/derive.py, rules {RULES_VERSION}. Generated; do not edit by hand.",
        "Nothing is declared: no constraint, no index, no required property, no fixed predicate.",
        "An Assertion states one thing about its SUBJECT. Its predicate is free text. The object is",
        "either a literal in the property value, or another Entity reached through OBJECT.",
        "An Entity carries its name in the property name: the shared rules of the validator ask",
        "every entity for an identifying value, whatever the schema.",
        STATUS_LINE,
    ]
    files[here / "S1-raw-triples/pg/schema.cql"] = write_cql(
        "Raw triples (S1), property-graph realisation", raw_origin, raw)
    files[here / "S1-raw-triples/pg/declaration.json"] = declaration(
        "S1-raw-triples/pg", "draft", None, raw, schema_file="schema.cql",
        enforcement=ENFORCEMENT_NONE, identifying_value={"node_type": "Entity", "property": RAW_NAME_PG})
    empty = {"node_types": [], "relationship_patterns": []}
    files[here / "S1-raw-triples/rdf/declaration.json"] = declaration(
        "S1-raw-triples/rdf", "draft", None, empty, schema_file=None,
        enforcement=ENFORCEMENT_NONE, identifying_value=RAW_NAME_RDF)
    return files


def run(here: Path = HERE, check: bool = False) -> int:
    """Write every generated file under `here`, or with `check` compare them with what is there.
    Returns 0, or 1 when a file differs (check) or a reply of the induction was refused."""
    refused: dict[str, list[str]] = {}
    files = build(here, refused)
    shown = here.relative_to(REPO) if here.is_relative_to(REPO) else here
    stale = []
    for path, content in files.items():
        data = content if isinstance(content, bytes) else content.encode("utf-8")
        if check:
            if not path.exists() or path.read_bytes() != data:
                stale.append(path)
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
    if check and stale:
        print("differs from derive.py:", *[str(p.relative_to(here.parent.parent)) for p in stale], sep="\n  ")
    print(f"{'checked' if check else 'wrote'} {len(files)} files under {shown}")
    for realisation, faults in refused.items():
        print(f"refused  {realisation}/{REPLY}: {len(faults)} fault{'s' if len(faults) != 1 else ''}; "
              f"nothing of this realisation was {'checked' if check else 'written'}", file=sys.stderr)
        for fault in faults:
            print(f"  - {fault}", file=sys.stderr)
    return 1 if refused or (check and stale) else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--check", action="store_true", help="compare with the files on disk, write nothing")
    args = parser.parse_args()
    return run(check=args.check)


if __name__ == "__main__":
    sys.exit(main())
