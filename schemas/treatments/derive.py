#!/usr/bin/env python3
"""Derive the schema realisations of the four treatments.

Input:  schemas/v5.0/schema.cql (the curated fessi schema, never changed here).
Output: one folder per treatment under schemas/treatments/, with one sub-folder
        per arm (pg, rdf). Every file in those folders is generated.

    python schemas/treatments/derive.py           # write all files
    python schemas/treatments/derive.py --check   # fail if a file on disk differs

The three rules applied here (flat derivation, property graph to RDF, raw-triple
pattern) are described in schemas/treatments/README.md. Standard library only.
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
REPO = HERE.parent.parent
CURATED = REPO / "schemas" / "v5.0" / "schema.cql"

RULES_VERSION = "draft-0.1"
NAMESPACE = "https://fessi.example/schema/{slug}#"
MAPPING_NS = "https://fessi.example/mapping#"

# Rule "flat": node types that reify a rule or a decision step, and the node
# types that could own a rule in the curated schema (the sources of HAS_RULE).
FLAT_REMOVED = ("DisposalRule", "Condition")
FLAT_RULE_OWNER_VIA = "HAS_RULE"

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
        "node_types": [node("Entity", []), node("Assertion", ["predicate", "value"])],
        "relationship_patterns": [
            {"type": "SUBJECT", "from": "Assertion", "to": ["Entity"], "properties": [], "group": None},
            {"type": "OBJECT", "from": "Assertion", "to": ["Entity"], "properties": [], "group": None},
        ],
    }


# --------------------------------------------------------------------------- writers

RULE = "// " + "=" * 77


def pattern_line(pattern: dict) -> str:
    props = " {" + ", ".join(pattern["properties"]) + "}" if pattern["properties"] else ""
    return f"//   ({pattern['from']})-[:{pattern['type']}{props}]->({'|'.join(pattern['to'])})"


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

    out = [f"# {title}", *[f"# {line}" for line in origin], ""]
    out += [
        f"@prefix fessi: <{namespace}> .",
        f"@prefix map:   <{MAPPING_NS}> .",
        "@prefix owl:   <http://www.w3.org/2002/07/owl#> .",
        "@prefix rdfs:  <http://www.w3.org/2000/01/rdf-schema#> .",
        "@prefix sh:    <http://www.w3.org/ns/shacl#> .",
        "",
        f"<{namespace[:-1]}> a owl:Ontology .",
        "map:edgeProperty a owl:AnnotationProperty .",
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
        out += ["", "# Relationship properties: carried by statements of the object property"]
        for rel, props in with_edge_props:
            out.append(f"fessi:{rel} map:edgeProperty " + " , ".join(f"fessi:{p}" for p in props) + " .")

    keyed = [n for n in nodes if n["unique"]]
    if keyed:
        out += ["", "# Keys: one per uniqueness constraint"]
        for node in keyed:
            keys = " , ".join(f"( fessi:{p} )" for p in node["unique"])
            out.append(f"fessi:{node['name']} owl:hasKey {keys} .")

    out += ["", "# Shapes: the exact relationship patterns, per source class"]
    for label in labels:
        own = [p for p in patterns if p["from"] == label]
        if not own:
            continue
        lines = [f"fessi:{label}Shape a sh:NodeShape ;", f"    sh:targetClass fessi:{label} ;"]
        entries = []
        for rel in ordered(p["type"] for p in own):
            targets = ordered(t for p in own if p["type"] == rel for t in p["to"])
            if len(targets) == 1:
                entries.append(f"    sh:property [ sh:path fessi:{rel} ; sh:class fessi:{targets[0]} ]")
            else:
                alternatives = " ".join(f"[ sh:class fessi:{t} ]" for t in targets)
                entries.append(f"    sh:property [ sh:path fessi:{rel} ; sh:or ( {alternatives} ) ]")
        lines.append(" ;\n".join(entries) + " .")
        out += lines
    return "\n".join(out) + "\n"


def declaration(realisation: str, status: str, source: dict | None, schema: dict | None,
                rdf_slug: str | None = None, schema_file: str | None = None) -> str:
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
    if arm == "rdf" and rdf_slug:
        body["namespace"] = NAMESPACE.format(slug=rdf_slug)
    if schema is not None:
        body["counts"] = counts(schema)
        body["node_types"] = [
            {key: node[key] for key in ("name", "layer", "properties", "unique", "indexed")}
            for node in schema["node_types"]
        ]
        body["relationship_patterns"] = [
            {key: pattern[key] for key in ("type", "from", "to", "properties")}
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


def build() -> dict[Path, str | bytes]:
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
        "Status: draft, not reviewed, not frozen.",
    ]
    files: dict[Path, str | bytes] = {}

    # S4, curated: the Cypher file is copied byte for byte
    files[HERE / "S4-curated/pg/schema.cql"] = CURATED.read_bytes()
    files[HERE / "S4-curated/pg/declaration.json"] = declaration(
        "S4-curated/pg", "draft", source, curated, schema_file="schema.cql")
    files[HERE / "S4-curated/rdf/schema.ttl"] = write_ttl(
        "s4", "Fessi curated v5.0 (S4), RDF realisation", origin, curated)
    files[HERE / "S4-curated/rdf/declaration.json"] = declaration(
        "S4-curated/rdf", "draft", source, curated, rdf_slug="s4", schema_file="schema.ttl")

    # S3, flat
    files[HERE / "S3-flat/pg/schema.cql"] = write_cql(
        "Fessi flat (S3), property-graph realisation", origin, flat)
    files[HERE / "S3-flat/pg/declaration.json"] = declaration(
        "S3-flat/pg", "draft", source, flat, schema_file="schema.cql")
    files[HERE / "S3-flat/rdf/schema.ttl"] = write_ttl(
        "s3", "Fessi flat (S3), RDF realisation", origin, flat)
    files[HERE / "S3-flat/rdf/declaration.json"] = declaration(
        "S3-flat/rdf", "draft", source, flat, rdf_slug="s3", schema_file="schema.ttl")

    # S2, induced: placeholders
    files[HERE / "S2-induced/pg/schema.cql"] = placeholder("//", "S2 (induced)", "property-graph")
    files[HERE / "S2-induced/pg/declaration.json"] = declaration(
        "S2-induced/pg", "placeholder", None, None, schema_file="schema.cql")
    files[HERE / "S2-induced/rdf/schema.ttl"] = placeholder("#", "S2 (induced)", "RDF")
    files[HERE / "S2-induced/rdf/declaration.json"] = declaration(
        "S2-induced/rdf", "placeholder", None, None, schema_file="schema.ttl")

    # S1, raw triples: a generic pattern in the property graph, no schema file in RDF
    raw_origin = [
        f"Written by schemas/treatments/derive.py, rules {RULES_VERSION}. Generated; do not edit by hand.",
        "Nothing is declared: no constraint, no index, no required property, no fixed predicate.",
        "An Assertion states one thing about its SUBJECT. Its predicate is free text. The object is",
        "either a literal in the property value, or another Entity reached through OBJECT.",
        "Status: draft, not reviewed, not frozen.",
    ]
    files[HERE / "S1-raw-triples/pg/schema.cql"] = write_cql(
        "Raw triples (S1), property-graph realisation", raw_origin, raw)
    files[HERE / "S1-raw-triples/pg/declaration.json"] = declaration(
        "S1-raw-triples/pg", "draft", None, raw, schema_file="schema.cql")
    empty = {"node_types": [], "relationship_patterns": []}
    files[HERE / "S1-raw-triples/rdf/declaration.json"] = declaration(
        "S1-raw-triples/rdf", "draft", None, empty, schema_file=None)
    return files


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("--check", action="store_true", help="compare with the files on disk, write nothing")
    args = parser.parse_args()
    files = build()
    stale = []
    for path, content in files.items():
        data = content if isinstance(content, bytes) else content.encode("utf-8")
        if args.check:
            if not path.exists() or path.read_bytes() != data:
                stale.append(path)
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
    if args.check and stale:
        print("differs from derive.py:", *[str(p.relative_to(REPO)) for p in stale], sep="\n  ")
        return 1
    print(f"{'checked' if args.check else 'wrote'} {len(files)} files under {HERE.relative_to(REPO)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
