#!/usr/bin/env python3
"""Render the schema description an agent is shown, from a realisation's declaration.json.

    python schemas/treatments/render.py                  # write description.construction.txt per realisation
    python schemas/treatments/render.py --check          # fail if a file on disk differs, write nothing
    python schemas/treatments/render.py S4-curated/pg    # print one description
    python schemas/treatments/render.py --sizes          # characters per section

The text fills the slot `schema_description` of the construction prompt. It is the declared-only
mode of the renderer: what the realisation declares, nothing a graph holds. Every word of it comes
from the declaration and from the fixed sentences below, which are the same for every realisation
of an arm. Nothing is written by hand per schema: no comment of a schema file, no note, no layer.

Sections, in this order: identification; types; properties; relationships with their end types;
constraints by kind; counts. A section without content says so.

An induced schema (S2) can declare what no other realisation does: which property identifies a
type, required properties, datatypes, lists of allowed values, and "at most one" for a relationship
pattern. These are shown in square brackets after the property, and as `at most one` after the
pattern; the constraint lines say what happens to a write that breaks them, from the declaration's
`enforcement` block like every other strength. A realisation that declares none of it gets exactly
the text it had before.

Two realisations cannot be rendered yet and are refused with the reason: an induced schema that
is still a placeholder, and an RDF realisation whose declaration names no namespace for the IRIs
of data (key `data_namespace`).
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
OUTPUT = "description.construction.txt"

# The keys of a declaration this renderer knows. An unknown key stops the rendering, so that
# nothing a declaration states is dropped without anybody noticing.
TOP_KEYS = {"realisation", "treatment", "arm", "status", "rules_version", "schema_file", "derived_from",
            "enforcement", "counts", "node_types", "relationship_patterns", "identifying_value",
            "namespace", "data_namespace", "relationship_property_form", "induction"}
NODE_KEYS = {"name", "layer", "properties", "unique", "indexed", "identifier", "required", "datatypes", "values"}
PROPERTY_KEYS = {"name", "declared_by", "note"}
PATTERN_KEYS = {"type", "from", "to", "properties", "at_most_one", "required", "datatypes", "values"}
ENFORCEMENT_KEYS = ("node_type_outside_the_list", "relationship_outside_the_patterns",
                    "uniqueness_violation", "undeclared_property")
# The strengths only a schema with such constraints states (an induced schema): constraint kind to key.
ENFORCEMENT_MORE = {"required": "required_property_missing", "datatypes": "datatype_violation",
                    "values": "value_outside_the_list", "at_most_one": "cardinality_violation"}
STRENGTHS = ("reject", "accept_and_count", "nothing_declared")
DATATYPES = ("string", "integer", "decimal", "boolean", "date", "datetime", "quantity")

SCHEMA_PREFIX = "fessi"          # the prefix the generated .ttl files use for the schema's namespace
NONE = "none declared."

# The fixed sentences. `pg` and `rdf` differ in the words for the same thing, never in content.
WORDS = {
    "pg": {
        "identification": "Schema {treatment} for the property graph, version {version}, as declared.",
        "types": "Node types ({n}):",
        "properties": "Properties (node type, then its properties{marked}):",
        "marked": "; a property marked * is unique, see the constraints",
        "bracketed": "; in square brackets what is declared for a property: identifier (its value identifies the "
                     "node), its datatype, required, and after \"one of\" its allowed values",
        "relationships": "Relationships ({patterns} patterns of {types} types{braces}{at_most_one}):",
        "braces": "; in braces the properties a relationship of that type carries",
        "at_most_one": "; a pattern followed by \"at most one\" allows a node to start only one relationship of "
                       "that pattern",
        "required": "a node carries a value for every property of its type marked required, and a relationship for "
                    "every property of its pattern marked required. A write that breaks this is rejected.",
        "required_open": "a node is meant to carry a value for every property of its type marked required, and a "
                         "relationship for every property of its pattern marked required. A write that breaks this "
                         "is accepted.",
        "datatypes": "the value of a property is a value of the datatype named in its brackets; a quantity is a number "
                     "with a unit of measure. A write that breaks this is rejected.",
        "datatypes_open": "the value of a property is meant to be a value of the datatype named in its brackets; a "
                          "quantity is a number with a unit of measure. A write that breaks this is accepted.",
        "cardinality": "a node starts at most one relationship of a pattern followed by \"at most one\". A write that "
                       "breaks this is rejected.",
        "cardinality_open": "a node is meant to start at most one relationship of a pattern followed by \"at most "
                            "one\". A write that breaks this is accepted.",
        "values": "the value of a property with \"one of\" in its brackets is one of the values listed there. A write "
                  "that breaks this is rejected.",
        "values_open": "the value of a property with \"one of\" in its brackets is meant to be one of the values "
                       "listed there. A write that breaks this is accepted.",
        "identifying": "The value that identifies a node of the type {type} is its property {property}.",
        "closed_types": "closed list. A node with a label that is not in the list above is rejected.",
        "open_types": "open list. A node with a label that is not in the list above is accepted.",
        "closed_endpoints": "closed list. A relationship whose type and end labels match no line above is rejected.",
        "open_endpoints": "open list. A relationship whose type and end labels match no line above is accepted.",
        "unique": "two nodes of one label never share a value of a property marked *. A write that breaks this is rejected.",
        "unique_open": "a value of a property marked * is meant to occur once per label. A write that breaks this is accepted.",
        "closed_properties": "closed lists. A property that is not named above is rejected.",
        "open_properties": "open lists. A property that is not named above is accepted.",
        "free_properties": "nothing is declared, so any property is accepted.",
        "counts": "Counts: {node_types} node types, {relationship_types} relationship types, "
                  "{relationship_patterns} relationship patterns, {uniqueness_constraints} uniqueness constraints.",
    },
    "rdf": {
        "identification": "Schema {treatment} for RDF, version {version}, as declared.",
        "types": "Classes ({n}):",
        "properties": "Properties (class, then its properties{marked}):",
        "marked": "; a property marked * is unique, see the constraints",
        "bracketed": "; in square brackets what is declared for a property: identifier (its value identifies the "
                     "entity), its datatype, required, and after \"one of\" its allowed values",
        "relationships": "Relations ({patterns} patterns of {types} properties, as subject class, property, "
                         "object class{braces}{at_most_one}):",
        "braces": "; in braces the properties a relation of that kind carries as an annotation",
        "at_most_one": "; a pattern followed by \"at most one\" allows an entity to be the subject of only one "
                       "relation of that pattern",
        "required": "an entity carries a value for every property of its class marked required, and a relation for "
                    "every property of its pattern marked required. A write that breaks this is rejected.",
        "required_open": "an entity is meant to carry a value for every property of its class marked required, and a "
                         "relation for every property of its pattern marked required. A write that breaks this is "
                         "accepted.",
        "datatypes": "the value of a property is a value of the datatype named in its brackets; a quantity is a number "
                     "with a unit of measure. A write that breaks this is rejected.",
        "datatypes_open": "the value of a property is meant to be a value of the datatype named in its brackets; a "
                          "quantity is a number with a unit of measure. A write that breaks this is accepted.",
        "cardinality": "an entity is the subject of at most one relation of a pattern followed by \"at most one\". A "
                       "write that breaks this is rejected.",
        "cardinality_open": "an entity is meant to be the subject of at most one relation of a pattern followed by "
                            "\"at most one\". A write that breaks this is accepted.",
        "values": "the value of a property with \"one of\" in its brackets is one of the values listed there. A write "
                  "that breaks this is rejected.",
        "values_open": "the value of a property with \"one of\" in its brackets is meant to be one of the values "
                       "listed there. A write that breaks this is accepted.",
        "identifying": "The value that identifies an entity is its {property} (<{iri}>).",
        "closed_types": "closed list. An entity with a class that is not in the list above is rejected.",
        "open_types": "open list. An entity with a class that is not in the list above is accepted.",
        "closed_endpoints": "closed list. A relation whose property and end classes match no line above is rejected.",
        "open_endpoints": "open list. A relation whose property and end classes match no line above is accepted.",
        "unique": "two entities of one class never share a value of a property marked *. A write that breaks this is rejected.",
        "unique_open": "a value of a property marked * is meant to occur once per class. A write that breaks this is accepted.",
        "closed_properties": "closed lists. A property that is not named above is rejected.",
        "open_properties": "open lists. A property that is not named above is accepted.",
        "free_properties": "nothing is declared, so any property is accepted, with a value or with another entity "
                           "as its object. Give a property an IRI in the namespace for data.",
        "counts": "Counts: {node_types} classes, {relationship_types} relation properties, "
                  "{relationship_patterns} relation patterns, {uniqueness_constraints} uniqueness constraints.",
    },
}
# The constraint kinds of the conformance inventory, in its order; `other properties` is the one
# line the inventory does not have. A kind a declaration does not state is "none declared".
KINDS = ("Labels and types", "Required properties", "Datatypes", "Uniqueness", "Cardinality", "Endpoints",
         "Closed value lists", "Other properties")
KINDS_RDF = ("Classes", "Required properties", "Datatypes", "Uniqueness", "Cardinality", "Endpoints",
             "Closed value lists", "Other properties")


class CannotRender(Exception):
    """This realisation has no description yet; the message says why."""


def realisations(here: Path = HERE) -> list[str]:
    return sorted(str(path.parent.relative_to(here)) for path in here.glob("*/*/declaration.json"))


def load(realisation: str, here: Path = HERE) -> dict:
    path = here / realisation / "declaration.json"
    if not path.is_file():
        raise CannotRender(f"{realisation}: no declaration.json")
    return json.loads(path.read_text(encoding="utf-8"))


def check_known(declaration: dict) -> None:
    def unknown(found, known, where: str) -> None:
        extra = sorted(set(found) - set(known))
        if extra:
            raise CannotRender(f"{declaration.get('realisation')}: {where} has keys this renderer does not know: "
                               f"{extra}. Extend render.py before this schema is shown to an agent.")

    unknown(declaration, TOP_KEYS, "the declaration")
    for node in declaration.get("node_types", []):
        unknown(node, NODE_KEYS, f"node type {node.get('name')}")
        for prop in node.get("properties", []):
            unknown(prop, PROPERTY_KEYS, f"property {node.get('name')}.{prop.get('name')}")
    for pattern in declaration.get("relationship_patterns", []):
        unknown(pattern, PATTERN_KEYS, f"pattern {pattern.get('type')}")
    enforcement = declaration.get("enforcement", {})
    unknown(enforcement, ENFORCEMENT_KEYS + tuple(ENFORCEMENT_MORE.values()), "the enforcement block")
    for key in ENFORCEMENT_KEYS + tuple(key for key in ENFORCEMENT_MORE.values() if key in enforcement):
        if enforcement.get(key) not in STRENGTHS:
            raise CannotRender(f"{declaration.get('realisation')}: enforcement.{key} is {enforcement.get(key)!r}; "
                               f"expected one of {list(STRENGTHS)}")
    check_declared_constraints(declaration)


def check_declared_constraints(declaration: dict) -> None:
    """What only an induced schema declares must name declared properties, and its strength must be stated."""
    realisation, enforcement = declaration.get("realisation"), declaration.get("enforcement", {})

    def stop(text: str) -> None:
        raise CannotRender(f"{realisation}: {text}")

    owners = [(f"node type {node.get('name')}", node, [prop["name"] for prop in node.get("properties", [])])
              for node in declaration.get("node_types", [])]
    owners += [(f"pattern {pattern.get('from')} {pattern.get('type')}", pattern, list(pattern.get("properties", [])))
               for pattern in declaration.get("relationship_patterns", [])]
    for where, owner, names in owners:
        if owner.get("identifier") is not None and owner["identifier"] not in names:
            stop(f"{where}: the identifier {owner['identifier']!r} is not one of its properties")
        if not isinstance(owner.get("required", []), list) or not isinstance(owner.get("datatypes", {}), dict) \
                or not isinstance(owner.get("values", {}), dict) or not isinstance(owner.get("at_most_one", False), bool):
            stop(f"{where}: required has to be a list, datatypes and values objects, at_most_one true or false")
        for key in ("required", "datatypes", "values"):
            for name in owner.get(key, []):
                if name not in names:
                    stop(f"{where}: {key} names {name!r}, which is not one of its properties")
        for name, datatype in owner.get("datatypes", {}).items():
            if datatype not in DATATYPES:
                stop(f"{where}: the datatype {datatype!r} of {name} is not one of {list(DATATYPES)}")
        for name, values in owner.get("values", {}).items():
            if not isinstance(values, list) or not values or any(isinstance(v, (list, dict)) or v is None for v in values):
                stop(f"{where}: the allowed values of {name} have to be a list of text, numbers, true or false")
        for kind, key in ENFORCEMENT_MORE.items():
            if owner.get(kind) and key not in enforcement:
                stop(f"{where} declares {kind.replace('_', ' ')}, but the enforcement block does not say what "
                     f"happens to a write that breaks it (key {key})")


def sections(declaration: dict) -> list[tuple[str, str]]:
    """The description as (section name, text) pairs, in the fixed order."""
    realisation, arm = declaration.get("realisation"), declaration.get("arm")
    if declaration.get("status") == "placeholder":
        raise CannotRender(f"{realisation}: the declaration is a placeholder; there is no induced schema yet")
    if arm not in WORDS:
        raise CannotRender(f"{realisation}: unknown arm {arm!r}")
    check_known(declaration)
    if arm == "rdf" and not declaration.get("data_namespace"):
        raise CannotRender(f"{realisation}: the declaration names no namespace for the IRIs of data "
                           f"(key data_namespace); the agent has to be told where to create them")
    words, enforcement = WORDS[arm], declaration["enforcement"]
    nodes, patterns = declaration.get("node_types", []), declaration.get("relationship_patterns", [])
    name = (lambda text: f"{SCHEMA_PREFIX}:{text}") if arm == "rdf" else (lambda text: text)
    any_unique = any(node.get("unique") for node in nodes)
    empty = not nodes and not patterns
    out: list[tuple[str, str]] = []

    def brackets(owner: dict, prop: str) -> str:
        """What the schema declares for one property of a node type or of a pattern, beyond its name."""
        parts = ["identifier"] if owner.get("identifier") == prop else []
        if prop in owner.get("datatypes", {}):
            parts.append(owner["datatypes"][prop])
        if prop in owner.get("required", []):
            parts.append("required")
        if prop in owner.get("values", {}):
            parts.append("one of " + " | ".join(json.dumps(value, ensure_ascii=False) for value in owner["values"][prop]))
        return f" [{', '.join(parts)}]" if parts else ""

    def declares(kind: str) -> bool:
        return any(owner.get(kind) for owner in [*nodes, *patterns])

    any_brackets = declares("identifier") or declares("required") or declares("datatypes") or declares("values")

    # 1. identification
    lines = [words["identification"].format(treatment=declaration["treatment"], version=declaration["rules_version"])]
    if arm == "rdf":
        if declaration.get("namespace"):
            lines.append(f"Namespace of the classes and properties, written below with the prefix "
                         f"{SCHEMA_PREFIX}: <{declaration['namespace']}>")
        lines.append(f"Namespace for the IRIs of the entities you create: <{declaration['data_namespace']}>")
    out.append(("identification", "\n".join(lines)))

    # 2. types
    listed = ", ".join(name(node["name"]) for node in nodes) if nodes else NONE
    out.append(("types", f"{words['types'].format(n=len(nodes))}\n{listed}"))

    # 3. properties
    lines = [words["properties"].format(marked=(words["marked"] if any_unique else "")
                                        + (words["bracketed"] if any_brackets else ""))]
    for node in nodes:
        unique = set(node.get("unique", []))
        props = ", ".join(name(prop["name"]) + ("*" if prop["name"] in unique else "") + brackets(node, prop["name"])
                          for prop in node["properties"])
        lines.append(f"- {name(node['name'])}: {props or NONE}")
    if not nodes:
        lines.append(NONE)
    identifying = declaration.get("identifying_value")
    if identifying:
        lines.append(words["identifying"].format(type=identifying.get("node_type"), property=identifying.get("property"),
                                                 iri=identifying.get("iri")))
    out.append(("properties", "\n".join(lines)))

    # 4. relationships with their end types
    any_braces = any(pattern.get("properties") for pattern in patterns)
    types = len({pattern["type"] for pattern in patterns})
    lines = [words["relationships"].format(patterns=len(patterns), types=types, braces=words["braces"] if any_braces else "",
                                           at_most_one=words["at_most_one"] if declares("at_most_one") else "")]
    for pattern in patterns:
        at_most_one = ", at most one" if pattern.get("at_most_one") else ""
        if arm == "pg":
            props = " {" + ", ".join(prop + brackets(pattern, prop) for prop in pattern["properties"]) + "}" \
                if pattern.get("properties") else ""
            lines.append(f"- (:{pattern['from']})-[:{pattern['type']}{props}]->(:{'|'.join(pattern['to'])}){at_most_one}")
        else:
            targets = [name(target) for target in pattern["to"]]
            target = targets[0] if len(targets) == 1 else "(" + " or ".join(targets) + ")"
            props = " {| " + ", ".join(name(prop) + brackets(pattern, prop) for prop in pattern["properties"]) + " |}" \
                if pattern.get("properties") else ""
            lines.append(f"- {name(pattern['from'])} {name(pattern['type'])} {target}{props}{at_most_one}")
    if not patterns:
        lines.append(NONE)
    form = declaration.get("relationship_property_form")
    if arm == "rdf" and form and any_braces:
        lines.append(f"A value on a relation is written as an annotation on its triple: {form['written_as']}")
    out.append(("relationships", "\n".join(lines)))

    # 5. constraints by kind
    def strength(key: str, closed: str, opened: str) -> str:
        return {"reject": words[closed], "accept_and_count": words[opened], "nothing_declared": NONE}[enforcement[key]]

    def declared(kind: str, sentence: str) -> str:
        """The line of a constraint kind only an induced schema can declare."""
        return strength(ENFORCEMENT_MORE[kind], sentence, f"{sentence}_open") if declares(kind) else NONE

    uniqueness = NONE if not any_unique else strength("uniqueness_violation", "unique", "unique_open")
    values = {
        0: strength("node_type_outside_the_list", "closed_types", "open_types") if nodes else NONE,
        1: declared("required", "required"), 2: declared("datatypes", "datatypes"), 3: uniqueness,
        4: declared("at_most_one", "cardinality"),
        5: strength("relationship_outside_the_patterns", "closed_endpoints", "open_endpoints") if patterns else NONE,
        6: declared("values", "values"),
        # A schema that declares no type and no pattern at all leaves "none declared" open to two readings
        # (nothing is restricted, or nothing is allowed). The pilot of 24 November showed both, so the line says it.
        7: words["free_properties"] if empty and enforcement["undeclared_property"] == "nothing_declared"
        else strength("undeclared_property", "closed_properties", "open_properties"),
    }
    kinds = KINDS_RDF if arm == "rdf" else KINDS
    lines = ["Constraints:"] + [f"- {kind}: {values[index]}" for index, kind in enumerate(kinds)]
    out.append(("constraints", "\n".join(lines)))

    # 6. counts
    counts = declaration.get("counts", {})
    out.append(("counts", words["counts"].format(**{key: counts.get(key, 0) for key in
                                                    ("node_types", "relationship_types", "relationship_patterns",
                                                     "uniqueness_constraints")})))
    return out


def render(declaration: dict) -> str:
    return "\n\n".join(text for _, text in sections(declaration)) + "\n"


def build(here: Path = HERE) -> tuple[dict[Path, str], dict[str, str]]:
    """(file to content) for what can be rendered, (realisation to reason) for what cannot."""
    files, refused = {}, {}
    for realisation in realisations(here):
        try:
            files[here / realisation / OUTPUT] = render(load(realisation, here))
        except CannotRender as reason:
            refused[realisation] = str(reason)
    return files, refused


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("realisation", nargs="?", help="print one description, for example S4-curated/pg")
    parser.add_argument("--check", action="store_true", help="compare with the files on disk, write nothing")
    parser.add_argument("--sizes", action="store_true", help="characters per section of every description")
    args = parser.parse_args()

    if args.realisation:
        try:
            sys.stdout.write(render(load(args.realisation.strip("/"))))
        except CannotRender as reason:
            print(reason, file=sys.stderr)
            return 1
        return 0

    files, refused = build()
    if args.sizes:
        for realisation in realisations():
            if realisation in refused:
                continue
            parts = sections(load(realisation))
            total = len(render(load(realisation)))
            print(f"{realisation}: {total} characters; " + ", ".join(f"{name} {len(text)}" for name, text in parts))
        return 0

    bad = []
    for path, content in files.items():
        relative = path.relative_to(HERE)
        if args.check:
            if not path.is_file() or path.read_text(encoding="utf-8") != content:
                bad.append(str(relative))
        else:
            path.write_text(content, encoding="utf-8")
            print(f"written  {relative}  ({len(content)} characters)")
    for realisation, reason in refused.items():
        stale = HERE / realisation / OUTPUT
        if stale.is_file():
            bad.append(f"{realisation}/{OUTPUT} (exists, but the realisation cannot be rendered)")
        print(f"refused  {reason}")
    if bad:
        print("differs from the renderer: " + ", ".join(bad), file=sys.stderr)
        return 1
    if args.check:
        print(f"{len(files)} descriptions match the renderer")
    return 0


if __name__ == "__main__":
    sys.exit(main())
