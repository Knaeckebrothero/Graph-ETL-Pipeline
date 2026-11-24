#!/usr/bin/env python3
"""Read the reply of a schema induction and turn it into the schema of an S2 realisation.

    python schemas/treatments/induced.py REPLY --arm pg     # can this reply be used? every fault is listed
    python schemas/treatments/induced.py REPLY --arm rdf

The reply is the one document the induction prompt asks for (experiments/prompts of the first
checkout, `bindings/induction.pg.txt` and `bindings/induction.rdf.txt`). It is stored unchanged
as `S2-induced/<arm>/induction.reply.json`; `derive.py` reads it from there.

The reader is strict and never repairs. A reply either is the document in the form the prompt
states, or it is refused with the list of everything that is wrong with it, and nothing is
generated from it. Two things around the document are tolerated and change nothing in it: white
space, and one code fence around the whole document (recorded as a note).

What counts as a fault:
- the reply is not one JSON document, or has text before or after it;
- a key is missing, unknown, or stated twice in one object;
- a value has the wrong type (a name that is not text, `required` that is not true or false, ...);
- a datatype outside the list of the prompt;
- a name that is not a plain name (letters, digits, underscore, not starting with a digit), because
  it has to be written unquoted in Cypher and as the local part of an IRI;
- an identifier, or a unique property (`unique`, in RDF `key`), that the type does not declare;
- a relationship that names a type the document does not declare, or no end type at all;
- a duplicate: two types of one name, two properties of one name in a type or a relationship, a
  property twice in `unique`, an end type twice in a pattern, one pattern (type, start, end)
  twice, a value twice in a list of allowed values;
- a list of allowed values that is empty or holds something other than text, numbers, true, false;
- no type at all;
- in RDF, where one namespace holds every term: one name used for a class and for a property, or
  for a property with values and for a property between resources.

The mapping (`to_schema`) changes one thing, by rule and for every type alike: the identifier of a
type counts as unique (`IDENTIFIER_IS_UNIQUE`). The shared rules of the validator take the value
that identifies an entity from the unique properties its type declares; an identifier that two
entities may share identifies neither. Where a reply did not list the identifier under `unique`
itself, the declaration says so (`induction.identifier_added_to_unique`).

Standard library only.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

# The datatypes the induction prompt offers, in its order.
DATATYPES = ("string", "integer", "decimal", "boolean", "date", "datetime", "quantity")
PLAIN_NAME = re.compile(r"[^\W\d]\w*")
FENCE_OPEN = re.compile(r"```[A-Za-z0-9_-]*")

# The identifier of a type is a unique property of it (see the module text). False keeps `unique`
# exactly as the reply lists it.
IDENTIFIER_IS_UNIQUE = True

# The two forms of the document. The content is the same; the words are the arm's.
FORMS = {
    "pg": {"types": "node_types", "relations": "relationship_patterns", "unique": "unique",
           "relation_name": "type", "relation_from": "from", "relation_to": "to",
           "relation_properties": "properties",
           "a_type": "node type", "a_relation": "relationship pattern"},
    "rdf": {"types": "classes", "relations": "object_properties", "unique": "key",
            "relation_name": "name", "relation_from": "domain", "relation_to": "range",
            "relation_properties": "annotations",
            "a_type": "class", "a_relation": "object property"},
}
PROPERTY_KEYS = ("name", "datatype", "required", "values")


@dataclass
class Reading:
    """What reading a reply gave: the document if it could be parsed, every fault, and notes."""
    arm: str
    document: Any = None
    faults: list[str] = field(default_factory=list)
    notes: list[str] = field(default_factory=list)
    in_code_fence: bool = False

    @property
    def usable(self) -> bool:
        return not self.faults


class Refused(Exception):
    """A reply that cannot be used. `faults` holds the full list."""

    def __init__(self, where: str, faults: list[str]):
        self.where, self.faults = where, list(faults)
        super().__init__(f"{where}: the induction reply is refused, {len(self.faults)} "
                         f"fault{'s' if len(self.faults) != 1 else ''}:\n" + "\n".join(f"  - {fault}" for fault in self.faults))


# --------------------------------------------------------------------------- the text around the document

def kind(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, str):
        return "text"
    if isinstance(value, (int, float)):
        return "a number"
    if isinstance(value, list):
        return "a list"
    if isinstance(value, dict):
        return "an object"
    return type(value).__name__


def shown(text: str, limit: int = 60) -> str:
    text = " ".join(text.split())
    return json.dumps(text if len(text) <= limit else text[: limit - 3] + "...", ensure_ascii=False)


def unwrap(text: str, reading: Reading) -> str:
    """The document's text without white space around it and without the one code fence around it."""
    body = text.lstrip("﻿").strip()
    if not body.startswith("```"):
        return body
    lines = body.split("\n")
    fences = [index for index, line in enumerate(lines) if line.strip().startswith("```")]
    if len(lines) >= 2 and fences == [0, len(lines) - 1] and FENCE_OPEN.fullmatch(lines[0].strip()) \
            and lines[-1].strip() == "```":
        reading.in_code_fence = True
        reading.notes.append("the document was taken out of a code fence around it; nothing inside was changed")
        return "\n".join(lines[1:-1]).strip()
    return body                                   # the fence is then text around the document: a fault below


def parse(text: str, reading: Reading) -> Any:
    """The JSON document of a reply, or None. Text around it and keys stated twice are faults."""
    twice: list[str] = []

    def pairs(found: list[tuple[str, Any]]) -> dict:
        names = [name for name, _ in found]
        for name in sorted({name for name in names if names.count(name) > 1}):
            twice.append(f"the key {json.dumps(name, ensure_ascii=False)} is stated twice in one object")
        return dict(found)

    def constant(name: str) -> Any:
        raise ValueError(f"{name} is not a JSON value")

    body = unwrap(text, reading)
    if not body:
        reading.faults.append("the reply is empty")
        return None
    start = 0
    if body[0] != "{":
        start = body.find("{")
        if start < 0:
            reading.faults.append(f"the reply holds no JSON document; it starts with {shown(body)}")
            return None
        reading.faults.append(f"text before the document: {shown(body[:start])}")
    decoder = json.JSONDecoder(object_pairs_hook=pairs, parse_constant=constant)
    try:
        document, end = decoder.raw_decode(body, start)
    except json.JSONDecodeError as error:
        line = body.count("\n", 0, error.pos) + 1
        column = error.pos - (body.rfind("\n", 0, error.pos) + 1) + 1
        reading.faults.append(f"the reply is not a JSON document: {error.msg} at line {line}, column {column} "
                              f"of the document, near {shown(body[max(0, error.pos - 20): error.pos + 20])}")
        return None
    except ValueError as error:
        reading.faults.append(f"the reply is not a JSON document: {error}")
        return None
    if body[end:].strip():
        reading.faults.append(f"text after the document: {shown(body[end:])}")
    reading.faults.extend(twice)
    return document


# --------------------------------------------------------------------------- the document

def quoted(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False)


def check_keys(found: dict, expected: tuple[str, ...], where: str, faults: list[str]) -> None:
    for key in expected:
        if key not in found:
            faults.append(f"{where}: the key {quoted(key)} is missing")
    for key in found:
        if key not in expected:
            faults.append(f"{where}: unknown key {quoted(key)}")


def check_name(value: Any, where: str, what: str, faults: list[str]) -> str | None:
    """A name as text, or None with the fault recorded."""
    if not isinstance(value, str):
        faults.append(f"{where}: {what} has to be text, found {kind(value)}")
        return None
    if not PLAIN_NAME.fullmatch(value):
        faults.append(f"{where}: {what} {quoted(value)} is not a plain name (letters, digits and underscore, "
                      f"not starting with a digit)")
        return None
    return value


def same_value(value: Any) -> tuple:
    """Two allowed values are the same when they are the same JSON value; true is not 1."""
    return (isinstance(value, bool), isinstance(value, str), value)


def check_property(found: Any, where: str, faults: list[str]) -> str | None:
    """One property object. Returns its name when it has a usable one."""
    if not isinstance(found, dict):
        faults.append(f"{where}: a property has to be an object, found {kind(found)}")
        return None
    name = check_name(found["name"], where, "the name of the property", faults) if "name" in found else None
    if name:
        where = f"{where} ({name})"
    check_keys(found, PROPERTY_KEYS, where, faults)
    if "datatype" in found and found["datatype"] is not None:
        datatype = found["datatype"]
        if not isinstance(datatype, str):
            faults.append(f"{where}: the datatype has to be text or null, found {kind(datatype)}")
        elif datatype not in DATATYPES:
            faults.append(f"{where}: the datatype {quoted(datatype)} is not one of {', '.join(DATATYPES)}")
    if "required" in found and not isinstance(found["required"], bool):
        faults.append(f"{where}: \"required\" has to be true or false, found {kind(found['required'])}")
    if "values" in found and found["values"] is not None:
        values = found["values"]
        if not isinstance(values, list):
            faults.append(f"{where}: \"values\" has to be a list or null, found {kind(values)}")
        elif not values:
            faults.append(f"{where}: the list of allowed values is empty; a property without such a list states null")
        else:
            seen: list[tuple] = []
            for index, value in enumerate(values):
                if isinstance(value, (list, dict)) or value is None:
                    faults.append(f"{where}: allowed value {index + 1} has to be text, a number, true or false, "
                                  f"found {kind(value)}")
                elif same_value(value) in seen:
                    faults.append(f"{where}: the allowed value {quoted(value)} is listed twice")
                else:
                    seen.append(same_value(value))
    return name


def check_properties(found: Any, where: str, key: str, faults: list[str]) -> list[str] | None:
    """A list of property objects. Returns the names it declares, or None when it is no list."""
    if not isinstance(found, list):
        faults.append(f"{where}: {quoted(key)} has to be a list, found {kind(found)}")
        return None
    names: list[str] = []
    for index, entry in enumerate(found):
        name = check_property(entry, f"{where}.{key}[{index}]", faults)
        if name is None:
            continue
        if name in names:
            faults.append(f"{where}: the property {quoted(name)} is declared twice")
        else:
            names.append(name)
    return names


def check_document(document: Any, arm: str) -> list[str]:
    """Every fault of a parsed document against the form of its arm."""
    form, faults = FORMS[arm], []
    if not isinstance(document, dict):
        return [f"the document has to be a JSON object, found {kind(document)}"]
    check_keys(document, (form["types"], form["relations"]), "the document", faults)

    # the types
    type_names: list[str] = []
    type_properties: dict[str, list[str]] = {}
    types = document.get(form["types"])
    types_readable = isinstance(types, list)
    if form["types"] in document and not types_readable:
        faults.append(f"{quoted(form['types'])} has to be a list, found {kind(types)}")
    for index, entry in enumerate(types if types_readable else []):
        where = f"{form['types']}[{index}]"
        if not isinstance(entry, dict):
            faults.append(f"{where}: a {form['a_type']} has to be an object, found {kind(entry)}")
            continue
        name = check_name(entry["name"], where, f"the name of the {form['a_type']}", faults) if "name" in entry else None
        if name:
            where = f"{where} ({name})"
            if name in type_names:
                faults.append(f"{where}: the {form['a_type']} {quoted(name)} is declared twice")
            else:
                type_names.append(name)
        check_keys(entry, ("name", "identifier", "properties", form["unique"]), where, faults)
        declared = check_properties(entry["properties"], where, "properties", faults) if "properties" in entry else None
        if name and name not in type_properties:
            type_properties[name] = declared or []
        if "identifier" in entry:
            identifier = entry["identifier"]
            if not isinstance(identifier, str):
                faults.append(f"{where}: the identifier has to be the name of a property, found {kind(identifier)}")
            elif declared is not None and identifier not in declared:
                faults.append(f"{where}: the identifier {quoted(identifier)} is not a property this {form['a_type']} declares")
        if form["unique"] in entry:
            unique = entry[form["unique"]]
            if not isinstance(unique, list):
                faults.append(f"{where}: {quoted(form['unique'])} has to be a list, found {kind(unique)}")
            else:
                seen: list[str] = []
                for prop in unique:
                    if not isinstance(prop, str):
                        faults.append(f"{where}: an entry of {quoted(form['unique'])} has to be the name of a property, "
                                      f"found {kind(prop)}")
                    elif prop in seen:
                        faults.append(f"{where}: {quoted(prop)} is listed twice in {quoted(form['unique'])}")
                    else:
                        seen.append(prop)
                        if declared is not None and prop not in declared:
                            faults.append(f"{where}: {quoted(prop)} in {quoted(form['unique'])} is not a property "
                                          f"this {form['a_type']} declares")
    if types_readable and not types:
        faults.append(f"{quoted(form['types'])} is empty: the schema declares no {form['a_type']}")

    # the relations
    relation_names: list[str] = []
    relation_properties: list[str] = []
    patterns: list[tuple[str, str, str]] = []
    relations = document.get(form["relations"])
    if form["relations"] in document and not isinstance(relations, list):
        faults.append(f"{quoted(form['relations'])} has to be a list, found {kind(relations)}")
    for index, entry in enumerate(relations if isinstance(relations, list) else []):
        where = f"{form['relations']}[{index}]"
        if not isinstance(entry, dict):
            faults.append(f"{where}: a {form['a_relation']} has to be an object, found {kind(entry)}")
            continue
        key_name, key_from, key_to, key_properties = (form["relation_name"], form["relation_from"],
                                                      form["relation_to"], form["relation_properties"])
        name = check_name(entry[key_name], where, f"the {quoted(key_name)} of the {form['a_relation']}", faults) \
            if key_name in entry else None
        if name:
            where = f"{where} ({name})"
            if name not in relation_names:
                relation_names.append(name)
        check_keys(entry, (key_name, key_from, key_to, "at_most_one", key_properties), where, faults)

        def end_type(value: Any, side: str) -> str | None:
            if not isinstance(value, str):
                faults.append(f"{where}: {quoted(side)} has to name a {form['a_type']}, found {kind(value)}")
                return None
            if types_readable and value not in type_names:
                faults.append(f"{where}: {quoted(side)} names {quoted(value)}, which is not a declared {form['a_type']}")
                return None
            return value

        start = end_type(entry[key_from], key_from) if key_from in entry else None
        ends: list[str] = []
        if key_to in entry:
            targets = entry[key_to]
            if not isinstance(targets, list):
                faults.append(f"{where}: {quoted(key_to)} has to be a list, found {kind(targets)}")
            elif not targets:
                faults.append(f"{where}: {quoted(key_to)} is empty: the {form['a_relation']} has no end")
            else:
                for target in targets:
                    if isinstance(target, str) and target in ends:
                        faults.append(f"{where}: {quoted(target)} is listed twice in {quoted(key_to)}")
                        continue
                    found = end_type(target, key_to)
                    if found:
                        ends.append(found)
        if "at_most_one" in entry and not isinstance(entry["at_most_one"], bool):
            faults.append(f"{where}: \"at_most_one\" has to be true or false, found {kind(entry['at_most_one'])}")
        if key_properties in entry:
            for prop in check_properties(entry[key_properties], where, key_properties, faults) or []:
                if prop not in relation_properties:
                    relation_properties.append(prop)
        if name and start:
            for target in ends:
                if (name, start, target) in patterns:
                    faults.append(f"{where}: the pattern from {quoted(start)} to {quoted(target)} is declared twice")
                else:
                    patterns.append((name, start, target))

    if arm == "rdf":                               # one namespace holds classes and both kinds of property
        value_properties = [prop for props in type_properties.values() for prop in props] + relation_properties
        for name in type_names:
            if name in value_properties or name in relation_names:
                faults.append(f"the name {quoted(name)} is used for a class and for a property; in RDF both would "
                              f"be the same IRI")
        for name in relation_names:
            if name in value_properties:
                faults.append(f"the name {quoted(name)} is used for a property with values and for an object "
                              f"property; in RDF both would be the same IRI")
    return faults


def read(text: str, arm: str) -> Reading:
    """Read a reply. `reading.usable` says whether a schema can be made of it; `reading.faults` why not."""
    if arm not in FORMS:
        raise ValueError(f"unknown arm {arm!r}")
    reading = Reading(arm=arm)
    reading.document = parse(text, reading)
    if reading.document is not None:
        reading.faults.extend(check_document(reading.document, arm))
    return reading


# --------------------------------------------------------------------------- from the document to a schema

def constraints_of(properties: list[dict]) -> dict:
    """What a list of property objects declares, in the keys a declaration.json carries."""
    return {
        "required": [prop["name"] for prop in properties if prop["required"]],
        "datatypes": {prop["name"]: prop["datatype"] for prop in properties if prop["datatype"] is not None},
        "values": {prop["name"]: list(prop["values"]) for prop in properties if prop["values"] is not None},
    }


def to_schema(reading: Reading) -> tuple[dict, dict]:
    """The schema of a usable reading in the shape derive.py handles, and what the mapping did.

    Nothing is renamed, dropped or completed. Lists keep the order of the reply. The one rule that adds
    something is IDENTIFIER_IS_UNIQUE."""
    if not reading.usable:
        raise ValueError("a reply with faults has no schema")
    form, document = FORMS[reading.arm], reading.document
    added: list[str] = []
    nodes = []
    for entry in document[form["types"]]:
        unique = list(entry[form["unique"]])
        if IDENTIFIER_IS_UNIQUE and entry["identifier"] not in unique:
            unique.insert(0, entry["identifier"])
            added.append(f"{entry['name']}.{entry['identifier']}")
        nodes.append({
            "name": entry["name"], "layer": None, "section_layer": None,
            "identifier": entry["identifier"],
            "properties": [{"name": prop["name"], "declared_by": "induction"} for prop in entry["properties"]],
            "unique": unique, "indexed": [],
            **constraints_of(entry["properties"]),
            "comment_lines": [], "statements": [],
        })
    patterns = []
    for entry in document[form["relations"]]:
        properties = entry[form["relation_properties"]]
        patterns.append({
            "type": entry[form["relation_name"]], "from": entry[form["relation_from"]],
            "to": list(entry[form["relation_to"]]),
            "properties": [prop["name"] for prop in properties],
            "group": None,
            "at_most_one": entry["at_most_one"],
            **constraints_of(properties),
        })
    done = {"in_code_fence": reading.in_code_fence, "identifier_counts_as_unique": IDENTIFIER_IS_UNIQUE,
            "identifier_added_to_unique": added}
    return {"node_types": nodes, "relationship_patterns": patterns}, done


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    parser.add_argument("reply", type=Path, help="the file that holds the model's reply, unchanged")
    parser.add_argument("--arm", choices=sorted(FORMS), required=True)
    args = parser.parse_args()
    reading = read(args.reply.read_text(encoding="utf-8"), args.arm)
    for note in reading.notes:
        print(f"note: {note}")
    if not reading.usable:
        print(f"refused: {len(reading.faults)} fault{'s' if len(reading.faults) != 1 else ''}")
        for fault in reading.faults:
            print(f"  - {fault}")
        return 1
    schema, done = to_schema(reading)
    form = FORMS[args.arm]
    print(f"usable: {len(schema['node_types'])} {form['types']}, {len(schema['relationship_patterns'])} "
          f"{form['relations']}")
    if done["identifier_added_to_unique"]:
        print("the identifier counts as unique and was not listed as such by the reply for: "
              + ", ".join(done["identifier_added_to_unique"]))
    return 0


if __name__ == "__main__":
    sys.exit(main())
