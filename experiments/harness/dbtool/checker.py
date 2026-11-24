"""The handover point between the database tool and the validator.

The tool never decides whether a write is allowed. The store runs the agent's
statement, the tool gets from the store what the statement changed, hands those
changes to a checker, and keeps or undoes them according to the answer.

A checker is any object with one method:

    check(changes: dict, lookup) -> list[dict]

It returns one entry per fault, each with the four agreed fields
`rule_id`, `element`, `expected`, `found`. An empty list means pass. It must not
change anything and must not talk to the store except through `lookup`.

Two things are optional. A checker may keep a list `observations` of what it
counted without rejecting (a property the schema does not name); after each call
the tool moves its entries into the record of that write. And it may offer
`not_checkable()`, the ids of the rules its last check could not decide; they go
into the record as `validator_not_checkable`.

`changes` describes one statement:

    {
      "cell_id": "...", "arm": "pg", "realisation": "S4-curated/pg",
      "unit_id": "...", "item_id": "...", "evidence": "...", "statement": "...", "parameters": {...} | null,
      "nodes": [
        {"key": "<element id>", "labels": ["WasteItem"], "properties": {...},
         "change": "created" | "changed",
         "changed_properties": {"name": {"old": ..., "new": ...}},
         "added_labels": [...], "removed_labels": [...]}
      ],
      "relationships": [
        {"key": "<element id>", "type": "ROUTES_TO", "from": "<element id>", "to": "<element id>",
         "properties": {...}, "change": "created" | "changed", "changed_properties": {...}}
      ],
      "ends": {"<element id>": {"labels": [...], "properties": {...}}},
      "deleted": {"nodes": ["<element id>"], "relationships": ["<element id>"]}
    }

`nodes` and `relationships` hold the state after the statement. `ends` holds
every node a listed relationship touches, whether the statement created it or
it was there before, so "does this end exist, and as what" needs no query.

`lookup.find(label, property, value)` returns the element ids of the nodes that
were in the graph before the statement and carry that label with that value.

An RDF cell hands over the same shape (fuseki_cell.py):

- a node is a subject with a class or a literal value; `key` is its IRI, or `_:` and
  the store's label for a blank node; `labels` are its classes, `properties` its
  literal values;
- a relationship is a triple whose object is a resource; `key` is the triple as one
  canonical N-Triples line; its `properties` are the values hung on it with the
  annotation form `s p o {| q v |}`;
- a class or predicate inside the schema's namespace is named by its local name,
  rdfs:label by `label`, every other IRI in full;
- text is text; a typed value is {"value": written form, "datatype": integer | decimal |
  boolean | date | datetime | duration, "datatype_iri": ...}, without "datatype" when the
  datatype is none of these; text with a language is {"value", "lang"}; a property
  with several values holds a list;
- `triples: {"added": [...], "removed": [...]}` is the exact change as N-Triples lines;
- every entry of `ends` has `exists`: whether that end is a node of the graph. In RDF a
  triple can point to a resource nothing describes; `lookup.entity_exists(key)` answers
  1 or 0 for a checker that wants to ask.
"""

from __future__ import annotations

import importlib
from typing import Any, Protocol


class Lookup(Protocol):
    def find(self, label: str | None, property: str, value: Any) -> list[str]: ...
    # RDF cells also offer entity_exists(key) -> 0 | 1


class Checker(Protocol):
    def check(self, changes: dict, lookup: Lookup) -> list[dict]: ...


class NoChecker:
    """Lets every write pass. For setting the tool up before a validator is wired in."""

    def check(self, changes: dict, lookup: Lookup) -> list[dict]:
        return []


def load_checker(spec: str | None) -> Checker:
    """`none`, or `package.module:factory` where factory() returns a checker."""
    if not spec or spec == "none":
        return NoChecker()
    module_name, _, attribute = spec.partition(":")
    factory = getattr(importlib.import_module(module_name), attribute or "checker")
    return factory()


REJECTION_FIELDS = ("rule_id", "element", "expected", "found")


def normalise(rejections: list[dict] | None) -> list[dict]:
    """Keep the four agreed fields, in order; a checker may return more."""
    out = []
    for rejection in rejections or []:
        missing = [field for field in REJECTION_FIELDS if field not in rejection]
        if missing:
            raise ValueError(f"a rejection lacks {missing}: {rejection!r}")
        out.append({field: rejection[field] for field in REJECTION_FIELDS})
    return out


def message(rejections: list[dict]) -> str:
    """One line per fault, all faults of the statement in one message."""
    return "\n".join(
        f"{r['rule_id']} at {r['element']}: expected {_text(r['expected'])}, found {_text(r['found'])}"
        for r in rejections
    )


def _text(value: Any) -> str:
    return value if isinstance(value, str) else repr(value)
