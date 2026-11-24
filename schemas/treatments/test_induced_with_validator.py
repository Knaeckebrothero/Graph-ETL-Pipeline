"""What Thesis B's validator enforces from the declaration of an induced schema, and what it does not.

Skipped unless the validator can be imported:

    PYTHONPATH=../Graph-ETL-Pipeline-thesis-b/experiments/thesis-b python -m pytest schemas/treatments -q

The induction prompt tells the model: an undeclared type or relation is rejected, an undeclared
property is accepted, a declared constraint is enforced. The first block of tests holds what the
validator does of this from a generated declaration. The second block (`test_gap_...`) records what
it does not do yet, as found on 23 November 2026 at validator commit 5d2c858: a test there fails on
the day Thesis B closes the gap, and is then turned round and moved up. The table in README.md
("Rule 6") is the same list in words.

The changes are written by hand in the shape the database tool hands over (dbtool/checker.py).
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

validator_checker = pytest.importorskip("validator.checker", reason="Thesis B's validator is not on the Python path")

import derive  # noqa: E402
import render  # noqa: E402
from test_induced import reply_pg, reply_rdf, text  # noqa: E402

PERSON = {"person_id": "p1", "name": "Ann"}
XSD = "http://www.w3.org/2001/XMLSchema#"


class Lookup:
    def __init__(self, found: dict | None = None):
        self.found = found or {}

    def find(self, label, prop, value):
        return self.found.get((label, prop, value), [])

    def entity_exists(self, key):
        return 1


@pytest.fixture(scope="module")
def checker(tmp_path_factory):
    """A checker whose root holds the two induced realisations made of the invented replies."""
    root = tmp_path_factory.mktemp("root")
    here = root / "schemas" / "treatments"
    for arm, reply in (("pg", reply_pg()), ("rdf", reply_rdf())):
        folder = here / "S2-induced" / arm
        folder.mkdir(parents=True)
        (folder / derive.REPLY).write_text(text(reply), encoding="utf-8")
    assert derive.run(here) == 0
    found = validator_checker.Checker(root=root)
    assert found.declaration("S2-induced/pg").status == "draft" and not found.warnings
    return found


def node(key, labels, properties):
    return {"key": key, "labels": labels, "properties": properties, "change": "created", "changed_properties": {},
            "added_labels": [], "removed_labels": []}


def relationship(key, kind, start, end, properties=None):
    return {"key": key, "type": kind, "from": start, "to": end, "properties": properties or {}, "change": "created",
            "changed_properties": {}}


ENDS = {"1": {"labels": ["Person"], "properties": PERSON}, "2": {"labels": ["Team"], "properties": {"title": "t"}},
        "3": {"labels": ["Team"], "properties": {"title": "u"}}}


def rules(checker, arm="pg", nodes=(), relationships=(), lookup=None) -> list[str]:
    changes = {"cell_id": "c", "arm": arm, "realisation": f"S2-induced/{arm}", "unit_id": "u", "item_id": "i",
               "statement": "CREATE ...", "nodes": list(nodes), "relationships": list(relationships),
               "ends": ENDS if relationships else {}, "deleted": {"nodes": [], "relationships": []}}
    checker.observations.clear()
    return [fault["rule_id"] for fault in checker.check(changes, lookup or Lookup())]


def typed(value: str, datatype: str) -> dict:
    """A typed RDF value as the database tool hands it over."""
    return {"value": value, "datatype": datatype, "datatype_iri": XSD + datatype}


# --------------------------------------------------------------------------- enforced today

def test_the_validator_loads_everything_the_declaration_carries(checker):
    inventory = checker.declaration("S2-induced/pg").constraint_inventory()
    assert inventory["label_and_type"] == 3 and inventory["relationship_pattern_expanded"] == 4
    assert inventory["uniqueness"] == 3 and inventory["required_property"] == 4
    assert inventory["datatype"] == 10 and inventory["closed_value_list"] == 2


def test_a_write_inside_the_schema_passes(checker):
    assert rules(checker, nodes=[node("1", ["Person"], {**PERSON, "born": "1990-01-02", "level": 2})]) == []
    assert rules(checker, relationships=[relationship("r", "MEMBER_OF", "1", "2", {"since": "2020-01-01"})]) == []
    assert rules(checker, "rdf", nodes=[node("https://d/p1", ["Person"], {"person_id": "p1", "born": typed("1990-01-02", "date")})]) == []


def test_an_undeclared_type_or_relation_is_rejected(checker):
    assert rules(checker, nodes=[node("1", ["Robot"], {"name": "x"})]) == ["TS-type"]
    assert rules(checker, relationships=[relationship("r", "LIKES", "1", "2")]) == ["TS-type"]
    assert rules(checker, relationships=[relationship("r", "MEMBER_OF", "2", "1", {"since": "2020-01-01"})]) == ["TS-endpoint"]


def test_an_undeclared_property_is_accepted_and_counted(checker):
    assert rules(checker, nodes=[node("1", ["Person"], {**PERSON, "shoe_size": 43})]) == []
    assert [o["kind"] for o in checker.observations] == ["TS-property"]


def test_a_required_property_of_a_type_is_enforced(checker):
    assert rules(checker, nodes=[node("1", ["Person"], {"person_id": "p1"})]) == ["TS-required"]


def test_the_identifier_is_enforced_as_unique_and_as_the_identifying_value(checker):
    again = Lookup({("Person", "person_id", "p1"): ["1"]})
    assert rules(checker, nodes=[node("2", ["Person"], PERSON)], lookup=again) == ["TS-unique"]
    assert rules(checker, nodes=[node("1", ["Person"], PERSON), node("2", ["Person"], PERSON)]) == ["TS-unique"]
    assert rules(checker, nodes=[node("1", ["Project"], {"open": True})]) == ["CS1"]      # nothing that identifies it
    # Person.person_id is unique only by the rule of induced.py: the reply did not list it
    assert reply_pg()["node_types"][0]["unique"] == []


def test_a_value_outside_the_list_is_rejected(checker):
    assert rules(checker, nodes=[node("1", ["Person"], {**PERSON, "level": 9})]) == ["TS-valuelist"]
    assert rules(checker, nodes=[node("1", ["Team"], {"title": "t", "kind": "odd"})]) == ["TS-valuelist"]


def test_a_typed_value_of_another_datatype_is_rejected(checker):
    assert rules(checker, nodes=[node("1", ["Person"], {"person_id": 7, "name": "Ann"})]) == ["TS-datatype"]
    assert rules(checker, "rdf", nodes=[node("https://d/p1", ["Person"], {"person_id": "p1", "born": typed("5", "integer")})]) \
        == ["TS-datatype"]
    assert rules(checker, "rdf", nodes=[node("https://d/p1", ["Person"], {"person_id": "p1", "born": typed("1990-13-02", "date")})]) \
        == ["CS4"]


# --------------------------------------------------------------------------- not enforced yet (23 November 2026)

def test_gap_text_passes_every_declared_datatype(checker):
    """A value handed over as text is not tested against the declared datatype. In the property graph every
    date arrives as text, so `date` and `datetime` are never tested there."""
    assert rules(checker, nodes=[node("1", ["Person"], {**PERSON, "born": "next week"})]) == []
    assert rules(checker, nodes=[node("1", ["Project"], {"code": "c", "open": "perhaps"})]) == []
    assert rules(checker, "rdf", nodes=[node("https://d/p1", ["Person"], {"person_id": "p1", "born": "soon"})]) == []


def test_gap_a_whole_number_is_no_decimal_and_no_number_is_a_quantity(checker):
    assert rules(checker, nodes=[node("1", ["Project"], {"code": "c", "score": 5})]) == ["TS-datatype"]
    assert rules(checker, nodes=[node("1", ["Team"], {"title": "t", "budget": 5})]) == ["TS-datatype"]
    assert rules(checker, nodes=[node("1", ["Team"], {"title": "t", "budget": "5 kg"})]) == []


def test_gap_at_most_one_is_not_enforced(checker):
    two = [relationship("r1", "LEADS", "1", "2"), relationship("r2", "LEADS", "1", "3")]
    assert rules(checker, relationships=two) == []


def test_gap_constraints_on_a_property_of_a_relationship_are_not_enforced(checker):
    assert rules(checker, relationships=[relationship("r", "MEMBER_OF", "1", "2", {})]) == []                 # required
    assert rules(checker, relationships=[relationship("r", "MEMBER_OF", "1", "2", {"since": 5})]) == []       # datatype
    assert rules(checker, relationships=[relationship("r", "MEMBER_OF", "1", "2",
                                                      {"since": "2020-01-01", "role": "boss"})]) == []      # value list


def test_gap_in_rdf_a_typed_value_never_matches_a_list_of_numbers(checker):
    """The tool hands a typed value over in its written form ("2"); the list holds the number 2."""
    inside = node("https://d/p1", ["Person"], {"person_id": "p1", "level": typed("2", "integer")})
    assert rules(checker, "rdf", nodes=[inside]) == ["TS-valuelist"]


def test_gap_a_property_with_several_values_and_a_value_list_stops_the_check(checker):
    with pytest.raises(TypeError):
        rules(checker, nodes=[node("1", ["Team"], {"title": "t", "kind": ["core", "guest"]})])


def test_gap_the_identifier_itself_need_not_be_there_if_another_naming_property_is(checker):
    """Any unique property of the type, or one called uid, id, key, name, title, label or code, serves as the
    identifying value. Here the declared identifier is missing and only `required` catches it."""
    assert rules(checker, nodes=[node("1", ["Person"], {"name": "Ann"})]) == ["TS-required"]


def test_the_description_and_the_declaration_name_the_same_things(checker):
    decl = checker.declaration("S2-induced/pg")
    out = render.render(render.load("S2-induced/pg", checker.root / "schemas" / "treatments"))
    for label, node_type in decl.node_types.items():
        assert f"- {label}: " in out
        for name in node_type["properties"]:
            assert name in out
