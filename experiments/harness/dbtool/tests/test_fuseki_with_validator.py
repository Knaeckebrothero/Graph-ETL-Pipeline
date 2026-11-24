"""The RDF side with Thesis B's validator as its checker, against the development Fuseki.

Skipped unless the validator can be imported:

    PYTHONPATH=experiments/thesis-b python -m pytest experiments/harness/dbtool/tests -q

One write that has to pass and one that has to fail per realisation, by the rules of
schemas/treatments/README.md; not a second copy of the validator's own tests.
"""
import json

import pytest

from conftest import HERE, fuseki_service, in_step

validator_checker = pytest.importorskip("validator.checker", reason="Thesis B's validator is not on the Python path")
REPO = HERE.parents[3]
S4 = "PREFIX : <https://fessi.example/schema/s4#> PREFIX d: <https://fessi.example/data/> PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n"
S3 = "PREFIX : <https://fessi.example/schema/s3#> PREFIX d: <https://fessi.example/data/>\n"
S1 = "PREFIX d: <https://fessi.example/data/> PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n"


@pytest.fixture(scope="module")
def vservice(tmp_path_factory):
    service = fuseki_service(tmp_path_factory, validator_checker.Checker(root=REPO))
    yield service
    service.cells["dev-rdf"].store.admin("DELETE WHERE { ?s ?p ?o }")
    service.stop()


@pytest.fixture
def vrdf(vservice):
    cell = vservice.cells["dev-rdf"]
    cell.store.admin("DELETE WHERE { ?s ?p ?o }")
    cell.mode, cell.realisation = "reject", "S4-curated/rdf"
    cell.log_path.write_text("")
    cell.open_unit({"unit_id": "u0001", "run_id": "run-test"})
    yield cell
    cell.close_unit()
    assert in_step(cell)


def attempt(cell):
    return [json.loads(line) for line in cell.log_path.read_text().splitlines()
            if json.loads(line)["record_type"] == "insert_attempt"][-1]


def rules(answer):
    return sorted(r["rule_id"] for r in answer.get("rejections", []))


CURATED_RULE = S4 + """INSERT DATA { d:wi-1 a :WasteItem ; :uid "wi-1" ; :name "Paint can" .
    d:ws-1 a :WasteStream ; :uid "ws-1" ; :name "Hazardous waste" . d:dr-1 a :DisposalRule ; :uid "dr-1" .
    d:wi-1 :HAS_RULE d:dr-1 . d:dr-1 :ROUTES_TO d:ws-1 }"""


def test_curated_takes_a_rule_node_and_refuses_the_direct_route(vrdf):
    assert vrdf.tool(CURATED_RULE, passage="p")["status"] == "stored"
    answer = vrdf.tool(S4 + "INSERT DATA { d:wi-1 :ROUTES_TO d:ws-1 }", passage="p")
    assert answer["status"] == "rejected" and rules(answer) == ["TS-endpoint"]
    assert all(set(r) == {"rule_id", "element", "expected", "found"} for r in answer["rejections"])
    assert vrdf.store.counts() == (10, 0)


def test_flat_takes_the_direct_route_and_refuses_a_rule_node(vrdf):
    vrdf.realisation = "S3-flat/rdf"
    stored = vrdf.tool(S3 + """INSERT DATA { d:wi-1 a :WasteItem ; :uid "wi-1" ; :name "Paint can" .
        d:ws-1 a :WasteStream ; :uid "ws-1" ; :name "Hazardous waste" . d:wi-1 :ROUTES_TO d:ws-1 }""", passage="p")
    assert stored["status"] == "stored"
    assert rules(vrdf.tool(S3 + 'INSERT DATA { d:dr-1 a :DisposalRule ; :uid "dr-1" }', passage="p")) == ["TS-type"]


def test_raw_triples_take_anything_with_a_name_and_refuse_an_entity_without_one(vrdf):
    vrdf.realisation = "S1-raw-triples/rdf"
    stored = vrdf.tool(S1 + """INSERT DATA { d:paint_can rdfs:label "Paint can" ; d:colour "red" .
        d:hazardous_waste rdfs:label "Hazardous waste" . d:paint_can d:goes_to d:hazardous_waste }""", passage="p")
    assert stored["status"] == "stored" and stored["created"] == {"nodes": 2, "relationships": 1}
    assert rules(vrdf.tool(S1 + 'INSERT DATA { d:thing d:weight "5 kg" }', passage="p")) == ["CS1"]


def test_a_name_from_another_vocabulary_is_not_taken_for_the_schemas_name(vrdf):
    answer = vrdf.tool(S4 + 'INSERT DATA { d:wi-1 a <https://example.org/other#WasteItem> ; :uid "wi-1" }', passage="p")
    assert rules(answer) == ["TS-type"] and "https://example.org/other#WasteItem" in answer["message"]


def test_a_property_the_schema_does_not_name_is_stored_and_counted(vrdf):
    answer = vrdf.tool(S4 + 'INSERT DATA { d:in-1 a :Instruction ; :uid "in-1" ; :text "Do not flush" }', passage="p")
    assert answer["status"] == "stored"
    record = attempt(vrdf)
    assert record["validator_result"] == "pass" and record["rejections"] == []
    assert len(record["observations"]) == 1 and "text" in json.dumps(record["observations"][0])


def test_a_unique_value_cannot_be_used_twice(vrdf):
    assert vrdf.tool(S4 + 'INSERT DATA { d:wi-1 a :WasteItem ; :name "Paint can" }', passage="p")["status"] == "stored"
    assert rules(vrdf.tool(S4 + 'INSERT DATA { d:wi-2 a :WasteItem ; :name "Paint can" ; :uid "other" }', passage="p")) == ["TS-unique"]
    changed = vrdf.tool(S4 + 'INSERT DATA { d:wi-1 :synonyms "Farbdose" }', passage="p")         # its own value is no clash
    assert changed["status"] == "stored"


def test_a_value_that_is_not_a_valid_form_of_its_datatype_is_refused(vrdf):
    answer = vrdf.tool(S4 + 'INSERT DATA { d:dr-1 a :DisposalRule ; :uid "dr-1" ; :valid_from "ab Januar"^^xsd:date }', passage="p")
    assert rules(answer) == ["CS4"]
    assert vrdf.tool(S4 + 'INSERT DATA { d:dr-1 a :DisposalRule ; :uid "dr-1" ; :valid_from "2024-01-01"^^xsd:date }',
                     passage="p", item_id=answer["id"])["status"] == "stored"


def test_a_value_on_a_relationship_is_checked_as_its_property(vrdf):
    stored = vrdf.tool(S4 + """INSERT DATA { d:p-1 a :Product ; :uid "p-1" . d:pc-1 a :PackagingComponent ; :uid "pc-1" .
                                             d:p-1 :HAS_COMPONENT d:pc-1 {| :qty 2 ; :colour "red" |} }""", passage="p")
    assert stored["status"] == "stored"
    assert [o["found"] for o in attempt(vrdf)["observations"]] == ["colour"]            # qty is declared, colour is counted


def test_log_only_keeps_what_reject_mode_refuses(vrdf):
    vrdf.mode = "log_only"
    stored = vrdf.tool(S4 + """INSERT DATA { d:wi-1 a :WasteItem ; :name "Paint can" . d:ws-1 a :WasteStream ; :name "Hazardous waste" .
                                             d:wi-1 :ROUTES_TO d:ws-1 }""", passage="p")
    assert stored["status"] == "stored"
    record = attempt(vrdf)
    assert record["validator_result"] == "reject" and record["engine_result"] == "ok" and record["accepted"]
    assert [r["rule_id"] for r in record["rejections"]] == ["TS-endpoint"]


def test_a_reference_to_nothing_is_not_decided_by_the_validator_as_it_stands(vrdf):
    """A relationship whose end is no node of the graph. In the property graph the store cannot hold one; in RDF
    it can. Validator 0.2.0 cannot ask the tool about an end outside the write and reports CS3 as not checkable,
    so the write is kept. The tool offers `entity_exists`; with it passed on, the same write is refused."""
    vrdf.tool(S4 + 'INSERT DATA { d:dr-1 a :DisposalRule ; :uid "dr-1" }', passage="p")
    dangling = S4 + "INSERT DATA { d:dr-1 :ROUTES_TO d:nowhere }"
    assert vrdf.tool(dangling, passage="p")["status"] == "stored"
    assert "CS3" in attempt(vrdf)["validator_not_checkable"]
    vrdf.tool(S4 + "DELETE DATA { d:dr-1 :ROUTES_TO d:nowhere }", passage="p")

    from validator.rules_write import WriteContext

    class WithLookup(validator_checker.Checker):
        def check(self, changes, lookup=None):
            decl = self.declaration(changes.get("realisation"))
            from validator.validator import Validator
            result = Validator(mode=self.mode, formats=self.formats, declaration=decl).check_write(
                changes, WriteContext(formats=self.formats, lookup=lookup, entity_exists=lookup.entity_exists))
            self.last = result
            return [r.to_log() for r in result.rejections]

    original, vrdf.checker = vrdf.checker, WithLookup(root=REPO)
    try:
        assert rules(vrdf.tool(dangling, passage="p")) == ["CS3"]
        assert vrdf.tool(S4 + 'INSERT DATA { d:ws-1 a :WasteStream ; :uid "ws-1" . d:dr-1 :ROUTES_TO d:ws-1 }', passage="p")["status"] == "stored"
    finally:
        vrdf.checker = original
