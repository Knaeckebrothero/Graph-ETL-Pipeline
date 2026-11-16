"""The tool with Thesis B's validator as its checker, against the development Neo4j.

Skipped unless the validator can be imported:

    PYTHONPATH=experiments/thesis-b python -m pytest experiments/harness/dbtool/tests -q

What is expected here follows from the rules of schemas/treatments/README.md (Rule 5: what is binding)
and from the common-scope rule list; it is not a second copy of the validator's own tests.
"""
import json

import pytest

from conftest import HERE, PASSWORD, URI
from dbtool.service import Service

validator_checker = pytest.importorskip("validator.checker", reason="Thesis B's validator is not on the Python path")
REPO = HERE.parents[3]


@pytest.fixture(scope="module")
def vservice(tmp_path_factory):
    service = Service(port=0, control_key="test-key")
    try:
        service.add_neo4j_cell("dev-pg", URI, "neo4j", PASSWORD, checker=validator_checker.Checker(root=REPO),
                               log_dir=tmp_path_factory.mktemp("records"), realisation="S4-curated/pg")
        service.start()
    except Exception as error:
        pytest.skip(f"the development Neo4j with the commit hook is not available: {error}")
    yield service
    service.cells["dev-pg"].store.admin("MATCH (n) DETACH DELETE n")
    service.stop(remove_hooks=True)


@pytest.fixture
def vcell(vservice):
    cell = vservice.cells["dev-pg"]
    cell.store.admin("MATCH (n) DETACH DELETE n")
    cell.mode, cell.realisation = "reject", "S4-curated/pg"
    cell.log_path.write_text("")
    cell.open_unit({"unit_id": "u0001", "run_id": "run-test"})
    yield cell
    cell.close_unit()


def attempt(cell):
    return [json.loads(line) for line in cell.log_path.read_text().splitlines()
            if json.loads(line)["record_type"] == "insert_attempt"][-1]


def rules(answer):
    return sorted(r["rule_id"] for r in answer.get("rejections", []))


CURATED_RULE = """MERGE (w:WasteItem {name: 'Paint can'}) MERGE (s:WasteStream {name: 'Hazardous waste'})
CREATE (r:DisposalRule {uid: 'dr-1'}) MERGE (w)-[:HAS_RULE]->(r) MERGE (r)-[:ROUTES_TO]->(s)"""
DIRECT_ROUTE = "CREATE (:WasteItem {name: 'Paint can'})-[:ROUTES_TO]->(:WasteStream {name: 'Hazardous waste'})"


def test_curated_takes_a_rule_node_and_refuses_the_direct_route(vcell):
    assert vcell.tool(CURATED_RULE, passage="p")["status"] == "stored"
    answer = vcell.tool("MATCH (w:WasteItem), (s:WasteStream) CREATE (w)-[:ROUTES_TO]->(s)", passage="p")
    assert answer["status"] == "rejected" and rules(answer) == ["TS-endpoint"]
    assert all(set(r) == {"rule_id", "element", "expected", "found"} for r in answer["rejections"])


def test_flat_takes_the_direct_route_and_refuses_a_rule_node(vcell):
    vcell.realisation = "S3-flat/pg"
    assert vcell.tool(DIRECT_ROUTE, passage="p")["status"] == "stored"
    assert rules(vcell.tool("CREATE (:DisposalRule {uid: 'dr-1'})", passage="p")) == ["TS-type"]


def test_a_property_the_schema_does_not_name_is_stored_and_counted(vcell):
    answer = vcell.tool("CREATE (:Instruction {uid: 'in-1', text: 'Do not flush'})", passage="p")
    assert answer["status"] == "stored"
    record = attempt(vcell)
    assert record["validator_result"] == "pass" and record["rejections"] == []
    assert len(record["observations"]) == 1 and "text" in json.dumps(record["observations"][0])


def test_labels_and_relationship_types_outside_the_list_are_rejected(vcell):
    vcell.tool(CURATED_RULE, passage="p")
    assert rules(vcell.tool("CREATE (:Gadget {name: 'thing'})", passage="p")) == ["TS-type"]
    assert rules(vcell.tool("MATCH (w:WasteItem) SET w:Checked", passage="p")) == ["TS-type"]
    assert rules(vcell.tool("MATCH (w:WasteItem), (s:WasteStream) CREATE (w)-[:GOES_TO]->(s)", passage="p")) == ["TS-type"]


def test_a_unique_value_cannot_be_used_twice(vcell):
    assert vcell.tool("CREATE (:WasteItem {name: 'Paint can'})", passage="p")["status"] == "stored"
    assert rules(vcell.tool("CREATE (:WasteItem {name: 'Paint can', uid: 'other'})", passage="p")) == ["TS-unique"]


def test_an_entity_needs_an_identifying_value_whatever_the_schema(vcell):
    assert rules(vcell.tool("CREATE (:WasteItem {synonyms: ['x']})", passage="p")) == ["CS1"]
    vcell.realisation = "S1-raw-triples/pg"
    assert rules(vcell.tool("CREATE (:Entity)", passage="p")) == ["CS1"]
    stored = vcell.tool("CREATE (e:Entity {name: 'Paint can'}) "
                        "CREATE (:Assertion {predicate: 'goes to', value: 'hazardous waste'})-[:SUBJECT]->(e)", passage="p")
    assert stored["status"] == "stored"


def test_log_only_keeps_what_reject_mode_refuses(vcell):
    vcell.mode = "log_only"
    assert vcell.tool(DIRECT_ROUTE, passage="p")["status"] == "stored"
    record = attempt(vcell)
    assert record["validator_result"] == "reject" and record["engine_result"] == "ok" and record["accepted"]
    assert [r["rule_id"] for r in record["rejections"]] == ["TS-endpoint"]
