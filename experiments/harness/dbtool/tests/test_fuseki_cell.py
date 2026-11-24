"""The RDF side of the tool against the development Fuseki (deploy/harness, dataset `cell`).

    HARNESS_FUSEKI_ENDPOINT   the dataset's address, default http://127.0.0.1:13030/cell
    HARNESS_FUSEKI_LOG        its change log as this process reads it
"""
import base64
import hashlib
import json
import time
import urllib.parse
import urllib.request

import pytest

from conftest import FUSEKI, FUSEKI_LOG, FUSEKI_PASSWORD, StandInChecker, in_step
from dbtool.fuseki_cell import Outcome, nt
from dbtool.service import Service

P = ("PREFIX : <https://fessi.example/schema/s4#> PREFIX d: <https://fessi.example/data/> "
     "PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n")
D = "https://fessi.example/data/"
RULE = P + """INSERT DATA {
  d:wi-1 a :WasteItem ; :uid "wi-1" ; :name "Paint can" .
  d:ws-1 a :WasteStream ; :uid "ws-1" ; :name "Hazardous waste" .
  d:dr-1 a :DisposalRule ; :uid "dr-1" .
  d:wi-1 :HAS_RULE d:dr-1 . d:dr-1 :ROUTES_TO d:ws-1 . }"""


def records(cell, record_type=None):
    lines = [json.loads(line) for line in cell.log_path.read_text().splitlines()]
    return [line for line in lines if record_type is None or line["record_type"] == record_type]


def warnings(cell):
    return [json.loads(line) for line in cell.warnings_path.read_text().splitlines()]


def graph(cell):
    """Every triple of the store, as the store writes it."""
    return sorted(row["triple"] for row in cell.store.admin("CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }"))


def past_the_tool(update):
    """A write straight to the store, as nothing in a run can do."""
    request = urllib.request.Request(FUSEKI + "/update", data=urllib.parse.urlencode({"update": update}).encode(),
                                     headers={"Authorization": "Basic " + base64.b64encode(
                                         f"admin:{FUSEKI_PASSWORD}".encode()).decode()})
    urllib.request.urlopen(request).read()


def test_a_write_is_stored_and_the_checker_sees_exactly_the_changes(rdf):
    answer = rdf.tool(RULE, item_id="i1", passage="Farbdosen gehören zum Sondermüll.")
    assert answer["status"] == "stored"
    assert answer["created"] == {"nodes": 3, "relationships": 2} and answer["triples"] == {"added": 10, "removed": 0}
    assert rdf.store.counts() == (10, 0) and rdf.store.size() == {"triples": 10}
    changes = rdf.test_checker.seen[-1]
    assert changes["arm"] == "rdf" and sorted(n["labels"][0] for n in changes["nodes"]) == ["DisposalRule", "WasteItem", "WasteStream"]
    assert all(n["change"] == "created" for n in changes["nodes"])
    item = next(n for n in changes["nodes"] if n["labels"] == ["WasteItem"])
    assert item["key"] == D + "wi-1" and item["properties"] == {"uid": "wi-1", "name": "Paint can"}
    assert {(changes["ends"][r["from"]]["labels"][0], r["type"], changes["ends"][r["to"]]["labels"][0])
            for r in changes["relationships"]} == {("WasteItem", "HAS_RULE", "DisposalRule"),
                                                   ("DisposalRule", "ROUTES_TO", "WasteStream")}
    assert changes["unit_id"] == "u0001" and changes["item_id"] == "i1" and changes["realisation"] == "S4-curated/rdf"
    record = records(rdf, "insert_attempt")[-1]
    assert record["engine_result"] == "ok" and record["accepted"] and record["validator_result"] == "pass"
    line = f"<{D}wi-1> <https://fessi.example/schema/s4#uid> \"wi-1\" ."
    assert len(record["element_ids"]) == 10 and hashlib.sha256(line.encode()).hexdigest() in record["element_ids"]
    assert line in changes["triples"]["added"]
    assert record["evidence"] == {"quote": "Farbdosen gehören zum Sondermüll.", "locator": None}
    assert record["tool_name"] == "sparql" and answer["id"] == "i1" and record["retry_index"] == 0


def test_a_rejected_write_is_taken_out_again_and_names_every_fault(rdf):
    rdf.tool(RULE, item_id="i1")
    before = graph(rdf)
    answer = rdf.tool(P + """INSERT { d:t-1 a :Tip ; :uid "t-1" ; :text "x" . ?w :HAS_TIP d:t-1 ; :ROUTES_TO ?s .
                                  d:f-1 a :Forbidden ; :uid "f-1" . [] a :Tip ; :uid "t-2" }
                         WHERE { ?w a :WasteItem . ?s a :WasteStream }""", item_id="i2", passage="p")
    assert answer["status"] == "rejected"
    assert sorted(r["rule_id"] for r in answer["rejections"]) == ["TS-endpoint", "TS-type"]
    assert "TS-endpoint" in answer["message"] and "TS-type" in answer["message"]
    assert graph(rdf) == before and rdf.store.counts() == (10, 0) and rdf.store.mirror.count == 10
    record = records(rdf, "insert_attempt")[-1]
    assert record["engine_result"] == "not_sent" and not record["accepted"] and record["validator_result"] == "reject"
    assert record["element_ids"] == [] and len(record["rejections"]) == 2
    assert len(record["changes"]["triples"]["added"]) == 9                   # what was written and taken out is kept


def test_a_rejected_delete_puts_everything_back(rdf):
    rdf.tool(RULE, item_id="i1")
    before = graph(rdf)
    original = rdf.checker
    rdf.checker = type("No", (), {"check": lambda self, changes, lookup: [
        {"rule_id": "TEST", "element": "x", "expected": "nothing", "found": "a change"}]})()
    try:
        answer = rdf.tool(P + 'DELETE { ?s ?p ?o } INSERT { d:new :uid "n" } WHERE { ?s ?p ?o }', item_id="i2")
    finally:
        rdf.checker = original
    assert answer["status"] == "rejected" and graph(rdf) == before


def test_log_only_mode_keeps_the_write_and_records_the_faults(rdf):
    rdf.mode = "log_only"
    answer = rdf.tool(P + 'INSERT DATA { d:f-1 a :Forbidden ; :uid "f-1" }', item_id="i1")
    assert answer["status"] == "stored" and rdf.store.counts() == (2, 0)
    record = records(rdf, "insert_attempt")[-1]
    assert record["validator_mode"] == "log_only" and record["validator_result"] == "reject"
    assert record["engine_result"] == "ok" and record["accepted"] and record["rejections"][0]["rule_id"] == "TS-type"


def test_reading_does_not_reach_the_checker(rdf):
    rdf.tool(RULE, item_id="i1")
    seen = len(rdf.test_checker.seen)
    answer = rdf.tool(P + "SELECT ?name ?rule ?nothing { ?w a :WasteItem ; :name ?name ; :HAS_RULE ?rule OPTIONAL { ?w :none ?nothing } }")
    assert answer == {"status": "result", "rows": [{"name": "Paint can", "rule": f"<{D}dr-1>", "nothing": None}],
                      "row_count": 1, "truncated": False}
    assert len(rdf.test_checker.seen) == seen and records(rdf)[-1]["record_type"] == "graph_read"
    assert rdf.tool(P + "ASK { d:wi-1 :HAS_RULE d:dr-1 }")["rows"] == [{"ask": True}]
    triples = rdf.tool(P + "CONSTRUCT { ?s :uid ?u } WHERE { ?s :uid ?u }")["rows"]
    assert len(triples) == 3 and all(row["triple"].endswith(" .") for row in triples)


def test_values_come_back_in_a_form_that_can_be_written_again(rdf):
    rdf.tool(P + """INSERT DATA { d:v a :WasteItem ; :uid "v" ; :n 3 ; :dec 2.50 ; :flag true ; :day "2024-01-01"^^xsd:date ;
                                  :word "Glas"@de ; :ref d:other }""", item_id="i1")
    row = rdf.tool(P + "SELECT * { d:v :n ?n ; :dec ?dec ; :flag ?flag ; :day ?day ; :word ?word ; :ref ?ref }")["rows"][0]
    assert row == {"n": 3, "dec": 2.5, "flag": True, "day": {"value": "2024-01-01", "datatype": "xsd:date"},
                   "word": {"value": "Glas", "lang": "de"}, "ref": f"<{D}other>"}
    node = rdf.test_checker.seen[-1]["nodes"][0]
    assert node["properties"]["n"] == {"value": "3", "datatype": "integer", "datatype_iri": "http://www.w3.org/2001/XMLSchema#integer"}
    assert node["properties"]["day"]["datatype"] == "date" and node["properties"]["word"] == {"value": "Glas", "lang": "de"}
    assert node["properties"]["dec"]["value"] == "2.5"       # the store keeps a number by its value, not as written
    relationship = rdf.test_checker.seen[-1]["relationships"][0]
    assert relationship["to"] == D + "other" and rdf.test_checker.seen[-1]["ends"][D + "other"]["exists"] is False


def test_two_spellings_of_one_number_are_one_value(rdf):
    spelled = P + """INSERT DATA { d:v a :WasteItem ; :uid "v" ; :n 05 ; :dec 2.50 ; :big 1.5e3 ; :flag "1"^^xsd:boolean ;
                                   :at "2024-01-01T10:00:00.500Z"^^xsd:dateTime ; :f "0.10"^^xsd:float ; :wrong "abc"^^xsd:integer }"""
    assert rdf.tool(spelled, item_id="i1")["triples"]["added"] == 9
    again = P + """INSERT DATA { d:v :n 5 ; :dec 2.5 ; :big 1500.0e0 ; :flag true ; :f "0.1"^^xsd:float ;
                                 :at "2024-01-01T10:00:00.5Z"^^xsd:dateTime }"""
    assert rdf.tool(again, item_id="i2")["status"] == "already_present"
    properties = rdf.test_checker.seen[-1]["nodes"][0]["properties"]
    assert properties["n"]["value"] == "5" and properties["f"]["value"] == "0.1" and properties["wrong"]["value"] == "abc"
    assert rdf.store.find("WasteItem", "n", "5") == [D + "v"]
    removed = rdf.tool(P + 'DELETE DATA { d:v :n 5 ; :dec 2.500 ; :flag true }', item_id="i3")
    assert removed["triples"] == {"added": 0, "removed": 3} and in_step(rdf)
    assert rdf.tool("DELETE WHERE { ?s ?p ?o }", item_id="i4")["triples"]["removed"] == 6 and rdf.store.counts() == (0, 0)


def test_a_list_that_came_apart_from_the_store_is_read_again(rdf):
    rdf.tool(RULE, item_id="i1")
    state = {subject: set(pairs) for subject, pairs in rdf.store.mirror.by_subject.items()}
    rdf.store.mirror.remove((f"<{D}wi-1>", "<https://fessi.example/schema/s4#uid>", '"wi-1"'))     # as if the list had a fault
    assert rdf.store.mirror.count == 9
    rdf.open_unit({"unit_id": "u0002", "run_id": "run-test"})
    assert rdf.store.mirror.by_subject == state and in_step(rdf)
    assert [w["kind"] for w in warnings(rdf)] == ["list_rebuilt"]
    assert rdf.store.find("WasteItem", "uid", "wi-1") == [D + "wi-1"]


def test_a_statement_that_changes_nothing_is_already_present(rdf):
    insert = P + 'INSERT DATA { d:wi-1 a :WasteItem ; :uid "wi-1" ; :name "Paint can" }'
    assert rdf.tool(insert, item_id="i1")["status"] == "stored"
    seen = len(rdf.test_checker.seen)
    answer = rdf.tool(insert, item_id="i1")
    assert answer["status"] == "already_present" and rdf.store.counts() == (3, 0)
    assert len(rdf.test_checker.seen) == seen
    record = records(rdf, "insert_attempt")[-1]
    assert record["engine_result"] == "already_present" and record["accepted"] and record["retry_index"] == 1
    assert sorted(record["element_ids"]) == sorted(records(rdf, "insert_attempt")[0]["element_ids"])
    assert rdf.tool(P + "DELETE WHERE { d:nobody ?p ?o }", item_id="i2")["status"] == "already_present"


def test_a_statement_that_adds_and_removes_the_same_thing_changed_nothing(rdf):
    seen = len(rdf.test_checker.seen)
    answer = rdf.tool(P + 'INSERT DATA { d:gone :uid "g" } ; DELETE WHERE { d:gone ?p ?o }', item_id="i1")
    assert answer["status"] == "already_present" and rdf.store.counts() == (0, 0)
    assert rdf.fault is None and len(rdf.test_checker.seen) == seen


def test_the_checker_can_ask_what_was_there_before(rdf):
    assert rdf.tool(P + 'INSERT DATA { d:wi-1 a :WasteItem ; :uid "wi-1" ; :name "Paint can" }', item_id="i1")["status"] == "stored"
    answer = rdf.tool(P + 'INSERT DATA { d:wi-2 a :WasteItem ; :uid "wi-1" ; :name "Paint tin" }', item_id="i2")
    assert answer["status"] == "rejected" and answer["rejections"][0]["rule_id"] == "TS-unique"
    assert rdf.store.counts() == (3, 0)
    assert rdf.store.find("WasteItem", "uid", "wi-1") == [D + "wi-1"] and rdf.store.find("Tip", "uid", "wi-1") == []
    assert rdf.store.find(None, "uid", {"value": "wi-1"}) == [D + "wi-1"] and rdf.store.find("WasteItem", "uid", "no") == []
    assert rdf.store.entity_exists(D + "wi-1") == 1 and rdf.store.entity_exists(D + "wi-2") == 0


def test_a_change_to_an_existing_node_and_a_delete_are_reported(rdf):
    rdf.tool(RULE, item_id="i1")
    rdf.tool(P + 'INSERT DATA { d:wi-1 :synonyms "Farbdose" ; a :Checked }', item_id="i2")
    node = rdf.test_checker.seen[-1]["nodes"][0]
    assert node["change"] == "changed" and node["changed_properties"] == {"synonyms": {"old": None, "new": "Farbdose"}}
    assert node["added_labels"] == ["Checked"] and node["properties"]["name"] == "Paint can"
    rdf.tool(P + 'DELETE { d:wi-1 :name ?old } INSERT { d:wi-1 :name "Paint tin" } WHERE { d:wi-1 :name ?old }', item_id="i3")
    assert rdf.test_checker.seen[-1]["nodes"][0]["changed_properties"] == {"name": {"old": "Paint can", "new": "Paint tin"}}
    answer = rdf.tool(P + "DELETE WHERE { d:dr-1 ?p ?o } ; DELETE WHERE { ?s ?p d:dr-1 }", item_id="i4")
    assert answer["status"] == "stored" and answer["deleted"] == 3 and answer["triples"] == {"added": 0, "removed": 4}
    deleted = rdf.test_checker.seen[-1]["deleted"]
    assert deleted["nodes"] == [D + "dr-1"] and len(deleted["relationships"]) == 2


def test_a_value_on_a_relationship_is_written_with_the_annotation_form_and_read_back(rdf):
    answer = rdf.tool(P + """INSERT DATA { d:p-1 a :Product ; :uid "p-1" . d:pc-1 a :PackagingComponent ; :uid "pc-1" .
                                           d:p-1 :HAS_COMPONENT d:pc-1 {| :qty 2 |} }""", item_id="i1")
    assert answer["status"] == "stored" and answer["created"] == {"nodes": 2, "relationships": 1}
    changes = rdf.test_checker.seen[-1]
    (relationship,) = changes["relationships"]
    assert relationship["type"] == "HAS_COMPONENT" and relationship["properties"]["qty"]["value"] == "2"
    assert relationship["from"] == D + "p-1" and relationship["to"] == D + "pc-1" and relationship["change"] == "created"
    assert relationship["key"] == f"<{D}p-1> <https://fessi.example/schema/s4#HAS_COMPONENT> <{D}pc-1> ."
    assert rdf.tool(P + "SELECT ?q { d:p-1 :HAS_COMPONENT d:pc-1 {| :qty ?q |} }")["rows"] == [{"q": 2}]
    rdf.tool(P + "INSERT DATA { d:p-1 :HAS_COMPONENT d:pc-1 {| :weight_g 5 |} }", item_id="i2")
    (changed,) = rdf.test_checker.seen[-1]["relationships"]
    assert changed["change"] == "changed" and set(changed["properties"]) == {"qty", "weight_g"}
    assert list(changed["changed_properties"]) == ["weight_g"] and rdf.test_checker.seen[-1]["nodes"] == []
    before = graph(rdf)                                  # a rejected value on a relationship goes, blank node and all
    original = rdf.checker
    rdf.checker = type("No", (), {"check": lambda self, changes, lookup: [
        {"rule_id": "TEST", "element": "x", "expected": "nothing", "found": "a change"}]})()
    try:
        assert rdf.tool(P + "INSERT DATA { d:p-1 :HAS_COMPONENT d:pc-1 {| :qty 9 |} }", item_id="i3")["status"] == "rejected"
    finally:
        rdf.checker = original
    assert graph(rdf) == before


def test_a_node_without_a_name_of_its_own_is_stored_and_can_be_taken_out(rdf):
    answer = rdf.tool(P + 'INSERT DATA { [] a :Tip ; :uid "t-1" ; :text "keep dry" }', item_id="i1")
    assert answer["status"] == "stored"
    node = rdf.test_checker.seen[-1]["nodes"][0]
    assert node["key"].startswith("_:") and node["labels"] == ["Tip"] and rdf.store.entity_exists(node["key"]) == 1
    assert rdf.tool(P + 'INSERT DATA { [] a :Forbidden ; :uid "f" }', item_id="i2")["status"] == "rejected"
    assert rdf.store.counts() == (3, 0)


def test_a_statement_the_store_cannot_read(rdf):
    answer = rdf.tool(P + "INSRT DATA { d:x :uid 'x' }", item_id="i1", passage="p")
    assert answer["status"] == "error" and "Parse error" in answer["message"]
    record = records(rdf, "insert_attempt")[-1]
    assert record["engine_result"] == "error" and not record["accepted"]
    assert record["rejections"][0]["rule_id"] == "CS0" and record["validator_result"] == "reject"
    answer = rdf.tool("SELECT ?s WHERE { ?s ?p }")
    assert answer["status"] == "error" and "id" not in answer
    assert records(rdf)[-1]["record_type"] == "statement_error"
    assert rdf.tool(P + 'INSERT DATA { d:x a :WasteItem ; :uid "x" }', item_id="i1", passage="p")["status"] == "stored"
    assert records(rdf, "insert_attempt")[-1]["retry_index"] == 1


def test_the_tool_names_a_write_and_the_agent_sends_the_name_back(rdf):
    first = rdf.tool(P + 'INSERT DATA { d:f a :Forbidden ; :uid "f" }', passage="p", locator="§ 3")
    assert first["status"] == "rejected" and first["id"] == "u0001-w001"
    assert rdf.tool("SELECT (COUNT(*) AS ?c) { ?s ?p ?o }").get("id") is None           # reading uses up no name
    second = rdf.tool(P + 'INSERT DATA { d:j a :WasteItem ; :name "Jar" }', item_id=first["id"], passage="p")
    assert second["status"] == "stored" and second["id"] == "u0001-w001"
    assert rdf.tool(P + 'INSERT DATA { d:l a :WasteItem ; :name "Lid" }')["id"] == "u0001-w002"
    attempts = records(rdf, "insert_attempt")
    assert [(a["item_id"], a["retry_index"]) for a in attempts] == [("u0001-w001", 0), ("u0001-w001", 1), ("u0001-w002", 0)]
    assert attempts[0]["evidence"] == {"quote": "p", "locator": "§ 3"} and attempts[2]["evidence"] is None


def test_commands_that_leave_the_one_graph_are_not_passed_on(rdf):
    for statement in ("LOAD <http://example.org/data.ttl>", "CLEAR DEFAULT", "DROP ALL", "CREATE GRAPH <urn:g>",
                      "INSERT DATA { GRAPH <urn:g> { <urn:a> <urn:b> <urn:c> } }", "SELECT * FROM <urn:g> { ?s ?p ?o }",
                      "SELECT * { SERVICE <http://example.org/sparql> { ?s ?p ?o } }", "COPY DEFAULT TO <urn:g>",
                      "WITH <urn:g> DELETE { ?s ?p ?o } WHERE { ?s ?p ?o }", "  "):
        assert rdf.tool(statement)["status"] == "refused", statement
    assert records(rdf)[-1]["record_type"] == "refused"
    kept = rdf.tool(P + 'INSERT DATA { d:t a :Tip ; :uid "t" ; :text "do not LOAD or DROP it; FROM here" }', item_id="i1")
    assert kept["status"] == "stored"
    answer = rdf.tool(P + 'INSERT DATA { d:x :uid "x" }', parameters={"rows": [1]})
    assert answer["status"] == "error" and "no parameters" in answer["message"]
    # what the check on the text would miss, the tool takes out again
    outcome = rdf.store.run("INSERT DATA { <urn:a> <urn:b> <urn:c> . GRAPH <urn:g> { <urn:a> <urn:b> <urn:c> } }", {})
    assert isinstance(outcome, Outcome) and outcome.kind == "error" and outcome.error_code == "DbTool.NamedGraph"
    assert rdf.store.counts() == (3, 0)


def test_a_write_that_goes_around_the_tool_is_undone(rdf):
    rdf.tool(RULE, item_id="i1")
    before = graph(rdf)
    past_the_tool('INSERT DATA { <urn:bypass> <urn:p> "x" } ; DELETE WHERE { <https://fessi.example/data/wi-1> ?p ?o }')
    assert graph(rdf) != before
    answer = rdf.tool("SELECT (COUNT(*) AS ?c) { ?s ?p ?o }")
    assert answer["rows"] == [{"c": 10}] and graph(rdf) == before
    assert [r["record_type"] for r in records(rdf)[-3:]] == ["unattributed_write", "warning", "graph_read"]
    undone = records(rdf, "unattributed_write")[-1]
    assert undone["undone"] and len(undone["changes"]["triples"]["removed"]) == 4
    assert [w["kind"] for w in warnings(rdf)] == ["unattributed_write"]
    past_the_tool('INSERT DATA { <urn:bypass> <urn:p> "y" }')
    rdf.open_unit({"unit_id": "u0002", "run_id": "run-test"})                # a unit opens on a checked store
    assert graph(rdf) == before


def test_no_unit_no_statement(rdf):
    rdf.close_unit()
    assert rdf.tool("ASK { ?s ?p ?o }")["status"] == "error"


def test_a_checker_that_fails_lets_nothing_through(rdf):
    original = rdf.checker
    rdf.checker = type("Broken", (), {"check": lambda self, changes, lookup: 1 / 0})()
    try:
        answer = rdf.tool(P + 'INSERT DATA { d:x a :WasteItem ; :name "x" }', item_id="i1")
    finally:
        rdf.checker = original
    assert answer["status"] == "rejected" and answer["rejections"][0]["rule_id"] == "CHECKER-FAULT"
    assert rdf.store.counts() == (0, 0) and [w["kind"] for w in warnings(rdf)] == ["checker_fault"]


def test_a_fault_of_the_tool_itself_leaves_nothing_unjudged(rdf):
    rdf.tool(RULE, item_id="i1")
    before, original = graph(rdf), rdf.store.judge
    rdf.store.judge = lambda changes: 1 / 0               # not the checker: the tool's own code fails after the write
    try:
        answer = rdf.tool(P + 'INSERT DATA { d:x a :WasteItem ; :name "x" }', item_id="i2")
    finally:
        rdf.store.judge = original
    assert answer["status"] == "error" and "fault of the harness" in answer["message"]
    assert graph(rdf) == before and in_step(rdf)
    assert [r["record_type"] for r in records(rdf)[-2:]] == ["tool_fault", "warning"]
    assert rdf.tool("ASK {}")["status"] == "error"         # the cell stays shut for this unit
    rdf.open_unit({"unit_id": "u0002", "run_id": "run-test"})
    assert rdf.tool(P + 'INSERT DATA { d:x a :WasteItem ; :name "x" }', item_id="i1")["status"] == "stored"


def test_a_reading_statement_shows_a_limited_number_of_rows(rdf):
    rows = " ".join(f'd:i{i} a :WasteItem ; :name "item {i}" .' for i in range(30))
    assert rdf.tool(P + f"INSERT DATA {{ {rows} }}", item_id="i1")["created"]["nodes"] == 30
    rdf.store.max_rows = 10
    answer = rdf.tool(P + "SELECT ?n { ?s :name ?n } ORDER BY ?n")
    assert answer["row_count"] == 10 and answer["truncated"] and records(rdf)[-1]["truncated"]
    answer = rdf.tool(P + "CONSTRUCT { ?s :name ?n } WHERE { ?s :name ?n }")
    assert answer["row_count"] == 10 and answer["truncated"]


def test_a_large_delete_is_allowed_and_reported(rdf, capsys):
    rdf.warn_deleted = 10
    rows = " ".join(f'd:o{i} a :WasteItem ; :name "old {i}" ; :HAS_TIP d:t{i} . d:t{i} a :Tip ; :uid "tip {i}" .' for i in range(4))
    rdf.tool(P + f"INSERT DATA {{ {rows} }}", item_id="i1")                               # 20 triples
    rdf.open_unit({"unit_id": "u0002", "run_id": "run-test"})                            # a later unit
    rdf.tool(P + 'INSERT DATA { d:mine a :WasteItem ; :name "mine" }', item_id="i1")
    assert rdf.tool(P + "DELETE WHERE { d:mine ?p ?o }", item_id="i2")["status"] == "stored"
    assert warnings(rdf) == []                                                           # a small delete is no warning
    rdf.tool(P + 'INSERT DATA { d:mine a :WasteItem ; :name "mine" }', item_id="i3")
    answer = rdf.tool("DELETE WHERE { ?s ?p ?o }", item_id="i4")
    assert answer["status"] == "stored" and answer["triples"] == {"added": 0, "removed": 22} and rdf.store.counts() == (0, 0)
    (warning,) = warnings(rdf)
    assert warning["kind"] == "bulk_delete" and warning["unit_id"] == "u0002" and warning["cell_id"] == "dev-rdf"
    assert warning["deleted_triples"] == 22 and warning["deleted_from_earlier_units"] == 20
    assert warning["graph_after"] == {"triples": 0} and "bulk_delete" in capsys.readouterr().err


def test_the_same_over_http(rdf_service, rdf):
    def call(method, path, body=None, key=None):
        request = urllib.request.Request(rdf_service.address + path, method=method,
                                         data=json.dumps(body).encode() if body is not None else None,
                                         headers={"Content-Type": "application/json", **({"X-Harness-Key": key} if key else {})})
        try:
            with urllib.request.urlopen(request) as response:
                return response.status, json.loads(response.read())
        except urllib.error.HTTPError as error:
            return error.code, json.loads(error.read())
    assert call("GET", "/health")[1] == {"status": "ok", "cells": ["dev-rdf"]}
    assert call("POST", "/cells/dev-rdf/unit", {"unit_id": "u9"})[0] == 403
    assert call("POST", "/cells/dev-rdf/unit", {"unit_id": "u9", "run_id": "r"}, key="test-key")[0] == 200
    status, answer = call("POST", "/cells/dev-rdf/tool", {"statement": P + 'INSERT DATA { d:j a :WasteItem ; :name "Jar" }', "id": "i1", "passage": "p"})
    assert status == 200 and answer["status"] == "stored" and answer["created"]["nodes"] == 1
    status, answer = call("POST", "/cells/dev-rdf/tool", {"statement": P + "SELECT ?name { ?w a :WasteItem ; :name ?name }"})
    assert answer["rows"] == [{"name": "Jar"}]
    assert call("DELETE", "/cells/dev-rdf/unit", key="test-key")[0] == 200


def test_the_tools_list_survives_a_restart_of_the_tool(rdf_service, rdf, tmp_path):
    rdf.tool(RULE, item_id="i1")
    rdf.tool(P + "INSERT DATA { d:wi-1 :HAS_TIP d:t-1 {| :qty 2 |} . d:t-1 a :Tip ; :uid \"t-1\" }", item_id="i2")
    rdf.tool(P + "DELETE WHERE { d:ws-1 :name ?n }", item_id="i3")
    rdf.tool(P + 'INSERT DATA { d:f a :Forbidden ; :uid "f" }', item_id="i4")           # rejected and taken out
    before, state = graph(rdf), {subject: set(pairs) for subject, pairs in rdf.store.mirror.by_subject.items()}
    second = Service(port=0)                                   # a second tool on the same store, log and mark
    restarted = second.add_fuseki_cell("dev-rdf", FUSEKI, "admin", FUSEKI_PASSWORD, change_log=FUSEKI_LOG,
                                       checker=StandInChecker(), log_dir=rdf.log_path.parent, realisation="S4-curated/rdf")
    try:
        restarted.store.start()
        assert restarted.store.mirror.by_subject == state and in_step(restarted)
        assert restarted.store.offset == FUSEKI_LOG.stat().st_size
        restarted.open_unit({"unit_id": "u0009", "run_id": "run-test"})
        answer = restarted.tool(P + 'INSERT DATA { d:wi-9 a :WasteItem ; :uid "wi-1" }', item_id="i1")
        assert answer["status"] == "rejected" and answer["rejections"][0]["rule_id"] == "TS-unique"
        restarted.close_unit()
    finally:
        restarted.store.close()
        second.server.server_close()
    rdf.store.start()                                          # the first tool reads what the second one did
    assert graph(rdf) == before and in_step(rdf)


def test_a_write_nobody_judged_is_taken_out_when_the_tool_starts(rdf_service, rdf):
    rdf.tool(RULE, item_id="i1")
    before = graph(rdf)
    past_the_tool('INSERT DATA { <urn:left> <urn:behind> "by a tool that died between a write and its verdict" }')
    second = Service(port=0)
    restarted = second.add_fuseki_cell("dev-rdf", FUSEKI, "admin", FUSEKI_PASSWORD, change_log=FUSEKI_LOG,
                                       checker=StandInChecker(), log_dir=rdf.log_path.parent, realisation="S4-curated/rdf")
    try:
        restarted.store.start()
        assert graph(restarted) == before and in_step(restarted)
        assert records(restarted, "unattributed_write")[-1]["undone"]
    finally:
        restarted.store.close()
        second.server.server_close()
    rdf.store.start()


def test_a_cell_whose_log_is_not_the_stores_log_does_not_open_a_unit(rdf, tmp_path):
    real = rdf.store.log_path
    rdf.store.log_path = tmp_path / "another.rdfp"
    try:
        with pytest.raises(RuntimeError, match="does not write its changes"):
            rdf.open_unit({"unit_id": "u0003"})
    finally:
        rdf.store.log_path = real
        rdf.store.start()
    rdf.open_unit({"unit_id": "u0003", "run_id": "run-test"})
    assert rdf.tool(P + 'INSERT DATA { d:x a :WasteItem ; :name "checked" }', item_id="i1")["status"] == "stored"


def test_terms_are_written_in_canonical_form():
    assert nt("3") == '"3"^^<http://www.w3.org/2001/XMLSchema#integer>' and nt("<_:b1>") == "_:b1"
    assert nt('"a\\tb \\"c\\" \\u00E4"@de') == '"a\\tb \\"c\\" ä"@de' and nt("true").endswith('#boolean>')
    assert nt('<<( <urn:s> <urn:p> 1.5 )>>') == '<<( <urn:s> <urn:p> "1.5"^^<http://www.w3.org/2001/XMLSchema#decimal> )>>'


def test_time_per_write_statement(rdf):
    started = time.monotonic()
    for i in range(50):
        assert rdf.tool(P + f'INSERT DATA {{ d:n{i} a :WasteItem ; :name "n {i}" }}', item_id=f"i{i}")["status"] == "stored"
    single = (time.monotonic() - started) / 50
    started = time.monotonic()
    for i in range(20):
        assert rdf.tool(P + f'INSERT DATA {{ d:f{i} a :Forbidden ; :uid "f {i}" }}', item_id=f"r{i}")["status"] == "rejected"
    rejected = (time.monotonic() - started) / 20
    rows = " ".join(f'd:b{i} a :WasteItem ; :name "b {i}" .' for i in range(500))
    started = time.monotonic()
    assert rdf.tool(P + f"INSERT DATA {{ {rows} }}", item_id="bulk")["created"]["nodes"] == 500
    bulk = time.monotonic() - started
    started = time.monotonic()
    for i in range(20):                                        # the store alone, for comparison; undone afterwards
        past_the_tool(f'INSERT DATA {{ <urn:raw{i}> <urn:p> "{i}" }}')
    raw = (time.monotonic() - started) / 20
    assert rdf.tool("ASK { <urn:raw0> ?p ?o }")["rows"] == [{"ask": False}]
    print(f"\nRDF write: {single * 1000:.0f} ms per stored statement, {rejected * 1000:.0f} ms per rejected one, "
          f"{bulk * 1000:.0f} ms for 500 nodes in one statement; the store alone {raw * 1000:.0f} ms per statement")
    assert single < 2.0
