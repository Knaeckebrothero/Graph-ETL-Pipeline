import json
import urllib.request

import pytest
from neo4j import GraphDatabase
from neo4j.exceptions import Neo4jError

from conftest import PASSWORD, URI

RULE = """MERGE (w:WasteItem {name: 'Paint can'}) ON CREATE SET w.uid = 'wi-1'
MERGE (s:WasteStream {name: 'Hazardous waste'}) ON CREATE SET s.uid = 'ws-1'
CREATE (r:DisposalRule {uid: 'dr-1'})
MERGE (w)-[:HAS_RULE]->(r) MERGE (r)-[:ROUTES_TO]->(s)"""


def records(cell, record_type=None):
    lines = [json.loads(line) for line in cell.log_path.read_text().splitlines()]
    return [line for line in lines if record_type is None or line["record_type"] == record_type]


def count(cell, pattern="(n)"):
    return cell.store.admin(f"MATCH {pattern} RETURN count(*) AS c")[0]["c"]


def test_a_write_is_stored_and_the_checker_sees_exactly_the_changes(cell):
    answer = cell.tool(RULE, item_id="i1", passage="Farbdosen gehören zum Sondermüll.")
    assert answer["status"] == "stored"
    assert answer["created"] == {"nodes": 3, "relationships": 2}
    assert count(cell) == 3
    changes = cell.test_checker.seen[-1]
    assert sorted(n["labels"][0] for n in changes["nodes"]) == ["DisposalRule", "WasteItem", "WasteStream"]
    assert all(n["change"] == "created" for n in changes["nodes"])
    assert {(changes["ends"][r["from"]]["labels"][0], r["type"], changes["ends"][r["to"]]["labels"][0])
            for r in changes["relationships"]} == {("WasteItem", "HAS_RULE", "DisposalRule"),
                                                   ("DisposalRule", "ROUTES_TO", "WasteStream")}
    assert changes["unit_id"] == "u0001" and changes["item_id"] == "i1" and changes["realisation"] == "S4-curated/pg"
    record = records(cell, "insert_attempt")[-1]
    assert record["engine_result"] == "ok" and record["accepted"] and record["validator_result"] == "pass"
    assert len(record["element_ids"]) == 5 and record["retry_index"] == 0
    assert record["evidence"] == "Farbdosen gehören zum Sondermüll." and record["tool_name"] == "cypher"
    assert record["run_id"] == "run-test" and record["stratum"] == "prose"


def test_a_rejected_write_leaves_nothing_and_names_every_fault(cell):
    cell.tool(RULE, item_id="i1")
    answer = cell.tool("""MATCH (w:WasteItem {name: 'Paint can'}), (s:WasteStream)
                          CREATE (t:Tip {uid: 't-1', text: 'x'}) CREATE (w)-[:HAS_TIP]->(t)
                          CREATE (w)-[:ROUTES_TO]->(s) CREATE (:Forbidden {x: 1})""", item_id="i2", passage="p")
    assert answer["status"] == "rejected"
    assert sorted(r["rule_id"] for r in answer["rejections"]) == ["TS-endpoint", "TS-type"]
    assert "TS-endpoint" in answer["message"] and "TS-type" in answer["message"]
    assert count(cell) == 3 and count(cell, "(:Tip)") == 0
    record = records(cell, "insert_attempt")[-1]
    assert record["engine_result"] == "not_sent" and not record["accepted"] and record["validator_result"] == "reject"
    assert record["element_ids"] == [] and len(record["rejections"]) == 2


def test_log_only_mode_keeps_the_write_and_records_the_faults(cell):
    cell.mode = "log_only"
    answer = cell.tool("CREATE (:Forbidden {x: 1})", item_id="i1")
    assert answer["status"] == "stored" and count(cell, "(:Forbidden)") == 1
    record = records(cell, "insert_attempt")[-1]
    assert record["validator_mode"] == "log_only" and record["validator_result"] == "reject"
    assert record["engine_result"] == "ok" and record["accepted"] and record["rejections"][0]["rule_id"] == "TS-type"


def test_reading_does_not_reach_the_checker(cell):
    cell.tool(RULE, item_id="i1")
    seen = len(cell.test_checker.seen)
    answer = cell.tool("MATCH (w:WasteItem)-[:HAS_RULE]->(r) RETURN w.name AS name, r.uid AS rule")
    assert answer == {"status": "result", "rows": [{"name": "Paint can", "rule": "dr-1"}], "row_count": 1, "truncated": False}
    assert len(cell.test_checker.seen) == seen
    assert records(cell)[-1]["record_type"] == "graph_read"


def test_a_statement_that_changes_nothing_is_already_present(cell):
    merge = "MERGE (w:WasteItem {name: 'Paint can'}) ON CREATE SET w.uid = 'wi-1'"
    assert cell.tool(merge, item_id="i1")["status"] == "stored"
    seen = len(cell.test_checker.seen)
    answer = cell.tool(merge, item_id="i1")
    assert answer["status"] == "already_present" and count(cell) == 1
    assert len(cell.test_checker.seen) == seen
    record = records(cell, "insert_attempt")[-1]
    assert record["engine_result"] == "already_present" and record["accepted"] and record["retry_index"] == 1


def test_the_checker_can_ask_what_was_there_before(cell):
    assert cell.tool("CREATE (:WasteItem {uid: 'wi-1', name: 'Paint can'})", item_id="i1")["status"] == "stored"
    answer = cell.tool("CREATE (:WasteItem {uid: 'wi-1', name: 'Paint tin'})", item_id="i2")
    assert answer["status"] == "rejected" and answer["rejections"][0]["rule_id"] == "TS-unique"
    assert count(cell) == 1


def test_a_change_to_an_existing_node_and_a_delete_are_reported(cell):
    cell.tool(RULE, item_id="i1")
    cell.tool("MATCH (w:WasteItem {name: 'Paint can'}) SET w.synonyms = ['Farbdose'] SET w:Checked", item_id="i2")
    node = cell.test_checker.seen[-1]["nodes"][0]
    assert node["change"] == "changed" and node["changed_properties"] == {"synonyms": {"old": None, "new": ["Farbdose"]}}
    assert node["added_labels"] == ["Checked"] and node["properties"]["name"] == "Paint can"
    answer = cell.tool("MATCH (r:DisposalRule) DETACH DELETE r", item_id="i3")
    assert answer["status"] == "stored" and answer["deleted"] == 3
    deleted = cell.test_checker.seen[-1]["deleted"]
    assert len(deleted["nodes"]) == 1 and len(deleted["relationships"]) == 2


def test_a_statement_the_store_cannot_read(cell):
    answer = cell.tool("CREAT (:WasteItem {name: 'x'})", item_id="i1", passage="p")
    assert answer["status"] == "error" and "Invalid input" in answer["message"]
    record = records(cell, "insert_attempt")[-1]
    assert record["engine_result"] == "error" and not record["accepted"]
    assert record["rejections"][0]["rule_id"] == "CS0" and record["validator_result"] == "reject"
    assert cell.tool("MATCH (n RETURN n")["status"] == "error"
    assert records(cell)[-1]["record_type"] == "statement_error"


def test_retries_of_the_same_item_are_counted_per_unit(cell):
    for expected in (0, 1, 2):
        cell.tool("CREATE (:Forbidden)", item_id="i7")
        assert records(cell, "insert_attempt")[-1]["retry_index"] == expected
    cell.open_unit({"unit_id": "u0002", "run_id": "run-test", "source_id": "src-1", "stratum": "prose"})
    cell.tool("CREATE (:Forbidden)", item_id="i7")
    record = records(cell, "insert_attempt")[-1]
    assert record["retry_index"] == 0 and record["unit_id"] == "u0002"


def test_schema_and_set_up_commands_are_not_passed_on(cell):
    for statement in ("CREATE CONSTRAINT x IF NOT EXISTS FOR (n:A) REQUIRE n.a IS UNIQUE",
                      "DROP INDEX waste_item_synonyms", "CALL apoc.trigger.removeAll()",
                      "CALL apoc.trigger.drop('neo4j', 'dbtool-dev-pg')", "CREATE DATABASE other", "  "):
        assert cell.tool(statement)["status"] == "refused", statement
    assert cell.tool("CREATE (:Tip {uid: 't', text: 'please create index cards; drop index fingers'})", item_id="i1")["status"] == "stored"


def test_a_write_that_goes_around_the_tool_is_undone(cell):
    driver = GraphDatabase.driver(URI, auth=("neo4j", PASSWORD))
    with driver.session() as session, pytest.raises(Neo4jError):
        session.run("CREATE (:Bypass)").consume()
    driver.close()
    assert count(cell, "(:Bypass)") == 0
    assert records(cell)[-1]["record_type"] == "unattributed_write"


def test_no_unit_no_statement(cell):
    cell.close_unit()
    assert cell.tool("MATCH (n) RETURN count(n)")["status"] == "error"


def test_a_checker_that_fails_lets_nothing_through(cell):
    original = cell.checker
    cell.checker = type("Broken", (), {"check": lambda self, changes, lookup: 1 / 0})()
    try:
        answer = cell.tool("CREATE (:WasteItem {name: 'x'})", item_id="i1")
    finally:
        cell.checker = original
    assert answer["status"] == "rejected" and answer["rejections"][0]["rule_id"] == "CHECKER-FAULT"
    assert count(cell) == 0


def test_the_same_over_http(service, cell):
    def call(method, path, body=None, key=None):
        request = urllib.request.Request(service.address + path, method=method,
                                         data=json.dumps(body).encode() if body is not None else None,
                                         headers={"Content-Type": "application/json", **({"X-Harness-Key": key} if key else {})})
        try:
            with urllib.request.urlopen(request) as response:
                return response.status, json.loads(response.read())
        except urllib.error.HTTPError as error:
            return error.code, json.loads(error.read())
    assert call("GET", "/health")[1] == {"status": "ok", "cells": ["dev-pg"]}
    assert call("POST", "/cells/dev-pg/unit", {"unit_id": "u9"})[0] == 403
    assert call("POST", "/cells/dev-pg/unit", {"unit_id": "u9", "run_id": "r"}, key="test-key")[0] == 200
    status, answer = call("POST", "/cells/dev-pg/tool", {"statement": "CREATE (:WasteItem {name: 'Jar'})", "id": "i1", "passage": "p"})
    assert status == 200 and answer["status"] == "stored"
    status, answer = call("POST", "/cells/dev-pg/tool", {"statement": "MATCH (w:WasteItem) RETURN w.name AS name"})
    assert answer["rows"] == [{"name": "Jar"}]
    assert call("POST", "/check/dev-pg", {"meta": {}})[0] == 403
    assert call("DELETE", "/cells/dev-pg/unit", key="test-key")[0] == 200


def test_a_statement_that_creates_and_removes_the_same_thing_changed_nothing(cell):
    seen = len(cell.test_checker.seen)
    answer = cell.tool("CREATE (n:WasteItem {name: 'gone'})-[:HAS_TIP]->(t:Tip {uid: 't'}) DETACH DELETE n, t", item_id="i1")
    assert answer["status"] == "already_present" and count(cell) == 0
    assert cell.fault is None and len(cell.test_checker.seen) == seen


def test_without_a_working_hook_the_cell_stops(service, cell):
    import time
    cell.store.remove_hook()
    time.sleep(2.5)                                     # the store drops the hook within its refresh interval
    try:
        answer = cell.tool("CREATE (:WasteItem {name: 'unchecked'})", item_id="i1", passage="p")
        assert answer["status"] == "error" and "not checking writes" in answer["message"]
        assert records(cell)[-1]["record_type"] == "hook_missing"
        assert cell.tool("MATCH (n) RETURN count(n)")["status"] == "error"          # the cell stays shut
        with pytest.raises(RuntimeError):
            cell.open_unit({"unit_id": "u0003"})                                    # and no unit opens without the hook
    finally:
        cell.store.install_hook(f"{service.address}/check/{cell.cell_id}", service.control_key)
        cell.store.admin("MATCH (n) DETACH DELETE n")
    cell.open_unit({"unit_id": "u0003", "run_id": "run-test"})
    assert cell.fault is None
    assert cell.tool("CREATE (:WasteItem {name: 'checked'})", item_id="i1")["status"] == "stored"
