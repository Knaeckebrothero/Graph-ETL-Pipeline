"""run_query, the client inside the workspace, against the running tool."""
import json
import subprocess
import sys

import pytest

from conftest import HERE

sys.path.insert(0, str(HERE.parent / "client"))
from run_query import run_query  # noqa: E402

CLIENT = str(HERE.parent / "client" / "run_query.py")


@pytest.fixture
def workspace(service, cell, monkeypatch):
    monkeypatch.setenv("RUN_QUERY_URL", f"{service.address}/cells/{cell.cell_id}/tool")
    monkeypatch.delenv("RUN_QUERY_KEY", raising=False)
    cell.tool_key = ""
    yield cell
    cell.tool_key = ""


def records(cell):
    return [json.loads(line) for line in cell.log_path.read_text().splitlines()]


def test_a_script_inserts_many_rows_with_one_statement(workspace):
    rows = [{"name": f"item {i}", "uid": f"wi-{i}"} for i in range(50)]
    answer = run_query("UNWIND $rows AS row CREATE (:WasteItem {name: row.name, uid: row.uid})",
                       parameters={"rows": rows}, passage="table 2, rows 1 to 50", locator="table 2")
    assert answer["status"] == "stored" and answer["created"] == {"nodes": 50, "relationships": 0}
    assert len(workspace.test_checker.seen[-1]["nodes"]) == 50
    record = records(workspace)[-1]
    assert record["parameters"] == {"rows": rows} and record["evidence"] == {"quote": "table 2, rows 1 to 50", "locator": "table 2"}
    assert run_query("MATCH (w:WasteItem) RETURN count(w) AS n")["rows"] == [{"n": 50}]


def test_a_script_inserts_in_a_loop_and_every_write_is_checked(workspace):
    seen = len(workspace.test_checker.seen)
    answers = [run_query("CREATE (:Tip {uid: $uid, text: $text})", parameters={"uid": f"t{i}", "text": "x"}, passage="p")
               for i in range(30)]
    assert {a["status"] for a in answers} == {"stored"} and len({a["id"] for a in answers}) == 30
    assert len(workspace.test_checker.seen) - seen == 30
    bad = run_query("CREATE (:Forbidden {uid: $uid})", parameters={"uid": "f1"}, passage="p")
    assert bad["status"] == "rejected" and bad["rejections"][0]["rule_id"] == "TS-type"
    fixed = run_query("CREATE (:Tip {uid: $uid, text: 'y'})", parameters={"uid": "f1"}, passage="p", id=bad["id"])
    assert fixed["status"] == "stored" and fixed["id"] == bad["id"]
    assert records(workspace)[-1]["retry_index"] == 1


def test_the_shell_command(workspace, tmp_path):
    def shell(*args, stdin=None):
        done = subprocess.run([sys.executable, CLIENT, *args], input=stdin, capture_output=True, text=True)
        return done.returncode, json.loads(done.stdout)
    code, answer = shell("--passage", "p", "--param", 'rows=[{"name": "Jar"}, {"name": "Lid"}]',
                         "UNWIND $rows AS row CREATE (:WasteItem {name: row.name})")
    assert code == 0 and answer["status"] == "stored" and answer["created"]["nodes"] == 2
    code, answer = shell("MATCH (w:WasteItem) RETURN w.name AS name ORDER BY name")
    assert code == 0 and [row["name"] for row in answer["rows"]] == ["Jar", "Lid"]
    code, answer = shell("--passage", "p", "CREATE (:Forbidden)")
    assert code == 1 and answer["status"] == "rejected"
    statement = tmp_path / "s.cypher"
    statement.write_text("CREATE (:Tip {uid: $uid, text: $text})")
    params = tmp_path / "p.json"
    params.write_text(json.dumps({"uid": "t1", "text": "from a file"}))
    assert shell("--file", str(statement), "--params-file", str(params), "--passage", "p")[0] == 0
    assert shell("-", stdin="MATCH (t:Tip) RETURN t.text AS text")[1]["rows"] == [{"text": "from a file"}]
    assert shell("--param", "name=plain words", "RETURN $name AS name")[1]["rows"] == [{"name": "plain words"}]


def test_a_workspace_reaches_only_its_own_cell(workspace, monkeypatch):
    workspace.tool_key = "secret-of-this-cell"
    assert run_query("RETURN 1 AS one") == {"status": "error", "message": "tool key missing or wrong"}
    monkeypatch.setenv("RUN_QUERY_KEY", "secret-of-this-cell")
    assert run_query("RETURN 1 AS one")["rows"] == [{"one": 1}]


def test_what_a_script_gets_when_things_are_wrong(workspace, monkeypatch):
    assert run_query("CREAT (:X)", passage="p")["status"] == "error"
    assert run_query("RETURN $x", parameters={"x": {1, 2}})["status"] == "error"          # not JSON
    assert run_query("RETURN 1", parameters=["not", "a", "map"])["status"] == "error"
    monkeypatch.setenv("RUN_QUERY_URL", "http://127.0.0.1:9/cells/none/tool")
    assert "cannot be reached" in run_query("RETURN 1")["message"]
    monkeypatch.delenv("RUN_QUERY_URL")
    assert "RUN_QUERY_URL" in run_query("RETURN 1")["message"]


def test_the_same_client_reaches_an_rdf_cell(rdf_service, rdf, monkeypatch):
    monkeypatch.setenv("RUN_QUERY_URL", f"{rdf_service.address}/cells/{rdf.cell_id}/tool")
    monkeypatch.delenv("RUN_QUERY_KEY", raising=False)
    prefixes = "PREFIX : <https://fessi.example/schema/s4#> PREFIX d: <https://fessi.example/data/> "
    rows = " ".join(f'd:i{i} a :WasteItem ; :name "item {i}" .' for i in range(50))
    answer = run_query(prefixes + f"INSERT DATA {{ {rows} }}", passage="table 2, rows 1 to 50", locator="table 2")
    assert answer["status"] == "stored" and answer["created"]["nodes"] == 50 and answer["triples"]["added"] == 100
    assert run_query(prefixes + "SELECT (COUNT(?w) AS ?n) { ?w a :WasteItem }")["rows"] == [{"n": 50}]
    bad = run_query(prefixes + 'INSERT DATA { d:f a :Forbidden ; :uid "f1" }', passage="p")
    assert bad["status"] == "rejected" and bad["rejections"][0]["rule_id"] == "TS-type"
    fixed = run_query(prefixes + 'INSERT DATA { d:f a :Tip ; :uid "f1" }', passage="p", id=bad["id"])
    assert fixed["status"] == "stored" and fixed["id"] == bad["id"]
    assert run_query(prefixes + "ASK { d:f a :Tip }", parameters={"x": 1})["status"] == "error"     # SPARQL takes none
    done = subprocess.run([sys.executable, CLIENT, prefixes + "SELECT ?n { d:i7 :name ?n }"], capture_output=True, text=True)
    assert done.returncode == 0 and json.loads(done.stdout)["rows"] == [{"n": "item 7"}]
