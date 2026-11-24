"""Tests run against the development stores of deploy/harness: Neo4j with the commit hook enabled,
Fuseki with the logged dataset `cell`. A store that is not there skips its tests."""
import os
import sys
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent.parent))        # experiments/harness  ->  import dbtool

from dbtool.service import Service  # noqa: E402

URI = os.environ.get("HARNESS_NEO4J_URI", "bolt://127.0.0.1:17687")
PASSWORD = os.environ.get("HARNESS_NEO4J_PASSWORD", "harness_dev")
FUSEKI = os.environ.get("HARNESS_FUSEKI_ENDPOINT", "http://127.0.0.1:13030/cell")
FUSEKI_PASSWORD = os.environ.get("HARNESS_FUSEKI_PASSWORD", "harness_dev")
FUSEKI_LOG = Path(os.environ.get("HARNESS_FUSEKI_LOG", HERE.parents[3] / "deploy/harness/fuseki/log/cell.rdfp"))


class StandInChecker:
    """Not a validator. Two fixed rules, enough to see a rejection travel through the tool."""

    def __init__(self):
        self.seen = []

    def check(self, changes, lookup):
        self.seen.append(changes)
        faults = []
        for node in changes["nodes"]:
            if "Forbidden" in node["labels"]:
                faults.append({"rule_id": "TS-type", "element": f"node:{node['labels']}",
                               "expected": "a declared label", "found": "Forbidden"})
            if node["change"] == "created" and "uid" in node["properties"] \
                    and lookup.find(node["labels"][0], "uid", node["properties"]["uid"]):
                faults.append({"rule_id": "TS-unique", "element": f"{node['labels'][0]}.uid",
                               "expected": "a value not used yet", "found": node["properties"]["uid"]})
        for rel in changes["relationships"]:
            ends = changes["ends"]
            if rel["type"] == "ROUTES_TO" and "WasteItem" in ends[rel["from"]]["labels"]:
                faults.append({"rule_id": "TS-endpoint", "element": "relationship:ROUTES_TO",
                               "expected": "(DisposalRule)-[:ROUTES_TO]->(WasteStream)",
                               "found": f"({ends[rel['from']]['labels'][0]})-[:ROUTES_TO]->({ends[rel['to']]['labels'][0]})"})
        return faults


class Recorder:
    """Keeps what a checker was handed, so a test can look at it."""

    def __init__(self, inner):
        self.inner, self.seen = inner, []

    def check(self, changes, lookup):
        self.seen.append(changes)
        return self.inner.check(changes, lookup)

    def __getattr__(self, name):                      # observations, not_checkable
        return getattr(self.inner, name)


@pytest.fixture(scope="module")
def service(tmp_path_factory):
    service = Service(port=0, control_key="test-key")
    checker = StandInChecker()
    try:
        cell = service.add_neo4j_cell("dev-pg", URI, "neo4j", PASSWORD, checker=checker,
                                      log_dir=tmp_path_factory.mktemp("records"), realisation="S4-curated/pg")
        service.start()
    except Exception as error:
        pytest.skip(f"the development Neo4j with the commit hook is not available: {error}")
    cell.test_checker = checker
    yield service
    cell.store.admin("MATCH (n) DETACH DELETE n")
    service.stop(remove_hooks=True)


@pytest.fixture
def cell(service):
    cell = service.cells["dev-pg"]
    cell.store.admin("MATCH (n) DETACH DELETE n")
    cell.mode = "reject"
    cell.test_checker.seen.clear()
    cell.log_path.write_text("")
    cell.open_unit({"unit_id": "u0001", "run_id": "run-test", "source_id": "src-1", "stratum": "prose"})
    yield cell
    cell.close_unit()


# ---------------------------------------------------------------------- the RDF side

def fuseki_service(tmp_path_factory, checker, realisation="S4-curated/rdf"):
    """A service with one RDF cell on the development Fuseki, or a skip."""
    service = Service(port=0, control_key="test-key")
    try:
        cell = service.add_fuseki_cell("dev-rdf", FUSEKI, "admin", FUSEKI_PASSWORD, change_log=FUSEKI_LOG,
                                       checker=checker, log_dir=tmp_path_factory.mktemp("records"),
                                       realisation=realisation)
        cell.store.start(wait_s=3.0)
        service.start()
    except Exception as error:
        pytest.skip(f"the development Fuseki with the logged dataset is not available: {error}")
    return service


def in_step(cell):
    """The tool's own list and the store agree, and nothing sits in a named graph."""
    triples, named = cell.store.counts()
    return triples == cell.store.mirror.count and named == 0


@pytest.fixture(scope="module")
def rdf_service(tmp_path_factory):
    checker = StandInChecker()
    service = fuseki_service(tmp_path_factory, checker)
    service.cells["dev-rdf"].test_checker = checker
    yield service
    service.cells["dev-rdf"].store.admin("DELETE WHERE { ?s ?p ?o }")
    service.stop()


@pytest.fixture
def rdf(rdf_service):
    cell = rdf_service.cells["dev-rdf"]
    cell.store.admin("DELETE WHERE { ?s ?p ?o }")
    cell.mode, cell.warn_deleted, cell.store.max_rows = "reject", 25, 200
    cell.test_checker.seen.clear()
    cell.log_path.write_text("")
    cell.warnings_path.write_text("")
    cell.open_unit({"unit_id": "u0001", "run_id": "run-test", "source_id": "src-1", "stratum": "prose"})
    yield cell
    cell.close_unit()
    assert in_step(cell), "the tool's list of the graph and the store differ"
