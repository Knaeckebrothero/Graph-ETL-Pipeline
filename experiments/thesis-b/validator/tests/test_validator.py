"""Expected properties of the shared validator on Thesis A's seven boundary graphs (Eval 2 package v0.1, section 5
preface: empty, full gold, half gold, duplicated gold, fully hallucinated, wrong qualifiers, text blob), plus the
treatment scope against a declaration.json. All offline, no database."""
import copy
import json

import pytest

from conftest import REPO
from validator import (COMMON_RULES, Declaration, EquivalenceTable, GraphView, UnitList, Validator, assertion_key,
                       CANONICAL_SCHEMA_PATH)
from validator.rules_common import Context

UNIT = "u004bb83eb485"
EQUIV = EquivalenceTable(
    predicates={"routes_to": ["disposed in", "goes to", "ROUTES_TO"], "has_fee": ["fee", "costs"], "located_at": ["is at"]},
    types={"WasteItem": ["waste item", "item"], "WasteStream": ["stream", "bin"], "Facility": ["recycling centre"]})
UNITS = UnitList(["EUR", "kg", "l", "piece"])


def gold(n=10):
    """n reference-like assertions: every one carries a qualifier and an anchor."""
    out = []
    for i in range(n):
        obj = ({"kind": "entity", "label": f"Stream {i % 3}", "type": "WasteStream"} if i % 2 == 0
               else {"kind": "literal", "value": 2.5 + i, "datatype": "decimal", "unit": "EUR"})
        out.append({"item_id": f"i{i:04d}", "subject": {"kind": "entity", "label": f"Item {i}", "type": "WasteItem"},
                    "predicate": "routes_to" if i % 2 == 0 else "has_fee", "object": obj,
                    "qualifiers": {"scope": "Frankfurt", "validity_time": {"from": "2024-01-01", "to": None}, "modality": "must"},
                    "provenance": {"unit_id": UNIT, "source_id": "bund-elektrog", "locator": f"§{i}", "quote": "..."}})
    return out


def boundary(name):
    g = gold()
    if name == "empty":
        return []
    if name == "full_gold":
        return g
    if name == "half_gold":
        return g[::2]
    if name == "duplicated_gold":
        dup = copy.deepcopy(g)
        for a in dup:
            a["item_id"] = a["item_id"] + "-dup"
            a["provenance"]["locator"] = "elsewhere"          # provenance is not part of the identity
        return g + dup
    if name == "hallucinated":
        h = copy.deepcopy(g)
        for a in h:
            a["provenance"] = {"unit_id": None, "source_id": None, "locator": None, "quote": None}   # no basis in any unit
        return h
    if name == "wrong_qualifiers":
        w = copy.deepcopy(g)
        for i, a in enumerate(w):
            a["qualifiers"] = ({"validity_time": {"from": "2025-12-31", "to": "2024-01-01"}} if i % 3 == 0 else
                               {"modality": "sometimes"} if i % 3 == 1 else {"zone": "x"})
        return w
    if name == "text_blob":
        return [{"item_id": "blob", "subject": {"kind": "entity", "label": "unit text", "type": None}, "predicate": "text",
                 "object": {"kind": "literal", "value": "lorem ipsum " * 50, "datatype": "string"}, "qualifiers": {},
                 "provenance": {"unit_id": UNIT, "locator": "whole unit"}}]
    raise ValueError(name)


@pytest.fixture
def v():
    return Validator(mode="reject", equivalence=EQUIV, units=UNITS)


# ----------------------------------------------------------------------------- common scope, expected properties
def test_full_gold_passes_every_rule(v):
    r = v.check_insert(boundary("full_gold"))
    assert r.result == "pass" and not r.blocks_write and r.not_checkable == []
    g = v.check_export(boundary("full_gold"))
    assert g["violations"] == 0 and g["checked_rules"] == len(COMMON_RULES) and g["not_checkable_rules"] == []


def test_half_gold_passes(v):
    assert v.check_insert(boundary("half_gold")).result == "pass"


def test_empty_export_is_not_estimable(v):
    g = v.check_export(boundary("empty"))
    assert g["assertions"] == 0 and g["violations"] == 0 and g["by_rule"] == {}


def test_duplicated_gold_violates_only_the_duplicate_rule(v):
    g = v.check_export(boundary("duplicated_gold"))
    assert set(g["by_rule"]) == {"CS-07"} and g["by_rule"]["CS-07"] == 10 and g["assertions_with_violation"] == 10
    r = v.check_insert(boundary("duplicated_gold"))
    assert r.result == "reject" and {x.rule_id for x in r.rejections} == {"CS-07"}
    # a second export of the same graph is unchanged (deterministic)
    assert v.check_export(boundary("duplicated_gold"))["by_rule"] == g["by_rule"]


def test_duplicate_against_the_graph_is_caught_and_identity_ignores_provenance(v):
    graph = GraphView()
    first = boundary("full_gold")
    assert v.check_insert(first, graph).result == "pass"
    graph.remember(first, Context(equivalence=EQUIV, units=UNITS))
    again = copy.deepcopy(first[:1])
    again[0]["provenance"]["locator"] = "another place"
    r = v.check_insert(again, graph)
    assert r.result == "reject" and r.rejections[0].rule_id == "CS-07"
    changed = copy.deepcopy(first[:1])
    changed[0]["qualifiers"]["scope"] = "Berlin"               # a different qualifier is a different proposition
    assert v.check_insert(changed, graph).result == "pass"


def test_hallucinated_fails_provenance(v):
    r = v.check_insert(boundary("hallucinated"))
    assert r.result == "reject" and {x.rule_id for x in r.rejections} == {"CS-06"} and len(r.rejections) == 10


def test_wrong_qualifiers_fail_cs05_only(v):
    r = v.check_insert(boundary("wrong_qualifiers"))
    assert r.result == "reject" and {x.rule_id for x in r.rejections} == {"CS-05"}
    assert len(r.items) == 10 and all(i.result == "reject" for i in r.items)


def test_text_blob_fails_well_formed_or_vocabulary(v):
    r = v.check_insert(boundary("text_blob"))
    assert r.result == "reject" and "CS-02" in {x.rule_id for x in r.rejections}      # 'text' is no predicate of the vocabulary


def test_aliases_are_mappable(v):
    a = boundary("full_gold")[0]
    a["predicate"], a["subject"]["type"], a["object"]["type"] = "disposed in", "waste item", "bin"
    assert v.check_insert([a]).result == "pass"


def test_datatypes_and_units(v):
    a = boundary("full_gold")[1]
    bad = copy.deepcopy(a); bad["object"]["value"] = "three euros"
    assert {x.rule_id for x in v.check_insert([bad]).rejections} == {"CS-04"}
    bad = copy.deepcopy(a); bad["object"]["unit"] = "USD"
    assert {x.rule_id for x in v.check_insert([bad]).rejections} == {"CS-04"}
    d = copy.deepcopy(a); d["object"] = {"kind": "literal", "value": "2024-02-30", "datatype": "date"}
    assert {x.rule_id for x in v.check_insert([d]).rejections} == {"CS-04"}
    ok = copy.deepcopy(a); ok["object"] = {"kind": "literal", "value": "2024-02-29", "datatype": "date"}
    assert v.check_insert([ok]).result == "pass"


def test_dangling_reference_by_id(v):
    a = boundary("full_gold")[0]
    a["object"] = {"kind": "entity", "id": "ent:42"}                     # id only, label unknown: must exist somewhere
    assert {x.rule_id for x in v.check_insert([a]).rejections} == {"CS-03"}
    assert v.check_insert([a], GraphView(entity_ids={"ent:42"})).result == "pass"
    intro = copy.deepcopy(boundary("full_gold")[2]); intro["subject"] = {"kind": "entity", "id": "ent:42", "label": "Stream X", "type": "WasteStream"}
    assert v.check_insert([intro, a]).result == "pass"                  # introduced in the same write


def test_well_formed(v):
    a = boundary("full_gold")[0]
    a["subject"] = {"kind": "entity", "label": "", "type": "WasteItem"}
    a["predicate"] = " "
    a["object"] = {"kind": "literal", "value": None}
    ids = [x.rule_id for x in v.check_insert([a]).rejections]
    assert ids.count("CS-01") == 3


# ----------------------------------------------------------------------------- bindings missing: not checkable, never approved silently
def test_without_bindings_rules_are_reported_not_checkable():
    v = Validator(mode="reject")
    r = v.check_insert(boundary("full_gold"))
    assert r.result == "pass" and {x.rule_id for x in r.not_checkable} == {"CS-02", "CS-04"}
    assert r.to_log()["validator_not_checkable"]
    g = v.check_export(boundary("full_gold"))
    assert g["not_checkable_rules"] == ["CS-02", "CS-04"] and g["checked_rules"] == len(COMMON_RULES) - 2


# ----------------------------------------------------------------------------- modes and the log record
def test_log_only_mode_records_but_does_not_block():
    v = Validator(mode="log_only", equivalence=EQUIV, units=UNITS)
    r = v.check_insert(boundary("hallucinated"))
    assert r.result == "reject" and not r.blocks_write
    log = r.to_log(item_id="i0003")
    assert log["validator_mode"] == "log_only" and log["validator_result"] == "reject"
    assert log["rejections"] and set(log["rejections"][0]) >= {"rule_id", "element", "expected", "found"}
    assert "log-only" in r.message()


def test_reject_mode_blocks_and_message_names_item_ids(v):
    r = v.check_insert(boundary("wrong_qualifiers")[:2])
    assert r.blocks_write
    m = r.message()
    assert m.startswith("rejected") and "item i0000" in m and "CS-05" in m and "same item ids" in m
    log = r.to_log(item_id="i0001")
    assert log["validator_result"] == "reject" and all(x["rule_id"] == "CS-05" for x in log["rejections"])
    assert v.check_insert(boundary("full_gold")[:1]).to_log() == {
        "validator_mode": "reject", "validator_result": "pass", "rejections": [], "validator_not_checkable": [], "validator_version": "0.1.0"}


def test_rejection_matches_shared_minimum_shape(v):
    pytest.importorskip("jsonschema")
    import jsonschema
    shared = REPO / "experiments" / "log-schema" / "shared-minimum-log.v1.schema.json"
    if not shared.exists():
        pytest.skip("Thesis A's schema not on this branch")
    schema = json.loads(shared.read_text(encoding="utf-8"))
    rej = v.check_insert(boundary("hallucinated")[:1]).to_log()["rejections"][0]
    jsonschema.validate(rej, {**schema["$defs"]["validator_rejection"], "$defs": schema["$defs"]})


def test_canonical_schema_accepts_gold_and_rejects_blob_without_object():
    pytest.importorskip("jsonschema")
    import jsonschema
    schema = json.loads(CANONICAL_SCHEMA_PATH.read_text(encoding="utf-8"))
    for a in boundary("full_gold"):
        jsonschema.validate(a, schema)
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate({"subject": {"label": "x"}, "predicate": "p", "qualifiers": {}, "provenance": {"unit_id": UNIT}}, schema)


# ----------------------------------------------------------------------------- treatment scope
DECL = {
    "realisation": "T/pg", "treatment": "S4", "arm": "pg", "status": "draft", "rules_version": "draft-0.1",
    "node_types": [
        {"name": "WasteItem", "layer": 1, "properties": [{"name": "uid", "declared_by": "constraint"}, {"name": "name", "declared_by": "constraint"},
                                                        {"name": "synonyms", "declared_by": "index"}], "unique": ["uid", "name"], "indexed": ["synonyms"]},
        {"name": "WasteStream", "layer": 1, "properties": [{"name": "uid", "declared_by": "constraint"}, {"name": "name", "declared_by": "constraint"}],
         "unique": ["uid", "name"], "indexed": []},
        {"name": "Product", "layer": 2, "properties": [{"name": "uid", "declared_by": "constraint"}, {"name": "ean", "declared_by": "constraint"}],
         "unique": ["uid", "ean"], "indexed": []},
        {"name": "PackagingComponent", "layer": 2, "properties": [{"name": "uid", "declared_by": "constraint"}], "unique": ["uid"], "indexed": []}],
    "relationship_patterns": [
        {"type": "ROUTES_TO", "from": "WasteItem", "to": ["WasteStream"], "properties": []},
        {"type": "HAS_COMPONENT", "from": "Product", "to": ["PackagingComponent"], "properties": ["qty", "weight_g"]}],
}


@pytest.fixture
def decl(tmp_path):
    p = tmp_path / "declaration.json"
    p.write_text(json.dumps(DECL))
    return Declaration.load(p)


def write_ok():
    return {"item_id": "i1", "nodes": [{"label": "WasteItem", "key": "n1", "properties": {"uid": "w1", "name": "banana peel"}},
                                       {"label": "WasteStream", "key": "n2", "properties": {"uid": "s1", "name": "Biotonne"}}],
            "relationships": [{"type": "ROUTES_TO", "from": "n1", "to": "n2", "properties": {}}]}


def test_declaration_loads_inventory(decl):
    inv = decl.constraint_inventory()
    assert inv["label_and_type"] == 4 and inv["relationship_pattern"] == 2 and inv["uniqueness"] == 7
    assert inv["required_property"] == 0 and inv["datatype"] == 0          # v5.0 declares none: n.a. on card P.4


def test_treatment_scope_passes_a_declared_write(decl):
    v = Validator(declaration=decl)
    r = v.check_write(write_ok())
    assert r.result == "pass" and r.scope == "local-treatment"


def test_treatment_scope_rules(decl):
    v = Validator(declaration=decl)
    w = write_ok(); w["nodes"][0]["label"] = "Rubbish"
    assert {x.rule_id for x in v.check_write(w).rejections} == {"TS-01", "TS-03"}      # unknown label, so the pattern cannot resolve
    w = write_ok(); w["nodes"][0]["properties"]["colour"] = "brown"
    assert {x.rule_id for x in v.check_write(w).rejections} == {"TS-02"}
    w = write_ok(); w["relationships"][0] = {"type": "ROUTES_TO", "from": "n2", "to": "n1", "properties": {}}
    assert {x.rule_id for x in v.check_write(w).rejections} == {"TS-03"}
    w = write_ok(); w["relationships"][0]["type"] = "LEADS_TO"
    assert {x.rule_id for x in v.check_write(w).rejections} == {"TS-01"}
    w = {"nodes": [{"label": "Product", "key": "p", "properties": {"uid": "p1"}}, {"label": "PackagingComponent", "key": "c", "properties": {"uid": "c1"}}],
         "relationships": [{"type": "HAS_COMPONENT", "from": "p", "to": "c", "properties": {"qty": 2, "colour": "red"}}]}
    assert {x.rule_id for x in v.check_write(w).rejections} == {"TS-05"}
    w = write_ok(); w["nodes"].append({"label": "WasteItem", "key": "n3", "properties": {"uid": "w1", "name": "other"}})
    assert {x.rule_id for x in v.check_write(w).rejections} == {"TS-04"}
    graph = GraphView(unique_values={("WasteStream", "name"): {"Biotonne"}}, node_labels={"ent:9": "WasteStream"})
    assert {x.rule_id for x in v.check_write(write_ok(), graph).rejections} == {"TS-04"}
    w = write_ok(); w["nodes"] = w["nodes"][:1]; w["relationships"][0]["to"] = "ent:9"        # endpoint in the graph
    assert v.check_write(w, graph).result == "pass"


def test_s1_declares_nothing_and_s2_placeholder_is_refused(tmp_path):
    p = tmp_path / "s1.json"
    p.write_text(json.dumps({"realisation": "S1-raw-triples/rdf", "treatment": "S1", "arm": "rdf", "status": "draft",
                             "node_types": [], "relationship_patterns": []}))
    d = Declaration.load(p)
    assert d.declares_nothing and Validator(declaration=d).check_write({"nodes": [{"label": "Anything", "key": "a"}]}).result == "pass"
    p2 = tmp_path / "s2.json"
    p2.write_text(json.dumps({"realisation": "S2-induced/pg", "treatment": "S2", "arm": "pg", "status": "placeholder"}))
    with pytest.raises(ValueError):
        Declaration.load(p2)


@pytest.mark.parametrize("real", ["S4-curated/pg", "S4-curated/rdf", "S3-flat/pg", "S3-flat/rdf", "S1-raw-triples/pg"])
def test_thesis_a_declarations_load(real):
    p = REPO / "schemas" / "treatments" / real
    if not (p / "declaration.json").exists():
        pytest.skip("Thesis A's schema-treatments draft is not on this branch")
    d = Declaration.load(p)
    assert d.realisation == real
    v = Validator(declaration=d)
    if real.startswith("S4"):
        inv = d.constraint_inventory()
        assert inv["label_and_type"] == 17 and inv["relationship_pattern"] == 33 and inv["uniqueness"] == 21
        # in S4 a rule routes to a stream; the item itself does not (Thesis A's flat rule 2 moves ROUTES_TO to the item only in S3)
        assert {x.rule_id for x in v.check_write(write_ok()).rejections} == {"TS-03"}
        w = {"nodes": [{"label": "DisposalRule", "key": "r", "properties": {"uid": "r1"}}, {"label": "WasteStream", "key": "s", "properties": {"uid": "s1"}}],
             "relationships": [{"type": "ROUTES_TO", "from": "r", "to": "s", "properties": {}}]}
        assert v.check_write(w).result == "pass"
    if real.startswith("S3"):
        assert "DisposalRule" not in d.node_types and ("ROUTES_TO", "WasteItem", "WasteStream") in d.patterns
        assert v.check_write(write_ok()).result == "pass"
