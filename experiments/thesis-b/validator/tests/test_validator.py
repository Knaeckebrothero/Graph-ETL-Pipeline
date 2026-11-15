"""Expected properties of the shared validator.

Export side (Thesis A's rule list v0.2, section 7, and card P.3): the seven boundary graphs (empty, full gold, half
gold, duplicated gold, fully hallucinated, wrong qualifiers, text blob) and one case per rule.
Write side: Thesis A's `changes` of the database tool (checker.py), the situations of its own tests
(test_neo4j_cell.py, 15 Nov 2026), the enforcement of Rule 5, the N-Triples adapter for the RDF side. All offline."""
import copy
import json
import os
import shutil

import pytest

from conftest import REPO
from validator import (CANONICAL_SCHEMA_PATH, CHANGES_SCHEMA_PATH, COMMON_RULES, Checker, Declaration, EquivalenceTable,
                       FormatTable, Validator, WriteContext, diff_from_ntriples, parse_ntriples)
from validator.rules_common import Context, assertion_key

UNIT = "u004bb83eb485"
EQUIV = EquivalenceTable(
    predicates={"routes_to": ["disposed in", "goes to", "ROUTES_TO"], "has_fee": ["fee", "costs"], "located_at": ["is at"]},
    types={"WasteItem": ["waste item", "item"], "WasteStream": ["stream", "bin"], "Facility": ["recycling centre"]})
FORMATS = FormatTable(units=["EUR", "kg", "l", "piece"])
EXAMPLES = REPO / "experiments" / "thesis-b" / "validator" / "examples"


# ============================================================================= export side
def gold(n=10):
    """n reference-like assertions: every one carries a qualifier and an anchor."""
    out = []
    for i in range(n):
        obj = ({"kind": "entity", "label": f"Stream {i % 3}", "type": "WasteStream"} if i % 2 == 0
               else {"kind": "literal", "value": 2.5 + i, "datatype": "decimal", "unit": "EUR"})
        out.append({"item_id": f"i{i:04d}", "elements": [f"4:x:{i}"], "subject": {"kind": "entity", "label": f"Item {i}", "type": "WasteItem"},
                    "predicate": "routes_to" if i % 2 == 0 else "has_fee", "object": obj,
                    "qualifiers": {"scope": "06412000", "validity_time": {"from": "2024-01-01", "to": None}, "modality": "must"},
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
            a["qualifiers"]["modality"] = "MUST"              # spelling normalisation
        return g + dup
    if name == "hallucinated":
        h = copy.deepcopy(g)
        for a in h:
            a["provenance"] = {"unit_id": None, "source_id": None, "locator": None, "quote": None}   # no basis in any unit
            a["elements"] = []
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
    return Validator(mode="reject", equivalence=EQUIV, formats=FORMATS)


def test_full_gold_passes_every_rule(v):
    g = v.check_export(boundary("full_gold"))
    assert g["violations"] == 0 and g["checked_rules"] == len(COMMON_RULES) and g["not_checkable_rules"] == []
    assert v.check_export(boundary("half_gold"))["violations"] == 0


def test_empty_export_is_not_estimable(v):
    g = v.check_export(boundary("empty"))
    assert g["assertions"] == 0 and g["violations"] == 0 and g["by_rule"] == {}


def test_duplicated_gold_violates_only_cs7_once_per_extra_occurrence_and_is_deterministic(v):
    g = v.check_export(boundary("duplicated_gold"))
    assert set(g["by_rule"]) == {"CS7"} and g["by_rule"]["CS7"] == 10 and g["assertions_with_violation"] == 10
    assert v.check_export(boundary("duplicated_gold"))["by_rule"] == g["by_rule"]
    changed = copy.deepcopy(boundary("full_gold")[:1]); changed[0]["qualifiers"]["scope"] = "DE"
    assert v.check_export(boundary("full_gold")[:1] + changed)["violations"] == 0      # a different qualifier is a different fact


def test_hallucinated_fails_cs6_on_every_assertion(v):
    g = v.check_export(boundary("hallucinated"))
    assert set(g["by_rule"]) == {"CS6"} and g["by_rule"]["CS6"] == 10


def test_cs6_follows_the_write_log_chain(v):
    log = {f"4:x:{i}": UNIT for i in range(10)}
    assert v.check_export(boundary("full_gold"), write_log=log)["violations"] == 0
    del log["4:x:3"]
    g = v.check_export(boundary("full_gold"), write_log=log)
    assert g["by_rule"] == {"CS6": 1} and g["detail"][3]["result"] == "reject"


def test_wrong_qualifiers_fail_cs5_only_and_missing_qualifiers_are_no_violation(v):
    g = v.check_export(boundary("wrong_qualifiers"))
    assert set(g["by_rule"]) == {"CS5"} and g["assertions_with_violation"] == 10
    bare = copy.deepcopy(boundary("full_gold")[:2])
    bare[0]["qualifiers"] = {}; bare[1]["qualifiers"] = None
    assert v.check_export(bare)["violations"] == 0
    for m in ("must", "may", "must not", "must_not", "MUST"):
        a = copy.deepcopy(boundary("full_gold")[0]); a["qualifiers"]["modality"] = m
        assert v.check_export([a])["violations"] == 0, m
    a = copy.deepcopy(boundary("full_gold")[0]); a["qualifiers"]["modality"] = "should"
    assert v.check_export([a])["by_rule"] == {"CS5": 1}
    a = copy.deepcopy(boundary("full_gold")[0]); a["qualifiers"]["scope"] = "0641"
    assert v.check_export([a])["by_rule"] == {"CS5": 1}


def test_text_blob_fails_cs2(v):
    g = v.check_export(boundary("text_blob"))
    assert "CS2" in g["by_rule"]


def test_aliases_are_mappable(v):
    a = boundary("full_gold")[0]
    a["predicate"], a["subject"]["type"], a["object"]["type"] = "disposed in", "waste item", "bin"
    assert v.check_export([a])["violations"] == 0


def test_cs4_datatypes_and_units_on_the_export(v):
    a = boundary("full_gold")[1]
    bad = copy.deepcopy(a); bad["object"]["value"] = "three euros"
    assert v.check_export([bad])["by_rule"] == {"CS4": 1}
    bad = copy.deepcopy(a); bad["object"]["unit"] = "USD"
    assert v.check_export([bad])["by_rule"] == {"CS4": 1}
    d = copy.deepcopy(a); d["object"] = {"kind": "literal", "value": "2024-02-30", "datatype": "date"}
    assert v.check_export([d])["by_rule"] == {"CS4": 1}
    ok = copy.deepcopy(a); ok["object"] = {"kind": "literal", "value": "2024-02-29", "datatype": "date"}
    assert v.check_export([ok])["violations"] == 0
    txt = copy.deepcopy(a); txt["object"] = {"kind": "literal", "value": "ab Januar", "datatype": None}   # untyped: text, passes
    assert v.check_export([txt])["violations"] == 0


def test_cs3_on_the_export_wants_an_identifying_value_somewhere(v):
    a = boundary("full_gold")[0]
    a["object"] = {"kind": "entity", "id": "ent:42"}                     # bare resource: no name, key or type anywhere
    assert v.check_export([a])["by_rule"] == {"CS3": 1}
    intro = copy.deepcopy(boundary("full_gold")[2]); intro["subject"] = {"kind": "entity", "id": "ent:42", "label": "Stream X", "type": "WasteStream"}
    assert v.check_export([intro, a])["violations"] == 0                 # identified elsewhere in the export


def test_cs1_on_the_export(v):
    a = boundary("full_gold")[0]
    a["subject"] = {"kind": "entity", "label": "", "type": "WasteItem"}
    a["predicate"] = " "
    a["object"] = {"kind": "literal", "value": None}
    assert v.check_export([a])["by_rule"] == {"CS1": 3, "CS2": 1} or v.check_export([a])["by_rule"]["CS1"] == 3


def test_without_bindings_rules_are_not_checkable_never_passed():
    g = Validator(mode="reject").check_export(boundary("full_gold"))
    assert g["not_checkable_rules"] == ["CS2", "CS4"] and g["checked_rules"] == len(COMMON_RULES) - 2 and g["violations"] == 0
    assert "provisional" in g["equivalence_table"]


def test_canonical_schema_accepts_gold_and_rejects_blob_without_object():
    pytest.importorskip("jsonschema")
    import jsonschema
    schema = json.loads(CANONICAL_SCHEMA_PATH.read_text(encoding="utf-8"))
    for a in boundary("full_gold"):
        jsonschema.validate(a, schema)
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate({"subject": {"label": "x"}, "predicate": "p", "qualifiers": {}, "provenance": {"unit_id": UNIT}}, schema)


# ============================================================================= write side: Thesis A's `changes`
DECL = {
    "realisation": "T/pg", "treatment": "S4", "arm": "pg", "status": "draft", "rules_version": "0.2",
    "enforcement": {"node_type_outside_the_list": "reject", "relationship_outside_the_patterns": "reject",
                    "uniqueness_violation": "reject", "undeclared_property": "accept_and_count"},
    "node_types": [
        {"name": "WasteItem", "layer": 1, "properties": [{"name": "uid"}, {"name": "name"}, {"name": "synonyms"}], "unique": ["uid", "name"]},
        {"name": "WasteStream", "layer": 1, "properties": [{"name": "uid"}, {"name": "name"}], "unique": ["uid", "name"]},
        {"name": "DisposalRule", "layer": 3, "properties": [{"name": "uid"}], "unique": ["uid"]},
        {"name": "Tip", "layer": 3, "properties": [{"name": "uid"}, {"name": "text"}], "unique": ["uid"]},
        {"name": "Product", "layer": 2, "properties": [{"name": "uid"}, {"name": "ean"}], "unique": ["uid", "ean"]},
        {"name": "PackagingComponent", "layer": 2, "properties": [{"name": "uid"}], "unique": ["uid"]}],
    "relationship_patterns": [
        {"type": "HAS_RULE", "from": "WasteItem", "to": ["DisposalRule"], "properties": []},
        {"type": "ROUTES_TO", "from": "DisposalRule", "to": ["WasteStream"], "properties": []},
        {"type": "HAS_TIP", "from": "WasteItem", "to": ["Tip"], "properties": []},
        {"type": "HAS_COMPONENT", "from": "Product", "to": ["PackagingComponent"], "properties": ["qty", "weight_g"]}],
}
S1_DECL = {"realisation": "S1/pg", "treatment": "S1", "arm": "pg", "status": "draft", "rules_version": "0.2",
           "enforcement": {k: "nothing_declared" for k in DECL["enforcement"]},
           "identifying_value": {"node_type": "Entity", "property": "name"},
           "node_types": [{"name": "Entity", "properties": [{"name": "name"}], "unique": []},
                          {"name": "Assertion", "properties": [{"name": "predicate"}, {"name": "value"}], "unique": []}],
           "relationship_patterns": [{"type": "SUBJECT", "from": "Assertion", "to": ["Entity"], "properties": []},
                                     {"type": "OBJECT", "from": "Assertion", "to": ["Entity"], "properties": []}]}


class Lookup:
    """Thesis A's lookup.find(label, property, value) -> element ids of nodes that were there before."""
    def __init__(self, hits=None):
        self.hits = hits or {}
        self.calls = []

    def find(self, label, prop, value):
        self.calls.append((label, prop, value))
        return list(self.hits.get((label, prop, value), []))


def node(key, labels, props, change="created", **more):
    return {"key": key, "labels": labels, "properties": props, "change": change, "changed_properties": {},
            "added_labels": [], "removed_labels": [], **more}


def rel(key, typ, frm, to, props=None, change="created"):
    return {"key": key, "type": typ, "from": frm, "to": to, "properties": props or {}, "change": change, "changed_properties": {}}


def changes(realisation="T/pg", nodes=(), relationships=(), ends=None, statement="CREATE ...", item_id="i1", **more):
    ends = dict(ends or {})
    for n in nodes:
        ends.setdefault(n["key"], {"labels": n["labels"], "properties": n["properties"]})
    used = {r["from"] for r in relationships} | {r["to"] for r in relationships}
    return {"cell_id": "dev-pg", "arm": "pg", "realisation": realisation, "unit_id": "u0001", "item_id": item_id, "evidence": "p",
            "statement": statement, "nodes": list(nodes), "relationships": list(relationships),
            "ends": {k: v for k, v in ends.items() if k in used}, "deleted": {"nodes": [], "relationships": []}, **more}


@pytest.fixture
def root(tmp_path):
    """A repository root with declarations: the synthetic ones, plus Thesis A's real files when the branch has them."""
    for name, d in (("T/pg", DECL), ("S1/pg", S1_DECL)):
        p = tmp_path / "schemas" / "treatments" / name
        p.mkdir(parents=True)
        (p / "declaration.json").write_text(json.dumps(d))
    p = tmp_path / "schemas" / "treatments" / "S2/pg"
    p.mkdir(parents=True)
    (p / "declaration.json").write_text(json.dumps({"realisation": "S2/pg", "treatment": "S2", "arm": "pg", "status": "placeholder"}))
    real = REPO / "schemas" / "treatments"
    if real.exists():
        for sub in real.iterdir():
            if sub.is_dir() and sub.name.startswith("S") and not (tmp_path / "schemas" / "treatments" / sub.name).exists():
                shutil.copytree(sub, tmp_path / "schemas" / "treatments" / sub.name)
    return tmp_path


@pytest.fixture
def checker(root):
    return Checker(root=root)


def test_thesis_a_rejected_write_names_every_fault_in_one_answer(checker):
    """Thesis A's test_a_rejected_write_leaves_nothing_and_names_every_fault, through the real rules: the Forbidden
    node has no identifying value (CS1; its TS-type is then not tested, list section 4), ROUTES_TO from a WasteItem
    matches no pattern (TS-endpoint); the Tip and HAS_TIP pass."""
    ch = changes(nodes=[node("n10", ["Tip"], {"uid": "t-1", "text": "x"}), node("n11", ["Forbidden"], {"x": 1})],
                 relationships=[rel("r20", "HAS_TIP", "n1", "n10"), rel("r21", "ROUTES_TO", "n1", "n3")],
                 ends={"n1": {"labels": ["WasteItem"], "properties": {"name": "Paint can"}}, "n3": {"labels": ["WasteStream"], "properties": {}}},
                 item_id="i2")
    faults = checker.check(ch, Lookup())
    assert sorted(f["rule_id"] for f in faults) == ["CS1", "TS-endpoint"]
    assert all({"rule_id", "element", "expected", "found"} <= set(f) for f in faults)
    assert "benchmark" not in json.dumps(faults).lower()
    ep = next(f for f in faults if f["rule_id"] == "TS-endpoint")
    assert ep["found"] == "(WasteItem)-[:ROUTES_TO]->(WasteStream)" and "(DisposalRule)-[:ROUTES_TO]->(WasteStream)" in ep["expected"]


def test_thesis_a_rule_write_passes(checker):
    ch = changes(nodes=[node("n1", ["WasteItem"], {"name": "Paint can", "uid": "wi-1"}), node("n3", ["WasteStream"], {"name": "Hazardous waste", "uid": "ws-1"}),
                        node("n2", ["DisposalRule"], {"uid": "dr-1"})],
                 relationships=[rel("r1", "HAS_RULE", "n1", "n2"), rel("r2", "ROUTES_TO", "n2", "n3")])
    assert checker.check(ch, Lookup()) == [] and checker.observations == [] and checker.not_checkable() == []
    assert checker.last.element_ids == ["n1", "n3", "n2", "r1", "r2"]


def test_uniqueness_asks_what_was_there_before(checker):
    ch = changes(nodes=[node("n12", ["WasteItem"], {"uid": "wi-1", "name": "Paint tin"})], item_id="i2")
    lookup = Lookup({("WasteItem", "uid", "wi-1"): ["n5"]})
    faults = checker.check(ch, lookup)
    assert [f["rule_id"] for f in faults] == ["TS-unique"] and ("WasteItem", "uid", "wi-1") in lookup.calls
    # the same value twice inside one statement is a violation too; a changed node keeping its own value is not
    ch = changes(nodes=[node("a", ["WasteItem"], {"uid": "w9", "name": "x"}), node("b", ["WasteItem"], {"uid": "w9", "name": "y"})])
    assert [f["rule_id"] for f in checker.check(ch, Lookup())] == ["TS-unique"]
    ch = changes(nodes=[node("n1", ["WasteItem"], {"uid": "wi-1", "name": "Paint can", "synonyms": ["Farbdose"]}, change="changed",
                             changed_properties={"synonyms": {"old": None, "new": ["Farbdose"]}})])
    assert checker.check(ch, Lookup({("WasteItem", "uid", "wi-1"): ["n1"], ("WasteItem", "name", "Paint can"): ["n1"]})) == []


def test_a_label_outside_the_list_is_rejected_also_when_added_to_an_existing_node(checker):
    ch = changes(nodes=[node("n1", ["WasteItem", "Checked"], {"uid": "wi-1", "name": "Paint can"}, change="changed", added_labels=["Checked"])])
    faults = checker.check(ch, Lookup({("WasteItem", "uid", "wi-1"): ["n1"], ("WasteItem", "name", "Paint can"): ["n1"]}))
    assert [(f["rule_id"], f["found"]) for f in faults] == [("TS-type", "Checked")]


def test_an_undeclared_property_is_accepted_and_counted_not_rejected(checker):
    ch = changes(nodes=[node("n1", ["WasteItem"], {"uid": "wi-1", "name": "Paint can", "colour": "red"})])
    assert checker.check(ch, Lookup()) == []
    assert [(o["kind"], o["found"]) for o in checker.observations] == [("TS-property", "colour")]
    ch = changes(nodes=[node("p", ["Product"], {"uid": "p1"}), node("c", ["PackagingComponent"], {"uid": "c1"})],
                 relationships=[rel("h", "HAS_COMPONENT", "p", "c", {"qty": 2, "colour": "red"})])
    checker.observations.clear()
    assert checker.check(ch, Lookup()) == [] and [o["found"] for o in checker.observations] == ["colour"]


def test_an_undeclared_relationship_type_and_a_wrong_end_type(checker):
    ch = changes(nodes=[node("n1", ["WasteItem"], {"uid": "w", "name": "x"}), node("n3", ["WasteStream"], {"uid": "s", "name": "y"})],
                 relationships=[rel("r", "LEADS_TO", "n1", "n3")])
    assert [f["rule_id"] for f in checker.check(ch, Lookup())] == ["TS-type"]
    ch = changes(nodes=[node("n1", ["WasteItem"], {"uid": "w", "name": "x"}), node("n3", ["WasteStream"], {"uid": "s", "name": "y"})],
                 relationships=[rel("r", "ROUTES_TO", "n3", "n1")])
    assert [f["rule_id"] for f in checker.check(ch, Lookup())] == ["TS-endpoint"]


def test_cs1_at_the_write(checker):
    ch = changes(nodes=[node("a", ["WasteItem"], {"uid": "w1", "name": "   "}),                 # empty value
                        node("b", [""], {"uid": "w2", "name": "x"}),                            # empty type
                        node("c", ["WasteStream"], {"uid": "s1", "name": "Bio\x00tonne"})],     # control character
                 relationships=[rel("r", "", "a", "c")])                                       # empty relation type
    faults = checker.check(ch, Lookup())
    assert {f["rule_id"] for f in faults} == {"CS1"} and len(faults) == 4
    assert checker.last.message().startswith("rejected") and checker.last.message().count("CS1") == 4


def test_s1_entities_need_a_name_but_assertion_nodes_do_not(checker):
    ch = changes(realisation="S1/pg",
                 nodes=[node("e1", ["Entity"], {"name": "Bananenschale"}), node("e2", ["Entity"], {}),
                        node("a1", ["Assertion"], {"predicate": "goes to", "value": "Biotonne"})],
                 relationships=[rel("r", "SUBJECT", "a1", "e1")])
    faults = checker.check(ch, Lookup())
    assert [(f["rule_id"], f["element"]) for f in faults] == [("CS1", "entity:e2/identifier")]
    # S1 declares nothing: a label outside its two types is no fault
    ch = changes(realisation="S1/pg", nodes=[node("q", ["Whatever"], {"name": "x"})])
    assert checker.check(ch, Lookup()) == []


def test_s2_placeholder_is_not_a_schema_and_says_so(checker):
    ch = changes(realisation="S2/pg", nodes=[node("q", ["Anything"], {"name": "x"})])
    assert checker.check(ch, Lookup()) == [] and any("placeholder" in w for w in checker.warnings)


def test_common_rules_first_then_schema_rules_per_item(checker):
    ch = changes(nodes=[node("a", ["Forbidden"], {"x": 1}), node("b", ["AlsoForbidden"], {"name": "named"})])
    faults = checker.check(ch, Lookup())
    assert [(f["rule_id"], f["element"]) for f in faults] == [("CS1", "entity:a/identifier"), ("TS-type", "entity:b/type")]


def test_cs4_at_the_write_on_typed_values_only(checker):
    ch = changes(nodes=[node("a", ["WasteItem"], {"uid": "w1", "name": "x", "valid_from": {"value": "ab Januar", "datatype": "date"},
                                                     "note": "ab Januar", "count": 3})])
    faults = checker.check(ch, Lookup())
    assert [(f["rule_id"], f["element"], f["expected"]) for f in faults] == [("CS4", "entity:a/attribute:valid_from", "ISO 8601 date (YYYY-MM-DD)")]
    ch = changes(nodes=[node("a", ["WasteItem"], {"uid": "w1", "name": "x", "weight": {"value": "2,5", "datatype": "quantity", "unit": "kg"}})])
    assert checker.check(ch, Lookup()) == [] and checker.not_checkable() == ["CS4"]       # no unit table yet
    checker.formats = FormatTable(units=["kg"])
    assert checker.check(ch, Lookup()) == [] and checker.not_checkable() == []


def test_rejections_and_log_fields_fit_thesis_a_schema(checker):
    pytest.importorskip("jsonschema")
    import jsonschema
    shared = REPO / "experiments" / "log-schema" / "shared-minimum-log.v1.schema.json"
    if not shared.exists():
        pytest.skip("Thesis A's schema not on this branch")
    schema = json.loads(shared.read_text(encoding="utf-8"))
    ch = changes(nodes=[node("n11", ["Forbidden"], {"x": 1})])
    for f in checker.check(ch, Lookup()):
        jsonschema.validate({k: f[k] for k in ("rule_id", "element", "expected", "found")}, {**schema["$defs"]["validator_rejection"], "$defs": schema["$defs"]})
    log = checker.last.to_log()
    assert log["validator_mode"] == "reject" and log["validator_result"] == "reject" and log["rejections"]
    assert checker.last.engine_result() == "not_sent"


def test_log_only_mode_records_but_does_not_block(root):
    c = Checker(root=root, mode="log_only")
    faults = c.check(changes(nodes=[node("n11", ["Forbidden"], {"x": 1})]), Lookup())
    assert faults and not c.last.blocks_write and "log-only" in c.last.message() and c.last.engine_result() == "ok"


def test_changes_example_fits_its_schema_and_gives_the_expected_faults(root):
    pytest.importorskip("jsonschema")
    import jsonschema
    ch = json.loads((EXAMPLES / "changes.example.json").read_text(encoding="utf-8"))
    jsonschema.validate(ch, json.loads(CHANGES_SCHEMA_PATH.read_text(encoding="utf-8")))
    real = root / "schemas" / "treatments" / "S4-curated" / "pg" / "declaration.json"
    if not real.exists():
        ch["realisation"] = "T/pg"
    faults = Checker(root=root).check(ch, Lookup())
    assert sorted(f["rule_id"] for f in faults) == ["CS1", "TS-endpoint"]


def test_checker_factory_and_root_from_environment(root, monkeypatch):
    from validator.checker import checker as factory
    monkeypatch.setenv("KG_VALIDATOR_ROOT", str(root))
    c = factory()
    assert c.check(changes(nodes=[node("n11", ["Forbidden"], {"x": 1})]), Lookup())[0]["rule_id"] == "CS1"


# ============================================================================= write side: RDF (N-Triples adapter)
def test_ntriples_adapter_reads_types_attributes_relations_and_annotations():
    d = diff_from_ntriples((EXAMPLES / "inserted.example.nt").read_text(encoding="utf-8"))
    by = {e.ref.rsplit("/", 1)[-1]: e for e in d.entities}
    assert by["wi-1"].types == ["WasteItem"] and by["wi-1"].identifier == "wi-1"
    assert {r.type for r in d.relations} == {"HAS_RULE", "ROUTES_TO", "HAS_COMPONENT"}
    hc = next(r for r in d.relations if r.type == "HAS_COMPONENT")
    assert [(a.name, a.value, a.datatype) for a in hc.attributes] == [("qty", "2", "integer")]
    with pytest.raises(ValueError):
        parse_ntriples("<a> <b> .")


def test_rdf_write_through_the_validator(root):
    decl = Declaration.load(root / "schemas" / "treatments" / "T/pg")
    v = Validator(declaration=decl)
    r = v.check_write((EXAMPLES / "inserted.example.nt").read_text(encoding="utf-8"), WriteContext())
    ids = sorted((x.rule_id, x.element.split("/")[-1]) for x in r.rejections)
    assert ids == [("CS4", "attribute:valid_from")]                       # "ab Januar"^^xsd:date: Fuseki would store it, the validator does not
    assert r.outcome == "reject" and r.blocks_write
    # an empty diff: MERGE found everything -> already present; CREATE produced nothing -> CS3
    assert v.check_write("", query="MERGE (a:WasteItem {uid: 'x'})").outcome == "already_present"
    r = v.check_write("", query="MATCH (a:WasteItem {name: 'none'}) CREATE (a)-[:HAS_RULE]->(:DisposalRule {uid: 'r'})")
    assert r.outcome == "reject" and r.rejections[0].rule_id == "CS3"


# ============================================================================= Thesis A's real declarations, when present
@pytest.mark.parametrize("real", ["S4-curated/pg", "S4-curated/rdf", "S3-flat/pg", "S3-flat/rdf", "S1-raw-triples/pg", "S1-raw-triples/rdf"])
def test_thesis_a_declarations_load(real, root):
    p = root / "schemas" / "treatments" / real
    if not (p / "declaration.json").exists():
        pytest.skip("Thesis A's schema-treatments branch is not checked out here")
    d = Declaration.load(p)
    assert d.realisation == real and d.enforcement["undeclared_property"] in ("accept_and_count", "nothing_declared")
    c = Checker(root=root)
    item = changes(realisation=real, nodes=[node("n1", ["WasteItem"], {"name": "banana peel", "uid": "w1"}), node("n2", ["WasteStream"], {"uid": "s1", "name": "Biotonne"})],
                   relationships=[rel("r", "ROUTES_TO", "n1", "n2")])
    faults = c.check(item, Lookup())
    if real.startswith("S4"):
        inv = d.constraint_inventory()
        assert inv["label_and_type"] == 17 and inv["relationship_pattern"] == 33 and inv["uniqueness"] == 21
        assert [f["rule_id"] for f in faults] == ["TS-endpoint"]           # in S4 a rule routes to a stream, the item does not
    elif real.startswith("S3"):
        assert faults == []                                                # Thesis A's flat rule 2 moves ROUTES_TO to the item
    else:
        assert d.declares_nothing or d.treatment == "S1"
        assert faults == [] or [f["rule_id"] for f in faults] == ["CS1"]   # S1: nothing declared; names are still required
