"""Tests of the induced schema (S2): the reader of an induction reply, the files made of it, its description.

    python -m pytest schemas/treatments/test_induced.py -q

Every reply here is invented, in a vocabulary that has nothing to do with the sources. Nothing is
written into the checkout: the files of a test go to a temporary folder.
"""

from __future__ import annotations

import copy
import hashlib
import json
import sys
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import derive  # noqa: E402
import induced  # noqa: E402
import render  # noqa: E402


def prop(name: str, datatype: str | None = None, required: bool = False, values: list | None = None) -> dict:
    return {"name": name, "datatype": datatype, "required": required, "values": values}


def reply_pg() -> dict:
    return {
        "node_types": [
            {"name": "Person", "identifier": "person_id",
             "properties": [prop("person_id", "string", True), prop("name", "string", True), prop("born", "date"),
                            prop("level", "integer", values=[1, 2, 3]), prop("remark")],
             "unique": []},
            {"name": "Team", "identifier": "title",
             "properties": [prop("title", "string", True), prop("kind", values=["core", "guest"]),
                            prop("budget", "quantity")],
             "unique": ["title"]},
            {"name": "Project", "identifier": "code",
             "properties": [prop("code", "string", True), prop("started", "datetime"), prop("open", "boolean"),
                            prop("score", "decimal")],
             "unique": ["code"]},
        ],
        "relationship_patterns": [
            {"type": "MEMBER_OF", "from": "Person", "to": ["Team"], "at_most_one": False,
             "properties": [prop("since", "date", True), prop("role", values=["lead", "member"])]},
            {"type": "LEADS", "from": "Person", "to": ["Team", "Project"], "at_most_one": True, "properties": []},
            {"type": "PART_OF", "from": "Project", "to": ["Project"], "at_most_one": True, "properties": []},
        ],
    }


def reply_rdf() -> dict:
    return {
        "classes": [
            {"name": "Person", "identifier": "person_id",
             "properties": [prop("person_id", "string", True), prop("born", "date"),
                            prop("level", "integer", values=[1, 2, 3])],
             "key": ["person_id"]},
            {"name": "Team", "identifier": "title",
             "properties": [prop("title", None, True), prop("kind", values=["core", "guest"]),
                            prop("budget", "quantity")],
             "key": []},
        ],
        "object_properties": [
            {"name": "memberOf", "domain": "Person", "range": ["Team"], "at_most_one": False,
             "annotations": [prop("since", "date", True)]},
            {"name": "leads", "domain": "Person", "range": ["Team", "Person"], "at_most_one": True, "annotations": []},
        ],
    }


REPLIES = {"pg": reply_pg, "rdf": reply_rdf}


def text(document: dict) -> str:
    return json.dumps(document, indent=2, ensure_ascii=False)


def faults(document, arm: str = "pg") -> list[str]:
    return induced.read(document if isinstance(document, str) else text(document), arm).faults


def one_fault(document, needle: str, arm: str = "pg") -> None:
    found = faults(document, arm)
    assert len(found) == 1 and needle in found[0], found


def generate(tmp_path: Path, replies: dict[str, str]) -> tuple[Path, int]:
    """Put replies into a copy-free treatments folder, run derive and render there. Returns (folder, derive's code)."""
    here = tmp_path / "schemas" / "treatments"
    for arm, reply in replies.items():
        folder = here / "S2-induced" / arm
        folder.mkdir(parents=True, exist_ok=True)
        (folder / derive.REPLY).write_text(reply, encoding="utf-8")
    code = derive.run(here)
    files, _ = render.build(here)
    for path, content in files.items():
        path.write_text(content, encoding="utf-8")
    return here, code


# --------------------------------------------------------------------------- the reader: what is usable

@pytest.mark.parametrize("arm", ["pg", "rdf"])
def test_a_valid_reply_is_usable_and_nothing_in_it_is_changed(arm):
    document = REPLIES[arm]()
    reading = induced.read(text(document), arm)
    assert reading.usable and reading.faults == [] and reading.notes == []
    assert reading.document == document


def test_white_space_around_the_document_is_nothing():
    reading = induced.read("\n\n  " + text(reply_pg()) + "\n\n", "pg")
    assert reading.usable and reading.notes == [] and not reading.in_code_fence


@pytest.mark.parametrize("opening", ["```", "```json"])
def test_one_code_fence_around_the_document_is_taken_off_and_noted(opening):
    reading = induced.read(f"{opening}\n{text(reply_pg())}\n```\n", "pg")
    assert reading.usable and reading.in_code_fence and len(reading.notes) == 1 and "code fence" in reading.notes[0]
    assert reading.document == reply_pg()


def test_every_fault_is_listed_not_only_the_first():
    document = reply_pg()
    document["node_types"][0]["properties"][2]["datatype"] = "float"           # 1
    document["node_types"][1]["identifier"] = "nickname"                       # 2
    del document["node_types"][2]["unique"]                                    # 3
    document["relationship_patterns"][0]["to"] = ["Club"]                      # 4
    document["relationship_patterns"][1]["at_most_one"] = "yes"                # 5
    found = faults("Here is the schema:\n" + text(document))                    # 6
    assert len(found) == 6, found


# --------------------------------------------------------------------------- the reader: one test per kind of fault

def test_fault_not_json():
    found = faults(text(reply_pg()).replace('"unique": []', '"unique": [', 1))
    assert len(found) == 1 and found[0].startswith("the reply is not a JSON document") and "line" in found[0]
    assert faults("") == ["the reply is empty"]
    assert faults("I could not design a schema.")[0].startswith("the reply holds no JSON document")
    assert "not a JSON value" in faults(text(reply_pg()).replace("[1, 2, 3]", "[1, NaN]").replace("[\n            1,\n            2,\n            3\n          ]", "[NaN]"))[0]


def test_fault_text_around_the_document():
    one_fault("The schema:\n" + text(reply_pg()), "text before the document")
    one_fault(text(reply_pg()) + "\nI hope this helps.", "text after the document")
    found = faults("Sure.\n```json\n" + text(reply_pg()) + "\n```")              # a fence is not all there is
    assert any("text before the document" in fault for fault in found)
    found = faults("```json\n" + text(reply_pg()) + "\n```\n```json\n{}\n```")   # two fences
    assert found and not induced.read("```json\n" + text(reply_pg()) + "\n```\n```json\n{}\n```", "pg").in_code_fence


def test_fault_missing_key():
    document = reply_pg()
    del document["relationship_patterns"]
    one_fault(document, 'the document: the key "relationship_patterns" is missing')
    document = reply_pg()
    del document["node_types"][0]["properties"][1]["required"]
    one_fault(document, 'node_types[0] (Person).properties[1] (name): the key "required" is missing')
    document = reply_pg()
    del document["relationship_patterns"][1]["at_most_one"]
    one_fault(document, 'relationship_patterns[1] (LEADS): the key "at_most_one" is missing')
    document = reply_rdf()
    del document["classes"][1]["key"]
    one_fault(document, 'classes[1] (Team): the key "key" is missing', "rdf")


def test_fault_unknown_key():
    document = reply_pg()
    document["comment"] = "designed with care"
    one_fault(document, 'the document: unknown key "comment"')
    document = reply_pg()
    document["node_types"][0]["properties"][0]["description"] = "the number of the person"
    one_fault(document, 'unknown key "description"')
    document = reply_rdf()                                   # the words of the other arm are unknown keys
    document["classes"][0]["unique"] = document["classes"][0].pop("key")
    found = faults(document, "rdf")
    assert len(found) == 2 and any('the key "key" is missing' in f for f in found) and any('unknown key "unique"' in f for f in found)


def test_fault_wrong_type():
    for change, needle in (
        (lambda d: d.update(node_types={}), '"node_types" has to be a list, found an object'),
        (lambda d: d["node_types"][0].update(properties="person_id, name"), '"properties" has to be a list, found text'),
        (lambda d: d["node_types"][0]["properties"][0].update(required="true"), '"required" has to be true or false, found text'),
        (lambda d: d["node_types"][0]["properties"][0].update(required=1), '"required" has to be true or false, found a number'),
        (lambda d: d["node_types"][0]["properties"][0].update(datatype=["string"]), "the datatype has to be text or null"),
        (lambda d: d["node_types"][0]["properties"][3].update(values="1|2|3"), '"values" has to be a list or null'),
        (lambda d: d["node_types"][0]["properties"][3].update(values=[1, [2, 3]]), "allowed value 2 has to be text, a number, true or false"),
        (lambda d: d["node_types"][0]["properties"][3].update(values=[]), "the list of allowed values is empty"),
        (lambda d: d["node_types"][1].update(unique="title"), '"unique" has to be a list, found text'),
        (lambda d: d["relationship_patterns"][0].update(to="Team"), '"to" has to be a list, found text'),
        (lambda d: d["relationship_patterns"][0].update(to=[]), '"to" is empty'),
        (lambda d: d["relationship_patterns"][1].update(at_most_one=None), '"at_most_one" has to be true or false, found null'),
        (lambda d: d["relationship_patterns"][1].update(properties=["since"]), "a property has to be an object, found text"),
    ):
        document = reply_pg()
        change(document)
        found = faults(document)
        assert any(needle in fault for fault in found), (needle, found)
    assert faults("[]")[0].startswith("the reply holds no JSON document")
    assert faults('{"node_types": 3, "relationship_patterns": []}') == ['"node_types" has to be a list, found a number']


def test_fault_datatype_outside_the_list():
    document = reply_pg()
    document["node_types"][2]["properties"][3]["datatype"] = "float"
    one_fault(document, 'the datatype "float" is not one of string, integer, decimal, boolean, date, datetime, quantity')
    document = reply_rdf()
    document["object_properties"][0]["annotations"][0]["datatype"] = "xsd:date"
    one_fault(document, 'the datatype "xsd:date" is not one of', "rdf")


def test_fault_name_that_is_not_a_plain_name():
    for change in (lambda d: d["node_types"][0].update(name="Natural Person"),
                   lambda d: d["node_types"][0].update(name="2ndPerson"),
                   lambda d: d["node_types"][0]["properties"][4].update(name="remark-text"),
                   lambda d: d["relationship_patterns"][2].update(type="PART OF"),
                   lambda d: d["node_types"][0].update(name="")):
        document = reply_pg()
        change(document)
        assert any("is not a plain name" in fault for fault in faults(document)), faults(document)
    document = reply_pg()                                    # letters of any script are plain
    document["node_types"][1]["properties"][1]["name"] = "größe"
    assert faults(document) == []


def test_fault_identifier_or_unique_property_that_is_not_declared():
    document = reply_pg()
    document["node_types"][0]["identifier"] = "passport"
    one_fault(document, 'node_types[0] (Person): the identifier "passport" is not a property this node type declares')
    document = reply_pg()
    document["node_types"][1]["unique"] = ["title", "motto"]
    one_fault(document, '"motto" in "unique" is not a property this node type declares')
    document = reply_rdf()
    document["classes"][0]["key"] = ["passport"]
    one_fault(document, '"passport" in "key" is not a property this class declares', "rdf")
    document = reply_rdf()
    document["classes"][1]["identifier"] = None
    one_fault(document, "the identifier has to be the name of a property, found null", "rdf")


def test_fault_relationship_that_names_an_undeclared_type():
    document = reply_pg()
    document["relationship_patterns"][0]["from"] = "Member"
    one_fault(document, 'relationship_patterns[0] (MEMBER_OF): "from" names "Member", which is not a declared node type')
    document = reply_pg()
    document["relationship_patterns"][1]["to"] = ["Team", "Department"]
    one_fault(document, '"to" names "Department", which is not a declared node type')
    document = reply_rdf()
    document["object_properties"][1]["range"] = ["Team", "Animal"]
    one_fault(document, 'object_properties[1] (leads): "range" names "Animal", which is not a declared class', "rdf")


def test_fault_duplicates():
    document = reply_pg()
    document["node_types"].append(copy.deepcopy(document["node_types"][0]))
    one_fault(document, 'the node type "Person" is declared twice')
    document = reply_pg()
    document["node_types"][0]["properties"].append(prop("name"))
    one_fault(document, 'the property "name" is declared twice')
    document = reply_pg()
    document["node_types"][1]["unique"] = ["title", "title"]
    one_fault(document, '"title" is listed twice in "unique"')
    document = reply_pg()
    document["relationship_patterns"][1]["to"] = ["Team", "Project", "Team"]
    one_fault(document, '"Team" is listed twice in "to"')
    document = reply_pg()                                    # the same pattern in a second entry
    document["relationship_patterns"].append({"type": "LEADS", "from": "Person", "to": ["Project"],
                                              "at_most_one": False, "properties": []})
    one_fault(document, 'the pattern from "Person" to "Project" is declared twice')
    document = reply_pg()
    document["node_types"][0]["properties"][3]["values"] = [1, 2, 2]
    one_fault(document, "the allowed value 2 is listed twice")
    document = reply_pg()                                    # true and 1 are two values, "1" and 1 as well
    document["node_types"][0]["properties"][4]["values"] = [1, True, "1"]
    assert faults(document) == []
    twice = text(reply_pg()).replace('"identifier": "title",', '"identifier": "title", "identifier": "title",')
    one_fault(twice, 'the key "identifier" is stated twice in one object')


def test_fault_no_type_at_all():
    one_fault({"node_types": [], "relationship_patterns": []}, '"node_types" is empty: the schema declares no node type')
    one_fault({"classes": [], "object_properties": []}, '"classes" is empty: the schema declares no class', "rdf")


def test_fault_rdf_one_name_for_two_kinds_of_term():
    document = reply_rdf()
    document["classes"][1]["properties"].append(prop("leads"))
    one_fault(document, 'the name "leads" is used for a property with values and for an object property', "rdf")
    document = reply_rdf()
    document["object_properties"][1]["name"] = "Team"
    one_fault(document, 'the name "Team" is used for a class and for a property', "rdf")
    document = reply_pg()                                    # in the property graph these are separate kinds of name
    document["node_types"][1]["properties"].append(prop("LEADS"))
    assert faults(document) == []


def test_two_patterns_of_one_type_and_start_are_no_duplicate():
    document = reply_pg()
    document["relationship_patterns"][1]["to"] = ["Team"]
    document["relationship_patterns"].append({"type": "LEADS", "from": "Person", "to": ["Project"],
                                              "at_most_one": False, "properties": []})
    assert faults(document) == []


# --------------------------------------------------------------------------- from the reply to the schema

def test_the_mapping_changes_nothing_but_the_identifier_rule():
    reading = induced.read(text(reply_pg()), "pg")
    schema, done = induced.to_schema(reading)
    document = reply_pg()
    assert [node["name"] for node in schema["node_types"]] == [node["name"] for node in document["node_types"]]
    for node, entry in zip(schema["node_types"], document["node_types"]):
        assert [p["name"] for p in node["properties"]] == [p["name"] for p in entry["properties"]]
        assert node["identifier"] == entry["identifier"]
        assert node["required"] == [p["name"] for p in entry["properties"] if p["required"]]
        assert node["datatypes"] == {p["name"]: p["datatype"] for p in entry["properties"] if p["datatype"]}
        assert node["values"] == {p["name"]: p["values"] for p in entry["properties"] if p["values"] is not None}
        assert [u for u in node["unique"] if u != entry["identifier"]] == [u for u in entry["unique"] if u != entry["identifier"]]
        assert entry["identifier"] in node["unique"]
    assert done["identifier_added_to_unique"] == ["Person.person_id"]          # Team and Project listed theirs
    for pattern, entry in zip(schema["relationship_patterns"], document["relationship_patterns"]):
        assert (pattern["type"], pattern["from"], pattern["to"], pattern["at_most_one"]) == \
               (entry["type"], entry["from"], entry["to"], entry["at_most_one"])
        assert pattern["properties"] == [p["name"] for p in entry["properties"]]
    assert schema["relationship_patterns"][0]["required"] == ["since"]
    assert schema["relationship_patterns"][0]["values"] == {"role": ["lead", "member"]}


def test_the_identifier_rule_can_be_switched_off(monkeypatch):
    monkeypatch.setattr(induced, "IDENTIFIER_IS_UNIQUE", False)
    schema, done = induced.to_schema(induced.read(text(reply_pg()), "pg"))
    assert schema["node_types"][0]["unique"] == [] and done["identifier_added_to_unique"] == []
    assert done["identifier_counts_as_unique"] is False


def test_a_reply_with_faults_has_no_schema():
    with pytest.raises(ValueError):
        induced.to_schema(induced.read("{}", "pg"))


# --------------------------------------------------------------------------- the generated files

def test_without_a_reply_the_induced_realisations_stay_placeholders(tmp_path):
    here, code = generate(tmp_path, {})
    assert code == 0
    for arm, name in (("pg", "schema.cql"), ("rdf", "schema.ttl")):
        for file in ("declaration.json", name):                # byte for byte what the checkout holds
            assert (here / "S2-induced" / arm / file).read_bytes() == (HERE / "S2-induced" / arm / file).read_bytes() \
                or json.loads((HERE / "S2-induced" / arm / "declaration.json").read_text())["status"] != "placeholder"
        assert json.loads((here / "S2-induced" / arm / "declaration.json").read_text())["status"] == "placeholder"
        assert not (here / "S2-induced" / arm / render.OUTPUT).exists()
    _, refused = render.build(here)
    assert sorted(refused) == ["S2-induced/pg", "S2-induced/rdf"]


@pytest.mark.parametrize("arm", ["pg", "rdf"])
def test_a_reply_gives_the_three_files_of_its_realisation(tmp_path, arm):
    reply = text(REPLIES[arm]())
    here, code = generate(tmp_path, {arm: reply})
    assert code == 0
    folder = here / "S2-induced" / arm
    assert (folder / derive.REPLY).read_text(encoding="utf-8") == reply          # the reply is left as it was
    decl = json.loads((folder / "declaration.json").read_text(encoding="utf-8"))
    assert decl["status"] == "draft" and decl["treatment"] == "S2" and decl["arm"] == arm
    assert decl["derived_from"] == {"file": f"schemas/treatments/S2-induced/{arm}/{derive.REPLY}",
                                    "sha256": hashlib.sha256(reply.encode("utf-8")).hexdigest()}
    assert decl["enforcement"] == derive.ENFORCEMENT_INDUCED
    assert decl["induction"]["identifier_counts_as_unique"] is True and decl["induction"]["in_code_fence"] is False
    document = REPLIES[arm]()
    form = induced.FORMS[arm]
    assert [node["name"] for node in decl["node_types"]] == [entry["name"] for entry in document[form["types"]]]
    assert decl["counts"]["relationship_patterns"] == len(document[form["relations"]])
    for pattern in decl["relationship_patterns"]:               # names only: the validator reads them as a set
        assert all(isinstance(name, str) for name in pattern["properties"])
    schema = (folder / decl["schema_file"]).read_text(encoding="utf-8")
    assert "PLACEHOLDER" not in schema and decl["derived_from"]["sha256"][:16] in schema
    for node in decl["node_types"]:
        assert node["name"] in schema
    description = (folder / render.OUTPUT).read_text(encoding="utf-8")
    assert description == render.render(decl)
    other = "rdf" if arm == "pg" else "pg"                       # the other arm has no reply: still a placeholder
    assert json.loads((here / "S2-induced" / other / "declaration.json").read_text())["status"] == "placeholder"
    assert derive.run(here, check=True) == 0                     # and what was written is what the script makes
    if arm == "rdf":
        assert decl["namespace"] == "https://fessi.example/schema/s2#" and decl["data_namespace"] == derive.DATA_NAMESPACE
        assert decl["relationship_property_form"] == derive.EDGE_PROPERTY_FORM
        assert "@prefix xsd:" in schema and "map:quantity a rdfs:Datatype" in schema
        assert "sh:path fessi:leads ; sh:or ( [ sh:class fessi:Team ] [ sh:class fessi:Person ] ) ; sh:maxCount 1" in schema
        assert "sh:path fessi:level ; sh:datatype xsd:integer ; sh:in ( 1 2 3 )" in schema
    else:
        assert "CREATE CONSTRAINT person_person_id_unique IF NOT EXISTS FOR (n:Person) REQUIRE n.person_id IS UNIQUE;" in schema
        assert "(Person)-[:LEADS]->(Team|Project)   // at most one per start node" in schema


def test_a_reply_in_a_code_fence_gives_the_same_files_and_says_so(tmp_path):
    plain, _ = generate(tmp_path / "plain", {"pg": text(reply_pg())})
    fenced, code = generate(tmp_path / "fenced", {"pg": "```json\n" + text(reply_pg()) + "\n```\n"})
    assert code == 0
    a = json.loads((plain / "S2-induced/pg/declaration.json").read_text())
    b = json.loads((fenced / "S2-induced/pg/declaration.json").read_text())
    assert b["induction"]["in_code_fence"] is True and a["induction"]["in_code_fence"] is False
    for key in ("node_types", "relationship_patterns", "counts", "enforcement"):
        assert a[key] == b[key]
    assert (plain / "S2-induced/pg" / render.OUTPUT).read_text() == (fenced / "S2-induced/pg" / render.OUTPUT).read_text()


def test_a_refused_reply_generates_nothing_for_its_arm(tmp_path, capsys):
    bad = reply_rdf()
    bad["classes"][0]["identifier"] = "passport"
    bad["object_properties"][0]["range"] = ["Club"]
    here, code = generate(tmp_path, {"pg": text(reply_pg()), "rdf": text(bad)})
    assert code == 1
    said = capsys.readouterr().err
    assert "refused  S2-induced/rdf/induction.reply.json: 2 faults" in said
    assert 'the identifier "passport"' in said and '"range" names "Club"' in said
    assert sorted(path.name for path in (here / "S2-induced" / "rdf").iterdir()) == [derive.REPLY]
    assert (here / "S2-induced" / "pg" / render.OUTPUT).is_file()            # the other arm is made all the same
    assert (here / "S4-curated" / "pg" / "declaration.json").is_file()
    assert derive.run(here, check=True) == 1                                  # and a check does not pass either
    with pytest.raises(induced.Refused) as refusal:
        derive.build(here)
    assert len(refusal.value.faults) == 2 and "S2-induced/rdf" in str(refusal.value)


def test_the_other_realisations_do_not_depend_on_a_reply(tmp_path):
    without = derive.build(tmp_path / "a")
    folder = tmp_path / "b" / "S2-induced" / "pg"
    folder.mkdir(parents=True)
    (folder / derive.REPLY).write_text(text(reply_pg()), encoding="utf-8")
    with_reply = derive.build(tmp_path / "b")
    for path, content in without.items():
        if "S2-induced/pg" not in path.as_posix():
            assert with_reply[tmp_path / "b" / path.relative_to(tmp_path / "a")] == content


# --------------------------------------------------------------------------- the description

def declaration_of(tmp_path: Path, arm: str) -> dict:
    here, _ = generate(tmp_path, {arm: text(REPLIES[arm]())})
    return render.load(f"S2-induced/{arm}", here)


def test_the_description_states_everything_the_reply_declares(tmp_path):
    out = render.render(declaration_of(tmp_path, "pg"))
    assert out.startswith("Schema S2 for the property graph, version 0.2, as declared.\n")
    assert "Node types (3):\nPerson, Team, Project\n" in out
    assert "- Person: person_id* [identifier, string, required], name [string, required], born [date], " \
           "level [integer, one of 1 | 2 | 3], remark\n" in out
    assert '- Team: title* [identifier, string, required], kind [one of "core" | "guest"], budget [quantity]\n' in out
    assert '- (:Person)-[:MEMBER_OF {since [date, required], role [one of "lead" | "member"]}]->(:Team)\n' in out
    assert "- (:Person)-[:LEADS]->(:Team|Project), at most one\n" in out
    constraints = out.split("Constraints:\n")[1].split("\n\n")[0].splitlines()
    assert len(constraints) == 8 and all("none declared" not in line for line in constraints)
    assert sum(line.endswith("is rejected.") for line in constraints) == 7
    assert constraints[7] == "- Other properties: open lists. A property that is not named above is accepted."
    assert "Counts: 3 node types, 3 relationship types, 3 relationship patterns, 3 uniqueness constraints." in out
    for hidden in ("induction", "sha256", "draft", "declared_by", "code fence"):
        assert hidden not in out


def test_the_rdf_description_uses_the_schema_prefix_and_names_both_namespaces(tmp_path):
    decl = declaration_of(tmp_path, "rdf")
    out = render.render(decl)
    assert "prefix fessi: <https://fessi.example/schema/s2#>" in out
    assert f"Namespace for the IRIs of the entities you create: <{decl['data_namespace']}>" in out
    assert "- fessi:Team: fessi:title* [identifier, required], fessi:kind [one of \"core\" | \"guest\"], " \
           "fessi:budget [quantity]\n" in out
    assert "- fessi:Person fessi:memberOf fessi:Team {| fessi:since [date, required] |}\n" in out
    assert "- fessi:Person fessi:leads (fessi:Team or fessi:Person), at most one\n" in out
    assert "A value on a relation is written as an annotation on its triple: s p o {| q v |} ." in out


def test_a_kind_the_reply_does_not_declare_is_none_declared(tmp_path):
    document = reply_pg()
    for node in document["node_types"]:
        for entry in node["properties"]:
            entry.update(datatype=None, values=None)
    for pattern in document["relationship_patterns"]:
        pattern["at_most_one"] = False
        for entry in pattern["properties"]:
            entry.update(datatype=None, values=None)
    here, _ = generate(tmp_path, {"pg": text(document)})
    out = render.render(render.load("S2-induced/pg", here))
    assert "- Datatypes: none declared." in out and "- Cardinality: none declared." in out
    assert "- Closed value lists: none declared." in out and "at most one" not in out
    assert "- Required properties: a node carries a value" in out


def test_the_strength_of_the_new_kinds_comes_from_the_enforcement_block(tmp_path):
    decl = declaration_of(tmp_path, "pg")
    decl["enforcement"].update(cardinality_violation="accept_and_count", datatype_violation="nothing_declared")
    out = render.render(decl)
    assert '- Cardinality: a node is meant to start at most one relationship of a pattern followed by "at most one". ' \
           "A write that breaks this is accepted." in out
    assert "- Datatypes: none declared." in out
    del decl["enforcement"]["value_outside_the_list"]              # a declared constraint without a stated strength
    with pytest.raises(render.CannotRender, match="value_outside_the_list"):
        render.render(decl)


def test_a_constraint_on_an_undeclared_property_stops_the_renderer(tmp_path):
    decl = declaration_of(tmp_path, "pg")
    broken = copy.deepcopy(decl)
    broken["node_types"][0]["required"].append("shoe_size")
    with pytest.raises(render.CannotRender, match="shoe_size"):
        render.render(broken)
    broken = copy.deepcopy(decl)
    broken["node_types"][0]["datatypes"]["born"] = "timestamp"
    with pytest.raises(render.CannotRender, match="timestamp"):
        render.render(broken)
    broken = copy.deepcopy(decl)
    broken["node_types"][0]["identifier"] = "passport"
    with pytest.raises(render.CannotRender, match="passport"):
        render.render(broken)


def test_the_sentences_do_not_change_with_the_schema(tmp_path):
    """Two induced schemas of one arm differ only in the lines that carry names and numbers."""
    import re

    other = reply_pg()
    other["node_types"] = other["node_types"][:2]
    other["relationship_patterns"] = other["relationship_patterns"][:2]
    other["relationship_patterns"][1]["to"] = ["Team"]

    def frame(document: dict, folder: str) -> list[str]:
        here, _ = generate(tmp_path / folder, {"pg": text(document)})
        lines = render.render(render.load("S2-induced/pg", here)).splitlines()
        kept = [line for index, line in enumerate(lines) if not line.startswith("- (")
                and not re.match(r"^- \w+: (?!closed|open|none|two|a |the )", line)
                and not (index and lines[index - 1].startswith("Node types ("))]
        return [re.sub(r"\d+", "N", line) for line in kept]

    assert frame(reply_pg(), "one") == frame(other, "two")


# --------------------------------------------------------------------------- the command line

def test_the_command_says_usable_or_lists_the_faults(tmp_path, capsys, monkeypatch):
    good, bad = tmp_path / "good.txt", tmp_path / "bad.txt"
    good.write_text("```json\n" + text(reply_rdf()) + "\n```", encoding="utf-8")
    bad.write_text(text(reply_pg()), encoding="utf-8")                       # the property-graph form, read as RDF
    monkeypatch.setattr(sys, "argv", ["induced.py", str(good), "--arm", "rdf"])
    assert induced.main() == 0
    said = capsys.readouterr().out
    assert "usable: 2 classes, 2 object_properties" in said and "note: the document was taken out of a code fence" in said
    assert "Team.title" in said
    monkeypatch.setattr(sys, "argv", ["induced.py", str(bad), "--arm", "rdf"])
    assert induced.main() == 1
    said = capsys.readouterr().out
    assert "refused: 4 faults" in said and 'the key "classes" is missing' in said and 'unknown key "node_types"' in said
