"""Tests of the schema description renderer.

    python -m pytest schemas/treatments/test_render.py -q
"""

from __future__ import annotations

import copy
import json
import re
import sys
from pathlib import Path

import pytest

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

import render  # noqa: E402

DECLARED_PG = ["S3-flat/pg", "S4-curated/pg"]
# Words of the sources' domain. The sentences around the lists must hold none of them.
DOMAIN_WORDS = ("waste", "abfall", "recycl", "dispos", "packag", "product", "bin", "container", "material",
                "hazard", "fee", "tonne", "müll", "entsorg")


def declaration(realisation: str) -> dict:
    return render.load(realisation)


def lines_of(realisation: str) -> list[str]:
    return render.render(declaration(realisation)).splitlines()


def list_line(line: str) -> bool:
    """A line that carries names of the schema: a list entry, or the one line of type names."""
    return line.startswith("- (") or bool(re.match(r"^- \w+: (?!closed|open|none|two|a value)", line))


INDUCED = ["S2-induced/pg", "S2-induced/rdf"]


def test_files_on_disk_match_the_renderer():
    files, refused = render.build()
    induced_by_now = [realisation for realisation in INDUCED if declaration(realisation)["status"] != "placeholder"]
    # S1, S3, S4 in both arms; an S2 realisation is a placeholder until its induction has run
    assert len(files) == 6 + len(induced_by_now) and sorted(refused) == sorted(set(INDUCED) - set(induced_by_now))
    for path, content in files.items():
        assert path.read_text(encoding="utf-8") == content, path
    for realisation in refused:
        assert not (HERE / realisation / render.OUTPUT).exists()


@pytest.mark.parametrize("realisation", DECLARED_PG)
def test_every_declared_element_is_shown_once(realisation):
    decl, text = declaration(realisation), render.render(declaration(realisation))
    for node in decl["node_types"]:
        assert len(re.findall(rf"^- {node['name']}: ", text, re.M)) == 1
        shown = re.search(rf"^- {node['name']}: (.*)$", text, re.M).group(1).split(", ")
        expected = [p["name"] + ("*" if p["name"] in node["unique"] else "") for p in node["properties"]]
        assert shown == expected
    pattern_lines = [line for line in text.splitlines() if line.startswith("- (:")]
    assert len(pattern_lines) == len(decl["relationship_patterns"]) == decl["counts"]["relationship_patterns"]
    for pattern, line in zip(decl["relationship_patterns"], pattern_lines):
        assert f"(:{pattern['from']})-[:{pattern['type']}" in line and line.endswith(f"->(:{'|'.join(pattern['to'])})")
        for prop in pattern["properties"]:
            assert prop in line
    stars = text.count("*") - text.count("marked *")
    assert stars == sum(len(node["unique"]) for node in decl["node_types"]) == decl["counts"]["uniqueness_constraints"]


@pytest.mark.parametrize("realisation", DECLARED_PG + ["S1-raw-triples/pg"])
def test_nothing_but_the_declaration_is_shown(realisation):
    decl, text = declaration(realisation), render.render(declaration(realisation))
    assert "layer" not in text.lower() and "index" not in text.lower()
    for node in decl["node_types"]:
        for prop in node["properties"]:
            if prop.get("note"):
                assert prop["note"] not in text
    assert "//" not in text and "Generated" not in text


def test_the_sentences_are_the_same_for_every_schema_of_the_arm():
    """Between two schemas only the lines that carry names and numbers differ."""
    def frame(realisation: str) -> list[str]:
        out = []
        for line in lines_of(realisation):
            if list_line(line) or (out and out[-1].startswith("Node types (")):
                continue                                    # names of the schema
            out.append(re.sub(r"\d+", "N", line).replace("Schema SN", "Schema S"))
        return out

    assert frame("S3-flat/pg") == frame("S4-curated/pg")


def test_the_sentences_hold_no_word_of_the_domain():
    fixed = " ".join(str(value) for words in render.WORDS.values() for value in words.values()).lower()
    fixed += " " + " ".join(render.KINDS + render.KINDS_RDF).lower() + " " + render.NONE
    for word in DOMAIN_WORDS:
        assert not re.search(rf"\b{word}", fixed), word


def test_the_strength_of_a_constraint_comes_from_the_enforcement_block():
    decl = declaration("S4-curated/pg")
    text = render.render(decl)
    assert "- Labels and types: closed list." in text and "- Endpoints: closed list." in text
    assert "- Other properties: open lists." in text and "- Uniqueness: two nodes of one label never share" in text
    changed = copy.deepcopy(decl)
    changed["enforcement"].update(node_type_outside_the_list="accept_and_count", undeclared_property="reject",
                                  relationship_outside_the_patterns="nothing_declared",
                                  uniqueness_violation="nothing_declared")
    text = render.render(changed)
    assert "- Labels and types: open list." in text and "- Other properties: closed lists." in text
    assert "- Endpoints: none declared." in text and "- Uniqueness: none declared." in text


def test_raw_triples_show_the_pattern_and_no_constraint():
    decl, text = declaration("S1-raw-triples/pg"), render.render(declaration("S1-raw-triples/pg"))
    assert "- Entity: name" in text and "- Assertion: predicate, value" in text
    assert "- (:Assertion)-[:SUBJECT]->(:Entity)" in text and "- (:Assertion)-[:OBJECT]->(:Entity)" in text
    assert "*" not in text and "closed" not in text and "open" not in text
    constraints = text.split("Constraints:\n")[1].split("\n\n")[0].splitlines()
    assert len(constraints) == 8 and all(line.endswith(": none declared.") for line in constraints)
    assert f"node of the type {decl['identifying_value']['node_type']} is its property {decl['identifying_value']['property']}" in text


def test_a_schema_that_declares_nothing_says_that_any_property_is_accepted():
    """Raw triples on RDF declare no class and no pattern. "none declared" alone was read both ways in the pilot."""
    text = render.render(declaration("S1-raw-triples/rdf"))
    constraints = text.split("Constraints:\n")[1].split("\n\n")[0].splitlines()
    assert len(constraints) == 8 and all(line.endswith(": none declared.") for line in constraints[:7])
    assert constraints[7] == ("- Other properties: nothing is declared, so any property is accepted, with a value or with "
                              "another entity as its object. Give a property an IRI in the namespace for data.")
    assert "closed" not in text and "rejected" not in text
    # the property graph's raw triples declare the generic pattern, so their text stays as it was
    assert "any property is accepted" not in render.render(declaration("S1-raw-triples/pg"))


def test_a_placeholder_is_refused():
    for realisation in INDUCED:
        decl = declaration(realisation)
        if decl["status"] == "placeholder":               # what the checkout holds until the induction has run
            with pytest.raises(render.CannotRender, match="placeholder"):
                render.render(decl)
        placeholder = {"realisation": realisation, "treatment": "S2", "arm": decl["arm"], "status": "placeholder",
                       "rules_version": decl["rules_version"], "schema_file": decl["schema_file"], "derived_from": None}
        with pytest.raises(render.CannotRender, match="placeholder"):
            render.render(placeholder)


def test_an_unknown_key_is_refused():
    decl = declaration("S4-curated/pg")
    decl["node_types"][0]["properties"][0]["datatype"] = "string"
    with pytest.raises(render.CannotRender, match="does not know"):
        render.render(decl)
    decl = declaration("S4-curated/pg")
    decl["enforcement"]["node_type_outside_the_list"] = "warn"
    with pytest.raises(render.CannotRender, match="expected one of"):
        render.render(decl)


@pytest.mark.parametrize("realisation", ["S1-raw-triples/rdf", "S3-flat/rdf", "S4-curated/rdf"])
def test_rdf_needs_a_namespace_for_data(realisation):
    decl = declaration(realisation)
    assert f"Namespace for the IRIs of the entities you create: <{decl['data_namespace']}>" in render.render(decl)
    without = {key: value for key, value in decl.items() if key != "data_namespace"}
    with pytest.raises(render.CannotRender, match="data_namespace"):
        render.render(without)
    decl["data_namespace"] = "https://data.invalid/cell/"
    text = render.render(decl)
    assert "Namespace for the IRIs of the entities you create: <https://data.invalid/cell/>" in text
    assert text.startswith(f"Schema {decl['treatment']} for RDF, version {decl['rules_version']}, as declared.")
    if decl["node_types"]:
        assert f"<{decl['namespace']}>" in text
        assert len([line for line in text.splitlines() if line.startswith("- fessi:") and "fessi:" in line[9:]]) \
            >= len(decl["relationship_patterns"])
        assert "- fessi:Product fessi:HAS_COMPONENT fessi:PackagingComponent {| fessi:qty, fessi:weight_g |}" in text
        assert decl["relationship_property_form"]["written_as"] in text
    else:
        assert "rdfs:label" in text and decl["identifying_value"]["iri"] in text
        assert "Classes (0):\nnone declared." in text


def test_both_arms_show_the_same_content():
    """An RDF description lists the same types, properties and patterns as its property-graph twin."""
    for treatment in ("S3-flat", "S4-curated"):
        pg, rdf = declaration(f"{treatment}/pg"), declaration(f"{treatment}/rdf")
        rdf["data_namespace"] = "https://data.invalid/cell/"
        pg_text, rdf_text = render.render(pg), render.render(rdf).replace("fessi:", "")
        for node in pg["node_types"]:
            assert re.search(rf"^- {node['name']}: (.*)$", pg_text, re.M).group(1) == \
                   re.search(rf"^- {node['name']}: (.*)$", rdf_text, re.M).group(1)
        assert pg_text.count("\n- (:") == len([line for line in rdf_text.splitlines()
                                                 if re.match(r"^- \w+ [A-Z_]+ ", line)])


RDF = ["S1-raw-triples/rdf", "S2-induced/rdf", "S3-flat/rdf", "S4-curated/rdf"]


def test_one_namespace_for_data_in_every_rdf_realisation():
    """Data IRIs do not carry the variant, and a data IRI can never be taken for a term of a schema."""
    found = {declaration(realisation).get("data_namespace") for realisation in RDF}
    assert found == {"https://fessi.example/data/"}
    data = found.pop()
    for realisation in RDF:
        schema = declaration(realisation).get("namespace")
        if schema:
            assert not data.startswith(schema) and not schema.startswith(data)


@pytest.mark.parametrize("realisation", ["S1-raw-triples/rdf", "S3-flat/rdf", "S4-curated/rdf"])
def test_the_names_shown_are_the_names_the_database_tool_hands_to_the_validator(realisation):
    """The text writes a class or property as fessi:<name>; the tool strips the schema's namespace and hands the
    validator <name>, which is the name in the declaration. rdfs:label, the identifying value of raw triples,
    is handed over as `label`; every other IRI stays whole."""
    tool = HERE.parents[1] / "experiments" / "harness"
    if not (tool / "dbtool" / "fuseki_cell.py").is_file():
        pytest.skip("the database tool is not in this checkout")
    sys.path.insert(0, str(tool))
    from dbtool.fuseki_cell import FusekiCell, namespace_of

    decl, text = declaration(realisation), render.render(declaration(realisation))
    assert namespace_of(realisation) == decl.get("namespace")
    cell = object.__new__(FusekiCell)
    cell._ns = decl.get("namespace")
    names = [node["name"] for node in decl["node_types"]] \
        + [prop["name"] for node in decl["node_types"] for prop in node["properties"]] \
        + [pattern["type"] for pattern in decl["relationship_patterns"]] \
        + [prop for pattern in decl["relationship_patterns"] for prop in pattern["properties"]]
    for name in names:
        assert f"{render.SCHEMA_PREFIX}:{name}" in text
        assert cell._name(f"<{decl['namespace']}{name}>") == name
    assert cell._name("<http://www.w3.org/2000/01/rdf-schema#label>") == "label"
    assert cell._name("<http://xmlns.com/foaf/0.1/name>") == "http://xmlns.com/foaf/0.1/name"
    assert cell._name(f"<{decl['data_namespace']}thing-1>") == f"{decl['data_namespace']}thing-1"
    if decl.get("identifying_value"):
        assert decl["identifying_value"]["iri"] == "http://www.w3.org/2000/01/rdf-schema#label"

