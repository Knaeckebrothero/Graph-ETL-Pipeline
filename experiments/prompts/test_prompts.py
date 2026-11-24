"""Tests of the prompt templates: slots, hashing, filling, and the rules the texts obey."""

from __future__ import annotations

import re
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
import prompts  # noqa: E402

CONSTRUCTION = {"schema_description": "SCHEMA-A", "max_self_corrections": 2, "max_rows_shown": 200, "unit": "UNIT TEXT"}
QUERYING = {"schema_description": "SCHEMA-A", "max_rows_shown": 200, "question": "QUESTION TEXT"}

# Words of the domain of the sources. No template and no binding may contain one (EX-07, EX-16).
DOMAIN_WORDS = ("waste", "abfall", "müll", "muell", "garbage", "rubbish", "trash", "recycl", "dispos", "entsorg",
                "bin", "bins", "tonne", "container", "fee", "fees", "gebühr", "packag", "verpack", "hazard",
                "municipal", "frankfurt", "münchen", "munich", "berlin", "avv", "product", "material", "collection",
                "landfill")
WHOLE_WORDS = {"bin", "bins", "fee", "fees"}      # the others also count as the start of a longer word


def all_texts() -> dict[str, str]:
    texts = {f"{role}.{part}": prompts.template(role, part) for role in prompts.ROLES for part in prompts.PARTS}
    texts.update({f"binding.{role}.{arm}": prompts.binding(role, arm) for role in prompts.ROLES for arm in prompts.ARMS})
    return texts


def test_every_text_has_exactly_its_declared_slots():
    for key, names in prompts.SLOTS.items():
        role, part = key.split(".")
        assert prompts.slots(prompts.template(role, part)) == names, key


def test_a_binding_has_no_slot():
    for role in prompts.ROLES:
        for arm in prompts.ARMS:
            assert prompts.slots(prompts.binding(role, arm)) == set(), (role, arm)


def test_no_text_contains_a_word_of_the_domain():
    for key, text in all_texts().items():
        lowered = text.lower()
        found = [word for word in DOMAIN_WORDS
                 if re.search(rf"\b{re.escape(word)}" + (r"\b" if word in WHOLE_WORDS else ""), lowered)]
        assert not found, f"{key}: {found}"


def test_the_domain_word_check_finds_a_word_and_spares_a_longer_one():
    pattern = lambda word: rf"\b{re.escape(word)}" + (r"\b" if word in WHOLE_WORDS else "")
    assert re.search(pattern("fee"), "the fee is") and not re.search(pattern("fee"), "no feedback")
    assert re.search(pattern("recycl"), "goes to recycling")


def test_the_hash_ignores_line_ends_and_trailing_white_space_and_nothing_else():
    text = prompts.template("querying", "system")
    assert prompts.sha256(text) == prompts.sha256(text.replace("\n", "  \r\n") + "\n\n")
    assert prompts.sha256(text) != prompts.sha256(text.replace("alone", "alone "))
    assert prompts.sha256(text) != prompts.sha256(text.replace("{{question}}", "x") + " x")


def test_hashes_cover_six_templates_and_six_bindings():
    keys = set(prompts.hashes())
    assert len(keys) == 12
    assert {"construction.system", "construction.user", "binding.construction.pg", "binding.induction.rdf"} <= keys
    assert all(re.fullmatch(r"[0-9a-f]{64}", value) for value in prompts.hashes().values())


def test_filling_refuses_a_missing_and_an_unknown_value():
    with pytest.raises(ValueError, match="missing values for \\['question'\\]"):
        prompts.fill(prompts.template("querying", "user"), {})
    with pytest.raises(ValueError, match="no slot for \\['unit'\\]"):
        prompts.fill(prompts.template("querying", "user"), {"question": "q", "unit": "u"})
    with pytest.raises(ValueError):
        prompts.assemble("querying", "pg", {**QUERYING, "unit": "u"})


def test_a_filled_prompt_has_no_open_slot():
    for arm in prompts.ARMS:
        for role, values in (("construction", CONSTRUCTION), ("querying", QUERYING),
                             ("induction", {"units": "UNITS"})):
            for text in prompts.assemble(role, arm, values).values():
                assert not prompts.slots(text), (role, arm)


def test_a_value_that_looks_like_a_slot_is_not_filled_again():
    messages = prompts.assemble("construction", "pg", {**CONSTRUCTION, "unit": "text with {{schema_description}} in it"})
    assert "text with {{schema_description}} in it" in messages["user"]


def test_between_schema_variants_only_the_schema_slot_differs():
    for role, values in (("construction", CONSTRUCTION), ("querying", QUERYING)):
        for arm in prompts.ARMS:
            first = prompts.assemble(role, arm, values)
            second = prompts.assemble(role, arm, {**values, "schema_description": "SCHEMA-B\nwith two lines"})
            assert first["user"] == second["user"]
            assert first["system"].replace("SCHEMA-A", "SCHEMA-B\nwith two lines") == second["system"]


def test_between_arms_only_the_binding_differs():
    for role, values in (("construction", CONSTRUCTION), ("querying", QUERYING),
                         ("induction", {"units": "UNITS"})):
        pg, rdf = prompts.assemble(role, "pg", values), prompts.assemble(role, "rdf", values)
        assert pg["user"] == rdf["user"]
        marker = "<<BINDING>>"
        assert pg["system"].replace(prompts.binding(role, "pg").rstrip("\n"), marker) == \
            rdf["system"].replace(prompts.binding(role, "rdf").rstrip("\n"), marker)


def test_the_bindings_name_the_tools_of_the_database_tool():
    """Named as the model sees them in the framework (decided 23 November): mcp__<connector>__<tool>."""
    assert "mcp__graph__cypher(statement, parameters, passage, locator, id)" in prompts.binding("construction", "pg")
    assert "mcp__graph__sparql(statement, passage, locator, id)" in prompts.binding("construction", "rdf")
    for arm, look in (("pg", "mcp__graph__cypher(statement)"), ("rdf", "mcp__graph__sparql(statement)")):
        querying = prompts.binding("querying", arm)
        assert look in querying and "mcp__graph__answer(statement)" in querying


def test_the_construction_text_names_every_answer_of_the_database_tool():
    text = prompts.template("construction", "system")
    for answer in ("stored", "already_present", "rejected", "error", "refused", "result"):
        assert f"- {answer}:" in text, answer


def test_the_texts_set_no_number_of_query_attempts_and_no_rule_on_how_to_write():
    """Decided 16 November: a text defines the result; the way there is the agent's."""
    querying = prompts.template("querying", "system")
    assert "attempt" not in querying and "max_self_corrections" not in prompts.slots(querying)
    assert "yours to judge" in querying and "yours to judge" in prompts.template("construction", "system")
    construction = prompts.template("construction", "system").lower()
    assert "delete" not in construction and "per statement" not in construction


def test_only_the_construction_text_names_the_kinds_of_circumstance():
    """Decided 16 November: the induction text leaves them out, so the schema is designed from the units alone."""
    kinds = ("obligation", "permission", "prohibition", "exception", "circumstance")
    construction = prompts.template("construction", "system").lower()
    assert all(word in construction for word in kinds)
    induction = "\n".join(prompts.template("induction", part) for part in prompts.PARTS)
    induction += "\n".join(prompts.binding("induction", arm) for arm in prompts.ARMS)
    assert not [word for word in kinds if word in induction.lower()]


def test_the_graph_is_the_only_state_kept_between_units():
    """Decided 17 November: no folder carries files from unit to unit."""
    assert not [key for key, text in all_texts().items() if "cloud" in text.lower()]
    assert "only thing kept" in prompts.template("construction", "system")


def test_no_text_states_a_budget():
    """Decided 23 November: the budgets stop a task, but the agent is not told them."""
    for key, text in all_texts().items():
        low = text.lower()
        assert "limits" not in low and "tool calls" not in low and "tokens" not in low and "seconds" not in low, key


def test_units_are_joined_in_order_under_numbered_lines():
    joined = prompts.join_units(["first\n", "second", "third\n\n"])
    assert joined == "=== unit 1 of 3 ===\nfirst\n\n=== unit 2 of 3 ===\nsecond\n\n=== unit 3 of 3 ===\nthird"


def test_development_units_are_read_in_id_order_and_checked_against_their_hash(tmp_path):
    import hashlib
    (tmp_path / "units").mkdir()
    rows = []
    for unit_id, bucket, text in (("u2", "development", "two"), ("u1", "development", "one"), ("u3", "pilot", "three")):
        (tmp_path / "units" / f"{unit_id}.txt").write_text(text, encoding="utf-8")
        rows.append(f"{unit_id},units/{unit_id}.txt,{hashlib.sha256(text.encode()).hexdigest()},{bucket}")
    index = tmp_path / "frozen.csv"
    index.write_text("unit_id,unit_file,unit_sha256,bucket\n" + "\n".join(rows) + "\n", encoding="utf-8")
    assert prompts.development_units(index, tmp_path) == ["one", "two"]
    (tmp_path / "units" / "u1.txt").write_text("changed", encoding="utf-8")
    with pytest.raises(ValueError, match="u1"):
        prompts.development_units(index, tmp_path)
