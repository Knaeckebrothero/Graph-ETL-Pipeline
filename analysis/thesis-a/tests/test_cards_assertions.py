"""Expected properties of the assertion-based cards: C.1, C.2, C.3, C.4, C.6, C.7, P.1, P.6."""
from dataclasses import replace
from fractions import Fraction

import pytest

from conftest import BOUNDARY, CLASSES, CORE, F1_DEFAULT, SUPPORTING, build_cell
from technique import NOT_ESTIMABLE, Item, ItemTable, ItemType, State, Tier, views

C = Fraction(30, 36)        # coverage of the synthetic mapping: 6 of 36 core assertions have no image


# ---------------------------------------------------------------- C.1

def test_c1_unchanged_by_the_content_of_the_graph():
    values = {g: views.coverage(build_cell(g)) for g in BOUNDARY}
    assert all(m.exact == C for m in values.values())
    assert values["full_gold"] == values["hallucinated"] == values["text_blob"]


def test_c1_mapping_with_no_image_gives_0_and_with_every_image_gives_1():
    assert views.coverage(build_cell("full_gold", f1=frozenset(CORE))).exact == 0
    assert views.coverage(build_cell("full_gold", f1=frozenset())).exact == 1


def test_c1_never_below_realised_coverage(boundary):
    _, table = boundary
    assert views.coverage(table).exact >= views.realised_coverage(table).exact


def test_c1_subgroup_is_the_same_computation_on_the_subgroup_rows():
    table = build_cell("full_gold")
    m = views.coverage(table, where=lambda a: a.jurisdiction == "Frankfurt")
    rows = [a for a in table.of_type(ItemType.ASSERTION) if a.tier is Tier.CORE and a.jurisdiction == "Frankfurt"]
    assert (m.k, m.n) == (sum(1 for a in rows if a.state is not State.F1), len(rows))


# ---------------------------------------------------------------- C.2

def test_c2_is_run_independent():
    vectors = [views.tier_vector(build_cell(g)) for g in BOUNDARY]
    assert all(v["supporting"] == vectors[0]["supporting"] and v["sizes"] == vectors[0]["sizes"] for v in vectors)
    assert vectors[0]["supporting"].exact == Fraction(10, 12)


def test_c2_tier_sizes_sum_to_the_number_of_anchored_assertions():
    v = views.tier_vector(build_cell("full_gold"))
    assert v["sizes"] == {"core": 36, "supporting": 12, "excluded": 2} and sum(v["sizes"].values()) == v["anchored"] == 50
    assert v["excluded_reasons"] == {"synthetic exclusion": 2}


def test_c2_moving_an_assertion_between_tiers_moves_it_between_denominators_never_into_both():
    before = build_cell("full_gold")
    after = ItemTable([replace(a, tier=Tier.SUPPORTING) if a.item_id == "a036" else a for a in before.items], before.cell)
    assert views.coverage(after).n == views.coverage(before).n - 1
    assert views.coverage(after, tier=Tier.SUPPORTING).n == views.coverage(before, tier=Tier.SUPPORTING).n + 1
    assert sum(views.tier_vector(after)["sizes"].values()) == 50


# ---------------------------------------------------------------- C.3

EQUAL = {c: 1 for c in CLASSES}


def test_c3_equals_unweighted_coverage_over_core_and_supporting_when_weights_are_equal():
    table = build_cell("full_gold")
    core, sup = views.coverage(table), views.coverage(table, tier=Tier.SUPPORTING)
    assert views.weighted_coverage(table, EQUAL).exact == Fraction(core.k + sup.k, core.n + sup.n)


def test_c3_unchanged_when_all_weights_are_multiplied_by_one_constant():
    table = build_cell("full_gold")
    w = {"A": Fraction(1), "B": Fraction(1), "C": Fraction(1, 2), "D": Fraction(1, 4)}      # test values only
    assert views.weighted_coverage(table, w).exact == views.weighted_coverage(table, {k: 7 * v for k, v in w.items()}).exact


def test_c3_lies_between_the_smallest_and_the_largest_class_wise_coverage():
    table = build_cell("full_gold")
    w = {"A": 3, "B": 1, "C": Fraction(1, 2), "D": Fraction(1, 4)}                          # test values only
    per_class = []
    for c in CLASSES:
        rows = [a for a in table.of_type(ItemType.ASSERTION) if a.tier in (Tier.CORE, Tier.SUPPORTING) and a.authority_class == c]
        per_class.append(Fraction(sum(1 for a in rows if a.state is not State.F1), len(rows)))
    assert min(per_class) <= views.weighted_coverage(table, w).exact <= max(per_class)


def test_c3_not_estimable_when_the_weight_sum_is_0_and_no_default_weights():
    table = build_cell("full_gold")
    assert views.weighted_coverage(table, {c: 0 for c in CLASSES}).status == NOT_ESTIMABLE
    with pytest.raises(TypeError):
        views.weighted_coverage(table)                      # the class weights have no default
    with pytest.raises(KeyError):
        views.weighted_coverage(table, {"A": 1})


# ---------------------------------------------------------------- C.4 and the identity

def test_c4_identity_is_exact_on_the_item_table(boundary):
    _, table = boundary
    ident = views.realised_identity(table)
    assert ident.holds and ident.left == views.realised_coverage(table).exact
    assert ident.right == views.coverage(table).exact * views.functional_populatability(table).exact


def test_c4_never_above_c_and_boundary_values(boundary):
    name, table = boundary
    cr = views.realised_coverage(table).exact
    assert cr <= C
    expected = {"empty": 0, "full_gold": C, "duplicated_gold": C, "half_gold": C / 2,
                "hallucinated": 0, "wrong_qualifiers": 0, "text_blob": 0}[name]
    assert cr == expected


def test_c4_shares_f1_f2_plus_f3_and_success_sum_to_1(boundary):
    _, table = boundary
    d = views.loss_decomposition(table)
    assert d["F1"] + d["F2+F3"] + d["success"] == 1 and d["F1"] == 1 - C


def test_identity_is_not_checkable_when_nothing_is_representable():
    table = build_cell("full_gold", f1=frozenset(CORE))
    assert views.realised_identity(table).holds is None and views.realised_coverage(table).exact == 0


# ---------------------------------------------------------------- P.1

def test_p1_boundary_values(boundary):
    name, table = boundary
    p = views.functional_populatability(table)
    expected = {"empty": 0, "full_gold": 1, "duplicated_gold": 1, "half_gold": Fraction(1, 2),
                "hallucinated": 0, "wrong_qualifiers": 0, "text_blob": 0}[name]
    assert p.exact == expected and p.n == 30


def test_p1_half_gold_is_exactly_one_half_with_exact_denominators():
    p = views.functional_populatability(build_cell("half_gold"))
    assert (p.k, p.n, p.exact) == (15, 30, Fraction(1, 2))


def test_p1_duplicated_gold_gives_1_with_no_inflation():
    assert views.functional_populatability(build_cell("duplicated_gold")) == views.functional_populatability(build_cell("full_gold"))


def test_p1_wrong_qualifiers_gives_0_with_an_f3_share_of_1():
    d = views.loss_decomposition(build_cell("wrong_qualifiers"))
    assert d["F3"] / (1 - d["F1"]) == 1 and d["success"] == 0


def test_p1_an_unqualified_assertion_is_untouched_by_wrong_qualifiers():
    """The card's '0 with an F3 share of 1' presupposes that every core assertion carries a qualifier."""
    table = build_cell("wrong_qualifiers", states={"a001": State.SUCCESS})       # a001 has no qualifier to get wrong
    assert views.functional_populatability(table).exact == Fraction(1, 30)


def test_p1_shares_of_f2_f3_and_success_over_the_representable_assertions_sum_to_1(boundary):
    _, table = boundary
    d = views.loss_decomposition(table)
    rep = 1 - d["F1"]
    assert (d["F2"] + d["F3"] + d["success"]) / rep == 1


def test_p1_not_estimable_when_no_core_assertion_is_representable():
    p = views.functional_populatability(build_cell("full_gold", f1=frozenset(CORE)))
    assert p.status == NOT_ESTIMABLE and p.exact is None


# ---------------------------------------------------------------- P.6

def test_p6_boundary_values(boundary):
    name, table = boundary
    m = views.reference_precision(table)
    if name in ("empty", "text_blob"):
        assert m.status == NOT_ESTIMABLE              # no candidate at any anchor
    elif name in ("full_gold", "duplicated_gold", "half_gold"):
        assert m.exact == 1
    else:
        assert m.exact == 0                           # hallucinated, wrong qualifiers


# ---------------------------------------------------------------- C.6 and C.7

def statements(states):
    return [Item(f"t{i:03d}", ItemType.INPUT_STATEMENT, s) for i, s in enumerate(states)]


def test_c6_three_readings_and_the_identity():
    table = ItemTable(statements([State.F1] * 4 + [State.F2] * 6 + [State.F3] * 2 + [State.SUCCESS] * 8)).validate()
    r = views.preservation(table)
    assert r["representational"].exact == Fraction(16, 20) and r["functional"].exact == Fraction(8, 16)
    assert r["realised"].exact == Fraction(8, 20) and r["identity_holds"]
    assert r["realised"].exact == r["representational"].exact * r["functional"].exact


def inventory(states_and_mentions):
    return [Item(f"i{i:03d}", ItemType.INVENTORY_ITEM, s, inventory_id="inv1", mentioned=m)
            for i, (s, m) in enumerate(states_and_mentions)]


def test_c7_representable_share_is_independent_of_sample_and_run():
    base = [(State.F1, True)] * 2 + [(State.F1, False)] * 2 + [(State.NOT_IN_SOURCE, False)] * 6 + [(State.SUCCESS, True)] * 10
    other = [(State.F1, False)] * 4 + [(State.F2, True)] * 9 + [(State.F3, True)] * 7        # other sample, other run
    a = views.inventory_completeness(ItemTable(inventory(base)).validate(), "inv1")
    b = views.inventory_completeness(ItemTable(inventory(other)).validate(), "inv1")
    assert a["representable_share"].exact == b["representable_share"].exact == Fraction(16, 20)


def test_c7_no_agent_is_charged_for_an_item_its_sources_did_not_contain():
    rows = [(State.NOT_IN_SOURCE, False)] * 8 + [(State.SUCCESS, True)] * 9 + [(State.F2, True)] * 3
    r = views.inventory_completeness(ItemTable(inventory(rows)).validate(), "inv1")
    assert (r["realised_over_mentioned"].k, r["realised_over_mentioned"].n) == (9, 12)
    assert r["sample_reach"]["mentioned_over_all"].exact == Fraction(12, 20)
    assert r["sample_reach"]["realised_over_all"].exact == Fraction(9, 20)


def test_c7_no_realised_row_for_a_capacity_only_or_unmentioned_inventory():
    rows = [(State.NOT_IN_SOURCE, False)] * 12
    r = views.inventory_completeness(ItemTable(inventory(rows)).validate(), "inv1")
    assert r["realised_over_mentioned"].status == NOT_ESTIMABLE and r["representable_share"].exact == 1
    full = [(State.SUCCESS, True)] * 12
    r = views.inventory_completeness(ItemTable(inventory(full)).validate(), "inv1", capacity_only=True)
    assert r["realised_over_mentioned"].status == NOT_ESTIMABLE and r["sample_reach"] is None
