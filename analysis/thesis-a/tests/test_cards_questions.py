"""Expected properties of the question-based cards: Q.1, Q.2, Q.3, Q.5, Q.6, Q.9."""
from dataclasses import replace
from fractions import Fraction

import pytest

from conftest import BOUNDARY, CORE, build_cell, gold_outcome, mixed_outcome
from technique import FUNNEL_LEVELS, K_OF_N, NOT_ESTIMABLE, ItemTable, ItemType, State, cq_row, funnel_level, views


# ---------------------------------------------------------------- Q.6 and eligibility

def test_q6_full_gold_and_empty():
    full = views.eligibility_rate(build_cell("full_gold", f1=frozenset()))
    assert full.exact == 1 and full.micro == 1
    empty = views.eligibility_rate(build_cell("empty"))
    assert empty.exact == 0 and empty.n == 31


def test_q6_counts_every_cq_once_whatever_the_number_of_passes():
    assert views.eligibility_rate(build_cell("full_gold", passes=(1, 2, 3))) == views.eligibility_rate(build_cell("full_gold", passes=(1,)))


def test_q6_ineligible_cqs_carry_inherited_state_and_k_of_n():
    table = build_cell("full_gold")                       # a007 and a019 have no image
    rows = {q.item_id: q for q in table.of_type(ItemType.CQ) if q.pass_id == 1 and not q.eligible}
    assert rows and all(q.state is State.F1 and q.partial_presence == (1, 2) for q in rows.values())


# ---------------------------------------------------------------- Q.1

def test_q1_full_gold_with_gold_queries_is_1():
    m = views.q_own(build_cell("full_gold", gold_outcome), 1)
    assert m.exact == 1 and m.micro == 1 and m.n_units == 14


def test_q1_empty_graph_is_not_estimable_because_no_cq_is_eligible():
    m = views.q_own(build_cell("empty", mixed_outcome), 1)
    assert m.status == NOT_ESTIMABLE and m.n == 0 and m.exact is None


def test_q1_equals_level_4_of_the_funnel_at_the_final_attempt():
    table = build_cell("full_gold", mixed_outcome)
    assert views.q_own(table, 1).exact == views.funnel(table, 1).final["semantically_correct"].exact


def test_q1_a_cq_with_no_produced_query_stays_in_the_denominator_and_fails():
    all_f4 = lambda i: ((State.F4, None), (State.F4, None))
    m = views.q_own(build_cell("full_gold", all_f4), 1)
    assert m.exact == 0 and m.n == 29


def test_q1_accidental_correctness_and_the_other_f6_sub_codes_are_failures():
    for code in ("accidental", "memorised_constant", "empty_without_discrimination", "boolean_without_witness", None):
        out = lambda i, code=code: ((State.F6, code), (State.F6, code))
        assert views.q_own(build_cell("full_gold", out), 1).exact == 0


def test_q1_is_a_cluster_macro_average_with_the_micro_row_beside_it():
    # all CQs of clusters of size 3 fail, all others succeed
    table = build_cell("full_gold", gold_outcome)
    big = {c for c in {q.cluster_id for q in table.of_type(ItemType.CQ)}
           if sum(1 for q in table.of_type(ItemType.CQ) if q.cluster_id == c and q.pass_id == 1) == 3}
    table.items = [replace(q, state=State.F4, first_state=State.F4) if q.item_type is ItemType.CQ and q.cluster_id in big else q
                   for q in table.items]
    m = views.q_own(table.validate(), 1)
    eligible_big = sum(1 for s in m.cluster_sizes if s == 3)
    assert m.exact == Fraction(m.n_units - eligible_big, m.n_units)
    assert m.micro == Fraction(m.n - 3 * eligible_big, m.n) and m.micro != m.exact


def test_q1_below_ten_clusters_gives_k_of_n():
    m = views.q_own(build_cell("half_gold", gold_outcome), 1)
    assert m.status == K_OF_N and m.n_units == 2 and m.rule_unit == "clusters" and m.exact is None


def test_q1_subgroup_and_q9_held_out_equals_q_own_when_only_held_out_families_exist():
    only = build_cell("full_gold", mixed_outcome, only_bucket="held-out", f1=frozenset())
    held = views.q_own(only, 1, where=lambda q: q.bucket == "held-out", min_n=5, card="Q.9")
    own = views.q_own(only, 1, min_n=5)
    assert held.exact == own.exact and held.n == own.n
    both = build_cell("full_gold", mixed_outcome, f1=frozenset())
    assert views.q_own(both, 1, where=lambda q: q.bucket == "held-out", min_n=5).n < views.q_own(both, 1).n


# ---------------------------------------------------------------- Q.2

def test_q2_rates_never_increase_from_one_level_to_the_next():
    f = views.funnel(build_cell("full_gold", mixed_outcome), 1)
    for attempt in (f.first, f.final):
        values = [attempt[level].exact for level in FUNNEL_LEVELS]
        assert values == sorted(values, reverse=True)
        assert len({attempt[level].n for level in FUNNEL_LEVELS}) == 1            # one denominator


def test_q2_repair_gain_is_final_minus_first():
    f = views.funnel(build_cell("full_gold", mixed_outcome), 1)
    for level in FUNNEL_LEVELS:
        assert f.repair_gain[level] == f.final[level].exact - f.first[level].exact
    assert f.repair_gain["semantically_correct"] > 0                               # the repaired parse failures


def test_q2_a_revision_that_breaks_a_working_query_gives_a_negative_repair_gain():
    def broken_by_revision(index):
        return ((State.SUCCESS, None), (State.F5, "runtime"))                      # worked first, last attempt fails
    f = views.funnel(build_cell("full_gold", broken_by_revision), 1)
    assert f.first["semantically_correct"].exact == 1 and f.final["semantically_correct"].exact == 0
    assert f.repair_gain["semantically_correct"] == -1 and f.repair_gain["executes"] == -1
    assert f.repair_gain["parses"] == 0 and f.repair_gain["produced"] == 0        # printed, not clipped


def test_q2_a_cq_that_fails_a_level_fails_all_later_ones():
    table = build_cell("full_gold", mixed_outcome)
    for q in table.of_type(ItemType.CQ):
        if q.eligible:
            reached = funnel_level(q.state, q.sub_code)
            passed = [reached >= i for i in range(1, 5)]
            assert passed == sorted(passed, reverse=True)


def test_q2_f6_sub_codes_are_shares_of_f6_and_sum_to_1():
    f = views.funnel(build_cell("full_gold", mixed_outcome), 1)
    assert sum(f.f6_sub_codes.values()) == 1 and "accidental" in f.f6_sub_codes
    assert views.funnel(build_cell("full_gold", gold_outcome), 1).f6_sub_codes is None


def test_q2_all_levels_are_1_with_gold_queries_and_ne_on_the_empty_graph():
    f = views.funnel(build_cell("full_gold", gold_outcome), 1)
    assert all(f.final[level].exact == 1 and f.repair_gain[level] == 0 for level in FUNNEL_LEVELS)
    e = views.funnel(build_cell("empty", gold_outcome), 1)
    assert all(e.final[level].status == NOT_ESTIMABLE and e.repair_gain[level] is None for level in FUNNEL_LEVELS)


# ---------------------------------------------------------------- Q.5 and the identity

@pytest.mark.parametrize("graph", BOUNDARY)
@pytest.mark.parametrize("outcome", [gold_outcome, mixed_outcome])
def test_q5_identity_is_exact_at_cq_level_in_every_pass(graph, outcome):
    table = build_cell(graph, outcome)
    for p in (1, 2, 3):
        ident = views.end_to_end_identity(table, p)
        if ident.holds is None:                           # no eligible CQ: Q_own is n.e.
            assert ident.left == 0
        else:
            assert ident.holds


def test_q5_never_above_the_eligibility_rate(boundary):
    _, table = boundary
    assert views.end_to_end(table, 1).exact <= views.eligibility_rate(table, 1).exact
    assert views.end_to_end(table, 1).micro <= views.eligibility_rate(table, 1).micro


def test_q5_empty_graph_is_0_and_ineligible_cqs_stay_in_the_denominator():
    m = views.end_to_end(build_cell("empty", gold_outcome), 1)
    assert m.exact == 0 and m.n == 31
    assert views.end_to_end(build_cell("full_gold", gold_outcome), 1).n == 31


def test_q5_macro_identity_needs_uniform_eligibility_inside_clusters():
    """Package 8.1, row 6: exact per CQ; under the cluster macro-average only when eligibility is
    uniform within every cluster. Here one cluster holds CQs with different required sets, and the
    queries are not all correct (with Q_own = 1 the product would still agree)."""
    table = build_cell("full_gold", mixed_outcome)
    st = table.assertion_states()
    k01 = [q for q in table.of_type(ItemType.CQ) if q.cluster_id == "k03" and q.pass_id == 1]
    odd = cq_row(k01[0].item_id, "k03", (CORE[4], CORE[6]), st, pass_id=1, bucket=k01[0].bucket)   # a007 has no image
    rows = [odd if (it.item_type is ItemType.CQ and it.item_id == odd.item_id and it.pass_id == 1) else it
            for it in table.items if not (it.item_type is ItemType.CQ and it.pass_id != 1)]
    mixed = ItemTable(rows, table.cell).validate()
    assert views.end_to_end_identity(mixed, 1).holds                                  # CQ level: exact
    macro = views.eligibility_rate(mixed, 1).exact * views.q_own(mixed, 1).exact
    assert macro != views.end_to_end(mixed, 1).exact                                  # macro level: not exact


def test_q5_product_of_the_three_axes_is_returned_as_an_approximation_with_its_deviation():
    r = views.product_approximation(build_cell("full_gold", mixed_outcome), 1)
    assert r["deviation"] == r["product"] - r["measured"]
    assert views.product_approximation(build_cell("empty", mixed_outcome), 1) is None   # Q_own is n.e.


# ---------------------------------------------------------------- Q.3

def test_q3_equals_q_own_in_every_cell_when_all_cells_have_the_same_eligible_set():
    a = build_cell("full_gold", mixed_outcome, treatment="S4")
    b = build_cell("duplicated_gold", gold_outcome, treatment="S3")
    r = views.q_intersection([a, b], 1)
    assert [m.exact for m in r.per_cell] == [views.q_own(a, 1).exact, views.q_own(b, 1).exact]
    assert not r.weak and r.n_clusters == 14


def test_q3_intersection_is_never_larger_than_any_own_set_and_has_one_denominator():
    a = build_cell("full_gold", mixed_outcome, treatment="S4")
    b = build_cell("full_gold", mixed_outcome, treatment="S1", f1=frozenset({"a001", "a033"}))
    c = build_cell("full_gold", mixed_outcome, treatment="S3", states={"a003": State.F3})
    r = views.q_intersection([a, b, c], 1)
    own = [views.q_own(t, 1).n for t in (a, b, c)]
    assert r.n_cqs <= min(own) and len({m.n for m in r.per_cell}) == 1 and r.per_cell[0].n == r.n_cqs


def test_q3_weak_intersection_flag():
    a = build_cell("full_gold", gold_outcome, treatment="S4", f1=frozenset())
    b = build_cell("half_gold", gold_outcome, treatment="S1", f1=frozenset())
    r = views.q_intersection([a, b], 1)
    assert r.weak and "fewer than 10 clusters" in r.weak_reasons
    assert all(m.status == K_OF_N for m in r.per_cell)
    with pytest.raises(ValueError):
        views.q_intersection([a], 1)
