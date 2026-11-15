"""Rules for every card (section 5) and section 6.5: n.e., "k of n", Wilson, aggregation (6.4)."""
from fractions import Fraction

import pytest

from conftest import build_cell, gold_outcome, mixed_outcome
from technique import K_OF_N, NOT_ESTIMABLE, RATE, ItemType, State, aggregate, item_rate, mean_of, ne_register, views, wilson
from technique.items import with_states


def test_empty_denominator_is_ne_never_0_and_never_1():
    m = item_rate("x", 0, 0)
    assert m.status == NOT_ESTIMABLE and m.exact is None and m.value is None and m.n == 0
    assert str(m) == "n.e. (n = 0)"


def test_below_ten_is_k_of_n_without_rate_and_without_interval():
    m = item_rate("x", 9, 9)
    assert m.status == K_OF_N and m.exact is None and m.interval is None and (m.k, m.n) == (9, 9)
    assert item_rate("x", 9, 10).status == RATE


def test_k_of_n_and_ne_are_excluded_from_aggregation_and_listed_in_the_register():
    parts = [item_rate("x", 5, 10), item_rate("x", 9, 9), item_rate("x", 0, 0)]
    agg = mean_of("x", parts)
    assert agg.mean == Fraction(1, 2) and agg.n_included == 1 and agg.excluded == ("2", "3")
    assert len(ne_register(parts)) == 2


def test_a_mean_with_no_estimable_component_is_ne_not_zero():
    agg = mean_of("x", [item_rate("x", 0, 0), None])
    assert agg.mean is None and agg.value is None and not agg.estimable and ne_register([agg])


def test_wilson_interval_known_values_and_bounds():
    lo, hi = wilson(5, 10)
    assert lo == pytest.approx(0.2366, abs=1e-4) and hi == pytest.approx(0.7634, abs=1e-4)
    lo, hi = wilson(0, 10)
    assert lo == 0.0 and hi == pytest.approx(0.2775, abs=1e-4)
    lo, hi = wilson(10, 10)
    assert hi == 1.0 and lo == pytest.approx(0.7225, abs=1e-4)
    a, b = wilson(3, 20), wilson(17, 20)                      # mirror image
    assert a[0] == pytest.approx(1 - b[1]) and a[1] == pytest.approx(1 - b[0])
    with pytest.raises(ValueError):
        wilson(1, 0)


def test_single_cell_item_rate_carries_a_wilson_interval():
    m = views.functional_populatability(build_cell("half_gold"))
    assert m.interval == wilson(15, 30) and m.interval[0] < 0.5 < m.interval[1]


def test_replication_value_is_the_mean_over_passes_with_the_first_pass_beside_it():
    # pass 1 all correct; passes 2 and 3 follow the mixed pattern
    t1 = build_cell("full_gold", gold_outcome, passes=(1,))
    t23 = build_cell("full_gold", mixed_outcome, passes=(2, 3))
    t1.items.extend(t23.of_type(ItemType.CQ))
    rep = aggregate.over_passes(t1.validate())
    q = views.q_own(t23, 2).exact
    assert rep.value.first == 1 and rep.value.mean == (1 + q + q) / 3
    assert rep.completed_passes == (1, 2, 3) and rep.completed_share == 1 and rep.value.complete


def test_a_partial_pass_is_reported_but_stays_out_of_the_mean():
    table = build_cell("full_gold", gold_outcome, passes=(1, 2))
    eligible = sorted({q.item_id for q in table.of_type(ItemType.CQ) if q.eligible})
    keep = set(eligible[:10])                                   # pass 2 stopped after ten questions
    table.items = [it for it in table.items if not (it.item_type is ItemType.CQ and it.pass_id == 2 and it.item_id not in keep)]
    rep = aggregate.over_passes(table.validate())
    assert rep.completed_passes == (1,) and rep.partial_passes == (2,) and 2 in rep.per_pass
    assert rep.value.n_included == 1 and not rep.value.complete
    assert rep.completed_share == Fraction(len(eligible) + 10, 3 * len(eligible))


def test_axis_value_is_the_mean_of_replication_rates_with_minimum_and_maximum():
    reps = [build_cell("full_gold", replication=1), build_cell("half_gold", replication=2), build_cell("empty", replication=3)]
    p = aggregate.axis_value("P.1", [views.functional_populatability(t) for t in reps], tables=reps)
    assert (p.mean, p.minimum, p.maximum) == (Fraction(1, 2), 0, 1) and p.n_included == 3
    assert p.sample_sd() == pytest.approx(0.5)


def test_counts_are_never_pooled_across_replications():
    # Q_own has a different denominator in each replication: the mean of rates is not the pooled rate
    a = build_cell("full_gold", mixed_outcome, replication=1)
    states = {x: State.F2 for x in ("a003", "a005", "a009")}        # three clusters lose eligibility
    b = build_cell("full_gold", mixed_outcome, replication=2, states=states)
    qa, qb = views.q_own(a, 1), views.q_own(b, 1)
    assert qa.estimable and qb.estimable and qa.n != qb.n
    axis = aggregate.axis_value("Q.1", [qa, qb, None])
    assert axis.mean == (qa.exact + qb.exact) / 2 and axis.excluded == ("replication 3",)
    assert axis.mean != Fraction(qa.k + qb.k, qa.n + qb.n)


def test_an_ne_replication_is_excluded_never_imputed():
    reps = [build_cell("full_gold", replication=1), build_cell("empty", replication=2)]
    prof = aggregate.profile(reps)
    assert prof["Q_own"].mean == 1 and prof["Q_own"].n_included == 1 and prof["Q_own"].excluded == ("replication 2",)
    assert prof["P_func"].mean == Fraction(1, 2)


def test_nothing_is_pooled_across_arms_or_treatments():
    a, b = build_cell("full_gold", arm="PG"), build_cell("full_gold", arm="RDF")
    with pytest.raises(ValueError, match="pooled"):
        aggregate.profile([a, b])
    with pytest.raises(ValueError, match="pooled"):
        aggregate.axis_value("P.1", [views.functional_populatability(a), views.functional_populatability(b)], tables=[a, b])


def test_coverage_must_be_identical_in_every_replication():
    a = build_cell("full_gold", replication=1)
    b = build_cell("empty", replication=2)
    assert aggregate.coverage_of_realisation([a, b]).exact == Fraction(5, 6)
    c = build_cell("full_gold", replication=3, f1=frozenset({"a033"}))
    with pytest.raises(ValueError, match="differs"):
        aggregate.coverage_of_realisation([a, c])


def test_canary_items_are_outside_every_denominator():
    from dataclasses import replace
    plain = build_cell("full_gold", mixed_outcome)
    extra = [replace(plain.assertion("a001"), item_id="c001", canary=True, state=State.F2)]
    with_canary = build_cell("full_gold", mixed_outcome, extra_items=extra)
    cq = replace(next(q for q in plain.of_type(ItemType.CQ) if q.pass_id == 1), item_id="qc01", canary=True,
                 required=("c001",), state=State.F2, first_state=State.F2, sub_code=None, first_sub_code=None,
                 partial_presence=(0, 1), attempt_index=None)
    with_canary.items.append(cq)
    with_canary.validate()
    for f in (views.coverage, views.functional_populatability, views.realised_coverage, views.reference_precision):
        assert f(with_canary) == f(plain)
    for f in (views.q_own, views.end_to_end):
        assert f(with_canary, 1) == f(plain, 1)
    assert views.eligibility_rate(with_canary) == views.eligibility_rate(plain)
    assert views.tier_vector(with_canary)["sizes"] == views.tier_vector(plain)["sizes"]


def test_unresolved_assertions_and_the_cqs_that_require_them_leave_the_denominators():
    from dataclasses import replace
    plain = build_cell("full_gold", mixed_outcome)
    flagged = build_cell("full_gold", mixed_outcome)
    flagged.items = [replace(it, unresolved=True) if it.item_id == "a001" else it for it in flagged.items]
    assert views.coverage(flagged).n == views.coverage(plain).n - 1
    assert views.functional_populatability(flagged).n == views.functional_populatability(plain).n - 1
    needs = {q.item_id for q in plain.of_type(ItemType.CQ) if "a001" in q.required}
    assert views.end_to_end(flagged, 1).n == views.end_to_end(plain, 1).n - len(needs)
    assert views.realised_identity(flagged).holds                     # same base set for C, P and C_r
    assert views.tier_vector(flagged)["unresolved"] == 1


def test_scale_is_fixed_by_the_empty_and_the_full_gold_graph():
    """Section 6.6: on the empty graph every run-dependent rate is 0 or n.e.; on full gold every one is 1."""
    empty, full = build_cell("empty"), build_cell("full_gold")
    run_dependent = [views.functional_populatability, views.realised_coverage, views.reference_precision,
                     lambda t: views.q_own(t, 1), lambda t: views.eligibility_rate(t), lambda t: views.end_to_end(t, 1)]
    for f in run_dependent:
        m = f(empty)
        assert m.status == NOT_ESTIMABLE or m.exact == 0
    # on the full-gold graph the conditional rates are 1; the rates over all items are bounded by coverage
    assert views.functional_populatability(full).exact == 1 and views.reference_precision(full).exact == 1
    assert views.q_own(full, 1).exact == 1
    assert views.realised_coverage(full).exact == views.coverage(full).exact
