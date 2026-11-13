"""Section 3: one state per item per cell, eligibility and inheritance."""
from dataclasses import replace

import pytest

from conftest import build_cell, cluster_requirements, gold_outcome, mixed_outcome
from technique import Item, ItemTable, ItemTableError, ItemType, State, Tier, cq_row, funnel_level, worst_upstream


def core(aid, state, **kw):
    return Item(aid, ItemType.ASSERTION, state, tier=Tier.CORE, **kw)


def test_assertion_takes_only_f1_f2_f3_or_success():
    for state in (State.F4, State.F5, State.F6, State.NOT_IN_SOURCE):
        with pytest.raises(ItemTableError):
            ItemTable([core("a001", state)]).validate()
    for state in (State.F1, State.F2, State.F3, State.SUCCESS):
        ItemTable([core("a001", state)]).validate()


def test_cq_takes_only_f1_to_f6_or_success():
    st = {"a001": State.SUCCESS}
    ok = cq_row("q001", "k01", ["a001"], st, pass_id=1, final=(State.F4, None))
    ItemTable([core("a001", State.SUCCESS), ok]).validate()
    with pytest.raises(ItemTableError):
        ItemTable([core("a001", State.SUCCESS), replace(ok, state=State.NOT_IN_SOURCE)]).validate()


def test_exactly_one_state_per_item_per_cell():
    with pytest.raises(ItemTableError, match="more than one row"):
        ItemTable([core("a001", State.SUCCESS), core("a001", State.F2)]).validate()


def test_only_inventory_items_can_be_not_in_source():
    inv = Item("i001", ItemType.INVENTORY_ITEM, State.NOT_IN_SOURCE, inventory_id="inv1", mentioned=False)
    ItemTable([inv]).validate()
    with pytest.raises(ItemTableError):
        ItemTable([Item("s1", ItemType.INPUT_STATEMENT, State.NOT_IN_SOURCE)]).validate()


def test_sub_codes_belong_to_their_state():
    st = {"a001": State.SUCCESS}
    base = [core("a001", State.SUCCESS)]
    with pytest.raises(ItemTableError):   # F5 without parse or runtime
        ItemTable(base + [replace(cq_row("q1", "k1", ["a001"], st, pass_id=1, final=(State.F4, None)), state=State.F5)]).validate()
    with pytest.raises(ItemTableError):   # an F6 sub-code on F4
        ItemTable(base + [cq_row("q1", "k1", ["a001"], st, pass_id=1, final=(State.F4, "accidental"))]).validate()
    ItemTable(base + [cq_row("q1", "k1", ["a001"], st, pass_id=1, final=(State.F6, None))]).validate()


def test_worst_upstream_is_f1_before_f2_before_f3():
    assert worst_upstream([State.SUCCESS, State.F3, State.F2, State.F1]) is State.F1
    assert worst_upstream([State.F3, State.F2, State.SUCCESS]) is State.F2
    assert worst_upstream([State.F3, State.SUCCESS]) is State.F3
    assert worst_upstream([State.SUCCESS, State.SUCCESS]) is None


def test_ineligible_cq_inherits_state_and_carries_k_of_n():
    st = {"a001": State.SUCCESS, "a002": State.F3, "a003": State.F2}
    row = cq_row("q001", "k01", ["a001", "a002", "a003"], st, pass_id=1)
    assert row.state is State.F2 and row.partial_presence == (1, 3) and not row.eligible


def test_cq_is_eligible_exactly_when_all_required_assertions_are_success():
    table = build_cell("half_gold", mixed_outcome)
    states = table.assertion_states()
    for q in table.of_type(ItemType.CQ):
        assert q.eligible == all(states[a] is State.SUCCESS for a in q.required)


def test_validation_rejects_an_eligible_cq_with_an_upstream_state_and_the_reverse():
    table = build_cell("full_gold")
    q = next(x for x in table.of_type(ItemType.CQ) if x.eligible)
    with pytest.raises(ItemTableError, match="eligible"):
        ItemTable([replace(x, state=State.F2) if x is q else x for x in table.items]).validate()
    empty = build_cell("empty")
    q = empty.of_type(ItemType.CQ)[0]
    with pytest.raises(ItemTableError, match="inherit"):
        ItemTable([replace(x, state=State.SUCCESS, partial_presence=None) if x is q else x for x in empty.items]).validate()


def test_required_assertions_must_lie_in_the_core_tier():
    items = [core("a001", State.SUCCESS), Item("s001", ItemType.ASSERTION, State.SUCCESS, tier=Tier.SUPPORTING)]
    st = {"a001": State.SUCCESS, "s001": State.SUCCESS}
    with pytest.raises(ItemTableError, match="core tier"):
        ItemTable(items + [cq_row("q1", "k1", ["a001", "s001"], st, pass_id=1, final=(State.SUCCESS, None))]).validate()


def test_final_attempt_may_be_below_the_first_attempt_unless_forbidden():
    st = {"a001": State.SUCCESS}
    worse = cq_row("q1", "k1", ["a001"], st, pass_id=1, first=(State.SUCCESS, None), final=(State.F4, None))
    ItemTable([core("a001", State.SUCCESS), worse]).validate()                         # the last attempt is scored
    with pytest.raises(ItemTableError, match="below the first"):
        ItemTable([core("a001", State.SUCCESS), worse]).validate(allow_regression=False)


def test_funnel_levels_are_cumulative_per_attempt():
    assert [funnel_level(*x) for x in [(State.F4, None), (State.F5, "parse"), (State.F5, "runtime"),
                                        (State.F6, None), (State.F6, "accidental"), (State.SUCCESS, None)]] == [0, 1, 2, 3, 3, 4]


def test_every_boundary_table_is_valid(boundary):
    _, table = boundary
    assert len(table.of_type(ItemType.ASSERTION)) == 50
    assert len({q.item_id for q in table.of_type(ItemType.CQ)}) == sum(n for _, n in cluster_requirements().values())
