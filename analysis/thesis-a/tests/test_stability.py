"""Card P.8, row (a): expected properties."""
from fractions import Fraction

from technique.stability import construction_stability, jaccard


def units(n, reps):
    return {f"u{i:03d}": reps(i) for i in range(n)}


def test_three_identical_replications_give_1():
    s = construction_stability(units(12, lambda i: [{("s", "p", i, "q=1")}] * 3))
    assert s.mean_pairwise_jaccard == 1 and s.stable_core_share == 1 and s.stable_core_pooled == 1 and s.n_units == 12


def test_disjoint_sets_give_0():
    s = construction_stability(units(12, lambda i: [{(i, "a")}, {(i, "b")}, {(i, "c")}]))
    assert s.mean_pairwise_jaccard == 0 and s.stable_core_share == 0


def test_value_does_not_depend_on_the_order_of_the_replications():
    reps = lambda i: [{1, 2, 3, i}, {2, 3}, {3, 4}]
    a = construction_stability(units(12, reps))
    b = construction_stability(units(12, lambda i: list(reversed(reps(i)))))
    c = construction_stability(units(12, lambda i: [reps(i)[1], reps(i)[2], reps(i)[0]]))
    assert a == b == c and 0 < a.mean_pairwise_jaccard < 1


def test_duplicated_assertions_do_not_change_it():
    a = construction_stability(units(12, lambda i: [[1, 2], [2, 3], [3]]))
    b = construction_stability(units(12, lambda i: [[1, 1, 2, 2], [2, 3, 3], [3, 3, 3]]))
    assert a == b


def test_known_value_and_stable_core():
    # pairs: {1,2}/{2,3} = 1/3, {1,2}/{2} = 1/2, {2,3}/{2} = 1/2 -> mean 4/9; core {2} of union {1,2,3} = 1/3
    s = construction_stability(units(10, lambda i: [{1, 2}, {2, 3}, {2}]))
    assert s.mean_pairwise_jaccard == Fraction(4, 9) and s.stable_core_share == Fraction(1, 3)


def test_qualifiers_are_part_of_an_assertions_identity():
    s = construction_stability(units(10, lambda i: [{("s", "p", "o", "valid_from=2026")}, {("s", "p", "o", "valid_from=2027")},
                                                    {("s", "p", "o", "valid_from=2026")}]))
    assert s.mean_pairwise_jaccard == Fraction(1, 3)


def test_a_unit_for_which_no_replication_exported_anything_leaves_the_denominator():
    data = units(11, lambda i: [{1}, {1}, {1}])
    data["u999"] = [set(), set(), set()]
    s = construction_stability(data)
    assert s.n_units == 11 and s.excluded_units == ("u999",) and s.mean_pairwise_jaccard == 1


def test_not_estimable_with_fewer_than_three_replications_or_without_units():
    assert construction_stability(units(12, lambda i: [{1}, {1}])).status == "n.e."
    assert construction_stability({}).status == "n.e."
    assert construction_stability(units(12, lambda i: [set(), set(), set()])).status == "n.e."


def test_below_ten_units_no_rate():
    s = construction_stability(units(9, lambda i: [{1}, {1}, {1}]))
    assert s.status == "k of n" and s.mean_pairwise_jaccard is None


def test_two_empty_replications_beside_a_non_empty_one_count_as_agreeing():
    """Choice (README): J(empty, empty) = 1 inside a unit that stays in the denominator."""
    assert jaccard(frozenset(), frozenset()) == 1
    s = construction_stability(units(10, lambda i: [set(), set(), {1}]))
    assert s.mean_pairwise_jaccard == Fraction(1, 3) and s.stable_core_share == 0
