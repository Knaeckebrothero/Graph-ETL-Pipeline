"""The identities and orderings of section 6.3 on 300 seeded random cells (seed in the test id)."""
import random
from fractions import Fraction

import pytest

from conftest import CORE, SUPPORTING, build_cell
from technique import FUNNEL_LEVELS, State, views

OUTCOMES = [((State.F4, None), (State.F4, None)), ((State.F5, "parse"), (State.F5, "parse")),
            ((State.F5, "parse"), (State.F5, "runtime")), ((State.F5, "runtime"), (State.F6, None)),
            ((State.F4, None), (State.SUCCESS, None)), ((State.F6, "accidental"), (State.F6, "accidental")),
            ((State.F6, "memorised_constant"), (State.SUCCESS, None)), ((State.SUCCESS, None), (State.SUCCESS, None))]


def random_cell(seed):
    rng = random.Random(seed)
    f1 = frozenset(a for a in CORE + SUPPORTING if rng.random() < rng.choice([0.0, 0.1, 0.4]))
    p_success = rng.choice([0.0, 0.3, 0.7, 0.95, 1.0])
    states = {a: (State.SUCCESS if rng.random() < p_success else rng.choice([State.F2, State.F3]))
              for a in CORE + SUPPORTING if a not in f1}
    picks = [rng.choice(OUTCOMES) for _ in range(64)]
    return build_cell("full_gold", lambda i: picks[i], f1=f1, states=states)


@pytest.mark.parametrize("seed", range(300))
def test_identities_and_orderings(seed):
    t = random_cell(seed)
    c, cr, p = views.coverage(t), views.realised_coverage(t), views.functional_populatability(t)
    assert cr.exact <= c.exact
    ident = views.realised_identity(t)
    assert ident.holds is not False
    if ident.holds:
        assert ident.left == cr.exact and (not p.estimable or ident.right == c.exact * p.exact)
    d = views.loss_decomposition(t)
    assert d["F1"] + d["F2"] + d["F3"] + d["success"] == 1 and d["F1"] == 1 - c.exact
    for pass_id in (1, 2, 3):
        e2e = views.end_to_end_identity(t, pass_id)
        assert e2e.holds is not False
        f = views.funnel(t, pass_id)
        for attempt in (f.first, f.final):
            values = [attempt[level].exact for level in FUNNEL_LEVELS if attempt[level].estimable]
            assert values == sorted(values, reverse=True)
        for level in FUNNEL_LEVELS:
            if f.repair_gain[level] is not None:
                assert f.repair_gain[level] >= 0          # this generator never revises a working query
        q, end, elig = views.q_own(t, pass_id), views.end_to_end(t, pass_id), views.eligibility_rate(t, pass_id)
        if q.estimable:
            assert q.exact == f.final["semantically_correct"].exact and 0 <= q.exact <= 1
        if end.estimable and elig.estimable:
            assert end.exact <= elig.exact and end.micro <= elig.micro
            assert end.micro == Fraction(end.k, end.n)
