"""Card P.8, row (a): run-to-run stability of construction.

Per source unit, the set of canonicalised assertions each replication exported for it. Function:
mean pairwise Jaccard over the three replication pairs, averaged over units; and the stable-core
share, the assertions present in all three replications over the three-way union.
Denominator: the units for which at least one replication exported something.
Not estimable for a realisation with fewer than three replications.
"""
from __future__ import annotations

from dataclasses import dataclass
from fractions import Fraction
from itertools import combinations
from typing import Hashable, Iterable, Mapping, Optional, Sequence

from .values import MIN_DENOMINATOR

REPLICATIONS = 3


def jaccard(a: frozenset, b: frozenset) -> Fraction:
    """|a and b| / |a or b|. Two empty sets count as identical (1); see README, choices."""
    union = a | b
    return Fraction(1) if not union else Fraction(len(a & b), len(union))


@dataclass(frozen=True)
class Stability:
    status: str                              # "rate", "k of n" or "n.e."
    mean_pairwise_jaccard: Optional[Fraction]
    stable_core_share: Optional[Fraction]    # mean over units of |in all three| / |three-way union|
    stable_core_pooled: Optional[Fraction]   # sum over units of |in all three| / sum of |union|
    n_units: int                             # units in the denominator
    excluded_units: tuple[str, ...]          # no replication exported anything: n.e. register
    note: str = ""


def construction_stability(exports: Mapping[str, Sequence[Iterable[Hashable]]], *,
                           min_n: int = MIN_DENOMINATOR) -> Stability:
    """`exports[unit_id]` holds one collection of canonical assertions per replication, in any order.
    Qualifiers are part of an assertion's identity, so the elements must already contain them."""
    lengths = {len(v) for v in exports.values()}
    if not exports or lengths != {REPLICATIONS}:
        return Stability("n.e.", None, None, None, 0, (),
                         "row (a) needs exactly three replications for every unit" if exports else "no units")
    per_unit_j, per_unit_core, inter_total, union_total, excluded = [], [], 0, 0, []
    for unit, reps in sorted(exports.items()):
        sets = [frozenset(r) for r in reps]
        union = frozenset().union(*sets)
        if not union:
            excluded.append(unit)
            continue
        per_unit_j.append(sum((jaccard(a, b) for a, b in combinations(sets, 2)), Fraction(0)) / 3)
        core = sets[0] & sets[1] & sets[2]
        per_unit_core.append(Fraction(len(core), len(union)))
        inter_total += len(core)
        union_total += len(union)
    n = len(per_unit_j)
    if n == 0:
        return Stability("n.e.", None, None, None, 0, tuple(excluded), "no replication exported anything for any unit")
    if n < min_n:
        return Stability("k of n", None, None, None, n, tuple(excluded), f"{n} units, below {min_n}")
    return Stability("rate", sum(per_unit_j, Fraction(0)) / n, sum(per_unit_core, Fraction(0)) / n,
                     Fraction(inter_total, union_total), n, tuple(excluded))
