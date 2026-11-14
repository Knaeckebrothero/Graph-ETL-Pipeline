"""Cards computed from the item table of a cell (package sections 3.5, 5 and 6).

Every function returns a Measure (see values.py), so "n.e." and "k of n" are explicit values.
Canary items are outside every denominator. Assertions flagged "unresolved", and the CQs that
require them, are outside the denominators of all C, P and Q figures (package 8.1, row 1).
`where` restricts a figure to a subgroup (stratum, jurisdiction, tier, bucket, ...): the same
computation on the subgroup's rows, under the same small-denominator rule (section 6.4, step 7).
"""
from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from fractions import Fraction
from typing import Mapping, Optional, Sequence

from .items import FUNNEL_LEVELS, Item, ItemTable, ItemType, Predicate, State, Tier, UPSTREAM, funnel_level
from .values import K_OF_N, MIN_DENOMINATOR, NOT_ESTIMABLE, RATE, Measure, item_rate, not_estimable, weighted_rate


# ---------------------------------------------------------------- scored sets

def scored_assertions(table: ItemTable, tier: Tier = Tier.CORE, where: Optional[Predicate] = None) -> list[Item]:
    return [a for a in table.of_type(ItemType.ASSERTION, where)
            if a.tier is tier and not a.canary and not a.unresolved]


def scored_cqs(table: ItemTable, pass_id: Optional[int], where: Optional[Predicate] = None) -> list[Item]:
    """CQ rows of one pass (or, with pass_id None, one row per CQ: its lowest pass)."""
    unresolved = {a.item_id for a in table.of_type(ItemType.ASSERTION) if a.unresolved}
    rows = [q for q in table.of_type(ItemType.CQ, where)
            if not q.canary and not (set(q.required) & unresolved)]
    if pass_id is not None:
        return [q for q in rows if q.pass_id == pass_id]
    first: dict[str, Item] = {}
    for q in sorted(rows, key=lambda r: (r.item_id, r.pass_id or 0)):
        first.setdefault(q.item_id, q)
    return list(first.values())


# ---------------------------------------------------------------- assertion-based cards

def coverage(table: ItemTable, *, tier: Tier = Tier.CORE, where: Optional[Predicate] = None,
             min_n: int = MIN_DENOMINATOR) -> Measure:
    """C.1 (core tier) and the rate of C.2 (supporting tier): share of the tier not in F1."""
    a = scored_assertions(table, tier, where)
    card = "C.1 coverage" if tier is Tier.CORE else f"C.2 coverage ({tier.value})"
    return item_rate(card, sum(1 for x in a if x.representable), len(a), min_n=min_n)


def tier_vector(table: ItemTable, *, min_n: int = MIN_DENOMINATOR) -> dict:
    """C.2: supporting coverage beside core coverage, the excluded count with reasons, the tier sizes."""
    anchored = [a for a in table.of_type(ItemType.ASSERTION) if not a.canary]
    sizes = {t.value: sum(1 for a in anchored if a.tier is t) for t in Tier}
    return {
        "core": coverage(table, tier=Tier.CORE, min_n=min_n),
        "supporting": coverage(table, tier=Tier.SUPPORTING, min_n=min_n),
        "sizes": sizes,
        "anchored": len(anchored),
        "excluded_reasons": dict(Counter(a.exclusion_reason or "no reason recorded"
                                         for a in anchored if a.tier is Tier.EXCLUDED)),
        "unresolved": sum(1 for a in anchored if a.unresolved),
    }


def weighted_coverage(table: ItemTable, weights: Mapping[str, object], *, where: Optional[Predicate] = None,
                      min_n: int = MIN_DENOMINATOR) -> Measure:
    """C.3: authority-weighted coverage over core and supporting. `weights` maps authority class
    to weight and has no default: the class weights are declared with version 1.0."""
    a = scored_assertions(table, Tier.CORE, where) + scored_assertions(table, Tier.SUPPORTING, where)
    missing = sorted({x.authority_class for x in a if x.authority_class not in weights}, key=str)
    if missing:
        raise KeyError(f"C.3: no weight for authority class {missing}")
    w = {k: Fraction(str(v)) if not isinstance(v, Fraction) else v for k, v in weights.items()}
    if any(v < 0 for v in w.values()):
        raise ValueError("C.3: weights must not be negative")
    num = sum((w[x.authority_class] for x in a if x.representable), Fraction(0))
    den = sum((w[x.authority_class] for x in a), Fraction(0))
    return weighted_rate("C.3 authority-weighted coverage", num, den, len(a), min_n=min_n)


def functional_populatability(table: ItemTable, *, where: Optional[Predicate] = None,
                              min_n: int = MIN_DENOMINATOR) -> Measure:
    """P.1: share of the representable core assertions in state success."""
    rep = [x for x in scored_assertions(table, Tier.CORE, where) if x.representable]
    return item_rate("P.1 functional populatability", sum(1 for x in rep if x.is_success), len(rep), min_n=min_n,
                     note="" if rep else "no core assertion is representable")


def realised_coverage(table: ItemTable, *, where: Optional[Predicate] = None,
                      min_n: int = MIN_DENOMINATOR) -> Measure:
    """C.4: share of all core assertions in state success."""
    a = scored_assertions(table, Tier.CORE, where)
    return item_rate("C.4 realised coverage", sum(1 for x in a if x.is_success), len(a), min_n=min_n)


def reference_precision(table: ItemTable, *, where: Optional[Predicate] = None,
                        min_n: int = MIN_DENOMINATOR) -> Measure:
    """P.6: success over success plus F3 (core assertions with a candidate at their anchor)."""
    a = [x for x in scored_assertions(table, Tier.CORE, where) if x.state in (State.SUCCESS, State.F3)]
    return item_rate("P.6 reference precision", sum(1 for x in a if x.is_success), len(a), min_n=min_n)


def loss_decomposition(table: ItemTable, *, where: Optional[Predicate] = None) -> Optional[dict[str, Fraction]]:
    """Shares of F1, F2, F3 and success over the core tier (they sum to 1); None when it is empty."""
    a = scored_assertions(table, Tier.CORE, where)
    if not a:
        return None
    c = Counter(x.state for x in a)
    n = len(a)
    out = {s.value: Fraction(c.get(s, 0), n) for s in (State.F1, State.F2, State.F3, State.SUCCESS)}
    out["F2+F3"] = out["F2"] + out["F3"]
    return out


@dataclass(frozen=True)
class Identity:
    """An arithmetic check of section 6.3 on exact counts. holds is None when a factor is n.e."""
    name: str
    left: Optional[Fraction]
    right: Optional[Fraction]
    holds: Optional[bool]
    factors: tuple = ()


def realised_identity(table: ItemTable, *, where: Optional[Predicate] = None) -> Identity:
    """C_r = C x P_func, on the counts |A|, |Rep| and |Succ| (independent of the printing rule)."""
    a = scored_assertions(table, Tier.CORE, where)
    rep = [x for x in a if x.representable]
    succ = [x for x in a if x.is_success]
    if not a or not rep:
        return Identity("C_r = C x P_func", Fraction(len(succ), len(a)) if a else None, None, None,
                        (len(a), len(rep), len(succ)))
    left = Fraction(len(succ), len(a))
    right = Fraction(len(rep), len(a)) * Fraction(len(succ), len(rep))
    return Identity("C_r = C x P_func", left, right, left == right, (len(a), len(rep), len(succ)))


# ---------------------------------------------------------------- C.6 and C.7

def preservation(table: ItemTable, *, where: Optional[Predicate] = None, min_n: int = MIN_DENOMINATOR) -> dict:
    """C.6: the state logic applied to input statements, in three readings on one sample."""
    s = [x for x in table.of_type(ItemType.INPUT_STATEMENT, where) if not x.canary]
    rep = [x for x in s if x.representable]
    succ = [x for x in s if x.is_success]
    holds = None
    if s and rep:
        holds = Fraction(len(succ), len(s)) == Fraction(len(rep), len(s)) * Fraction(len(succ), len(rep))
    return {
        "representational": item_rate("C.6 preservation, representational", len(rep), len(s), unit="statements", min_n=min_n),
        "functional": item_rate("C.6 preservation, functional", len(succ), len(rep), unit="statements", min_n=min_n),
        "realised": item_rate("C.6 preservation, realised", len(succ), len(s), unit="statements", min_n=min_n),
        "identity_holds": holds,
    }


def inventory_completeness(table: ItemTable, inventory_id: str, *, capacity_only: bool = False,
                           min_n: int = MIN_DENOMINATOR) -> dict:
    """C.7: representable share; realised over mentioned; sample reach (context only)."""
    items = [x for x in table.of_type(ItemType.INVENTORY_ITEM) if x.inventory_id == inventory_id and not x.canary]
    mentioned = [x for x in items if x.mentioned]
    realised = [x for x in items if x.is_success]
    out = {"representable_share": item_rate("C.7 representable share", sum(1 for x in items if x.representable),
                                             len(items), unit="inventory items", min_n=min_n)}
    if capacity_only or not mentioned:
        note = "inventory declared capacity-only" if capacity_only else "the sample mentions no item of this inventory"
        out["realised_over_mentioned"] = not_estimable("C.7 realised over mentioned", unit="inventory items", note=note)
        out["sample_reach"] = None if capacity_only else {
            "mentioned_over_all": item_rate("C.7 mentioned over all", 0, len(items), unit="inventory items", min_n=min_n)}
        return out
    out["realised_over_mentioned"] = item_rate("C.7 realised over mentioned", len(realised), len(mentioned),
                                               unit="inventory items", min_n=min_n)
    out["sample_reach"] = {
        "mentioned_over_all": item_rate("C.7 mentioned over all", len(mentioned), len(items), unit="inventory items", min_n=min_n),
        "realised_over_all": item_rate("C.7 realised over all", len(realised), len(items), unit="inventory items", min_n=min_n),
    }
    return out


# ---------------------------------------------------------------- question-based cards

def cluster_macro(card: str, rows: Sequence[Item], hit: Predicate, *, min_n: int = MIN_DENOMINATOR) -> Measure:
    """Cluster macro-average (section 5.3): the mean over clusters of the within-cluster share.

    The small-denominator rule counts clusters. The CQ micro-average is the secondary row.
    """
    clusters: dict[str, list[Item]] = defaultdict(list)
    for q in rows:
        clusters[q.cluster_id].append(q)
    n, k = len(rows), sum(1 for q in rows if hit(q))
    sizes = tuple(sorted(len(v) for v in clusters.values()))
    if not clusters:
        return Measure(card, NOT_ESTIMABLE, None, 0, None, "CQs", 0, "clusters")
    if len(clusters) < min_n:
        return Measure(card, K_OF_N, k, n, None, "CQs", len(clusters), "clusters", cluster_sizes=sizes)
    macro = sum((Fraction(sum(1 for q in v if hit(q)), len(v)) for v in clusters.values()), Fraction(0)) / len(clusters)
    return Measure(card, RATE, k, n, macro, "CQs", len(clusters), "clusters", micro=Fraction(k, n), cluster_sizes=sizes)


def eligibility_rate(table: ItemTable, pass_id: Optional[int] = None, *, where: Optional[Predicate] = None,
                     min_n: int = MIN_DENOMINATOR) -> Measure:
    """Q.6: share of the catalogue that is eligible. Eligibility belongs to the cell, so with
    pass_id None every CQ is counted once; with a pass id the rows of that pass are used."""
    rows = scored_cqs(table, pass_id, where)
    return cluster_macro("Q.6 eligibility rate", rows, lambda q: q.eligible, min_n=min_n)


def q_own(table: ItemTable, pass_id: int, *, where: Optional[Predicate] = None,
          min_n: int = MIN_DENOMINATOR, card: str = "Q.1 own-set queryability") -> Measure:
    """Q.1: share of the eligible CQs that are semantically correct at the final attempt.
    Q.9 is the same function with `where` selecting the held-out bucket."""
    rows = [q for q in scored_cqs(table, pass_id, where) if q.eligible]
    return cluster_macro(card, rows, lambda q: q.is_success, min_n=min_n)


def end_to_end(table: ItemTable, pass_id: int, *, where: Optional[Predicate] = None,
               min_n: int = MIN_DENOMINATOR) -> Measure:
    """Q.5: share of all CQs of the catalogue in state success; ineligible CQs stay in the denominator."""
    return cluster_macro("Q.5 end-to-end answerability", scored_cqs(table, pass_id, where),
                         lambda q: q.is_success, min_n=min_n)


def end_to_end_identity(table: ItemTable, pass_id: int, *, where: Optional[Predicate] = None) -> Identity:
    """end-to-end = eligibility rate x Q_own, at CQ level, on the rows of one pass (section 6.3)."""
    rows = scored_cqs(table, pass_id, where)
    elig = [q for q in rows if q.eligible]
    succ = [q for q in rows if q.is_success]
    if not rows or not elig:
        return Identity("end-to-end = eligibility x Q_own (CQ level)", Fraction(len(succ), len(rows)) if rows else None,
                        None, None, (len(rows), len(elig), len(succ)))
    left = Fraction(len(succ), len(rows))
    right = Fraction(len(elig), len(rows)) * Fraction(len(succ), len(elig))
    return Identity("end-to-end = eligibility x Q_own (CQ level)", left, right, left == right,
                    (len(rows), len(elig), len(succ)))


def product_approximation(table: ItemTable, pass_id: int) -> Optional[dict[str, Fraction]]:
    """C x P_func x Q_own beside the measured end-to-end figure, with the deviation (card Q.5).
    Shown once as an approximation and never used as a result. None when a factor is not a rate."""
    parts = [coverage(table), functional_populatability(table), q_own(table, pass_id), end_to_end(table, pass_id)]
    if not all(m.estimable for m in parts):
        return None
    product = parts[0].exact * parts[1].exact * parts[2].exact
    return {"product": product, "measured": parts[3].exact, "deviation": product - parts[3].exact}


@dataclass(frozen=True)
class Funnel:
    """Q.2: four cumulative rates at the first and at the final attempt, on one denominator."""
    first: dict[str, Measure]
    final: dict[str, Measure]
    repair_gain: dict[str, Optional[Fraction]]
    f6_sub_codes: Optional[dict[str, Fraction]]     # shares of F6 at the final attempt; None when no F6


def funnel(table: ItemTable, pass_id: int, *, where: Optional[Predicate] = None,
           min_n: int = MIN_DENOMINATOR) -> Funnel:
    rows = [q for q in scored_cqs(table, pass_id, where) if q.eligible]
    first, final, gain = {}, {}, {}
    for i, level in enumerate(FUNNEL_LEVELS, start=1):
        first[level] = cluster_macro(f"Q.2 funnel, first attempt, {level}", rows,
                                     lambda q, i=i: funnel_level(q.first_state, q.first_sub_code) >= i, min_n=min_n)
        final[level] = cluster_macro(f"Q.2 funnel, final attempt, {level}", rows,
                                     lambda q, i=i: funnel_level(q.state, q.sub_code) >= i, min_n=min_n)
        gain[level] = (final[level].exact - first[level].exact
                       if first[level].estimable and final[level].estimable else None)
    f6 = [q for q in rows if q.state is State.F6]
    shares = None
    if f6:
        c = Counter(q.sub_code or "answer mismatch, no sub-code" for q in f6)
        shares = {code: Fraction(n, len(f6)) for code, n in sorted(c.items())}
    return Funnel(first, final, gain, shares)


@dataclass(frozen=True)
class Intersection:
    """Q.3: Q on the CQs eligible in every cell of a stated comparison."""
    per_cell: tuple[Measure, ...]
    n_cqs: int
    n_clusters: int
    weak: bool
    weak_reasons: tuple[str, ...]
    smallest_own_clusters: int


def q_intersection(tables: Sequence[ItemTable], pass_id: int, *, where: Optional[Predicate] = None,
                   min_n: int = MIN_DENOMINATOR) -> Intersection:
    if len(tables) < 2:
        raise ValueError("Q.3 needs a comparison of at least two cells")
    own = [{q.item_id: q for q in scored_cqs(t, pass_id, where) if q.eligible} for t in tables]
    common = set(own[0]).intersection(*[set(o) for o in own[1:]])
    per_cell = tuple(cluster_macro("Q.3 intersection queryability", [o[i] for i in sorted(common)],
                                   lambda q: q.is_success, min_n=min_n) for o in own)
    clusters = {own[0][i].cluster_id for i in common}
    smallest = min(len({q.cluster_id for q in o.values()}) for o in own)
    reasons = []
    if len(clusters) < 10:
        reasons.append("fewer than 10 clusters")
    if 2 * len(clusters) < smallest:
        reasons.append("fewer than half the clusters of the smallest own eligible set")
    return Intersection(per_cell, len(common), len(clusters), bool(reasons), tuple(reasons), smallest)
