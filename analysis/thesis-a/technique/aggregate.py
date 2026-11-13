"""Order of aggregation (package section 6.4): item, cluster, pass, replication.

Step 3: a replication's value of a question-based rate is the mean over its completed query
passes (up to three); the first-pass value is printed beside it and the completed share is reported.
Step 4: the axis value of a realisation is the arithmetic mean of the replication rates, with the
minimum and the maximum beside it. Counts are never pooled across replications.
Step 6: nothing is pooled across arms or condition sets; `axis_value` refuses mixed cells.
"""
from __future__ import annotations

from dataclasses import dataclass, replace
from fractions import Fraction
from typing import Callable, Optional, Sequence

from .items import ItemTable, Predicate
from .values import Aggregate, Measure, mean_of
from . import views

PLANNED_PASSES = 3
PLANNED_REPLICATIONS = 3


@dataclass(frozen=True)
class PassReport:
    """A replication's value of one question-based rate with what was completed."""
    value: Aggregate                      # mean over the completed passes; .first is the pass-1 value
    per_pass: dict[int, Measure]          # every pass that has rows, complete or not
    completed_passes: tuple[int, ...]
    partial_passes: tuple[int, ...]       # passes with rows for only a part of the reference pass
    completed_share: Optional[Fraction]   # eligible-CQ rows present over planned passes x eligible CQs


def over_passes(table: ItemTable, rate: Callable[..., Measure] = views.q_own, *, where: Optional[Predicate] = None,
                planned: int = PLANNED_PASSES, **kwargs) -> PassReport:
    """Section 6.4 step 3. A pass is complete when it holds a row for every eligible CQ of pass 1
    (README, choices); partial passes are reported per pass and stay out of the mean."""
    passes = table.passes()
    eligible = {p: {q.item_id for q in views.scored_cqs(table, p, where) if q.eligible} for p in passes}
    per_pass = {p: rate(table, p, where=where, **kwargs) for p in passes}
    ref = eligible.get(1, set())
    complete = tuple(p for p in passes if 1 in eligible and eligible[p] >= ref)
    partial = tuple(p for p in passes if p not in complete)
    card = next(iter(per_pass.values())).card if per_pass else "question-based rate"
    agg = mean_of(card, [per_pass[p] for p in complete], n_planned=planned, labels=[f"pass {p}" for p in complete])
    first = per_pass[1].exact if 1 in per_pass and per_pass[1].estimable else None
    agg = replace(agg, first=first)
    share = (Fraction(sum(len(eligible[p] & ref) for p in passes), planned * len(ref)) if ref else None)
    return PassReport(agg, per_pass, complete, partial, share)


def _cell_key(table: ItemTable) -> tuple:
    return tuple(table.cell.get(k) for k in ("treatment", "arm", "condition_set"))


def axis_value(card: str, replication_values: Sequence, *, planned: int = PLANNED_REPLICATIONS,
               tables: Optional[Sequence[ItemTable]] = None) -> Aggregate:
    """Section 6.4 step 4: mean of the replication rates with minimum and maximum.

    A replication whose rate is "n.e." or "k of n" is excluded, never imputed, and named in
    `.excluded`. Pass `tables` to have the function refuse cells of different realisations or
    condition sets (step 6).
    """
    if tables is not None and len({_cell_key(t) for t in tables}) > 1:
        raise ValueError("nothing is pooled across treatments, arms or condition sets (section 6.4, step 6)")
    comps = [v.value if isinstance(v, PassReport) else v for v in replication_values]
    return mean_of(card, comps, n_planned=planned, labels=[f"replication {i + 1}" for i in range(len(comps))])


def coverage_of_realisation(tables: Sequence[ItemTable], **kwargs) -> Measure:
    """C.1 is a property of the realisation: identical in every replication. Raises if it is not."""
    values = [views.coverage(t, **kwargs) for t in tables]
    if len({(m.status, m.k, m.n) for m in values}) != 1:
        raise ValueError("coverage differs between replications of one realisation: "
                         + ", ".join(str(m) for m in values))
    return values[0]


def profile(tables: Sequence[ItemTable], *, planned: int = PLANNED_REPLICATIONS) -> dict:
    """The three profile axes of one realisation in one arm under one condition set."""
    if len({_cell_key(t) for t in tables}) > 1:
        raise ValueError("nothing is pooled across treatments, arms or condition sets (section 6.4, step 6)")
    reports = [over_passes(t) for t in tables]
    return {
        "C": coverage_of_realisation(tables),
        "P_func": axis_value("P.1 functional populatability", [views.functional_populatability(t) for t in tables], planned=planned),
        "Q_own": axis_value("Q.1 own-set queryability", reports, planned=planned),
        "Q_own_first_pass": axis_value("Q.1 own-set queryability, first pass", [r.value.first for r in reports], planned=planned),
        "pass_reports": reports,
    }
