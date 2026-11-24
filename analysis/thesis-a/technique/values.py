"""Values a card can take: a rate, "k of n", or "n.e." (not estimable).

Package section 5 (rules for every card) and section 6.5:
  - empty denominator gives "n.e." with n = 0, never 0 and never 1;
  - below a denominator of 10 (items for assertion-based cards, clusters for question-based
    cards) the count is printed as "k of n" with no rate and no interval, is excluded from
    aggregation and is listed in the n.e. register;
  - an n.e. component is excluded, never imputed;
  - interval of a single-cell rate: Wilson score, 95 %.

All rates are kept as exact fractions, so the identities of section 6.3 can be checked exactly.
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from fractions import Fraction
from typing import Iterable, Optional, Sequence

MIN_DENOMINATOR = 10          # section 5 and 6.5 (BU-04, SC-18)
Z_95 = 1.959963984540054      # two-sided 95 %

RATE, K_OF_N, NOT_ESTIMABLE = "rate", "k of n", "n.e."


def wilson(k: int, n: int, z: float = Z_95) -> tuple[float, float]:
    """Wilson score interval for k successes in n trials."""
    if n <= 0:
        raise ValueError("Wilson interval needs n > 0")
    if not 0 <= k <= n:
        raise ValueError("Wilson interval needs 0 <= k <= n")
    p = k / n
    denom = 1 + z * z / n
    centre = (p + z * z / (2 * n)) / denom
    half = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / denom
    # at k = 0 the lower bound and at k = n the upper bound are exactly 0 and 1; avoid rounding noise
    return (0.0 if k == 0 else max(0.0, centre - half), 1.0 if k == n else min(1.0, centre + half))


@dataclass(frozen=True)
class Measure:
    """One printed figure of one card for one cell (or one subgroup of a cell).

    status   "rate", "k of n" or "n.e."
    k, n     numerator and denominator in observation units (assertions, CQs, source units, ...)
    exact    the rate as a fraction; None unless status is "rate"
    n_units  the denominator in the unit of the small-denominator rule: items for item-based
             cards, clusters for question-based cards
    micro    for question-based cards: the CQ micro-average k / n (secondary row); None otherwise
    interval Wilson 95 % interval; only for item-based rates (see README, choices)
    """
    card: str
    status: str
    k: Optional[int]
    n: int
    exact: Optional[Fraction]
    unit: str = "items"
    n_units: int = 0
    rule_unit: str = "items"
    micro: Optional[Fraction] = None
    interval: Optional[tuple[float, float]] = None
    cluster_sizes: tuple[int, ...] = ()
    note: str = ""

    @property
    def estimable(self) -> bool:
        return self.status == RATE

    @property
    def value(self) -> Optional[float]:
        return float(self.exact) if self.exact is not None else None

    def __str__(self) -> str:
        if self.status == NOT_ESTIMABLE:
            return f"n.e. (n = {self.n})"
        if self.status == K_OF_N:
            extra = f", {self.n_units} {self.rule_unit}" if self.rule_unit != self.unit else ""
            return f"{self.k} of {self.n} {self.unit}{extra}"
        return f"{self.value:.3f} ({self.k} of {self.n} {self.unit})"


def item_rate(card: str, k: int, n: int, *, unit: str = "assertions", min_n: int = MIN_DENOMINATOR,
              note: str = "") -> Measure:
    """A share over items: n.e. when n = 0, "k of n" below min_n, else a rate with Wilson interval."""
    if k < 0 or n < 0 or k > n:
        raise ValueError(f"{card}: need 0 <= k <= n, got k={k}, n={n}")
    if n == 0:
        return Measure(card, NOT_ESTIMABLE, None, 0, None, unit, 0, unit, note=note)
    if n < min_n:
        return Measure(card, K_OF_N, k, n, None, unit, n, unit, note=note)
    return Measure(card, RATE, k, n, Fraction(k, n), unit, n, unit, interval=wilson(k, n), note=note)


def not_estimable(card: str, *, unit: str = "items", note: str = "") -> Measure:
    return Measure(card, NOT_ESTIMABLE, None, 0, None, unit, 0, unit, note=note)


def weighted_rate(card: str, num: Fraction, den: Fraction, n_items: int, *, unit: str = "assertions",
                  min_n: int = MIN_DENOMINATOR, note: str = "") -> Measure:
    """A weighted share (card C.3). n.e. when the weight sum is 0. No Wilson interval (not a count)."""
    if den == 0:
        return Measure(card, NOT_ESTIMABLE, None, n_items, None, unit, n_items, unit, note=note or "weight sum is 0")
    if n_items < min_n:
        return Measure(card, K_OF_N, None, n_items, None, unit, n_items, unit, note=note)
    return Measure(card, RATE, None, n_items, Fraction(num) / Fraction(den), unit, n_items, unit, note=note)


@dataclass(frozen=True)
class Aggregate:
    """Mean of component rates with minimum and maximum (section 6.4, steps 3 and 4).

    Components that are "n.e." or "k of n" are excluded, never imputed; they are listed in
    `excluded`. With no estimable component the aggregate is itself n.e. (mean is None).
    """
    card: str
    mean: Optional[Fraction]
    minimum: Optional[Fraction]
    maximum: Optional[Fraction]
    n_included: int
    n_planned: int
    excluded: tuple[str, ...] = ()
    first: Optional[Fraction] = None      # first-pass value, printed beside a pass mean
    components: tuple[Optional[Fraction], ...] = field(default_factory=tuple)

    @property
    def estimable(self) -> bool:
        return self.mean is not None

    @property
    def value(self) -> Optional[float]:
        return float(self.mean) if self.mean is not None else None

    @property
    def completed_share(self) -> Fraction:
        return Fraction(len(self.components), self.n_planned) if self.n_planned else Fraction(0)

    @property
    def complete(self) -> bool:
        return self.n_included == self.n_planned

    def sample_sd(self) -> Optional[float]:
        """Standard deviation over the included components (n - 1 in the denominator)."""
        xs = [float(x) for x in self.components if x is not None]
        if len(xs) < 2:
            return None
        m = sum(xs) / len(xs)
        return math.sqrt(sum((x - m) ** 2 for x in xs) / (len(xs) - 1))

    def __str__(self) -> str:
        if self.mean is None:
            return f"n.e. (0 of {self.n_planned} components estimable)"
        return (f"{float(self.mean):.3f} [{float(self.minimum):.3f}, {float(self.maximum):.3f}] "
                f"({self.n_included} of {self.n_planned})")


def _as_fraction(x) -> Optional[Fraction]:
    if x is None:
        return None
    if isinstance(x, Measure):
        return x.exact if x.estimable else None
    if isinstance(x, Aggregate):
        return x.mean
    return Fraction(x)


def mean_of(card: str, components: Sequence, *, n_planned: Optional[int] = None,
            labels: Optional[Sequence[str]] = None) -> Aggregate:
    """Arithmetic mean of component rates; n.e. and "k of n" components are excluded.

    `components` are Measures, Aggregates, Fractions or None, in order (pass 1 first, or
    replication 1 first). Counts are never pooled: only the rates enter.
    """
    fr = tuple(_as_fraction(c) for c in components)
    labels = list(labels) if labels is not None else [str(i + 1) for i in range(len(fr))]
    ok = [x for x in fr if x is not None]
    excluded = tuple(lab for lab, x in zip(labels, fr) if x is None)
    planned = n_planned if n_planned is not None else len(fr)
    if not ok:
        return Aggregate(card, None, None, None, 0, planned, excluded, None, fr)
    return Aggregate(card, sum(ok, Fraction(0)) / len(ok), min(ok), max(ok), len(ok), planned, excluded,
                     fr[0] if fr else None, fr)


def ne_register(measures: Iterable) -> list[str]:
    """The n.e. register: every figure that is "n.e." or "k of n", as printed."""
    out = []
    for m in measures:
        if isinstance(m, Measure) and not m.estimable:
            out.append(f"{m.card}: {m}" + (f" [{m.note}]" if m.note else ""))
        elif isinstance(m, Aggregate) and not m.estimable:
            out.append(f"{m.card}: {m}")
    return out
