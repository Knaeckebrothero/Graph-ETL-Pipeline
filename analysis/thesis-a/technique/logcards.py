"""Cards computed from the run log (records of shared-minimum-log/1): P.2, P.9 and P.10.

A void run appears on no card except X.4, so every function here refuses a void run.
Tokens are read from `model_call` records only: `input` and `output` are the totals,
`cached_input` and `reasoning` are parts of them and are never added on top.
"""
from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
from fractions import Fraction
from typing import Callable, Iterable, Mapping, Optional

from .values import MIN_DENOMINATOR, Measure, item_rate, not_estimable


class LogError(ValueError):
    """The log cannot be scored as it is."""


class VoidRun(LogError):
    """The run is void: it is kept in the log and scored on no card except X.4."""


@dataclass
class RunLog:
    run_id: str
    by_type: dict[str, list[dict]]

    def of(self, record_type: str) -> list[dict]:
        return self.by_type.get(record_type, [])


def read_run(records: Iterable[Mapping]) -> RunLog:
    by_type: dict[str, list[dict]] = defaultdict(list)
    run_ids = set()
    for r in records:
        by_type[r.get("record_type")].append(dict(r))
        if "run_id" in r:
            run_ids.add(r["run_id"])
    if len(run_ids) != 1:
        raise LogError(f"expected the records of exactly one run, found run ids {sorted(run_ids)}")
    ends = by_type.get("run_end", [])
    if any(e.get("status") == "void" for e in ends):
        raise VoidRun(f"run {next(iter(run_ids))} is void")
    return RunLog(next(iter(run_ids)), dict(by_type))


def _units(log: RunLog) -> tuple[dict[str, dict], dict[str, dict]]:
    """Unit id -> its (last) source_start and its (last) source_end record."""
    starts = {r["unit_id"]: r for r in log.of("source_start")}
    ends = {r["unit_id"]: r for r in log.of("source_end")}
    open_units = sorted(set(starts) - set(ends))
    if open_units:
        raise LogError(f"units without a source_end record cannot be classified: {open_units[:5]}")
    orphans = sorted(set(ends) - set(starts))
    if orphans:
        raise LogError(f"source_end without source_start: {orphans[:5]}")
    return starts, ends


def population_success(records: Iterable[Mapping], *, min_n: int = MIN_DENOMINATOR) -> dict:
    """P.2: share of source units without the fatal-failure flag, overall and per modality stratum;
    abandoned units by reason code."""
    log = read_run(records)
    starts, ends = _units(log)
    def rate(units, label):
        return item_rate(label, sum(1 for u in units if not ends[u]["fatal"]), len(units), unit="source units", min_n=min_n)
    strata: dict[str, list[str]] = defaultdict(list)
    for u, s in starts.items():
        strata[s["stratum"]].append(u)
    return {
        "overall": rate(list(starts), "P.2 document-level population success"),
        "per_stratum": {s: rate(us, f"P.2 document-level population success ({s})") for s, us in sorted(strata.items())},
        "abandoned_by_reason": dict(Counter(e["abandon_reason"] for e in ends.values() if e["fatal"])),
    }


def precommit_friction(records: Iterable[Mapping], *, min_n: int = MIN_DENOMINATOR) -> dict:
    """P.9, three rows: rejected write attempts over all write attempts; retries until acceptance
    per accepted write; abandoned units over all units. Only rejections by the shared validator in
    reject mode count; engine-native errors never do. What a log-only validator would have
    rejected is returned separately and is not a rejection."""
    log = read_run(records)
    starts, ends = _units(log)
    attempts = log.of("insert_attempt")
    is_reject = lambda a: a["validator_mode"] == "reject" and a["validator_result"] == "reject"
    accepted = [a for a in attempts if a["accepted"]]
    per_unit: dict[str, dict] = {}
    for u in starts:
        ua = [a for a in attempts if a["unit_id"] == u]
        acc = [a for a in ua if a["accepted"]]
        per_unit[u] = {"attempts": len(ua), "rejected": sum(1 for a in ua if is_reject(a)),
                       "retries_per_accepted_write": (Fraction(sum(a["retry_index"] for a in acc), len(acc)) if acc else None)}
    return {
        "rejected_share": item_rate("P.9 rejected write attempts", sum(1 for a in attempts if is_reject(a)), len(attempts),
                                    unit="write attempts", min_n=min_n),
        "retries_per_accepted_write": (Fraction(sum(a["retry_index"] for a in accepted), len(accepted)) if accepted else None),
        "accepted_writes": len(accepted),
        "abandoned_units": item_rate("P.9 abandoned units", sum(1 for e in ends.values() if e["final_status"] == "abandoned"),
                                     len(ends), unit="source units", min_n=min_n),
        "logged_only_violations": sum(1 for a in attempts if a["validator_mode"] == "log_only" and a["validator_result"] == "reject"),
        "per_unit": per_unit,
    }


@dataclass(frozen=True)
class PriceTable:
    """The price table frozen with the sample. Prices per one million tokens, in `currency`;
    `to_euro` converts one unit of `currency` into euro at the rate of the table's date.
    There is no default table: the caller passes the frozen one."""
    prices: Mapping[str, Mapping[str, object]]     # price key -> {"input", "cached_input", "output"}
    to_euro: object
    dated: str
    currency: str = "USD"

    def price(self, key: str, category: str) -> Fraction:
        if key not in self.prices:
            raise LogError(f"no price row for model {key!r} in the price table of {self.dated}")
        return Fraction(str(self.prices[key][category]))


BOOKINGS = ("full_input_price", "cache_price")


def resource_efficiency(records: Iterable[Mapping], price_table: PriceTable, *, cached_input_booking: str,
                        n_success: Optional[int], stage: str = "construction",
                        price_key: Callable[[Mapping], str] = lambda call: call["returned_model"]) -> dict:
    """P.10: wall-clock seconds, tokens by category and euro, per source unit and per
    confirmed-correct assertion. Raw units only.

    cached_input_booking  "full_input_price": every input token at the input price;
                          "cache_price": the cached part at the cached-input price.
                          No default: the booking rule is declared with the price table.
    n_success             assertions in state success of the cell (from the item table); 0 gives
                          "n.e." for the per-assertion rows, never 0 and never infinity.
    """
    if cached_input_booking not in BOOKINGS:
        raise ValueError(f"cached_input_booking must be one of {BOOKINGS}")
    log = read_run(records)
    starts, _ = _units(log)
    calls = [c for c in log.of("model_call") if c["stage"] == stage]
    tokens = {"input": 0, "cached_input": 0, "output": 0, "reasoning": 0}
    not_reported = {"cached_input": 0, "reasoning": 0}
    without_tokens, seconds, cost = 0, Fraction(0), Fraction(0)
    for c in calls:
        seconds += Fraction(c["latency_ms"], 1000)
        t = c.get("tokens")
        if t is None:
            without_tokens += 1
            continue
        tokens["input"] += t["input"]
        tokens["output"] += t["output"]
        for part in ("cached_input", "reasoning"):
            if t.get(part) is None:
                not_reported[part] += 1
            else:
                tokens[part] += t[part]
        key = price_key(c)
        cached = t.get("cached_input") or 0
        if cached_input_booking == "cache_price":
            cost += (t["input"] - cached) * price_table.price(key, "input") + cached * price_table.price(key, "cached_input")
        else:
            cost += t["input"] * price_table.price(key, "input")
        cost += t["output"] * price_table.price(key, "output")
    euro = cost / 1_000_000 * Fraction(str(price_table.to_euro))
    totals = {"wall_clock_s": seconds, "tokens": dict(tokens), "euro": euro}
    def per(n):
        if not n:
            return None                                # n.e.
        return {"wall_clock_s": seconds / n, "tokens": {k: Fraction(v, n) for k, v in tokens.items()}, "euro": euro / n}
    return {
        "totals": totals,
        "per_source_unit": per(len(starts)),
        "per_confirmed_correct_assertion": per(n_success),
        "units_processed": len(starts),
        "n_success": n_success,
        "calls": len(calls),
        "calls_without_tokens": without_tokens,
        "calls_not_reporting": not_reported,
        "booking": cached_input_booking,
        "price_table_dated": price_table.dated,
    }
