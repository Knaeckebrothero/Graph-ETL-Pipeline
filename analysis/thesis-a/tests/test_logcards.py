"""Cards from the run log: P.2, P.9, P.10 (records of shared-minimum-log/1)."""
import json
import pathlib
from fractions import Fraction

import pytest

from conftest import make_run
from technique import NOT_ESTIMABLE
from technique.logcards import LogError, PriceTable, VoidRun, population_success, precommit_friction, resource_efficiency

PRICES = PriceTable({"model-x": {"input": "0.30", "cached_input": "0.006", "output": "1.20"}}, to_euro="0.9", dated="test table")


# ---------------------------------------------------------------- P.2

def test_p2_is_1_when_no_unit_carries_the_flag_and_0_when_all_do():
    assert population_success(make_run(12))["overall"].exact == 1
    assert population_success(make_run(12, fatal=range(12)))["overall"].exact == 0


def test_p2_per_stratum_and_abandoned_by_reason():
    r = population_success(make_run(40, fatal=(0, 4, 8)))            # three units of the first stratum
    assert r["overall"].exact == Fraction(37, 40)
    assert r["per_stratum"]["numeric tabular"].exact == Fraction(7, 10) and r["per_stratum"]["free text"].exact == 1
    assert r["abandoned_by_reason"] == {"budget_exhausted": 3}


def test_p2_is_independent_of_what_was_written_for_the_units_that_succeeded():
    a = population_success(make_run(12, fatal=(1,)))
    b = population_success(make_run(12, fatal=(1,), reject_first=(2, 3, 4), engine_error=(5,)))
    assert a["overall"] == b["overall"]


def test_p2_small_strata_are_k_of_n():
    r = population_success(make_run(12))
    assert all(m.status == "k of n" for m in r["per_stratum"].values())


def test_a_void_run_appears_on_no_card():
    for f in (population_success, precommit_friction):
        with pytest.raises(VoidRun):
            f(make_run(12, status="void"))
    with pytest.raises(VoidRun):
        resource_efficiency(make_run(12, status="void"), PRICES, cached_input_booking="full_input_price", n_success=5)


def test_a_unit_without_an_end_record_is_not_silently_scored():
    recs = [r for r in make_run(12) if not (r["record_type"] == "source_end" and r["unit_id"] == "u003")]
    with pytest.raises(LogError, match="without a source_end"):
        population_success(recs)
    with pytest.raises(LogError, match="exactly one run"):
        population_success(make_run(3) + make_run(3, run_id="PG-S4-A0-r2"))


# ---------------------------------------------------------------- P.9

def test_p9_three_rows():
    r = precommit_friction(make_run(20, reject_first=(0, 1, 2, 3), fatal=(19,)))
    # 19 units write; four of them need a second attempt: 23 attempts, 4 rejected
    assert (r["rejected_share"].k, r["rejected_share"].n) == (4, 23)
    assert r["retries_per_accepted_write"] == Fraction(4, 19) and r["accepted_writes"] == 19
    assert r["abandoned_units"].exact == Fraction(1, 20)


def test_p9_zero_rejections_by_construction_when_the_validator_only_logs():
    r = precommit_friction(make_run(20, reject_first=(0, 1, 2, 3), validator_mode="log_only"))
    assert r["rejected_share"].exact == 0 and r["logged_only_violations"] == 4


def test_p9_a_unit_accepted_at_the_first_attempt_contributes_zero_retries():
    r = precommit_friction(make_run(12, reject_first=(5,)))
    assert r["per_unit"]["u000"]["retries_per_accepted_write"] == 0 and r["per_unit"]["u000"]["rejected"] == 0
    assert r["per_unit"]["u005"]["retries_per_accepted_write"] == 1 and r["per_unit"]["u005"]["rejected"] == 1
    assert precommit_friction(make_run(12))["retries_per_accepted_write"] == 0


def test_p9_engine_native_errors_are_never_counted():
    r = precommit_friction(make_run(12, engine_error=(0, 1, 2)))
    assert r["rejected_share"].exact == 0 and r["rejected_share"].n == 15


# ---------------------------------------------------------------- P.10

def cost(recs, table=PRICES, booking="full_input_price", n_success=6, **kw):
    return resource_efficiency(recs, table, cached_input_booking=booking, n_success=n_success, **kw)


def test_p10_doubling_every_price_doubles_euro_and_leaves_seconds_and_tokens_unchanged():
    recs = make_run(12)
    double = PriceTable({"model-x": {"input": "0.60", "cached_input": "0.012", "output": "2.40"}}, to_euro="0.9", dated="doubled")
    a, b = cost(recs), cost(recs, double)
    assert b["totals"]["euro"] == 2 * a["totals"]["euro"]
    assert b["totals"]["tokens"] == a["totals"]["tokens"] and b["totals"]["wall_clock_s"] == a["totals"]["wall_clock_s"]
    assert b["per_source_unit"]["euro"] == 2 * a["per_source_unit"]["euro"]


def test_p10_zero_successes_give_ne_not_0_and_not_infinity():
    r = cost(make_run(12), n_success=0)
    assert r["per_confirmed_correct_assertion"] is None and r["per_source_unit"] is not None


def test_p10_tokens_are_summed_once_parts_are_not_added_on_top():
    r = cost(make_run(12))                                  # 12 calls of 1000 in (400 cached), 300 out (200 reasoning)
    t = r["totals"]["tokens"]
    assert t == {"input": 12000, "cached_input": 4800, "output": 3600, "reasoning": 2400}
    # full booking: 12000 x 0.30 + 3600 x 1.20 per million, times 0.9
    assert r["totals"]["euro"] == (Fraction(12000) * Fraction("0.30") + Fraction(3600) * Fraction("1.20")) / 1_000_000 * Fraction("0.9")
    assert r["per_source_unit"]["tokens"]["input"] == 1000 and r["per_confirmed_correct_assertion"]["tokens"]["output"] == 600
    assert r["totals"]["wall_clock_s"] == 24 and r["per_source_unit"]["wall_clock_s"] == 2


def test_p10_the_booking_rule_has_no_default_and_changes_only_euro():
    recs = make_run(12)
    with pytest.raises(TypeError):
        resource_efficiency(recs, PRICES, n_success=6)
    with pytest.raises(ValueError):
        cost(recs, booking="cheapest")
    full, cache = cost(recs), cost(recs, booking="cache_price")
    assert cache["totals"]["tokens"] == full["totals"]["tokens"] and cache["totals"]["euro"] < full["totals"]["euro"]
    expected = (Fraction(7200) * Fraction("0.30") + Fraction(4800) * Fraction("0.006") + Fraction(3600) * Fraction("1.20")) / 1_000_000 * Fraction("0.9")
    assert cache["totals"]["euro"] == expected


def test_p10_counts_construction_calls_only_and_needs_a_price_row():
    with_queries = make_run(12, extra_stage_calls=5)
    assert cost(with_queries)["totals"] == cost(make_run(12))["totals"]
    assert cost(with_queries, stage="querying")["calls"] == 5
    other = PriceTable({"model-y": {"input": 1, "cached_input": 1, "output": 1}}, to_euro=1, dated="wrong table")
    with pytest.raises(LogError, match="no price row"):
        cost(make_run(12), other)


def test_p10_a_call_without_token_counts_is_reported_not_counted_as_zero():
    recs = make_run(12)
    call = next(r for r in recs if r["record_type"] == "model_call")
    call["tokens"] = None
    r = cost(recs)
    assert r["calls_without_tokens"] == 1 and r["totals"]["tokens"]["input"] == 11000


# ---------------------------------------------------------------- the example file of the log schema

def test_example_log_of_the_schema_can_be_read():
    path = pathlib.Path(__file__).resolve().parents[3] / "experiments" / "log-schema" / "examples" / "valid.jsonl"
    if not path.exists():
        pytest.skip("experiments/log-schema/examples/valid.jsonl is not in this working tree")
    recs = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    recs = [r for r in recs if r["record_type"] != "provider_snapshot"]          # kept in its own file, no run id
    p2 = population_success(recs)
    assert (p2["overall"].k, p2["overall"].n) == (1, 1) and p2["overall"].status == "k of n"
    p9 = precommit_friction(recs)
    assert (p9["rejected_share"].k, p9["rejected_share"].n) == (1, 2) and p9["retries_per_accepted_write"] == 1
    table = PriceTable({"deepseek-flash": {"input": "0.30", "cached_input": "0.006", "output": "1.20"}}, to_euro=1, dated="example")
    p10 = resource_efficiency(recs, table, cached_input_booking="full_input_price", n_success=0)
    assert p10["totals"]["tokens"] == {"input": 12500, "cached_input": 5100, "output": 2700, "reasoning": 1900}
    assert p10["calls"] == 2 and p10["per_confirmed_correct_assertion"] is None
