"""Synthetic fixtures. Every id is made up (a001, k01, q001, u001); nothing here is reference content.

The seven boundary cases of package section 5 are built at item-table level: the state each
reference item would take is written down directly. That stands in for exporter and matcher,
which do not exist yet, so these are not the BU-12 tests "through the real exporter".
"""
import pathlib
import sys

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from technique import Item, ItemTable, ItemType, State, Tier, cq_row  # noqa: E402

STRATA = ("numeric tabular", "tabular-categorical", "catalogue", "free text")
JURISDICTIONS = ("Frankfurt", "campus", "Munich", "Berlin")
CLASSES = ("A", "B", "C", "D")
CORE = [f"a{i:03d}" for i in range(1, 37)]            # 36 core assertions
SUPPORTING = [f"s{i:03d}" for i in range(1, 13)]      # 12 supporting assertions
EXCLUDED = ["x001", "x002"]
# six of 36 core assertions and two of 12 supporting ones have no image under the synthetic mapping
F1_DEFAULT = frozenset({"a007", "a019", "a033", "a034", "a035", "a036", "s003", "s006"})
N_CLUSTERS = 16
BOUNDARY = ("empty", "full_gold", "half_gold", "duplicated_gold", "hallucinated", "wrong_qualifiers", "text_blob")


def cluster_requirements():
    """Cluster k -> (required core assertions, number of CQs). Clusters are defined by one
    required-assertion set, so eligibility is uniform inside a cluster. Every fourth cluster
    requires one assertion, the others two; clusters hold one, two or three CQs."""
    out = {}
    for i in range(N_CLUSTERS):
        required = (CORE[2 * i],) if i % 4 == 0 else (CORE[2 * i], CORE[2 * i + 1])
        out[f"k{i + 1:02d}"] = (required, 1 + i % 3)
    return out


def assertion_state(graph: str, representable_index: int) -> State:
    """State of the n-th representable assertion on a boundary graph."""
    if graph in ("empty", "text_blob"):
        return State.F2
    if graph in ("full_gold", "duplicated_gold"):
        return State.SUCCESS
    if graph == "half_gold":
        return State.SUCCESS if representable_index % 2 == 0 else State.F2
    if graph in ("hallucinated", "wrong_qualifiers"):   # every assertion of the fixture carries a qualifier
        return State.F3
    raise ValueError(graph)


def gold_outcome(index: int):
    return (State.SUCCESS, None), (State.SUCCESS, None)


def mixed_outcome(index: int):
    """(first attempt, final attempt) by CQ index: a spread over all funnel exits."""
    return [((State.F5, "parse"), (State.SUCCESS, None)),      # repaired
            ((State.F4, None), (State.F4, None)),              # no query
            ((State.F6, "accidental"), (State.F6, "accidental")),
            ((State.F5, "runtime"), (State.F6, None)),         # runs after repair, answer wrong
            ((State.SUCCESS, None), (State.SUCCESS, None))][index % 5]


def build_cell(graph="full_gold", outcome=gold_outcome, passes=(1, 2, 3), f1=F1_DEFAULT, *, replication=1,
               treatment="S4", arm="PG", states=None, extra_items=(), only_bucket=None):
    """An item table for one cell. `states` overrides single assertion states after the graph rule."""
    items, rep_index = [], 0
    for n, (aid, tier) in enumerate([(a, Tier.CORE) for a in CORE] + [(s, Tier.SUPPORTING) for s in SUPPORTING]
                                    + [(x, Tier.EXCLUDED) for x in EXCLUDED]):
        if aid in f1:
            st = State.F1
        else:
            st = assertion_state(graph, rep_index)
            rep_index += 1
        if states and aid in states:
            st = states[aid]
        items.append(Item(aid, ItemType.ASSERTION, st, tier=tier, stratum=STRATA[n % 4], jurisdiction=JURISDICTIONS[n % 4],
                          authority_class=CLASSES[n % 4], match_tier=1 if st is State.SUCCESS else None,
                          exclusion_reason="synthetic exclusion" if tier is Tier.EXCLUDED else None,
                          evidence=f"matcher:{aid}"))
    table = ItemTable(items, {"treatment": treatment, "arm": arm, "condition_set": "K1", "replication": replication})
    st = table.assertion_states()
    index = 0
    for c, (cluster, (required, size)) in enumerate(cluster_requirements().items()):
        bucket = "held-out" if c % 2 else "development"
        for j in range(size):
            index += 1
            if only_bucket and bucket != only_bucket:
                continue
            first, final = outcome(index)
            for p in passes:
                table.items.append(cq_row(f"q{index:03d}", cluster, required, st, pass_id=p, first=first, final=final,
                                          bucket=bucket, family=f"fam{c % 4}", stratum=STRATA[c % 4]))
    table.items.extend(extra_items)
    return table.validate()


@pytest.fixture(params=BOUNDARY)
def boundary(request):
    return request.param, build_cell(request.param)


def make_run(n_units=12, *, run_id="PG-S4-A0-r1", fatal=(), reject_first=(), validator_mode="reject", status="completed",
             engine_error=(), input_tokens=1000, cached=400, output=300, reasoning=200, latency_ms=2000,
             extra_stage_calls=0):
    """Synthetic records of shared-minimum-log/1 for one run. Unit i has stratum STRATA[i % 4]."""
    recs = [{"record_type": "run_start", "ts": "2026-11-23T10:00:00Z", "run_id": run_id}]
    for i in range(n_units):
        u = f"u{i:03d}"
        recs.append({"record_type": "source_start", "ts": "2026-11-23T10:00:01Z", "run_id": run_id, "unit_id": u,
                     "stratum": STRATA[i % 4]})
        attempts = []
        if i in reject_first:
            attempts.append(("reject", "not_sent" if validator_mode == "reject" else "ok", validator_mode != "reject", 0))
            if validator_mode == "reject":
                attempts.append(("pass", "ok", True, 1))
        elif i in engine_error:
            attempts.append(("pass", "error", False, 0))
            attempts.append(("pass", "ok", True, 1))
        elif i not in fatal:
            attempts.append(("pass", "ok", True, 0))
        for n, (vres, eres, acc, retry) in enumerate(attempts):
            call = f"{run_id}-{u}-c{n}"
            recs.append({"record_type": "model_call", "ts": "2026-11-23T10:00:02Z", "run_id": run_id, "call_id": call,
                         "stage": "construction", "unit_id": u, "cq_id": None, "pass_id": None, "step_index": n,
                         "returned_model": "model-x", "latency_ms": latency_ms,
                         "tokens": {"input": input_tokens, "cached_input": cached, "output": output, "reasoning": reasoning}})
            recs.append({"record_type": "insert_attempt", "ts": "2026-11-23T10:00:03Z", "run_id": run_id,
                         "attempt_id": f"{run_id}-{u}-a{n}", "call_id": call, "unit_id": u, "retry_index": retry,
                         "validator_mode": validator_mode, "validator_result": vres, "engine_result": eres, "accepted": acc,
                         "rejections": [{"rule_id": "V1", "element": "e", "expected": "x", "found": "y"}] if vres == "reject" else []})
        recs.append({"record_type": "source_end", "ts": "2026-11-23T10:00:09Z", "run_id": run_id, "unit_id": u,
                     "final_status": "abandoned" if i in fatal else "completed", "fatal": i in fatal,
                     "abandon_reason": "budget_exhausted" if i in fatal else None, "attempts": len(attempts)})
    for n in range(extra_stage_calls):
        recs.append({"record_type": "model_call", "ts": "2027-01-04T08:00:00Z", "run_id": run_id, "call_id": f"{run_id}-q{n}",
                     "stage": "querying", "unit_id": None, "cq_id": f"q{n:03d}", "pass_id": 1, "step_index": 0,
                     "returned_model": "model-x", "latency_ms": 5000,
                     "tokens": {"input": 9000, "cached_input": 0, "output": 900, "reasoning": 100}})
    recs.append({"record_type": "run_end", "ts": "2026-11-23T12:00:00Z", "run_id": run_id, "status": status})
    return recs
