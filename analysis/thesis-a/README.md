# Thesis A: scorer core, version 0.1

First version of the scoring code of the schema-fitness technique. It follows "Eval 2 package, Technique v0.1" of 12 November 2026, sections 3 (stage model and item table), 5 (metric cards) and 6 (formulas and order of aggregation). **Status: draft, not frozen, nothing here has scored a run.** The package is frozen as version 1.0 on 23 November; this code changes with it until then.

## What exists

| File | Content | Package |
|---|---|---|
| `technique/items.py` | Item table of one cell: item types, states F1 to F6, success, not in source, sub-codes, tier, strata, match tier, first and final attempt; validation; eligibility and inheritance of the worst upstream state with "k of n" | 3.2 to 3.4, 3.6 |
| `technique/values.py` | A figure is a rate, "k of n" or "n.e."; Wilson 95 % interval; mean over components with minimum and maximum; n.e. register | 5 (rules for every card), 6.5 |
| `technique/views.py` | C.1, C.2, C.3, C.4, C.6, C.7, P.1, P.6, Q.1, Q.2, Q.3, Q.5, Q.6, Q.9; the identities of 6.3 on exact counts; the product of the three axes as an approximation | 3.5, 5, 6.1 to 6.3 |
| `technique/aggregate.py` | Mean over completed passes with the first-pass value; mean over replications with minimum and maximum; refusal to pool across arms, treatments or condition sets | 6.4 |
| `technique/stability.py` | P.8 row (a): mean pairwise Jaccard and stable-core share over three replications | 5.2 |
| `technique/logcards.py` | P.2, P.9, P.10 from records of `experiments/log-schema/shared-minimum-log.v1.schema.json`; a void run is refused | 5.2, 6.7 |
| `tests/` | 475 tests: one per expected property of each implemented card on synthetic item tables for the seven boundary cases, the rules for every card, the aggregation order, and the identities on 300 seeded random cells | 5 ("expected properties are unit tests") |

All rates are exact fractions, so an identity is checked with `==`, not with a tolerance.

```bash
cd analysis/thesis-a
python3 -m pytest -q          # standard library only; pytest is the system one (not in .venv)
```

Weights, the booking of cached input and the price table have no defaults in the code: the caller passes them. The threshold of 10 for small denominators is the package's and is a constant (`MIN_DENOMINATOR`).

## What these tests are not

They are tests at item-table level. The state of every reference item is written into the fixture directly (`tests/conftest.py`). The package asks for tests "on seven synthetic boundary graphs per arm, built through the real loader, exporter and item table" (BU-12). Those need the exporters and the matcher and are not built. Consequences:

- "Duplicated gold" and "full gold" are the same item table here. That duplicates do not inflate a figure, and that a second export gives the same value, is not tested.
- "Fully hallucinated" and "wrong qualifiers" are both written as F3 at every representable anchor; that the matcher really assigns F3 is not tested.
- Every id in the fixtures is made up (`a001`, `k01`, `q001`, `u001`). There is no reference content in this folder.

## Not built

Exporters and loaders per arm; the matcher; the validator (P.3, P.4); vocabulary coherence (P.5); supportedness (P.7); qualifier completeness (C.5, needs one row per qualifier); P.8 rows (b) and (c); oracle, gold-subset and difference cards (Q.4, Q.7, Q.8); the cross-cutting cards X.1 to X.5; the scenario layer, dominance and the bootstrap of section 6.5; subgroup tables as output; any reading of real files except the run log.

## Where the package text left a choice

Each entry quotes the sentence concerned and says what the code does. These are candidate rows for the technique's change log.

1. **Passes in the item table.** Section 3.3 lists the columns "Failure state, sub-code, attempt index" with "first and final attempt for CQs" and no pass; card Q.1 has as unit "One eligible CQ per replication graph and pass." The code keys a CQ row by CQ id and pass id (1 to 3). An ineligible CQ needs a row in every pass, or the end-to-end denominator of that pass shrinks.
2. **Attempt index.** Section 3.3: "Failure state, sub-code, attempt index". The code stores the state at the first attempt, the state at the final attempt and the index of the final attempt. The attempt at which each funnel level was first reached is not stored.
3. **Sub-codes.** Section 3.2: "Query invalid. Sub-codes: parse failure; runtime error" and "Answer wrong: the query executes but the result does not match. Sub-codes: accidental correctness, memorised constant, empty without discrimination, boolean without witness." The code requires `parse` or `runtime` on every F5, because the level "parses" cannot be computed without it. F6 without a sub-code is allowed and printed as "answer mismatch, no sub-code": the four named codes do not cover a plain wrong answer.
4. **Required assertions.** Section 6.1: "Req(q) ⊆ A the required assertions of q". The code rejects a CQ that requires a supporting, excluded or canary assertion (canary CQs excepted). A CQ with an empty required set is accepted and is always eligible; the package does not say whether one may exist.
5. **Unresolved assertions and coverage.** Section 5: "Assertions flagged "unresolved" and the CQs that require them are outside the denominators of the P and Q cards (BE-12)." The code uses the reading proposed in the package's own section 8.1, row 1: unresolved assertions are outside C as well, so that C_r = C × P_func stays exact. In C.2 ("The three tier sizes sum to the number of anchored assertions.") the tier sizes still count them, and their number is printed beside the sizes.
6. **What "k of n" counts on a question card.** Section 5: "Below a denominator of 10 (items for assertion-based cards, clusters for question-based cards) per cell and stratum, the count is printed as "k of n" with no rate and no interval". The threshold is applied to clusters; the printed k and n are CQs, with the number of clusters beside them.
7. **Small denominators on other units.** The same sentence names only assertions and clusters. The code applies 10 to the card's own unit for P.2 and P.9 (source units, write attempts), P.8 (source units), C.6 (input statements) and C.7 (inventory items). It is not applied to P.10, which prints totals and means in raw units.
8. **Wilson interval on question cards.** Section 6.5: "Interval of a single-cell rate: Wilson score, 95 %." The code gives a Wilson interval to item-based rates only. A cluster macro-average is not k successes in n trials, and the CQs of a cluster are paraphrases, so neither the macro nor the micro row gets one.
9. **Completed passes.** Section 6.4, step 3: "A replication's value of a question-based rate is the mean over its completed query passes (up to three); the first-pass value is printed beside it and the completed share per cell is reported (EX-09)." The code counts a pass as completed when it holds a row for every eligible CQ of pass 1. A partial pass (for example the gold subset only) is reported per pass and stays out of the mean. Completed share = eligible-CQ rows present, over three times the eligible CQs of pass 1.
10. **A missing or not estimable replication.** Section 6.4, step 4: "The reported axis value of a realisation is the arithmetic mean of the three replication rates, with the minimum and the maximum beside it." Section 6.5: "An n.e. component is excluded, never imputed." The code averages the estimable replications, names the excluded ones and marks the value incomplete. Whether an axis with two of three replications may enter a scenario score is not decided anywhere.
11. **Identities with a not estimable factor.** Card C.4: "The identity is exact on the item table." When no core assertion is representable, P.1 is n.e. ("When no core assertion is representable (C = 0).") and the product does not exist while C_r is 0. The code returns "not checkable" for the identity in that case, and likewise for end-to-end = eligibility rate × Q_own when no CQ is eligible.
12. **Eligibility rate and passes.** Card Q.6: "Unit of observation. One CQ per cell." Card Q.5: "One CQ of the frozen catalogue per replication graph and pass." The code counts every CQ once for Q.6 and uses the rows of one pass for the identity.
13. **Intersection queryability.** Card Q.3: "Flagged "weak intersection" when it has fewer than 10 clusters or fewer than half the clusters of the smallest own eligible set." The code computes the intersection per pass over the CQs that are eligible in every cell and have a row in that pass, and reads "fewer than half" as twice the intersection's clusters being fewer than the smallest own set's clusters.
14. **Jaccard of two empty sets.** Card P.8: "A unit for which no replication exported anything leaves the denominator of row (a)." Nothing is said about a unit for which two of three replications exported nothing. The code counts two empty sets as identical (1), so (nothing, nothing, {x}) gives 1/3, the same as ({x}, {x}, nothing).
15. **Stable-core share.** Card P.8: "the stable-core share, the assertions present in all three replications over the three-way union". Per unit and then averaged, or pooled over units, is not said. The code returns both; the per-unit mean is the one with the card's denominator.
16. **Standard deviation.** Card P.8: "Beside it: the standard deviation of each axis over the three replications." The code uses the sample standard deviation (n − 1).
17. **Units in the log.** Card P.2: "Abandoned units are listed by reason code: budget exhausted, crashed twice, resumed cell." A unit can start more than once (retry, resume). The code takes the last `source_end` of a unit and stops with an error when a unit has none.
18. **Rejections of a log-only validator.** Card P.9: "Zero rejections by construction for a configuration whose validator only logs." The log schema records `validator_result: reject` also in mode `log_only`. The code counts a rejection only in mode `reject` and returns the log-only cases separately.
19. **Retries.** Card P.9: "retries until acceptance per accepted write". The code takes the mean `retry_index` of the accepted writes. A write that is never accepted counts in the rejected share and not here.
20. **Wall-clock time.** Card P.10: "Wall-clock seconds, tokens by category (input, cached input, output, reasoning, as the runtime reports them) and euro"; section 6.7: "Per model call: the model identifier with its version evidence, tokens by category, wall-clock seconds, pass id." The code sums `latency_ms` of the model calls. The elapsed time of a unit (`source_end.wall_clock_s`), which includes tools and validation, is a different figure and is not used.
21. **Which calls belong to P.10.** Card P.10: "Time and resources the population consumed." The code counts calls of stage `construction`; induction and querying calls are left out unless another stage is asked for.
22. **Denominators of P.10.** "per confirmed-correct assertion (denominator: assertions in state success)" does not name the tier. The caller passes the count. "Units processed" is read as the units with a `source_start`.
23. **Cached input and reasoning tokens.** The booking of cached input is an open decision, so the function has a required argument with two rules (`full_input_price`, `cache_price`). Reasoning tokens are a part of the output tokens and are priced as output.
24. **Price row.** The price is looked up under the model string the API returned. For A0 that is the alias `deepseek-flash`, so the frozen table needs a row under that name, or the caller maps the name.

## What the tests showed to be wrong or not implementable as written

1. **Card C.7 cannot be computed from one state per inventory item.** Section 3.2: "Inventory items are a second item type with one extra state, not in source: no sampled unit mentions the item." Card C.7 needs two independent facts per item: whether it is representable ("The representable share is constant per treatment and independent of the sample and of the run.") and whether the sample mentions it ("over the items that the frozen sample mentions"). An item that is not representable and not mentioned cannot be told from one that is not representable and mentioned. The code adds a mention flag to inventory items and lets F1 take precedence over "not in source".
2. **Card P.1, "wrong qualifiers 0 with an F3 share of 1", holds only if every core assertion carries a qualifier.** An assertion without qualifiers is correct on a graph whose only fault is wrong or missing qualifiers (`test_p1_an_unqualified_assertion_is_untouched_by_wrong_qualifiers`). Either the boundary reference consists of qualified assertions only, or the property is stated over the qualified assertions. The same applies to P.6 "wrong qualifiers 0".
3. **Section 6.6 contradicts card C.4.** Section 6.6: "on the empty graph every run-dependent rate is 0 or n.e.; on the full-gold graph every one is 1." Card C.4: "full gold equals C". Realised coverage, the eligibility rate and end-to-end answerability stay below 1 on the full-gold graph whenever an assertion is not representable. The sentence holds for the conditional rates (P.1, P.6, Q.1 and the funnel).
4. **Card Q.6, "Full gold 1", holds only at coverage 1.** A CQ that requires a non-representable assertion inherits F1 on every graph, the full-gold one included. In the fixture with six assertions without an image the eligibility rate on full gold is 0.875.
5. **Card Q.2, "Final is never below first at any level", is not guaranteed by the definitions.** If the final attempt means the last attempt, an agent that revises a query which already executed can end lower than it began. The property holds by construction only if a repair follows a failure and never a success, or if the levels are counted cumulatively over attempts. **Settled on 13 November in the package:** the final attempt is the last attempt and is the one scored, so final may be below first and a negative repair gain is printed. The validation accepts such rows; `validate(allow_regression=False)` rejects them for a harness that only repairs after a failure.
6. **The macro form of end-to-end = eligibility rate × Q_own** fails when eligibility differs inside a cluster and not all queries are correct (`test_q5_macro_identity_needs_uniform_eligibility_inside_clusters`). This confirms row 6 of the package's section 8.1; the identity is printed at CQ level only.
