# Eval 2 second reading (open point 15): Thesis B's reading of Technique v0.1

**Read:** `docs/technique/eval2-package-v0.1.md`, branch `inputs-2026-11-13` at 9325984 (package version 0.1 of
12 Nov, text state of 15 Nov), sections 2 to 7, together with `analysis/thesis-a/README.md` (scorer core 0.1, its
list of 24 choices and 6 corrections) and `docs/technique/common-scope-rules.md` v0.2. **Reading date:** 21 Nov 2026
(due 16 Nov; late because the validator was built first, see `DECISIONS-2026-11-13.md`, 15 Nov). Effort cap one day.
Output: the tables below, sent to Thesis A; the finding rows go into the technique's change log with Thesis B's
initials; Thesis B cites the reading date.

## The four checks (EV-02, BU-06)

| Check | Question per card | Fails when |
|---|---|---|
| C1 unit and denominator | Is the unit of observation named? Is the denominator explicit, with the n.e. rule for an empty denominator? | "rate" without a denominator; a denominator that mixes units (assertions and CQs); n.e. undefined |
| C2 no double counting | Does any item enter two components of the same aggregate? Do F1 to F6 states partition the items? | an assertion counted in coverage and populatability of the same composite; a CQ in two eligibility sets |
| C3 forward trace | Does the card point to a requirement id, and does the requirement point back to the card and to an analysis? | orphan card; requirement without card; analysis using a metric with no card |
| C4 no domain constant | Does the definition work on any domain without a fessi-specific constant, list or threshold? | a fee, a waste type, a jurisdiction name or a fixed count in the definition (allowed only in the domain binding table) |

## Structure checks (whole package)

- 31 questions in section 4 (7 + 10 + 9 + 5), 31 cards in section 5; each card answers exactly one question and
  names the requirements it serves: **pass**.
- Every requirement R1 to R15 is named by at least one card or by a structural element (R11 change log, R13 card
  template, R15 binding tables): **pass**.
- Sections 2 to 6 scanned for domain and product terms (waste, fessi, Abfall, municipal, city, law, flyer, Neo4j,
  Cypher, SPARQL, RDF, Fuseki): none outside the two allowed places (section 7 scenarios; the binding tables).
  "Euro" is a unit, not a constant. Two constants found inside cards, rows C.3 and P.10 below.
- Rule list v0.2 and the validator (Thesis B, 0.2.0) agree with card P.3 (CS1 to CS7 on the export) and card P.9
  (CS1, CS3, CS4 at the write; CS0 a rejected attempt; repeats outside the three rows; one message per readable
  write): **pass**.
- Card P.2's reason codes equal the `abandon_reason` enum of `shared-minimum-log/1` (budget_exhausted,
  crashed_twice, resumed_cell): **pass**.
- Thesis A's six corrections in `analysis/thesis-a/README.md` (C.7 two facts, P.1 qualifier condition, 6.6 vs C.4,
  Q.6 full gold, Q.2 negative repair gain, Q.5 macro identity) are already in the text state read, except the Q.5
  wording (row Q.5 below).

## Review table (one row per card)

Severity: **blocker** = the card cannot be computed or contradicts another card; **fix** = text must change before
v1.0; **note** = for the change log, no text change needed. "—" = nothing found on that check.

| Card | C1 | C2 | C3 | C4 | Finding | Severity |
|---|---|---|---|---|---|---|
| C.1 Coverage | — | — | — | — | nothing found | — |
| C.2 Tier vector | — | — | — | — | nothing found | — |
| C.3 Authority-weighted coverage | — | — | — | **x** | The instrument field names the four authority classes with their weights (A 1.0, B 1.0, C 0.5, D 0.25). Authority classes are a Domain Binding item (1.4); their weights belong there, or in 7.4 where they already stand as a proposal. Card text: "one weight per authority class, declared in the Domain Binding with version 1.0". | fix |
| C.4 Realised coverage | — | x | — | — | Identity C_r = C × P_func is exact only if "unresolved" assertions (section 5 preface, BE-12) leave A as well, not only the P and Q denominators. Thesis A's scorer already does this (README choice 5); the package text does not say it. Section 5 preface: "outside the denominators of the C, P and Q cards". | fix |
| C.5 Qualifier completeness | — | — | — | — | nothing found | — |
| C.6 Source-information preservation | — | — | — | — | nothing found; the pooled side figure is marked "never evidence" | — |
| C.7 Closed-set domain completeness | — | — | — | — | Section 3.2 carries the two independent facts per inventory item (Thesis A's correction 1): resolved | — |
| P.1 Functional populatability | — | — | — | — | "wrong qualifiers 0 with an F3 share of 1" carries the condition "when every representable core assertion carries a qualifier" (Thesis A's correction 2): resolved | — |
| P.2 Document-level population success | — | — | — | — | nothing found; reason codes match the log schema | — |
| P.3 Common-scope conformance | — | — | — | — | nothing found; the denominator names the unit of the first accepted write and the row for an untraceable assertion; CS2 and CS7 marked provisional | — |
| P.4 Treatment-scope conformance | x | — | — | — | "violations per applicable constraint after commit": under the enforcement of the schema treatments v0.2 (S3 and S4 reject TS-type, TS-endpoint and TS-unique at the write) the committed graph has zero violations of those constraints by construction in reject mode, and the table says nothing. Propose two columns per constraint: rejected before commit (from the validator record, `rule_id` TS-*) and violated after commit (0 expected in reject mode, informative under A6 log-only). Rule 5's "undeclared property" is counted and never a violation; say where that count is printed (P.4 or P.5 row (c)). | fix |
| P.5 Vocabulary coherence | — | — | — | — | nothing found; the overlap with P.3 is declared on both cards | — |
| P.6 Reference precision | — | — | — | — | nothing found | — |
| P.7 Supportedness | x | — | — | — | "a stratified random sample of equal size per realisation": a realisation has three cells; the card does not say whether the sample pools the three replications' exports or draws from one, nor what happens when a realisation has fewer non-reference assertions than the sample size (take all, print n). Both change the denominator "all sampled assertions of the realisation". | fix |
| P.8 Run-to-run stability | — | — | — | — | nothing found; Thesis A's choices 14 to 16 (two empty sets count as identical, stable core per unit then mean, sample standard deviation) are sound and should go into the card text | note |
| P.9 Pre-commit friction | x | x | — | — | (a) Rejections at the write come from two rule families: common scope (CS0, CS1, CS3, CS4; every realisation) and treatment scope (TS-*; only where `enforcement` says reject, S3 and S4; S1 rejects nothing). Row 1 mixes a treatment-independent and a treatment-dependent count, so friction is not comparable across treatments as one figure. Print row 1 split by rule family (the validator record carries `rule_id`). (b) A write item that is never accepted while its unit still completes (the agent gives up on one fact after `max_self_corrections`) enters row 1 and neither row 2 nor row 3; Thesis A's choice 19 confirms. Add a count "write items never accepted" per unit, or state the omission on the card. | fix |
| P.10 Resource efficiency | x | — | — | **x** | "euro = tokens × the price table frozen with the sample ... at list price". Since 20 Nov A0 (and A2) are served from Thesis A's own hardware; an open-weights model served locally has no list price. The card needs the rule for a model without a vendor price: euro n.e. (tokens and seconds stay), or a declared proxy row (the vendor list price of the same weights on the table's date, labelled proxy). Consequence for 7.5: requirement (1) of scenario U3 is decided by the token form of B for a family with euro n.e. (B is "stated in tokens of the primary agent and in euro"); say so. The price key is the returned model string (Thesis A's choice 24): the local server's returned string must be in the table. Not a domain constant, but a hosting constant hidden in a card. | fix |
| Q.1 Own-set queryability | — | — | — | — | nothing found | — |
| Q.2 Query funnel | — | — | — | — | negative repair gain printed, not clipped (Thesis A's correction 5): resolved | — |
| Q.3 Intersection queryability | — | — | — | — | nothing found; the "fewer than half" reading of choice 13 should go into the card | note |
| Q.4 Oracle queryability | — | — | — | — | nothing found | — |
| Q.5 End-to-end answerability | — | x | — | — | "Identity, printed with both factors ... end-to-end = eligibility rate (Q.6) × Q_own" with "The identity is exact": Q_own is a cluster macro-average (5.3 preface), the eligibility rate a CQ-level share; the product equals the CQ-level end-to-end share only with the micro-average row of Q_own. Section 6.3 says "at CQ level, per pass"; the card should say "with the CQ micro-average of Q_own" (Thesis A's correction 6 confirms by test). | fix |
| Q.6 Eligibility rate | — | — | — | — | "Full gold: 1 when coverage is 1, otherwise ..." (Thesis A's correction 4): resolved | — |
| Q.7 Gold-subset figures | — | — | — | — | nothing found | — |
| Q.8 Difference diagnostics | — | — | — | — | nothing found | — |
| Q.9 Leakage-controlled queryability | — | — | — | — | nothing found | — |
| X.1 Reference reliability | — | — | — | — | nothing found | — |
| X.2 Matcher audit | — | — | — | — | nothing found | — |
| X.3 Instrument audits | — | — | — | — | nothing found | — |
| X.4 Run validity | x | — | — | — | "calls whose returned model identifier or fingerprint differs from the first call of the run": a locally served model may return no fingerprint (null in the log). Say that null against null is no drift and null against a value is a drift. | note |
| X.5 Leakage diagnostics | — | — | — | — | nothing found | — |

## Findings outside the cards

| Section | Check | Finding | Severity |
|---|---|---|---|
| 2, R9 "Tested in: Eval 2 (tests green before the pilot)" | C3 | The tests that exist (`analysis/thesis-a/tests`, 477) are at item-table level: the state of every item is written into the fixture. Section 5 preface demands tests "on seven synthetic boundary graphs per arm, built through the real loader, exporter and item table"; Thesis A's README says so itself. The reading can confirm the item-table tests, not the boundary-graph tests. Split the column: item-table tests at Eval 2, loader/exporter boundary tests at F5 (28 Dec), and say in R9 which was green when. | fix |
| `analysis/thesis-a/README.md`, choices 5 and 6 | C3 | Both refer to "the package's own section 8.1"; the package on the branch ends with 7.6. Either a section 8 (decisions) is missing from the committed text or the references are stale. | fix |
| 5 preface, small-denominator rule | C1 | Names "items for assertion-based cards, clusters for question-based cards" only. Thesis A's choice 7 applies 10 to the card's own unit for P.2, P.8, P.9, C.6, C.7 and not to P.10. Put that sentence into the preface. | note |
| 6.6 | C1 | Distinguishes conditional rates (1 on full gold) from realised coverage, eligibility and end-to-end (1 only at coverage 1): Thesis A's correction 3 resolved | — |
| 7.4, authority weights | C4 | The right place for the weights (see C.3); "proposal, to be declared with version 1.0" is fine | — |
| 7.5, budget B | C1 | "fixed with version 1.0 from the pilot costs": the pilot has not run on 21 Nov, so B cannot be fixed on 23 Nov. Log the slip, name the new date; the move-once rule stays. | note |
| 7.5, requirement (2) | C1 | "an error ceiling ... (open until 23 November)": still open; same slip | note |

**Summary:** 31 cards read; blockers 0; fixes 9 (C.3, C.4, P.4, P.7, P.9, P.10, Q.5, R9, section 8.1 references);
notes 6. Nothing in sections 2 to 6 depends on the domain except the two constants named (C.3 weights, P.10 price
rule under local hosting).

## Thesis B's five questions, answered by the reading

1. **Which log fields does each card consume?** P.2: `source_end.fatal`, `final_status`, `abandon_reason`, the
   modality stratum from the manifest. P.9: `insert_attempt.accepted`, `validator_mode`, `validator_result`,
   `rejections[].rule_id`, `retry_index`, `engine_result` (already_present leaves the rows), `source_end.repeated_writes`
   and `final_status`. P.10: `model_call` tokens by category, `latency_ms`, `returned_model` (price key), stage
   construction only. X.4: `run_end`, `retried_from_snapshot`, returned model id and fingerprint per call, pass
   completion. P.7 and P.3 (CS6): `insert_attempt.evidence {quote, locator}` and `element_ids`. All present in
   kgc-trace/1 version 1.1: **no change request for F3.** To watch: under local serving the returned model string
   must be stable and in the price table.
2. **Denominator of resource efficiency:** two, units processed (units with a `source_start`) and assertions in
   state success (core tier; count passed by the caller). Not representable ones.
3. **Pre-commit friction:** rejected attempts over all attempts, retries (`retry_index`) per accepted write,
   abandoned units over all units; per source unit and per realisation; repeats outside; log-only rejections
   returned separately and not counted. Open: the split by rule family and the never-accepted items (row P.9).
4. **Run-to-run stability:** mean pairwise Jaccard over the three replication pairs per unit, averaged over units,
   plus the stable-core share; no Krippendorff; the standard deviation of each axis beside it.
5. **Equivalence table:** Domain Binding (1.4), frozen 28 Dec; CS2, CS7 and P.5 read it. Identity: a duplicate
   is a CS7 violation on P.3 and no inflation on P.1 ("duplicated gold 1"), so a duplicated fact is a match, never a
   miss, and is counted once as a conformance fault.

## Log entry (for Thesis A's change log)

`2026-11-21 | Eval 2 second reading by Yagubi (package v0.1, text of 15 Nov) | cards read: 31 | blockers: 0 | fixes: 9 | notes: 6 | time: see effort ledger`
