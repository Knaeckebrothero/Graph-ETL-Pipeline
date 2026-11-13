# Indicator names: Thesis B's metrics against Thesis A's populatability components (open point 13, due 13 Nov)

Rule (agreement 5, TE-07): the same indicator carries the same name and metric card in both theses. Everything
that is not the same indicator carries a different name. Thesis A's component names are taken from TE-07; their
cards arrive on 13 Nov, so the right-hand column is confirmed in the Eval 2 reading (13 to 16 Nov).

## 1. Shared indicators (same name, same card, one owner)

| Indicator | Owner of the card | Unit of observation | Denominator | Source record |
|---|---|---|---|---|
| `tokens_in`, `tokens_out` (plus cache/reasoning tokens where reported) | A | model call attempt | — (sums over all attempts, failed included; `complete` flag) | `model_call.usage` |
| `cost` | A | run / source | — (known tokens × price table; lower bound if incomplete) | `*.resources.cost` |
| `wall_clock_ms` | A | run / source | — | `run_end.resources`, `source_end.resources` |
| `validator_rejects` | A | insert attempt | — (count) | `insert_attempt.validator.result = reject` |
| `insert_attempts`, `retries_until_acceptance`, `abandoned_units`, `fatal_failure` | A | source unit | — | `source_end.*` |

Thesis B reports these with Thesis A's names and cites Thesis A's cards.

## 2. Thesis A's ten components and Thesis B's nearest metric (different names, different units)

| # | Thesis A component (TE-07) | A measures (as understood from BU-03/04; confirm 13 Nov) | Thesis B's related metric | Why not the same indicator |
|---|---|---|---|---|
| 1 | functional populatability | reference assertions present and correct in the final graph / representable assertions | `strict_recall` (identity-strict) | B's denominator is every reference fact of the source, not only representable ones; B resolves identity by explicit mapping, not by equivalence table |
| 2 | document-level population success | sources whose required assertions all reached state success / sources | `technical_completion`, `source_end.final_status` | B's unit is the run or source status, independent of the reference |
| 3 | common-scope conformance | constraint checks on the final graph common to all treatments | `datatype_valid_after`, `schema_valid_after` | B's unit is the candidate revision before commit, not the committed graph |
| 4 | treatment-scope conformance per applicable constraint | constraint checks specific to a treatment / applicable constraints | — | no B counterpart; B cites A |
| 5 | vocabulary coherence | predicate/term use against the schema vocabulary in the final graph | `schema_valid_before` (unknown-predicate errors) | different unit (graph vocabulary vs candidate error category) |
| 6 | reference precision | final assertions matching the reference / final assertions | `strict_precision`, `lenient_precision` | B reports two bases (confirmed / with provisional identity mappings) and two resolutions (strict / lenient); A has one matcher |
| 7 | supportedness | final assertions supported by a source passage (audit) / sampled assertions | `provenance_completeness` | B checks the chain (snapshot hash, locator, quote, artefact, events), not whether the passage supports the claim; a fully traced fact can still be wrong |
| 8 | run-to-run stability | agreement of outcomes across r1..r3 | `repetition_dispersion` (per configuration: min/max/mean of each B metric) | B reports dispersion per configuration, A per treatment |
| 9 | pre-commit friction | rejects and retries before commit, diagnostic, unscored | `correction_success_rate`, `attempt_success_rate`, `delta_strict_f1_correction` | same raw events, different role: A diagnoses, B scores; B's denominators are candidates that failed first validation / successful revise calls |
| 10 | resource efficiency | cost per populated assertion (denominator to confirm) | `cost_per_correct_fact` | B divides by identity-strict true positives, A by populated assertions |

## 3. Thesis B's metric cards (names B uses; implemented in evaluator 0.2.1 unless marked)

| B metric | Layer | Unit | Numerator | Denominator | Records |
|---|---|---|---|---|---|
| `lenient_precision/recall/f1` | extraction quality | fact (name/alias resolution) | matched facts | predicted / reference facts | final graph export, reference |
| `strict_precision/recall/f1` | extraction quality | fact (explicit identity) | matched facts on basis confirmed / with provisional | assessable predicted / reference facts | export, identity map, `reconciliation` |
| `link_accuracy`, `wrong_link_rate` | identity | link (entity slot of a write) | correct links | assessable links | `reconciliation`, `item/persisted` |
| `reuse_accuracy`, `new_entity_accuracy`, `duplicate_creation_rate`, `resolution_accuracy` | identity | reference entity | per README §Identity | assessable reference entities | same |
| `incorrect_merge_rate` | identity | graph entity | nodes denoting ≥ 2 identities | judged nodes | same |
| `schema_valid_before/after`, `datatype_valid_before/after`, `grounding_valid_before/after` | insertion and datatype quality | candidate | valid at first / latest validation | candidates validated at least once | `item/validated`, `insert_attempt.validator` |
| `correction_success_rate` | process quality | candidate | failed first, valid last | candidates that failed first validation (0/n in A4, never undefined) | same |
| `attempt_success_rate`, `refused_attempts` | process quality | revise call | revisions that validated | successful revise calls | `item/revised`, `item/revision_refused` |
| `delta_strict_f1_correction` (planned) | process quality | run | strict F1 after − strict F1 of first revisions | — | `insert_attempt` by `retry_index` |
| `technical_completion` | process quality | run | `completed` | runs with known status (unknown reported separately) | `run_end.status` |
| `failure_class_distribution` (planned) | process quality | run / source / item | — | — | `*.failure_class` |
| `provenance_completeness` | process quality | provenance record / fact | all seven links hold | records / facts | artefacts, `item/*` |
| `intake_coverage`, `source_reread_count` (planned) | process quality | source | segments read | segments total | `intake`, `gap_trigger` |
| `tokens`, `cost`, `wall_clock_ms`, `cost_per_correct_fact` | resources | run | shared names (section 1) | — / strict TP | `*.resources` |
| `repetition_dispersion` (planned) | all | configuration | — | — | per-run reports |

Card format for the Eval 2 reading: attribute, unit of observation, denominator or n.e., function, procedure
with version, scale type, unit, expected properties, requirement ids (BU-02). Thesis B's cards use the same
columns so that the two sets can be read side by side.

## 4. Downstream utility (Thesis B)

Chatbot benchmark on populated graphs (fessi RAG, graph-only vs graph + web) is Thesis B's; Thesis A's
queryability (agent-generated CQ queries, F4 to F6 states) is Thesis A's. Different units (chat answer vs CQ
query), different names. Thesis B drops this layer if the January window is too short; the decision is logged.
