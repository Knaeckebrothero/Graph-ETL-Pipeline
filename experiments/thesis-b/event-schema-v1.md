# Thesis B instrumentation specification: `kgc-trace/1` (draft for F3, 13 Nov 2026)

Owner: Thesis B (Yagubi). Answers open points 4 and 12 of the demarcation agreement.
Machine-readable schema: `src/kgbench/spec/kgc-trace-v1.schema.json` (JSON Schema 2020-12).
Reference derivation from a real trace: `src/kgbench/trace.py` (`kgbench trace-export <run_dir> --validate`).
Status: proposal, local, uncommitted. Nothing here changes the meaning of Thesis A's shared minimum fields.

## 1. Principles

1. **One record per event, JSON Lines, one file per run.** Every record carries `run_id`, `source_id`, `ts`, `seq`
   and `origin` (which raw log events it came from). A run of A0 in SRW and a run of kgbench produce the same record
   kinds.
2. **Nothing is estimated.** A number the harness did not observe is `null`, with a reason field where one exists
   (`usage_source`, `tokens.attribution`, `cost.reason`). Null means unknown, never zero.
3. **Full payloads live next to the trace.** Model requests and responses, tool arguments and results, and every
   candidate revision are stored as content-hashed files; records point at them (`artifact_ref`). SRW's audit log
   truncates tool arguments to 200 characters; that is not enough for Thesis B, so the harness extension stores the
   full payload before truncation.
4. **Stable item identity across self-corrections.** Every candidate assertion the agent proposes gets an `item_id`
   at creation; a self-correction is a new `revision` of the same item. Without this, "retries until acceptance"
   (Thesis A) and "correction effect" (Thesis B) cannot be computed. This is the one hard requirement on A0's
   harness.
5. **Thesis A's shared minimum is a view on the same stream.** `run_start`, `run_end`, `source_start`, `source_end`
   and `insert_attempt` carry every field of agreement section 4.3. Thesis A's code reads those; Thesis B's code
   reads everything.

## 2. Record kinds

| kind | one per | what it records | derived? |
|---|---|---|---|
| `run_start` | run | ids, arm, agent configuration, requested model, prompt hashes, tool hash, container digests, sample manifest hash, launcher, code commit, initial graph hash, pilot flag | no |
| `run_end` | run | status, final graph hash, global validator results, resources (wall clock, tokens, cost with price-table id), counts, failure class | no (yes if the raw log lacks a run end: status `unknown`) |
| `source_start` | source unit | modality, source hash, number of segments, Thesis A's manifest fields (stratum, jurisdiction, authority class, bucket incl. canary, chunk rule, seed, retrieval date, source-version hash) | yes (manifest fields copied) |
| `source_end` | source unit | final status, fatal-failure flag, insert attempts, rejects, accepted, abandoned items, max retry index until acceptance, resources, failure class | yes |
| `model_call` | transport attempt of a model call | operation id, attempt index, status, requested and provider-returned model id, provider fingerprint, requested sampling parameters, stop reason, usage, usage source, latency, tools requested, request/response artefacts | no |
| `tool_call` | tool call | operation id, requesting model operation, step index, tool name and role, status, error flag, duration, payload hash, argument/result artefacts, item ids | no |
| `insert_attempt` | (item, revision) submitted to the validator | **Thesis A's per-insert-attempt record**: attempt id, step and retry index, item id and revision, tool, modality, payload hash, validator result with rule ids, engine result with assertion ids, latency, tokens (attribution-aware), cost, requesting model operation | no |
| `item` | lifecycle event of a candidate | `created` (mentions, predicate, evidence), `validated`, `revised` (correction budget), `revision_refused`, `persisted` (assertion ids), `abandoned` (reason, failure class) | no (abandoned: yes) |
| `reconciliation` | entity slot decision | item, role, mention surface and type, stage (`match` before writing, `write` at the write tool), decision (reuse / create / unresolved / contained), method (exact, fuzzy, graph query, model choice, bound), chosen entity, candidate list with scores | no (write stage: yes, from the final graph) |
| `intake` | read of the source | read index, segments read, cumulative coverage | no |
| `gap_trigger` | gap-detection event | `source_reread`, `schema_requested`, `graph_queried`, `validator_feedback` | yes |

Field-level definitions, enums and required fields: the JSON Schema. Units: durations in milliseconds, tokens as
integers as reported by the provider, cost in the price table's currency.

## 3. Coverage of Thesis A's shared minimum (agreement 4.3, open point 12)

| Thesis A field | kgc-trace/1 record.field | kgbench today |
|---|---|---|
| run manifest: `run_id`, `schema_id`, `arm`, `agent_config_id` | `run_start.*` | yes (`arm = kgbench-json`) |
| sample manifest hash | `run_start.sample_manifest_sha256` | null (no sample manifest yet) |
| model identifiers and versions | `run_start.model.requested_model_id`, `model_call.response_model_id` | yes |
| prompt hashes | `run_start.prompt_hashes` | version string + sha256 of the system prompt |
| tool versions | `run_start.tools_hash` | yes |
| container digests | `run_start.container_digests` | null (no container) |
| launcher | `run_start.launcher` | CLI flag |
| start and end time | `run_start.ts`, `run_end.ts` | yes |
| final graph-state hash | `run_end.final_graph_hash` | yes |
| global validator results | `run_end.global_validator` | null (no global rules in kgbench) |
| per insert attempt: `run_id`, `attempt_id`, `source_id`, modality, step index, timestamp, tool name, payload hash | `insert_attempt.*` | yes |
| validator result (`pass` / `reject` with rule id, element, expected, found) | `insert_attempt.validator` | rule id = `category:field`; `expected`/`found` null (kgbench validator does not emit them; A0's must) |
| engine result | `insert_attempt.engine.result` (`written`, `duplicate`, `refused`, `failed`, `unconfirmed`, `not_attempted`) | yes |
| retry index | `insert_attempt.retry_index` (= revision of the item) | yes |
| tokens in and out, latency, cost per attempt | `insert_attempt.tokens` (`attribution: exact` only if the harness can attribute one model turn to one attempt; otherwise `none` and the link `model_operation_id`), `latency_ms`, `cost` | attribution `none`; latency = validator tool duration |
| per source: start and end, attempts, rejects, final status | `source_start`, `source_end` | yes |
| fatal-failure flag | `source_end.fatal_failure` | yes |
| retries until acceptance | `source_end.max_retry_index_until_acceptance`, per item via `insert_attempt.retry_index` | yes |
| abandoned units | `source_end.abandoned_items`, `item/abandoned` | yes |
| wall-clock time | `run_end.resources.wall_clock_ms`, `source_end.resources.wall_clock_ms` | yes |
| tokens or cost with the price table | `*.resources.tokens`, `*.resources.cost.price_table_id` | yes (cost only with a pricing file) |
| provider-returned model id per call | `model_call.response_model_id` | yes |
| **cost record (handoff 1 Nov, SC-14, point 12):** exact model snapshot per call, never an alias | `model_call.response_model_id` (as returned) next to `model_call.requested_model_id` (catalog id); snapshot rule and tokenizer version from Thesis A's F2 table recorded in `run_start.model` | yes for API models; open-weight commit hash and tokenizer version need the F2 snapshot rule |
| cost record: token categories as the runtime reports them | `model_call.usage` (`input_tokens`, `output_tokens`, `reasoning_tokens`, `cache_read_tokens`, `cache_write_tokens`), `usage_source` | yes; categories the provider does not report stay null |
| cost record: call counts | `run_end.counts.model_operations` (logical calls), `run_end.counts.model_attempts` (incl. transport retries), `tool_calls` | yes |
| cost record: euro | computed by Thesis A from the price table frozen at F2; `*.resources.cost.value` is Thesis B's lower-bound estimate with `price_table_id` and `complete` flag, for monitoring only | yes (estimate only) |
| per written assertion: the source unit the agent was processing | `insert_attempt.source_id` + `engine.assertion_ids`; `item/persisted.assertion_ids` | yes |
| **decoding record (handoff 8 Nov, EX-10, point 20 b):** requested sampling parameters per call | `model_call.sampling` (`temperature`, `top_p`, `seed`, `reasoning_budget`, `max_output_tokens`; the string `not_exposed` where the snapshot has no such parameter) | null (kgbench does not record decoding per call yet; exporter reads `settings.decoding`) |
| decoding record: returned model id and fingerprint | `model_call.response_model_id`, `model_call.response_fingerprint` | model id yes; fingerprint null in kgbench (Anthropic API returns none; OpenAI-compatible hosts return `system_fingerprint` when exposed) |
| **manifest fields per source unit (EX-05, EX-16, point 20 b):** hash, stratum, jurisdiction, authority class, bucket incl. canary, chunk rule, seed, retrieval date, source-version hash | `source_start.source_sha256`, `source_start.manifest.*` | hash yes; the rest null (no F2 manifest) |
| per CQ attempt: pass id (EX-09) | Thesis A's query harness; not part of this trace | — |
| per CQ attempt | Thesis A's query harness; not part of this trace | — |
| export (canonical assertion form) | Thesis A's exporter; not part of this trace | — |

Open with Thesis A at F3: how the SRW harness receives the per-unit manifest fields (copied from the F2 manifest by `source_id`, never derived), the `attempt_id` scheme (Thesis A: run id + step index + attempt index; Thesis B
adds the item id so that one write call with several items stays unique), the validator's `rule_id`, `expected`
and `found` vocabulary, and whether A0's write tool can attribute a model turn to one attempt (`tokens.attribution`).

## 4. Thesis B's extensions (open point 4)

Everything in `item`, `reconciliation`, `intake`, `gap_trigger`, plus on `tool_call` the full-payload artefacts and
the requesting model operation, plus the failure classes. These feed the process-quality layer:

| B metric family | records used |
|---|---|
| validity before / after correction, correction effect | `item/validated`, `item/revised`, `item/revision_refused`, `insert_attempt.validator` |
| identity (link accuracy, duplicate creation, incorrect merge) | `reconciliation`, `item/created.mentions`, `item/persisted.assertion_ids`, final graph export |
| provenance-chain completeness | `item/created.evidence`, `item/persisted`, artefact references |
| technical completion, failure taxonomy | `run_end.status`, `*.failure_class`, `model_call.status`, `tool_call.is_error` |
| resources | `model_call.usage`, `*.resources`, price table |
| intake coverage and gap detection | `intake`, `gap_trigger` |

## 5. Failure taxonomy v0.1 (draft, not frozen at F3)

`none`, `transport`, `provider_permanent`, `budget_exhausted`, `output_truncated`, `validator_persistent_reject`,
`valid_not_persisted`, `never_validated`, `harness_error`, `cancelled`, `unknown`. Assigned at run, source and item
level by rules in `trace.py` (`_abandon_reason`, `RUN_FAILURE_CLASS`). The taxonomy is Thesis B's component and may
grow after the pilot; the enum in the schema is extended, never reinterpreted.

## 6. What the harness extension in SRW must do (Thesis B's pull requests)

1. Emit `kgc-trace/1` records next to SRW's own audit log, or store enough raw events that `trace.py`-style
   derivation is possible. Preferred: a small event writer called from the write tool, the validator and the
   model client.
2. Assign `item_id` at the first proposal of an assertion and keep it through self-corrections (requirement 4 above).
3. Store full model requests/responses and tool arguments/results as content-hashed files under the run directory.
4. Log the provider-returned model id and usage on every call, including failed transport attempts.
5. Record `launcher`, container digests and the sample manifest hash in `run_start`.

Acceptance test: the pilot (16 to 20 Nov) produces a trace per run that passes `kgbench trace-validate` and whose
`source_end` counts agree with Thesis A's own computation of rejects and attempts.
