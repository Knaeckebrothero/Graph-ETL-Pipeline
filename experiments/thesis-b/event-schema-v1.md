# Thesis B instrumentation specification: `kgc-trace/1`, version 1.1 (14 Nov 2026)

Owner: Thesis B (Yagubi). Answers open points 4 and 12 of the demarcation agreement; implements agreement 4.3 of
version 1.0 (14 Nov 2026): kgc-trace/1 carries Thesis A's shared minimum `shared-minimum-log/1` under Thesis A's field
names, so that one file passes both checkers.
Machine-readable schema: `src/kgbench/spec/kgc-trace-v1.1.schema.json` (JSON Schema 2020-12).
Reference derivation from a real trace: `src/kgbench/trace.py` (`kgbench trace-export <run_dir> --validate --shared-minimum <Thesis A's schema>`).
Status: version 1.1 delivered 14 Nov 2026 as a pull request to the shared repository (`experiments/thesis-b/`); the field
specification is frozen on 16 Nov 2026 before the pilot (F3, second step). Nothing here changes the meaning of Thesis A's
shared minimum fields.

## 1. Principles

1. **One record per event, JSON Lines, one file per run.** Every record carries `record_type`, `run_id`, `source_id`,
   `ts`, `seq` and `origin` (which raw log events it came from). A run of A0 in SRW and a run of kgbench produce the
   same record types.
2. **Nothing is estimated.** A number the harness did not observe is `null`, with a reason field where one exists
   (`usage_source`, `cost.reason`). Null means unknown, never zero.
3. **Full payloads live next to the trace.** Model requests and responses, tool arguments and results, and every
   candidate revision are stored as content-hashed files; records point at them (`artifact_ref`). SRW's audit log
   truncates tool arguments to 200 characters; that is not enough for Thesis B, so the harness extension stores the
   full payload before truncation.
4. **Stable item identity across self-corrections.** Every candidate assertion the agent proposes gets an `item_id`
   at creation; a self-correction is a new `revision` of the same item and a new `insert_attempt` with the same
   `item_id`. Without this, "retries until acceptance" (Thesis A) and "correction effect" (Thesis B) cannot be
   computed. Agreed with Thesis A on 13/14 Nov 2026 and in `shared-minimum-log/1` since then.
5. **Thesis A's shared minimum is a view on the same stream.** `run_start`, `run_end`, `source_start`, `source_end`,
   `model_call` and `insert_attempt` carry every field of `shared-minimum-log/1` under Thesis A's names; Thesis B's
   detail sits beside them as extensions (allowed by Thesis A's README: "Extensions are free"). Thesis A's code reads
   the shared fields; Thesis B's code reads everything.

## 2. Record types

| record_type | one per | Thesis A's fields (shared minimum) | Thesis B's extensions | derived? |
|---|---|---|---|---|
| `run_start` | run | `log_schema`, `schema_id`, `arm`, `treatment`, `agent_config_id`, `replication`, `run_kind`, `launcher`, `sample_manifest_sha256`, `unit_order_sha256`, `harness_commit`, `container_digests`, `tool_versions`, `prompt_sha256`, `schema_rendering_sha256`, `models[]` (snapshot rule), `sampling{stage}`, `budgets` | `harness_run_id`, `cell_source`, `model` (harness view), `prompt_version`, `tools_hash`, `code`, `initial_graph_hash`, `pilot`, `settings`, `launcher_name` | no |
| `run_end` | run | `status` completed/void, `void_reason`, `final_graph_state_sha256`, `global_validator`, `export`, `resume_count` | `harness_status` (9 values), `final_graph_hash`, `resources` (wall clock, tokens, cost estimate with price-table id), `counts`, `failure_class`, `error`, `warnings` | no (yes if the raw log lacks a run end: void/unknown) |
| `source_start` | source unit | `unit_id`, `order_index`, `unit_sha256`, `source_sha256`, `stratum`, `jurisdiction`, `authority_class`, `bucket`, `chunk_rule`, `selection_seed`, `retrieval_date` (the unit's manifest row, copied) | `modality`, `segments_total` | yes |
| `source_end` | source unit | `unit_id`, `final_status` completed/abandoned, `fatal`, `abandon_reason`, `retried_from_snapshot`, `attempts`, `rejects`, `accepted_writes`, `model_calls`, `wall_clock_s`, `graph_state_sha256` | `harness_final_status` (completed/partial/abandoned/fatal/unknown), `abandoned_items`, `items`, `max_retry_index_until_acceptance`, `resources`, `failure_class` | yes |
| `model_call` | transport attempt of a model call | `call_id`, `stage`, `unit_id`, `cq_id`, `pass_id`, `step_index`, `requested` (temperature, top_p, seed, thinking, reasoning_effort, max_output_tokens, not_effective), `returned_model`, `system_fingerprint`, `tokens` (input, cached_input, output, reasoning; **the only place tokens are recorded**), `finish_reason`, `latency_ms`, `error` | `operation_id`, `attempt_index`, `status`, `requested_model_id`, `response_id`, `usage` (raw provider object), `usage_source`, `tools_requested`, `artifacts`, `error_detail` | no |
| `insert_attempt` | fact candidate in a write-tool call | `attempt_id` (`{run_id}-s{step}-a{n}`), `call_id`, `item_id`, `unit_id`, `stratum`, `step_index`, `retry_index`, `tool_name`, `payload_sha256`, `validator_mode`, `validator_result`, `rejections[rule_id, element, expected, found]`, `engine_result` ok/error/not_sent, `engine_error`, `accepted`, `latency_ms` | `item_revision`, `modality`, `validator_ran`, `engine_detail` (written/duplicate/refused/failed/unconfirmed/not_attempted, assertion ids, entities created), `model_operation_id`, `rejections[].category`, `rejections[].message` | no |
| `tool_call` | tool call | — | `operation_id`, `model_operation_id`, `call_id`, `step_index`, `tool_name`, `tool_role`, `status`, `is_error`, `duration_ms`, `payload_sha256`, `artifacts`, `item_ids`, `error` | no |
| `item` | lifecycle event of a candidate | — | `created` (mentions, predicate, evidence), `validated`, `revised` (correction budget), `revision_refused`, `persisted` (assertion ids), `abandoned` (reason, failure class) | no (abandoned: yes) |
| `reconciliation` | entity slot decision | — | item, role, mention, stage (`match` / `write`), decision, method, chosen entity, candidate list | no (write stage: yes) |
| `intake` | read of the source | — | read index, segments read, cumulative coverage | no |
| `gap_trigger` | gap-detection event | — | `source_reread`, `schema_requested`, `graph_queried`, `validator_feedback` | yes |
| `run_resume`, `cq_attempt` | (SRW only) | copies of Thesis A's definitions, so that an SRW trace carrying them passes kgc-trace/1.1 | — | — |

Field-level definitions, enums and required fields: the JSON Schema. Units: durations in milliseconds (`wall_clock_s`
in seconds, Thesis A's field), tokens as integers as reported by the provider, cost in the price table's currency.

## 3. Coverage of Thesis A's shared minimum (agreement 4.3, open point 12)

| Thesis A field (shared-minimum-log/1) | kgc-trace/1.1 | kgbench today |
|---|---|---|
| run manifest: `run_id`, `schema_id`, `arm`, `treatment`, `agent_config_id`, `replication`, `run_kind`, `launcher` | `run_start.*`, same names | from the cell file (`--cell`); kgbench's own run id in `harness_run_id`, `cell_source` says where the values came from |
| sample manifest hash, unit order hash | `run_start.sample_manifest_sha256`, `run_start.unit_order_sha256` | cell file; unit order = sha256 of the one-element unit list |
| models with version evidence (snapshot rule) | `run_start.models[]` | identifier sent and family; served version, published weights, tokenizer null unless the cell file supplies them |
| prompt hashes, tool versions, container digests, harness commit, schema rendering hash | `run_start.prompt_sha256`, `tool_versions`, `container_digests`, `harness_commit`, `schema_rendering_sha256` | yes (sha256 of the system prompt; kgbench version and tools hash; `{}`; git commit; schema sha256) |
| configured sampling per stage, budgets | `run_start.sampling.construction`, `run_start.budgets` | requested object with `not_sent` for parameters kgbench does not send; tool-call and correction budgets |
| start and end time | `run_start.ts`, `run_end.ts` | yes |
| final graph-state hash, global validator results, export pointer | `run_end.final_graph_state_sha256`, `global_validator`, `export` | sha256 of the final graph file; null (kgbench has no global rules, no canonical exporter) |
| per unit: manifest row | `source_start.*` flat, Thesis A's column names | cell file (null without one) |
| per unit: final status, fatal flag, reason for giving up, attempts, rejects, accepted writes, model calls, wall clock, graph hash | `source_end.*` | yes; `abandon_reason` also `crashed` / `unknown` (requested additions, see review file) |
| per model call: call id, stage, unit or CQ, requested sampling, returned model string, fingerprint, tokens by category, finish reason, latency, error | `model_call.*` | yes; fingerprint null (Anthropic returns none); tokens null when the provider reported none |
| per insert attempt: `attempt_id`, `call_id`, `item_id`, `unit_id`, `source_id`, stratum, step index, retry index, tool name, payload hash, validator mode and result, rejections with rule id/element/expected/found, engine result, accepted, latency | `insert_attempt.*` | yes; `expected`/`found` null where kgbench's validator does not emit them (A0's must); a write refused before the validator is a `reject` with rule `kgbench:write-precondition` |
| retries until acceptance | per item via `insert_attempt.item_id` + `retry_index`; `source_end.max_retry_index_until_acceptance` | yes |
| tokens or cost with the price table | tokens on `model_call.tokens`; euro computed by Thesis A; `*.resources.cost` is Thesis B's monitoring estimate (extension) | yes (estimate only with a pricing file) |
| per written assertion: the source unit the agent was processing | `insert_attempt.unit_id` + `engine_detail.assertion_ids`; `item/persisted.assertion_ids` | yes |
| per CQ attempt, run resume | `cq_attempt`, `run_resume` (copies of Thesis A's definitions) | not written by kgbench |
| export (canonical assertion form) | `run_end.export` points to Thesis A's exporter output | null |

## 4. Thesis B's extensions (open point 4)

Everything in `item`, `reconciliation`, `intake`, `gap_trigger`, `tool_call`, plus the extension fields listed in
section 2 and the failure classes. These feed the process-quality layer:

| B metric family | records used |
|---|---|
| validity before / after correction, correction effect | `item/validated`, `item/revised`, `item/revision_refused`, `insert_attempt` (item_id, retry_index, validator_result, rejections) |
| identity (link accuracy, duplicate creation, incorrect merge) | `reconciliation`, `item/created.mentions`, `item/persisted.assertion_ids`, final graph export |
| provenance-chain completeness | `item/created.evidence`, `item/persisted`, artefact references |
| technical completion, failure taxonomy | `run_end.harness_status`, `*.failure_class`, `model_call.status`, `tool_call.is_error` |
| resources | `model_call.tokens`, `model_call.usage`, `*.resources`, price table |
| intake coverage and gap detection | `intake`, `gap_trigger` |

## 5. Failure taxonomy v0.1 (draft, not frozen at F3)

`none`, `transport`, `provider_permanent`, `budget_exhausted`, `output_truncated`, `validator_persistent_reject`,
`valid_not_persisted`, `never_validated`, `harness_error`, `cancelled`, `unknown`. Assigned at run, source and item
level by rules in `trace.py` (`_abandon_reason`, `RUN_FAILURE_CLASS`). The taxonomy is Thesis B's component and may
grow after the pilot; the enum in the schema is extended, never reinterpreted.

## 6. What the harness extension in SRW must do (Thesis B's pull requests)

1. Emit `kgc-trace/1.1` records next to SRW's own audit log, or store enough raw events that `trace.py`-style
   derivation is possible. Preferred: a small event writer called from the write tool, the validator and the
   model client.
2. Assign `item_id` at the first write of a fact candidate, return it in the rejection message, accept it on the
   corrected write (Thesis A's README, "The item id").
3. Store full model requests/responses and tool arguments/results as content-hashed files under the run directory.
4. Log the provider-returned model string, fingerprint and tokens on every call, including failed transport attempts.
5. Copy the unit's manifest row into `source_start`; record `launcher`, container digests, the sample manifest hash
   and the models' version evidence in `run_start`.

Acceptance test: the pilot (16 to 20 Nov) produces a trace per run that passes `kgbench trace-validate --shared-minimum`
(both checkers) and whose `source_end` counts agree with Thesis A's own computation of rejects and attempts.

## 7. Changes from version 1.0 (13 Nov) to 1.1 (14 Nov)

Field map and the requests to Thesis A: `experiments/thesis-b/shared-minimum-review.md` in the shared repository.
In one line: `kind` became `record_type`; the shared fields took Thesis A's names; `attempt_id` took Thesis A's form and
`item_id` became its own field; tokens and cost left the insert attempt; the unit's manifest row was flattened;
`run_end.status` and `source_end.final_status` took Thesis A's two-valued enums with the finer harness status beside them.
