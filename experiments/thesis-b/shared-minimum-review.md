# Thesis B's review of `shared-minimum-log/1` and the realignment of `kgc-trace/1` (version 1.1, 14 Nov 2026)

Agreement 4.3 (version 1.0): Thesis A specifies the shared minimum as the JSON Schema `shared-minimum-log/1`; Thesis B's
`kgc-trace/1` carries these fields under Thesis A's names from its version 1.1, so that one file passes both checkers.
This file records what Thesis B checked, what it changed, and the few places where it asks Thesis A for a small change.

Reviewed: `experiments/log-schema/` on branch `inputs-2026-11-13` at commit b51f227 (schema, README, `validate_log.py`,
examples), 14 Nov 2026.

## 1. The six departures from the agreement's field list

| # | Departure (Thesis A's README) | Thesis B | What version 1.1 does |
|---|---|---|---|
| 1 | Tokens once per model call; an attempt points to its call | agreed | `insert_attempt.tokens` and `insert_attempt.cost` removed; `insert_attempt.call_id` and `tool_call.call_id` point to the `model_call`; `model_call.tokens` in Thesis A's four categories, computed from the provider's own fields (`cached_input` is a part of `input`), the raw provider object stays in `model_call.usage` |
| 2 | No euro field | agreed | cost stays only in `source_end.resources.cost` / `run_end.resources.cost` as Thesis B's monitoring estimate with `price_table_id` (an extension, as the README allows) |
| 3 | No correctness on `cq_attempt` | agreed; Thesis B writes no `cq_attempt` | `cq_attempt` and `run_resume` are in the version 1.1 enum as copies of Thesis A's definitions so that an SRW trace carrying them passes kgc-trace/1.1 too |
| 4 | `unit_id` beside `source_id` | agreed | `unit_id` on `source_start`, `source_end`, `model_call`, `insert_attempt`; `source_id` stays in the envelope as the file id. kgbench: one unit per file, `unit_id = source_id` |
| 5 | `K2`, `K3` in the agent slot of `run_id` | agreed | no effect on Thesis B's records |
| 6 | `item_id` on every insert attempt, one record per fact candidate (Thesis B's request, in since b51f227) | yes | `insert_attempt.item_id`, stable across the self-corrections of one candidate; `attempt_id` keeps Thesis A's form `{run_id}-s{step}-a{n}` and no longer contains the item id |

## 2. Field map, version 1.0 to 1.1

| Version 1.0 | Version 1.1 (Thesis A's name) | Note |
|---|---|---|
| `kind` | `record_type` | envelope |
| `trace_format: kgc-trace/1` | `trace_format: kgc-trace/1.1`, plus `run_start.log_schema: shared-minimum-log/1` | |
| `attempt_id = {run_id}:s{step}:{item_id}:r{retry}` | `attempt_id = {run_id}-s{step}-a{n}`, `item_id` as its own field | agreed 13/14 Nov |
| `source_start.manifest{...}` | flat: `unit_id`, `order_index`, `unit_sha256`, `source_sha256`, `stratum`, `jurisdiction`, `authority_class`, `bucket`, `chunk_rule`, `selection_seed`, `retrieval_date` | copied from the sample manifest, never derived |
| `run_start.model{}` | `run_start.models[]` (role, family, access type, identifier sent, served version, published weights, tokenizer, runtime) | snapshot rule; the harness view stays as extension `model` |
| `run_start.prompt_hashes` | `run_start.prompt_sha256` (+ extension `prompt_version`) | |
| `run_start.tools_hash` | `run_start.tool_versions` (+ extension `tools_hash`) | |
| `run_start.code.git_commit` | `run_start.harness_commit` (+ extension `code`) | |
| `run_start.pilot` | `run_start.run_kind` main/pilot/probe (+ extension `pilot`) | |
| `run_start.launcher` (free string) | `run_start.launcher` A/B (+ extension `launcher_name`) | |
| `run_start.schema_sha256` | `run_start.schema_rendering_sha256` (+ `schema_sha256` kept) | |
| `model_call.sampling` | `model_call.requested` with `thinking`, `reasoning_effort`, `not_effective` | `not_sent` marks a parameter the harness did not send (provider default), distinct from Thesis A's `not_exposed` |
| `model_call.response_model_id` / `response_fingerprint` / `stop_reason` | `returned_model` / `system_fingerprint` / `finish_reason` | |
| `model_call.usage` | `model_call.tokens` {input, cached_input, output, reasoning} (+ raw `usage` kept) | |
| `model_call.error{}` | `model_call.error` (message) + extension `error_detail{}` | |
| `insert_attempt.validator{result, rejects}` | `validator_mode`, `validator_result`, `rejections[rule_id, element, expected, found]` (+ `category`, `message`) | |
| `insert_attempt.engine{}` | `engine_result` ok/error/not_sent, `engine_error`, `accepted` (+ extension `engine_detail{}` with written/duplicate/refused/failed/unconfirmed/not_attempted, assertion ids, entities created) | a write the tool refused before the engine is `not_sent` |
| `insert_attempt.tokens`, `.cost` | removed (departures 1, 2) | |
| `source_end.final_status` (5 values), `fatal_failure`, `accepted` | `final_status` completed/abandoned, `fatal`, `abandon_reason`, `retried_from_snapshot`, `accepted_writes`, `model_calls`, `wall_clock_s`, `graph_state_sha256` (+ extension `harness_final_status`) | |
| `run_end.status` (9 values), `final_graph_hash` | `status` completed/void, `void_reason`, `final_graph_state_sha256`, `export`, `resume_count` (+ extensions `harness_status`, `final_graph_hash`) | |

Everything else of version 1.0 (`tool_call`, `item`, `reconciliation`, `intake`, `gap_trigger`, `origin`, `resources`,
failure classes) is unchanged and stays an extension in Thesis A's sense.

## 3. Where kgbench cannot fill Thesis A's non-null fields

Thesis A's schema does not allow `null` for the cell and manifest values (`run_id` pattern, `arm`, `treatment`,
`replication`, `sample_manifest_sha256`, `unit_order_sha256`, `harness_commit`, `stratum`, `jurisdiction`,
`authority_class`, `bucket`, `chunk_rule`, `selection_seed`, `retrieval_date`, `payload_sha256`, `latency_ms`,
`wall_clock_s`, `graph_state_sha256`). That is right for SRW, where the launcher has the sample manifest. kgbench has no
manifest, so its exporter takes these values from a cell file (`kgbench trace-export --cell`) and says in
`run_start.cell_source` whether they came from the harness, were supplied from the manifest, or are placeholders.

`sample.kgc.jsonl` in this folder was exported with `sample-cell.placeholder.json`: the kgbench prototype run on a JSON
graph store is labelled `PG-S4-A0-r1-probe1` so that Thesis A's checker can be run on it. The labels, the manifest hash
and the unit row are placeholders and are marked as such (`cell_source: placeholder`). The content of the trace
(calls, tool calls, candidates, validations, writes) is the real run.

## 4. Small changes Thesis B asks of Thesis A

These do not block anything; version 1.1 passes Thesis A's checker as it is.

1. **`source_end.abandon_reason`**: the enum `budget_exhausted | crashed_twice | resumed_cell` has no value for a unit
   given up after one crash that is not retried, or for a truncated raw log. Thesis B writes `crashed` and `unknown`
   there and asks that both be added. Until then an abandoned kgbench unit fails Thesis A's checker on this one field.
2. **`requested_parameter`**: next to `not_exposed` (the snapshot has no such parameter) Thesis B uses `not_sent`
   (the harness did not send the parameter, provider default in force). Both are strings and pass the schema; the
   README could name the second value so that the two are not confused.
3. **`model_call.latency_ms`, `source_end.wall_clock_s`, `source_end.graph_state_sha256`, `insert_attempt.payload_sha256`**
   are non-nullable. Fine for SRW. If a crashed run has no duration, the harness has to write 0 or omit the record;
   Thesis B suggests allowing `null` (unknown, never zero, as the README itself says).
4. **`validator_result` for a write the tool refused before the validator ran** (kgbench: "latest revision not
   validated"): Thesis B records it as `reject` with `rule_id: kgbench:write-precondition` and `engine_result: not_sent`,
   because the refusal is a rule of the harness. If Thesis A prefers a third value, Thesis B follows.

## 5. How to check

```bash
pip install jsonschema
python3 experiments/thesis-b/validate_trace.py experiments/thesis-b/sample.kgc.jsonl \
    --shared-minimum experiments/log-schema/shared-minimum-log.v1.schema.json      # 0 errors, 0 errors
python3 experiments/log-schema/validate_log.py experiments/thesis-b/sample.kgc.jsonl  # Thesis A's checker: 0 errors
```

State on 14 Nov 2026: both commands report 0 errors on the sample (77 records: 9 insert attempts, 5 model calls, the
rest extension records that Thesis A's checker counts as such).
