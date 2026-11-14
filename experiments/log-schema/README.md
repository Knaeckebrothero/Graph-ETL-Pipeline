# Shared minimum log fields, version 1 (draft)

**Status: draft of 14 November 2026, not frozen.** The field specification is frozen on 16 November 2026, before the pilot. Thesis B agreed to the fields on 13 November and asked for one addition, `item_id`, which is in since 14 November. Its own event schema (kgc-trace/1) carries these fields under the same names from its version 1.1.

Thesis A computes its populatability and queryability figures from these fields with its own code, which is why Thesis A specifies them (agreement, section 4.3).

| File | What it is |
|---|---|
| `shared-minimum-log.v1.schema.json` | JSON Schema (draft 2020-12), id `shared-minimum-log/1` |
| `validate_log.py` | Checks a log file against the schema and against the rules below |
| `examples/valid.jsonl` | A made-up run with every record type. **Synthetic, not a run result** |
| `examples/invalid.jsonl` | Six records that must fail |

```bash
pip install jsonschema
python3 experiments/log-schema/validate_log.py experiments/log-schema/examples/valid.jsonl     # 0 errors
python3 experiments/log-schema/validate_log.py experiments/log-schema/examples/invalid.jsonl   # 12 errors, exit code 1
```

## Format

- JSON Lines: one JSON object per line, appended as events happen, never edited afterwards.
- Every record has `record_type` and `ts` (UTC, RFC 3339, ending in `Z`). Every record of a run has `run_id`.
- **Extensions are free.** A record type not listed below only needs `record_type` and `ts`. Extra fields on a listed record are allowed. An extension never changes the meaning of a field defined here.
- `null` means unknown or not reported. It never means zero.
- A sampling parameter the model does not expose is written as the string `not_exposed`. The key is never left out.
- No euro amount is logged. Euro is computed afterwards from the tokens and the dated price table. A cost field of Thesis B is an extension.

## Record types

| `record_type` | Written | Agreement 4.3 level | Used for |
|---|---|---|---|
| `run_start` | once, first | run manifest | identity of the run: schema, arm, agent configuration, replication, sample hash, models with version evidence, prompt hashes, configured sampling, budgets |
| `run_end` | once, last | run manifest | status, final graph-state hash, whole-graph validator result, pointer to the canonical export |
| `run_resume` | per resume | run validity | resumes after a VM crash |
| `source_start` | per unit | per source | the unit's row of the sample manifest, under the manifest's column names |
| `source_end` | per unit | per source | final status, fatal flag, reason for giving up, attempts, rejects, wall-clock time |
| `model_call` | per call | cost record | requested sampling, returned model string, fingerprint, tokens by category, finish reason. **The only place where tokens are recorded** |
| `insert_attempt` | per fact candidate in a write-tool call | per insert attempt | `item_id` of the candidate; validator result with rule id, element, expected and found; engine result; retry index |
| `cq_attempt` | per query attempt | per CQ attempt | pass, attempt index, query text, parses, executes, result hash |
| `provider_snapshot` | daily, own file | snapshot rule | stored copy of a provider's update page or model list |

Field-level descriptions are in the schema file.

## Rules across records (checked by `validate_log.py`)

1. `tokens.cached_input` is a part of `tokens.input`, and `tokens.reasoning` is a part of `tokens.output`. Sums use `input` and `output` only, so nothing is counted twice.
2. `insert_attempt.call_id` and `cq_attempt.call_id` name a `model_call` of the same file. Tokens are read from that call and never copied to the attempt: one call can issue several writes.
3. `attempt_id` starts with the `run_id`.
4. `rejections` is non-empty exactly when `validator_result` is `reject`. A write rejected in mode `reject` has `engine_result` `not_sent` and is not accepted. An accepted write has `engine_result` `ok`.
5. `source_end` follows a `source_start` of the same unit. `fatal` is true and `abandon_reason` is set exactly when `final_status` is `abandoned`. `rejects` never exceeds `attempts`.
6. In a `model_call`, `cq_id` and `pass_id` are set exactly when `stage` is `querying`.
7. In a `cq_attempt` without a query (`produced` false), `parses` and `executes` are `null`. A query that executes has parsed.
8. Attempts that share an `item_id` lie in one unit and carry the retry indices 0, 1, 2 in a row.

## The item id

Every fact candidate gets an `item_id` when it is first written. If the validator rejects the write, the rejection message returns the id, and the model resends the corrected write with it. All attempts at one candidate therefore share one id, which is what "retries until acceptance" and the comparison of configurations with zero, one or two self-corrections need. The `attempt_id` keeps its form (run id, step index, attempt index) and does not contain the item id. A harness without an item concept writes `null`.

## Where this draft departs from the field list of the agreement

Thesis B agreed to these five points on 13 November 2026; the agreement states them in its sections 4.2 and 4.3.

1. **Tokens per call, not per attempt.** The agreement lists "tokens in and out" per insert attempt. Here the attempt points to its model call. Reason: one call can issue several writes, and copied tokens would be counted twice.
2. **No `cost` field.** The older field list names a cost per insert attempt; the later cost-record paragraph of the same section says no euro field is logged. This draft follows the later paragraph.
3. **No `correctness` on `cq_attempt`.** Correctness is decided by the scoring protocol after F5 from the stored result, not by the harness at run time. The record stores the result hash and a path to the result instead.
4. **`unit_id` beside `source_id`.** Since the sample is cut into source units, a run processes units. `source_id` is the file a unit was cut from (`sources/manifest.csv`); `unit_id` is the unit (sample manifest). The identifier table of the agreement (4.2) does not list `unit_id` yet.
5. **Identifier of an anchor cell.** The `run_id` scheme has an agent slot (`A0`, `A1`, ...). The scheme does not say what stands there for Thesis A's anchor families. The pattern accepts any letter-digit code; a proposal is `K2` and `K3`, the names of the condition sets.

## Not covered yet

- The canonical export has its own format (one file per run, written by the read-only exporter of the arm). `run_end.export` only points to it.
- The deviation table is kept outside the log.
- The fixture, oracle and probe runs reuse `model_call` and `cq_attempt`; whether they need a field of their own is open until the query harness exists.
