# Agent configurations A0 to A10 (Thesis B; list to freeze 13 Nov 2026, open points 3 and 18)

Source of truth: `src/kgbench/spec/agent-configs/A*.json`, validated by `kgbench configs-check` and
`tests/test_agent_configs.py`. Rules enforced there: every configuration names A0 as baseline; it differs from A0
in exactly one block (model, tooling, prompting, resources); inside tooling/prompting/resources exactly one knob
differs; model variants may change family, id, protocol and hosting together but never decoding; at most three
configurations are pre-selected and they include a model family other than A0's.

## 1. The list

| ID | Block | Knob (`varies`) | Value vs A0 | Stage | Pre-registered expectation | Harness must expose |
|---|---|---|---|---|---|---|
| A0-v1 | — | — | baseline | — | — | see `a0-baseline-proposal.md` §6 |
| **A1-v1** | model | `model.family` | openai (GPT-6 family, pinned) | — | treatment ranking stable; cost/token and reject rate differ | catalog entry |
| **A2-v1** | model | `model.family` | open-weight Qwen3.8-27B, self-hosted (fixed 13 Nov; Gemma 4 is Thesis A's anchor family and no longer an alternative) | — | more rejects, more abandoned items, lower cost, more transport failures | catalog entry, GPU time |
| A3-v1 | model | `model.family` | google (Gemini 4), optional | — | as A1 | catalog entry |
| A4-v1 | tooling | `tooling.max_self_corrections` | 0 | validation | validity after = before; more abandoned; fewer calls | `max_self_corrections` |
| A5-v1 | tooling | `tooling.max_self_corrections` | 1 | validation | between A4 and A0 (dose) | same |
| **A6-v1** | tooling | `tooling.validator_mode` | log_only | validation | higher write rate, lower conformance; hidden errors visible | log-only switch (offered by A) |
| A7-v1 | tooling | `tooling.read_tools` | false | reconciliation | duplicates and wrong reuse; strict scores drop, lenient do not | read-tool group switch |
| A8-v1 | prompting | `prompting.examples` | dev_set_v1 (template id changes with it) | extraction | fewer first-attempt rejects | template variant |
| A9-v1 | prompting | `prompting.source_delivery` | chunked | intake | qualifiers lost across chunk boundaries | delivery mode + chunk size |
| A10-v1 | resources | `resources.budget_scale` | 0.5 | — | more budget aborts, lower recall, lower cost | per-source budgets |

Bold = pre-selected for Thesis A's robustness check (open point 18): A1, A2, A6. Fallback: A4 replaces A2 if no
open-weight model with tool calling is hostable by 13 Nov. Rationale follows Thesis A's diversity order: model
family first (two), then tooling (one that opens the closed loop), prompting last.

## 2. Cells (open point 3; numbers fixed 21 Nov from pilot cost)

```
pre-selected A1, A2, A6:   3 × 4 treatments × PG × up to 3 reps  = up to 36 runs
remaining A3–A5, A7–A10:   7 × S4 (+ S1 contrast) × PG × up to 2  = up to 28 runs
RDF, only if go on 20 Nov: 3 × 4 × RDF × 1                         = up to 12 runs
```

Each run processes the whole frozen sample. Inclusion rule for Thesis A's check (agreement 4.1): a configuration
enters only if its runs completed on all four treatments with complete logs; exclusions are listed with the reason.

## 3. Which pairs answer which question

| Question | Comparison | Thesis B layer |
|---|---|---|
| Does the model family change the treatment ranking? | A0 vs A1 vs A2 (vs A3) | all |
| Does self-correction repair what the validator rejects? | A4 vs A5 vs A0 | validity before/after, correction effect |
| What does the closed loop hide? | A0 vs A6 | insertion quality, export conformance |
| Do read tools prevent identity errors? | A0 vs A7 | identity (strict vs lenient) |
| Do examples reduce friction? | A0 vs A8 | validity before correction |
| Does chunking lose qualifiers? | A0 vs A9 | extraction quality (strict facts) |
| What does quality cost? | A0 vs A10 | resources, technical completion |

## 4. Dropped, with reason

- Fixed-pipeline controller: needs an own harness image (swappable harness); harness is Thesis A's.
- Schema retrieval: changes the schema input, which agreement 4.5 keeps unchanged.
- Validator feedback pass/fail only: changes Thesis A's validator messages.
- Reasoning level: not comparable across model families.

## 5. Versioning

A configuration file is immutable once frozen (`status: frozen`). Any change after F3 produces `-v2` and new
`run_id`s (agreement 4.6). `run_start.agent_config_id` references the exact version.
