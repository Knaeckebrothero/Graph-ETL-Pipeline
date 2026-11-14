# Thesis B deliverables for F3 (13 Nov 2026)

Specifications that Thesis B contributes to the shared experiment: the instrumentation schema, the agent configurations, and the A0 proposal. Each item names the open point of the demarcation agreement (Draft 1h / reply Draft 1i) it answers. Source of truth is Thesis B's own repository (`kgbench`); this folder carries the copies that Thesis A reviews and the harness implements.

| Deliverable | Open point | Prose | Machine-readable | Check (in kgbench) |
|---|---|---|---|---|
| Instrumentation specification `kgc-trace/1`, **version 1.1** (carries `shared-minimum-log/1` under Thesis A's names) | 4, 12, 20 b; agreement 4.3 | `event-schema-v1.md` | `kgc-trace-v1.1.schema.json` | `kgbench trace-export <run> --validate --shared-minimum <schema>` |
| Agent configurations A0 to A10, pre-selection A1/A2/A6 | 3, 18 | `agent-configs.md` | `agent-configs/A*.json`, `agent-config.schema.json` | `kgbench configs-check` |
| A0 baseline proposal (model, decoding, tools, prompts, budgets, parameters the harness must expose) | A0 agreed | `a0-baseline-proposal.md` | `agent-configs/A0-v1.json` | — |
| A0 prompt template, construction role (slot list, per-arm binding, hash rule, A8 variant) | A0 agreed (templates: both, F3) | `a0-prompt-templates-proposal.md` | — | ids and hashes recorded at F3 |
| Indicator mapping against Thesis A's ten populatability components | 13 | `indicator-mapping.md` | — | confirmed in the Eval 2 reading |
| Eval 2 second-reading checklist | 15 | `eval2-review-checklist.md` | — | filled in 13 to 15 Nov |
| Example trace in `kgc-trace/1.1` (one kgbench prototype run; cell labels are placeholders, see `shared-minimum-review.md` §3) | 4 | — | `sample.kgc.jsonl`, `sample-cell.placeholder.json` | `validate_trace.py`, Thesis A's `validate_log.py` |
| Checker: schema plus cross-record rules (ordering, counts, stable `item_id`, Thesis A's `attempt_id` form, rejections on reject, `call_id` links); `--shared-minimum` also runs Thesis A's schema and cross-record rules | 4, 12 | — | `validate_trace.py` | `pip install jsonschema` |
| Review of `shared-minimum-log/1`: the six departures, the field map 1.0 to 1.1, where kgbench needs a cell file, four small requests to Thesis A | agreement 4.3 | `shared-minimum-review.md` | — | — |
| **Shared harness validator**, core version 0.1.0 (agreement 3.2, Thesis B's since 14 Nov): common-scope rules CS-01 to CS-07 of card P.3 on canonical form, treatment-scope rules TS-01 to TS-05 from `schemas/treatments/*/declaration.json`, reject and log-only mode, `insert_attempt` log fields and `run_end.global_validator`; draft canonical-form schema for Thesis A to confirm; what still needs Thesis A's inputs is listed in its README §5 | validator (3.2), P.3, P.4, P.9 | `validator/README.md` | `validator/*.py`, `validator/canonical.schema.json` | `python3 -m pytest experiments/thesis-b/validator/tests` (26 tests with Thesis A's declarations present, 20 without) |
| Thesis B reply to the demarcation agreement Draft 1h (Draft 1i, supersedes 8 Nov) | 1 to 20 | `reply-draft-1i.md` | — | — |
| Freeze decisions and open items of 13 Nov | — | `DECISIONS-2026-11-13.md` | — | — |
| Written confirmations for the signature of Draft 1i (six items, three corrections) | — | `CONFIRMATIONS-2026-11-13.md` | — | — |

Decisions the harness owner has to make for the schema to be implementable (see `event-schema-v1.md`):

1. A stable `item_id` across self-corrections of the same fact candidate (in `shared-minimum-log/1` since 14 Nov).
2. `attempt_id` keeps Thesis A's form `{run_id}-s{step}-a{n}`; `item_id` is its own field on every insert attempt (agreed 13/14 Nov; replaces Thesis B's earlier `{run_id}:s{step}:{item_id}:r{retry}`).
3. Validator rejections carry `rule_id`, `element`, `expected`, `found`.

Decision of 14 Nov 2026 (agreement version 1.0, 3.1/3.2): the harness validator moved from Thesis A to Thesis B. Thesis B builds it (`validator/`); Thesis A fixes what it checks (common-scope rule list of the technique, constraints of each realisation) and builds the handover point in the write tools. Frozen at F4 (23 Nov) with Thesis A's rule list.

Decisions of 12/13 Nov 2026:

- A0 = DeepSeek V4.1 Flash through the DeepSeek API (alias `deepseek-flash`, served version DeepSeek-V4.1-Flash-1017), thinking mode enabled, effort high proposed; Thesis A adds the catalog row; A0 runs on SRW's openai-chat path, not on the Claude-Code image. No fallback model (Claude Sonnet 5.5 is the drafting family).
- A1 = `gpt-6.1-sol` on Thesis B's own key. Temperature and top-p cannot be set for this model, so A1 deviates from EX-10 by necessity; every call records `temperature: not_exposed`, `top_p: not_exposed` and the reasoning effort actually sent (`high`, the same effort level as A0's thinking mode).
- A2 = Qwen 3.8 27B on the university GPU. A3 (Gemini 4 family) stays in the list as best effort.
- All other configurations share A0's decoding (temperature 0.2, top-p 1, one seed per replication).
- Harness commitments accepted: stable `item_id`, Thesis A's `attempt_id` form with `item_id` beside it, validator rejections with `rule_id`, `element`, `expected`, `found`.

## Checking a trace

```bash
pip install jsonschema
python3 experiments/thesis-b/validate_trace.py experiments/thesis-b/sample.kgc.jsonl \
    --shared-minimum experiments/log-schema/shared-minimum-log.v1.schema.json     # 0 errors (kgc-trace/1.1), 0 errors (shared minimum)
python3 experiments/log-schema/validate_log.py experiments/thesis-b/sample.kgc.jsonl  # Thesis A's own checker: 0 errors
```

State on 14 Nov 2026 (version 1.1): the sample passes both checkers. The 59 records Thesis A's checker counts as
"extension" are Thesis B's `tool_call`, `item`, `reconciliation`, `intake` and `gap_trigger` records. The cell labels of
the sample (`PG-S4-A0-r1-probe1`, S4, PG, the unit's manifest row) are placeholders supplied at export, because the
kgbench prototype run is not a cell of the experiment; `run_start.cell_source` says so. Exported with:

```bash
kgbench trace-export runs/<run> --out experiments/thesis-b/sample.kgc.jsonl --agent-config-id A0-v1 \
    --cell experiments/thesis-b/sample-cell.placeholder.json --validate --shared-minimum <Thesis A's schema>
```
