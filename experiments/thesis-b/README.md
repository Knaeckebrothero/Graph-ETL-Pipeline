# Thesis B deliverables for F3 (13 Nov 2026)

Specifications that Thesis B contributes to the shared experiment: the instrumentation schema, the agent configurations, and the A0 proposal. Each item names the open point of the demarcation agreement (Draft 1h / reply Draft 1i) it answers. Source of truth is Thesis B's own repository (`kgbench`); this folder carries the copies that Thesis A reviews and the harness implements.

| Deliverable | Open point | Prose | Machine-readable | Check (in kgbench) |
|---|---|---|---|---|
| Instrumentation specification `kgc-trace/1` | 4, 12, 20 b | `event-schema-v1.md` | `kgc-trace-v1.schema.json` | `kgbench trace-export <run> --validate` |
| Agent configurations A0 to A10, pre-selection A1/A2/A6 | 3, 18 | `agent-configs.md` | `agent-configs/A*.json`, `agent-config.schema.json` | `kgbench configs-check` |
| A0 baseline proposal (model, decoding, tools, prompts, budgets, parameters the harness must expose) | A0 agreed | `a0-baseline-proposal.md` | `agent-configs/A0-v1.json` | — |
| A0 prompt template, construction role (slot list, per-arm binding, hash rule, A8 variant) | A0 agreed (templates: both, F3) | `a0-prompt-templates-proposal.md` | — | ids and hashes recorded at F3 |
| Indicator mapping against Thesis A's ten populatability components | 13 | `indicator-mapping.md` | — | confirmed in the Eval 2 reading |
| Eval 2 second-reading checklist | 15 | `eval2-review-checklist.md` | — | filled in 13 to 15 Nov |
| Example trace in `kgc-trace/1` (one prototype run, exported and validated) | 4 | — | `sample.kgc.jsonl` | — |
| Thesis B reply to the demarcation agreement Draft 1h (Draft 1i, supersedes 8 Nov) | 1 to 20 | `reply-draft-1i.md` | — | — |
| Freeze decisions and open items of 13 Nov | — | `DECISIONS-2026-11-13.md` | — | — |
| Written confirmations for the signature of Draft 1i (six items, three corrections) | — | `CONFIRMATIONS-2026-11-13.md` | — | — |

Decisions the harness owner has to make for the schema to be implementable (see `event-schema-v1.md`):

1. A stable `item_id` across self-corrections of the same fact candidate.
2. `attempt_id` = `{run_id}:s{step}:{item_id}:r{retry}`.
3. Validator rejections carry `rule_id`, `element`, `expected`, `found`.

Decisions of 12/13 Nov 2026:

- A0 = DeepSeek V4.1 Flash through the DeepSeek API (alias `deepseek-flash`, served version DeepSeek-V4.1-Flash-1017), thinking mode enabled, effort high proposed; Thesis A adds the catalog row; A0 runs on SRW's openai-chat path, not on the Claude-Code image. No fallback model (Claude Sonnet 5.5 is the drafting family).
- A1 = `gpt-6.1-sol` on Thesis B's own key. Temperature and top-p cannot be set for this model, so A1 deviates from EX-10 by necessity; every call records `temperature: not_exposed`, `top_p: not_exposed` and the reasoning effort actually sent (`medium`).
- A2 = Qwen 3.8 27B on the university GPU. A3 (Gemini 4 family) stays in the list as best effort.
- All other configurations share A0's decoding (temperature 0.2, top-p 1, one seed per replication).
- Harness commitments accepted: stable `item_id`, the `attempt_id` scheme above, validator rejections with `rule_id`, `element`, `expected`, `found`.
