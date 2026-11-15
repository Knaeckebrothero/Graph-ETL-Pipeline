# A0 baseline: Thesis B's proposal for the joint decision on 13 Nov 2026 (F3)

Agreement 4.5: A0 = model and version, tool set, prompts, retry policy, decoding. Agreed by both, then frozen.
Machine-readable: `src/kgbench/spec/agent-configs/A0-v1.json`. Status: proposal.

## 1. Model

Criteria, in this order: (1) available in the SRW catalog with tool calling and usage reporting; (2) pinned
snapshot id so that a rerun in January uses the same weights; (3) cost of one full-sample run × 24 Thesis A cells
× Thesis B's cells within the credits; (4) a second model family must remain affordable for A1; (5) a harness path
that speaks the model's API shape (the Claude-Code image speaks anthropic-messages only).

| Option | For | Against |
|---|---|---|
| **DeepSeek V4.1 Flash, pinned snapshot** (proposal since the call of 12 Nov, Thesis A's suggestion) | open weights (reproducible by weights hash); cheapest per token, so A4 to A10 and Thesis A's 24 cells stay within the credits; openai-chat, exposes temperature and top-p | tool-calling reliability unknown until the pilot; does not run on the Claude-Code harness image (anthropic-messages only), so the harness path is decided on 13 Nov; provider fingerprint only if the host exposes it |
| Claude Opus 5.5 | highest quality | 24 + ~60 full-sample runs likely exceed the budget; quality ceiling hides configuration effects |
| GPT-6 family | second family available as A0 instead of A1 | less pilot continuity; usage fields differ (reasoning tokens) |
| Open-weight on the university endpoint | free per token; reproducible weights | tool-calling reliability unknown until the pilot; GPU scheduling; would make A2 the baseline and lose the "mainstream API" baseline |

Decision 13 Nov (agreement Draft 1i, 4.5): A0 is DeepSeek V4.1 Flash through the DeepSeek API, alias `deepseek-flash`, served version DeepSeek-V4.1-Flash-1017, thinking mode enabled (temperature not effective, no seed; effort level proposed high), top-p 1. There is no fallback model: Claude Sonnet 5.5 is the drafting family (EX-14) and excluded from every run agent. If the pilot shows weak tool calling, both authors decide on 21 Nov; the shared-cell rule F.8 and the anchor families GLM-5.3-Flash and Gemma 4 31B stay untouched.

## 2. Decoding

Thesis A fixed the values in register session 7 (EX-10, 4 Nov) and Thesis B accepts them: construction and S2
induction at `temperature 0.2`, `top_p 1`, one fixed `seed` per replication; querying at `temperature 0` with
pass-specific seeds (Thesis A's query harness). Every sampling parameter the pinned snapshot exposes is fixed at F3
and recorded per call; a parameter the snapshot does not expose is recorded as `not_exposed, provider default in
force`. Reasoning setting: provider default, identical for A0 and every A1 to A10 (model variants keep A0 decoding;
checked by `tests/test_agent_configs.py`). `max_output_tokens`: set with the templates. The returned model id and
fingerprint are logged per call (`model_call.returned_model`, `model_call.system_fingerprint`); a mid-run change
is a deviation. Goal is auditability, not bit-level reproducibility.

## 3. Tool set

- Write tool per arm (Cypher in PG; SPARQL in RDF), owned by Thesis A.
- Validator in reject mode: local per-insert rules with uniform error messages carrying `rule_id`, `element`,
  `expected`, `found`; global end-of-run rules reported in `run_end.global_validator`.
- Read tools: graph query (existing entities, counts) and schema introspection. A7 switches this group off, so
  the harness exposes it as one switch.
- Transport and task retry (EX-12): one retry per source unit, resume from the snapshot taken after the previous
  unit; void runs are kept and marked.
- Retry policy: at most two self-corrections per item, then the item is logged as failed (`item/abandoned`,
  failure class `validator_persistent_reject`). The limit is a parameter (`max_self_corrections`), because A4/A5 set
  it to 0/1.

## 4. Prompts

Three frozen templates, one per agent role (EX-06, EX-07): construction, querying, S2 induction. Each is filled only
in the schema slot by Thesis A's renderer with a per-arm query-language binding. No domain examples. Thesis B drafts
the construction template (system plus per-source user part, `docs/spec/a0-prompt-templates-proposal.md`); the
querying and induction templates are Thesis A's roles and Thesis B reads them before F3. Template ids and hashes go
into `run_start.prompt_hashes`. A8 uses a construction variant with its own template id; A9 changes only how the
source unit is delivered inside the task (sub-chunks), not the template text.

## 5. Budgets

Per source unit (EX-12: one agent task per unit, fresh context, persistent VM and store per cell): maximum tool
calls, maximum model turns and a token budget, absolute values set from the pilot's observed distribution (proposal:
2 × the pilot's 95th percentile). The workspace-listing rule (what the agent sees of the VM workspace at task start)
is Thesis A's and is recorded in `run_start.settings`. A10 halves them (`budget_scale 0.5`). Transport retries:
fixed policy identical in every configuration (proposal: 2 retries, exponential backoff), logged per attempt.

## 6. Parameters A0's harness must expose (union of `requires_from_harness` in the configuration files)

| Parameter | Used by |
|---|---|
| model catalog entry (model id, protocol, endpoint) | A1, A2, A3 |
| `max_self_corrections` | A4, A5 |
| `validator_mode` reject / log-only | A6 |
| read-tool group on / off | A7 |
| template variant id (few-shot from development sources) | A8 |
| source delivery whole / chunked (+ chunk size) | A9 |
| per-source tool-call and turn budgets | A10 |
| provider-returned model id, fingerprint, sampling parameters and usage per call; source-unit manifest fields; full payload artefacts; stable `item_id` across corrections | all (instrumentation) |

## 7. What Thesis B needs recorded for A0 at F3

`agent_config_id = A0-v1`, model catalog entry, template ids with hashes, validator rule set version, budgets,
decoding settings, SRW commit and harness image digest. One JSON file in the shared repository, hashed; the same
file is referenced by every `run_start`.
