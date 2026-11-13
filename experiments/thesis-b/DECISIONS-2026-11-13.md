# F3 freeze, 13 Nov 2026: decisions and open items (Thesis B view)

Written so that the freeze can be closed in writing. Items under "decided" come from the calls of 12 and 13 Nov; items under "open" need Thesis A's answer.

## Decided

1. **A0** = DeepSeek V4.1 Flash, pinned snapshot. Thesis A adds the catalog row (`model_config_matrix.yaml` has only `deepseek-v4-flash` and `deepseek-v4-pro` so far). A0 runs on SRW's openai-chat path, not on the Claude-Code image.
2. **A1** = `gpt-6.1-sol` on Thesis B's own OpenAI key (budget 500 USD). Temperature and top-p cannot be set for this model; A1 therefore deviates from EX-10 by necessity. Every call records `temperature: not_exposed`, `top_p: not_exposed` and the reasoning effort sent (`medium`).
3. **A2** = Qwen 3.8 27B on the university GPU. **A3** (Gemini 4 family) stays in the list as best effort. A4 to A10 use the A0 model.
4. **Log fields**: `kgc-trace/1` (`event-schema-v1.md`, `kgc-trace-v1.schema.json`). Thesis A's minimum is covered field by field (§3). Harness commitments accepted: stable `item_id` across self-corrections; `attempt_id` = `{run_id}:s{step}:{item_id}:r{retry}`; validator rejections carry `rule_id`, `element`, `expected`, `found`.
5. **Cells** (Thesis A's build queue and query order of 12 Nov): A1, A2, A6 × S1 to S4 × PG × 3 repetitions = 36 graphs, guaranteed in build rounds 1 to 3 (each round: Thesis A's 8 A0 graphs, then Thesis B's 12). RDF: the same three × S1 to S4 × 1 repetition = 12 graphs after the RDF go of 20 Nov. Remaining seven configurations: S4 (+S1) PG, up to 2 repetitions, up to 28 graphs, best effort. Query order: Thesis B's pre-selected cells get pass 1 at position 4 (18,000 + 6,000 tasks, guaranteed); the other seven at position 6, best effort; passes 2 and 3 only after Thesis A's. Cells without pass 1 are reported as `not queried`.
6. **Cost**: A1 on Thesis B's credits; the other Thesis B cells on Thesis A's credits up to a cap fixed on 21 Nov from the pilot cost of one Thesis B cell.

## Open (Thesis A)

- a) **Pilot 16 to 20 Nov** includes one Thesis B configuration (A6 or A4) on S4-PG, so that the cap of 21 Nov has a measured cell cost and the log fields are exercised once.
- b) **Drafting family**: which model family drafts the competency questions? It must not be OpenAI, Qwen, DeepSeek or Gemini (all used by Thesis B).
- c) **Delivery dates**: F2 sample manifest with bucket labels, Eval 2 package (metric cards), hours template (a proposal is in `hours-log-template.csv`), validator rule list.
- d) Merge `thesis-b/f3-spec` directly or via pull request?

## Next from Thesis B

- After a) to d): all configuration files set to `status: frozen`, template ids and hashes recorded, change-log row; Thesis A makes the freeze commit.
- Instrumentation code for the openai-chat path in SRW as a pull request before 16 Nov.
- Offered drafts to relieve Thesis A: catalog row for `deepseek-v4.1-flash`, sample-manifest JSON schema with the point-20 b fields, validator reject vocabulary, pilot job manifest for A6/A4 on S4-PG. Thesis A keeps ownership and decides.
