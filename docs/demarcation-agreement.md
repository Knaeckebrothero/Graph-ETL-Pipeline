# Demarcation and interface agreement, Master theses A and B

**Status:** Version 1.0, 14 November 2026, for signature. The content was agreed in the call of 13 November 2026. Thesis B confirmed in writing on 13 November 2026 the six points that Draft 1i had left open (pull request 4 of the shared repository) and on 14 November 2026 the text as a whole, with the addition of an item identifier in the log fields. The document folds in Thesis B's reply of 9 November 2026, which accepts all 20 open points of Draft 1h with one deviation and stated conditions, and the outcomes of the meetings of 12 and 13 November 2026. Draft 1 of 23 October 2026 was prepared by Niklas Hall for the meeting with Rachid Yagubi on 24 October 2026; all changes since then are in the log in section 7. The text of record is the copy in the shared repository (`docs/demarcation-agreement.md`, without matriculation numbers), approved by both authors as a pull request and tagged F1. After signature the document is sent to the supervisors and attached as an appendix to both theses.

**Why this document exists.** Both theses are registered as individual works (Einzelarbeit). They share one empirical substrate, the schema-population runs, so that the runs are executed once instead of twice and both analyses are comparable. Each thesis must remain assessable on its own. This document records who owns which contribution, what exactly is shared, how the shared parts are specified, and how each thesis acknowledges the other. It is the written form of what the two authors have agreed in conversation since October 2026.

## 1. Parties

| | Thesis A | Thesis B |
|---|---|---|
| Author | Niklas Hall | Rachid Yagubi |
| Title | A Technique for Evaluating Knowledge-Graph Schema Fitness across Coverage, Populatability, and Queryability | A Process Model for Evaluating Agent-Driven Knowledge Graph Construction: Design, Instrumentation, and a Case Study in the Waste-Disposal Domain (registered wording) |
| Programme | M.Sc. Wirtschaftsinformatik (PO 2022), Fb 2, Frankfurt UAS | same |
| Supervisors | Prof. Dr. Josef Fink (Betreuer), Prof. Dr. Sebastian Bremm (Korreferent) | same |
| Registration, period | 6 Aug 2026; 15 Sep 2026 to 16 Feb 2027 | Registered 26 Sep 2026; submission 27 Feb 2027 (22 weeks) |
| Object of comparison | Schema treatments (the agent is held fixed) | Agent configurations (the schema is a condition) |
| Primary contribution | An evaluation technique for schema fitness, demonstrated on four schema treatments in two data-model paradigms | A process model of the agent's construction process (intake, extraction, validation, reconciliation) with its own trace events, failure taxonomy and metric catalogue, demonstrated on the same runs; distinct from Thesis A's stage model of schema-induced losses and its metric cards |

## 2. Principle

**Shared research infrastructure, separate research contributions.**

1. Each thesis states its own research questions, designs its own analyses, writes its own prose and is graded alone. There is no shared chapter and no shared text.
2. The shared parts are infrastructure and raw data: the source sample, the run orchestration, the raw run logs and the model and tool baseline. Sharing them saves compute and makes the two analyses comparable. It does not create a joint result.
3. **Disappearance test.** Either thesis stays assessable if the other were withdrawn. Either author can repeat or complete the runs alone with the shared code and specifications. No primary measure of either thesis depends on work that only the other author can do.
4. Every component has exactly one owner (section 3). The owner decides its design and is responsible for its description in their thesis. The other author may use it and must cite it.
5. After a freeze date (section 4.6), shared components change only by agreement of both authors, with the change and its date logged in this document.

## 3. Contribution partition

### 3.1 Owned by Thesis A (Hall)

| Component | Content |
|---|---|
| Evaluation technique | Constructs coverage, populatability, queryability; measurement model; normalisation, Pareto and weight-space analysis; evaluation of the technique, including the perturbation probes |
| Schema treatments | S1 EAV/raw triples, S2 LLM-induced, S3 flat fessi, S4 curated fessi v5.0; one realisation per paradigm (8 in total); characterisation matrix; induction protocol for S2; freeze of all realisations |
| Schema-description protocol | How each treatment is presented to the construction and query agents (schema package) |
| Reference benchmark and competency questions | Tiered reference assertions with provenance; about 500 CQs; codebook; primary annotation and adjudication; freeze. Used by Thesis B as a fixed instrument for its output-quality layer; Rachid Yagubi acts as independent blind second verifier (see 3.3 and 5) |
| Construction harness | Superhuman Remote Worker (SRW), Niklas Hall's own agent framework (superhuman-remote-worker.com), pinned to one commit per run: fresh VM and fresh graph database per agent, multi-provider scheduling; baseline agent configuration A0; write tools for Neo4j/Cypher (existing) and Jena Fuseki/SPARQL (to build); harness validator (local per-insert rules, global end-of-run rules, uniform error messages); read-only exporters to the canonical assertion form |
| Query harness | Querying agent, CQ execution, answer scoring |
| Scoring and analysis code of Thesis A | Everything that turns raw logs and exports into Thesis A's measures |

### 3.2 Owned by Thesis B (Yagubi)

| Component | Content |
|---|---|
| Process model and evaluation methodology | A process model of the agent's construction process (intake, extraction, validation, reconciliation); its layers (extraction quality, process quality, insertion and datatype quality, downstream utility); metric definitions and measurement protocols |
| Agent configurations | A1 to A10 (table in 4.1): their design, rationale and documentation (A0 is the shared baseline, section 3.3) |
| Instrumentation extensions | The event schema kgc-trace/1: log fields beyond the shared minimum (section 4.3), including untruncated model requests, responses and tool payloads as artefacts; implemented as pull requests to the shared harness with Thesis B as author of record, reviewed and merged by Thesis A |
| Failure-mode taxonomy, process and cost analyses | |
| Downstream benchmark | Chatbot benchmark and retrieval conditions on the populated graphs |
| Scoring and analysis code of Thesis B | |

**Agreed (Thesis B's answer to point 2, 9 Nov 2026).** The harness, the write tools and the validator are Thesis A's (section 3.1). Thesis B owns A1 to A10, the event schema, its implementation as pull requests to the shared harness, and its analysis code. Thesis B launches and monitors its own cells through SRW (section 4.7); Thesis A operates the infrastructure.

### 3.3 Shared

| Component | Specified by | Notes |
|---|---|---|
| Source sample | Thesis A (sampling protocol, strata) | Frozen with a SHA-256 manifest; identical in all runs of both theses |
| Baseline agent configuration A0 | Both, before the freeze | DeepSeek V4.1 Flash through the DeepSeek API (section 4.5); model, tools, prompts, validator in reject mode, retry limit; frozen; the fixed instrument of Thesis A and the baseline of Thesis B |
| Run orchestration | Thesis A (code), both (execution) | Runs are launched once; the launcher is recorded in the run manifest |
| Raw run logs | Shared minimum fields by Thesis A, extensions by Thesis B | Write-once, hashed; both authors have read access to all of them |
| Model and tool baseline | Both | Model identifiers and versions, container digests, tool versions, pinned |
| Repository | Thesis A (owner), both (write access) | `Graph-ETL-Pipeline`, private; authorship of every file follows from git history |

- **Reference verification.** The reference assertions and CQs are designed, drafted and primarily verified by Thesis A. Thesis B verifies every item independently and blind to Thesis A's coding (target 100 per cent, minimum the 15 per cent of FR-16), before either party inspects run outputs and never on held-out material before F5. Disagreements are adjudicated by Thesis A and logged with both original codings; the agreement figure is reported in both theses. The reference remains Thesis A's component; Thesis B uses the frozen set as a fixed instrument. Calendar : codebook written in November; calibration sample of about 30 assertions and 50 CQs coded by both in the first week of December 2026; full second pass between the calibration and F5 (28 Dec 2026); agreement reported as percent agreement and Krippendorff's alpha per slot and stratum (BE-18). Two further coding tasks follow the same procedure: the source-preservation sample of input statements (BU-13), verified before F5 together with the reference, and a subsample of the supportedness audit of run outputs (BU-16), coded in January 2027 after F5. The blind pass also checks, per CQ, the rule-based archetype assignment against the codebook rule (BE-14) and the canonical answer set with its witness and the predicted denotation per required assertion (BE-16, BE-17); CQs that fail blind verification are excluded and counted against the stratum target (BE-11). Pre-filled drafts of reference assertions and CQs come from one drafting family that no run agent in either thesis uses: Claude Sonnet 5.5 through the Anthropic API (named 11 Nov 2026, EX-14); Thesis B's configurations avoid that family, and neither thesis applies a language model to run output. In the pilot week Rachid reads the stage figure blind and states which inputs each actor sees; if he cannot, Thesis A adds a BPMN swimlane diagram of the harness at F4 (BU-11). Thesis B's fixtures are generated by Thesis A's per-engine adapters from the JSONL master (BE-09).
- **Conditions of Thesis B, agreed (reply of 9 Nov 2026, points 7, 11, 15, 16, 17 and 19).** Verification: target 100 per cent, minimum 15 per cent; Thesis A delivers the codebook, with slots for the three CQ checks, by 28 November (addition F.6); calibration 1 to 4 December; blind coding sheets; Thesis B's effort is re-estimated after the calibration, fallback the 15 per cent subsample. Blinding rule: until its full pass is done, Thesis B sees only counts, status and cost fields of its own cells. Eval 2 reading: Thesis B reads Thesis A's goal-question-metric tables, metric cards, formulas and the four scenario paragraphs against four checks, at most one day, by 16 November, package from Thesis A on 14 November. Audit second coding (point 17): 29 December to 23 January, about 12 hours, in batches from 29 December to 9 January with codebook slots and one worked example each, after the reference pass; batch sizes fixed at F4. Scored outputs (point 19): Thesis B scores its own layers from 28 December without the Scoring Protocol Specification; before that only completeness checks and count, status and cost figures; Thesis A delivers the canonical exports of Thesis B's cells and the frozen reference with the F5 tag on 28 December and the scored outputs for Thesis B's cells by 8 January. Cost ledger (point 16): both log hours on shared work weekly from 3 November; Thesis B keeps its own file until Thesis A's sheet arrives.

## 4. Technical interface

### 4.1 Run matrix

Factors and levels:

| Factor | Symbol | Levels |
|---|---|---|
| Schema treatment | S | S1 EAV/raw triples, S2 LLM-induced (induced once per arm, frozen), S3 flat fessi, S4 curated fessi v5.0 |
| Data-model paradigm (arm) | R | PG (property graph, Neo4j/Cypher), RDF (Jena Fuseki/SPARQL) |
| Agent configuration | A | A0 baseline; A1 to A10 (Thesis B, table below) |
| Source sample | X | The frozen sample, all of it, in every cell |
| Repetition | r | r1, r2, r3 |

- **Thesis A primary cells:** S × R × A0 × r = 4 × 2 × 3 = 24 construction runs, each followed by the full CQ set. Thesis A is complete with these cells alone.
- **Thesis A anchor cells:** two open-weight anchor families, GLM-5.3-Flash (hosted, Z.ai API) and Gemma 4 26B A4B (run locally on the L40S GPUs; `google/gemma-4-26B-A4B-it`; changed on 13 November 2026 from Gemma 4 31B, because the 26B model is the one deployed there), named on 11 November 2026 and confirmed at F3, run every realisation and arm after the 24 primary cells: floor one replication per family on the full sample with query pass 1 (16 builds, guaranteed), target three replications (best effort); reported as parallel profiles per family, never pooled (EX-11). Neither duplicates one of Thesis B's pre-selected configurations (point 18). Shared-cell rule (Thesis B's addition F.8): an anchor that coincides with a pre-selected configuration runs once, both theses cite the same `run_id`, and the cost is booked once. The anchors do not change Thesis B's matrix.
- **Query passes (agreed 12 Nov 2026):** every CQ is posed three times per replication graph with fresh context and pass-specific seeds (EX-09). The querying week of 4 to 8 January runs in the order of the table "Query order" below; one task is one question asked once against one graph, about 500 questions per pass. Pass 1 is guaranteed for Thesis A's graphs and for Thesis B's three pre-selected configurations, and Thesis B's figures use pass 1 only. This narrows Draft 1h, which promised pass 1 on all of Thesis B's cells before Thesis A's passes 2 and 3. What does not finish is reported as not queried; the completed share is a manifest field. Thesis A's oracle, fixture, equal-budget and probe runs keep their priority (point 14). Task time and throughput per key come from the pilot; the sizes are rechecked on 21 November.
- **Thesis B cells (agreed 12 Nov 2026):** Thesis B's run matrix is as in its reply of 9 November (table "Thesis B's cells" below), up to 76 construction runs; repetitions and the S1 contrast are fixed on 21 November from the pilot cost of one Thesis B cell. Only A1, A2 and A6 cover all four schemas, and only they enter Thesis A's agent-dependence check. The other seven configurations serve Thesis B alone. In RDF, the check includes Thesis B's configurations only if their RDF cells run after the go on 20 November. Otherwise Thesis A reports agent dependence in RDF across its anchor families only. Every run processes the whole frozen sample, canary units included.
- **Use of the other's cells:** Thesis A uses Thesis B's cells (those of the pre-selected A1, A2 and A6) only for a ranking-stability check in the evaluation of its technique, never in its primary measures. Thesis B uses the A0 cells as its baseline. Neither thesis pools cells across arms. Which of Thesis B's configurations enter Thesis A's check is fixed by an inclusion rule written before any results are seen: every configuration whose runs completed on all four treatments, processed at least a declared share of the sample and have complete logs is included; cells excluded on these technical grounds are listed with the reason, and no configuration is excluded for its results. Fallback under Thesis A's control (amended 12 Nov 2026, EX-04): if by F6 fewer than one of Thesis B's configurations per arm passes the rule, or none uses a different model family from A0, the anchor floor (two model families other than A0, all four treatments, full sample, one replication, built and queried at pass 1) meets the fallback in every arm in which it is complete by F6. Only in an arm where it is not complete does Thesis A run a second model family on a stratified quarter of the sample, all four treatments, one repetition, in the buffer of 9 to 16 January, and report the same checks on that subset.
- **Validator:** the harness validator runs in reject mode in every primary cell of both theses (closed-loop population). "Validator in log-only mode" is available as an agent-configuration factor for Thesis B, not as a schema treatment.
- **Perturbation probes (Thesis A):** 6 to 10 post-construction and benchmark probes without new runs, plus 1 to 2 schema perturbations re-run on a small source subset. These are Thesis A's runs; Thesis B may ignore them.
- **Pilot:** a pilot on a small source subset precedes the main runs; pilot runs are logged like main runs and marked `pilot` in the manifest. The pilot populates development sources with all four treatments in both arms, because Thesis A's one permitted pooling round draws candidate omissions from the pilot graphs (BE-07); pilot results are excluded from evidence in both theses. The pilot includes one Thesis B configuration, A6 (validator in log-only mode), on S4 in the PG arm (addition F.3); the pilot is the acceptance test of all log fields (addition F.2).

**Thesis B's cells** (reply of 9 Nov 2026):

| Group | Configurations | Treatments × arm | Repetitions | Runs (max) |
|---|---|---|---|---|
| Pre-selected | A1, A2, A6 | S1 to S4 × PG | up to 3 | 36 |
| Remaining | A3 to A5, A7 to A10 | S4 (+ S1) × PG | up to 2 | 28 |
| RDF (after the go on 20 Nov, if the pilot cost allows) | A1, A2, A6 | S1 to S4 × RDF | 1 | 12 |

**Thesis B's configurations.** Each differs from A0 in exactly one documented way; schema input and decoding stay identical to A0. Preliminary in the reply of 9 November; delivered as configuration files on 13 November (pull request 4) and set to frozen on 16 November together with the prompt templates.

| ID | Difference from A0 | Block |
|---|---|---|
| A1 (pre-selected) | Second API family: GPT-6.1 Sol | model |
| A2 (pre-selected) | Open-weight, self-hosted: Qwen3.8-27B; differs from both anchors | model |
| A3 | Third API family: Gemini 4 (optional) | model |
| A4 (fallback for the pre-selection) | Zero self-corrections | tooling |
| A5 | One self-correction | tooling |
| A6 (pre-selected) | Validator in log-only mode | tooling |
| A7 | No read tools before writing | tooling |
| A8 | Few-shot template, 2 to 3 examples from the development bucket only | prompting |
| A9 | Source unit delivered in sub-chunks inside one task; dropped at F3 if not possible | prompting |
| A10 | Half the per-unit budget | resources |

**Query order** in the week of 4 to 8 January 2027 (agreed 12 Nov 2026):

| Order | What | Tasks | Status |
|---|---|---|---|
| 1 | Thesis A's 24 graphs, pass 1 | 12,000 | guaranteed |
| 2 | Thesis A's 16 anchor graphs, pass 1 | 8,000 | guaranteed |
| 3 | Gold subset (60 questions), passes 2 and 3, Thesis A's 24 graphs | 2,880 | guaranteed |
| 4 | Thesis B's A1, A2, A6, pass 1 | 18,000 (+6,000 if its RDF cells run) | guaranteed |
| 5 | Rest of Thesis A's passes 2 and 3 | about 21,000 | best effort |
| 6 | Thesis B's other seven configurations, pass 1 | about 14,000 | best effort |

### 4.2 Identifiers

| Identifier | Scheme | Example |
|---|---|---|
| `run_id` | `{arm}-{treatment}-{agent}-{rep}` plus `-pilot` or `-probe{n}` where applicable | `PG-S4-A0-r2`, `RDF-S2-A1-r1-pilot` |
| `schema_id` | treatment and arm, with version and freeze date | `S4-PG-v5.0-2026-11-xx` |
| `agent_config_id` | `A0`, `A1`, ... with a version suffix | `A0-v1` |
| agent slot of the `run_id` for Thesis A's anchor cells | `K2` (hosted anchor), `K3` (local anchor) | `PG-S4-K2-r1` |
| `source_id` | the source file, as in `sources/manifest.csv` | |
| `unit_id` | the source unit, as in the sample manifest; a unit is cut from one source file | `u004bb83eb485` |
| `assertion_id`, `cq_id` | from the frozen benchmark | |
| `attempt_id` | `run_id` + step index + attempt index | |
| `item_id` | one per fact candidate, the same for every write attempt at that candidate; SRW assigns it at the first write, returns it in the rejection message, and the model resends it with the correction; `null` for a harness without an item concept | |

A re-run after any change gets a new `run_id`; nothing is overwritten.

### 4.3 Shared minimum log fields

Thesis A specifies these fields because its populatability indicators are computed from them by Thesis A's own code. Thesis B adds fields freely; added fields never change the meaning of the ones below. Format: JSON Lines, one record per event. 

| Level | Fields |
|---|---|
| Run manifest | `run_id`, `schema_id`, `arm`, `agent_config_id`, sample manifest hash, model identifiers and versions, prompt hashes, tool versions, container digests, launcher, start and end time, final graph-state hash, global validator results |
| Per model call | `run_id`, call id, stage, unit or CQ, requested sampling parameters, returned model string, fingerprint where returned, tokens by category (input, cached input, output, reasoning), finish reason, latency, error. Tokens are recorded here and nowhere else |
| Per insert attempt (one record per fact candidate) | `run_id`, `attempt_id`, `item_id`, the call that issued it, `unit_id`, `source_id`, source modality, step index, timestamp, tool name, payload hash, validator result (`pass`, or `reject` with rule id, element, expected, found), engine result, retry index, latency |
| Per source unit | `run_id`, `unit_id`, `source_id`, start and end, attempts, rejects, final status with the fatal flag and the reason for giving up |
| Per CQ attempt (query harness, Thesis A) | `run_id`, `cq_id`, pass id, attempt index, the call that issued it, query text, syntax valid, executes, result hash and stored result, latency. Correctness is decided by the scoring protocol after F5 and is not a log field |
| Export | canonical assertion form of the final graph, one file per run, produced by the read-only exporter of the arm |

Cost record (SC-14, 1 November 2026): the model identifiers in the run manifest are exact snapshots, never aliases (the provider's dated versioned identifier for closed models, the Hugging Face commit hash of the downloaded revision for open weights, the tokenizer version where separate); per insert attempt and per CQ attempt the token categories as the runtime reports them (input, output, reasoning, tool calls) and the number of calls, besides wall-clock time; euro is computed by Thesis A afterwards from one price table frozen at F2 with its date (list price for closed models, cited OpenRouter median for open weights, nearest listed size class labelled imputed), so no euro field is logged. 

Decoding, pass and manifest fields (EX-05, EX-09, EX-10, EX-16; 8 November 2026): per call the requested sampling parameters (temperature, top-p, seed, reasoning budget, maximum output tokens; 'not exposed, provider default in force' where the snapshot has none), the returned model id and the provider fingerprint where returned; per CQ attempt a pass id (1 to 3); the contract fixes these field names, and Thesis B's configurations record their own values. The sample manifest carries per source unit its hash, modality stratum, jurisdiction, authority class, bucket (development, pilot, held-out, canary), the chunk-size rule and selection seed, the retrieval date and the source-version hash. 

**Schema file of the shared minimum.** Thesis A delivers the shared minimum as a JSON Schema, `shared-minimum-log/1`, with example records and a checker, in `experiments/log-schema/` of the shared repository (state of 14 November 2026; frozen on 16 November). The table above is the field list as agreed on 13 and 14 November 2026. Against Draft 1h it differs in six points, all accepted by Thesis B: tokens are recorded once per model call and an attempt points to its call (one call can issue several writes); no cost field (the cost record logs no euro); no correctness field on a CQ attempt; `unit_id` for the source unit beside `source_id` for the file it was cut from; `K2` and `K3` in the agent slot of the `run_id` for Thesis A's anchor cells; and, at Thesis B's request, `item_id` on every insert attempt, with one record per fact candidate. The form of the `attempt_id` is unchanged.

**Thesis B's extensions (kgc-trace/1).** Thesis B's event schema kgc-trace/1 (JSON Lines, JSON Schema with tests) carries the shared minimum under Thesis A's field names and adds, per level: model call (operation and attempt id, returned model id and fingerprint, requested sampling parameters or `not_exposed`, tokens by category, latency, stop reason, full request and response as artefacts); tool call (tool name, full arguments and result as artefacts, error flag, duration, step index); candidate (revision, evidence with source, locator and quote, validator result per revision with rule ids, correction outcome); reconciliation (mention decision, candidates with scores, method); source unit (the manifest fields on `source_start`); intake and gaps; termination (run end status, final status per source, failure class of Thesis B's taxonomy); cost (price-table id, computed cost, completeness flag). Every record carries `run_id` and `source_id`; every number carries unit and origin; missing means unknown, never zero. The field list was delivered on 13 November as kgc-trace/1 (pull request 4); its version 1.1, which carries the shared minimum under Thesis A's field names so that one file passes both checkers, follows as a pull request by 15 November. The field specification is frozen on 16 November, before the pilot (F3, second step); non-semantic implementation fixes are allowed until F4, each logged as a row in Thesis A's deviation table (addition F.7).

**Indicator names and cost (points 12 and 13).** Only tokens, cost, wall-clock time and the validator-reject count share a name and a metric card between the theses; Thesis A's pre-commit friction (diagnostic) and Thesis B's correction effect (scored) are listed side by side in Thesis B's mapping table, delivered on 13 November (pull request 4) and confirmed in the Eval 2 reading. Euro is computed by Thesis A from the dated price table; Thesis B's own cost field is a monitoring lower bound only.

**Snapshot rule as it can be applied.** On 12 November 2026 neither DeepSeek, Z.ai nor OpenAI offers a dated identifier for the models used here; `deepseek-flash` is an alias that was re-pointed once, on 17 October 2026. The cost record therefore holds, for a model served under an alias: the alias as sent, the served version named in the provider's documentation on the run date, the repository and commit of the matching published weights (labelled as published, not verified as served), and per call the returned model string, the `system_fingerprint` where returned and the request timestamp. A daily job in the run window stores the provider's update page and model list. A change of the returned model string or fingerprint inside a cell, or an announced re-pointing in the run window, is a row in the deviation table; the runs continue on what the API serves, and no other host is substituted (Thesis A's decision of 13 November, EX-11 as amended). Additional fields: cached prompt tokens as their own token category, finish reason, the thinking setting. Thesis B applies the same rule to `gpt-6.1-sol`.

### 4.4 Source sample

Defined by Thesis A's sampling protocol: stratified by source type over the catalogued fessi sources, with a written target-population statement. Frozen with a manifest of SHA-256 hashes and licences. The same sample is used in every cell of both theses. A fixed small subset serves pilot and probe runs. Jurisdictions per Thesis A's decision BE-02: Frankfurt and the Frankfurt UAS campus at least 70 per cent of sources, reference assertions and CQs; two contrast municipalities, Munich and Berlin, about 15 per cent each (Prof. Fink's confirmation is requested by Thesis A and needed before the full manifest is hashed on 18 November); four modality strata (numeric tabular, tabular-categorical, catalogue, free text). Thesis B confirmed on 9 November 2026 (point 10). Thesis A's decisions of 13 November 2026 on the draft cut: the shares are counted over the units tied to a city or the campus, and units of federal law and of the product catalogue are reported as their own groups; statutes and federal acts are sampled inputs; city statistics tables are out; each modality stratum has eight development and three pilot units, the pilot units disjoint from development, the three numeric pilot units taken from the Munich and Berlin fee statutes; the sample is planned without written campus regulations and without the operators' own guides, for which consent is requested (BE-02 and EX-05 as amended). Source units are self-contained chunks cut once at F2 at a fixed size per modality (a table segment with its header and file identity, a catalogue entry with its category path, a document section with its heading path and jurisdiction), byte-identical in every cell of both theses (EX-05). The manifest contains a 'canary' bucket of synthetic units (15 to 20 invented rules of a fictitious municipality under neutral file names) that every cell of both theses processes as part of the sample; canary results are a leakage diagnostic per agent family and never enter a headline denominator (EX-16).

### 4.5 Agent configuration A0

**A0 is DeepSeek V4.1 Flash** (decided 11 Nov 2026, EX-11 as amended): accessed through the DeepSeek API with Thesis A's own key under the identifier `deepseek-flash`, served version DeepSeek-V4.1-Flash-1017 on 12 November 2026, published weights `deepseek-ai/DeepSeek-V4.1-Flash-1017` at commit `08c8657f02291e11b6f6b63e0e16014f8e748518`; the full record with every decoding parameter is Thesis A's delivery for F3. A0 runs with the provider's thinking mode enabled (Thesis A's decision of 13 November 2026, EX-10 as amended: without thinking the agents are much worse and nobody runs agents in production that way). In that mode the provider ignores temperature and exposes no seed, so the temperatures and seeds named below are requested and recorded as not effective for A0; top-p is sent as 1; the thinking setting, the effort level and the reasoning tokens are recorded per call. The effort level is high, sent explicitly (the provider offers low, high and max). A0 has no fallback model: Claude Sonnet 5.5, which Thesis B's earlier reply had named, is the drafting family and excluded from every run agent; if the pilot shows weak tool calling, both authors decide on 21 November. Thesis B's configurations copy the setting. The three numbers of A0 that need a dry run (maximum output tokens per call, per-unit budget, query timeout) are fixed on 16 November before the pilot starts; Thesis B's configuration A10 (half budget) is defined relative to the per-unit budget fixed then. Agreed by both before the freeze and then fixed: model and version, tool set (write tools per arm, validator in reject mode, read tools), prompts (one frozen template per agent role in which only the schema slot varies, filled by Thesis A's renderer, with a per-arm query-language binding, EX-07), retry policy (at most two self-corrections per turn, then the item is logged as failed), temperature and seeds where the provider allows. Thesis B's configurations A1 to A10 vary from A0 in documented ways and keep the schema input format unchanged so that cells stay comparable. Decoding of A0: construction and S2 induction at T = 0.2 and top-p = 1 where exposed with one fixed seed per replication, querying at T = 0 with pass-specific seeds, every exposed sampling parameter fixed at F3 and recorded per call (EX-10). Each construction task covers one source unit with a fresh context; the task carries the declared-only schema rendering, the unit, the write-tool protocol and a listing of workspace file names and sizes (never their content); a per-unit budget of tool calls and tokens is part of A0, exhaustion is logged as an abandoned unit (EX-12). The S2 induction prompt is a third frozen template at F3 (EX-06). Thesis B's configurations record the same decoding fields with their own values.

### 4.6 Freeze order and dates

Dates set on 23 October 2026 from Thesis A's planning (FR-18, FR-19) and confirmed by Thesis B on 9 November 2026 (point 6); F1 moved to 13 November. Every freeze stores a dated, hashed copy; a later change creates a new version and new run IDs.

| Step | What is frozen | Before | Date |
|---|---|---|---|
| F1 | This document, settled and signed | pilot runs | settled 13 Nov 2026 (was 6 Nov); signed and tagged with version 1.0 of 14 Nov 2026 |
| F2 | Source sample and manifest. In two steps: the development and pilot buckets are hashed by 14 Nov, which is all the S2 induction and the pilot read; the full manifest with the held-out bucket, the contrast municipalities and the canary units is hashed by 18 Nov, before any main run reads it | pilot runs | 13 Nov 2026 (names, price table, snapshot rule); 14 and 18 Nov 2026 (manifest hashes) |
| F3 | In two steps. **13 November:** A0's model, access path and decoding settings (4.5), the names of all model families, and Thesis B's configuration list A1 to A10 with the pre-selection (delivered as files, pull request 4). **16 November, before the pilot starts:** the three prompt templates with their ids and hashes, the three numbers of A0 (maximum output tokens, per-unit budget, query timeout), the log-field specification (`shared-minimum-log/1` with `item_id`, and kgc-trace/1 in version 1.1), and Thesis B's configuration files set to frozen. Non-semantic implementation fixes until F4, logged | pilot runs | 13 and 16 Nov 2026 |
| F4 | Schema realisations: S4 first, then S3 derived from it, S1; S2 induced once per arm. Thesis A's technique v1.0 is tagged in its repository at this date (requirements, metric cards, mappings, renderers, validator rules, binding tables); Thesis B cites the tag it used | main runs | 23 Nov 2026 |
| F5 | Reference benchmark and CQ set. Second tag: equivalence table, matcher rules, scoring protocol; a post-freeze change is logged in Thesis A's deviation table with its action (rerun, re-score or flag), and Thesis B cites the tag it used | first scoring | 28 Dec 2026 |
| F6 | Analysis gate: Thesis B cells that exist by this date enter Thesis A's stability check; later cells do not | Thesis A analysis | 8 Jan 2027 |

- **Run window:** pilot on a small slice 16 to 20 Nov 2026 (RDF go or no-go on 20 Nov); Thesis B's repetitions and the cost cap fixed on 21 Nov; construction runs 23 Nov to 20 Dec 2026, buffer for reruns and Thesis B cells to 1 Jan 2027; Thesis B's cells are built by 1 Jan; querying runs and probes 4 to 8 Jan 2027; the buffers 21 Dec to 1 Jan and 9 to 16 Jan are Thesis A's for probes and reruns (point 14).
- **Other dates agreed:** pull requests for Thesis B's instrumentation in by Sun 15 Nov; codebook 28 Nov; calibration 1 to 4 Dec; exports of Thesis B's cells and the frozen reference with the F5 tag 28 Dec; scored outputs for Thesis B's cells by 8 Jan 2027; audit second coding 29 Dec to 23 Jan.

### 4.7 Operation, storage and access

- **Hardware and execution.** Runs execute through SRW on Niklas Hall's infrastructure (university VM, second server, homelab, desktop GPUs, API credits and subscriptions). Cells are scheduled whenever capacity is free; each cell's configuration, including the pinned model ID, is fixed before scheduling, and the model ID returned by the provider is logged on every call. Both authors may launch runs through the same orchestration; the launcher is recorded in the manifest. If one author drops out, the other completes the matrix alone.
- **Storage.** Raw logs and exports live under `experiments/runs/<run_id>/` in the shared repository, or in a shared write-once store with a hash manifest in the repository if too large for git. Nothing under a `run_id` is edited after the run ends.
- **Access.** Both authors have read access to all raw logs and exports, and either author can pull raw logs and exports in bulk (addition F.5). Thesis B has its own login to SRW on the university server and read access to the raw store from F3; repository write access is given; its instrumentation arrives as pull requests that Thesis A reviews (addition F.1). Thesis A merges them before the pilot on 16 November if they are in by Sunday 15 November; otherwise the pilot runs on the shared minimum fields and the extensions follow before F4 (addition F.2). Each author's analysis code and derived data live in their own folder or repository.
- **Cost (addition F.4).** Thesis A's credits cover Thesis B's cells that run on A0's model (A6, and A4, A5 and A7 to A10 as far as they run) up to a cap fixed on 21 November from the pilot cost, in euro at list price; cells beyond the cap, and all cells on other model families, run on Thesis B's own keys and cost. Calls on Thesis A's DeepSeek key run in the provider's off-peak hours as far as the calendar allows: no new source unit starts on that key in the peak hours (01:00 to 04:00 and 06:00 to 10:00 UTC on weekdays) unless a round is behind schedule, and a running unit finishes; the threshold for 'behind schedule' is fixed on 21 November from the pilot's throughput and is set so that the guaranteed builds of rounds 1 to 3 (Thesis A's 8 and Thesis B's 12 per round) finish by 1 January under the rule; the rule applies to every cell on that key alike, Thesis B's included.
- **Independent deployment (point 1).** SRW with A0 can be deployed by Thesis B alone from the repository (instructions, A0 image digest, configuration). Thesis B's fallback runner kgbench works on a reduced sample; its results are not comparable with SRW cells and never enter Thesis A's robustness check.
- **Build queue (agreed 12 Nov 2026).** One shared build queue in SRW for both theses, 23 November to 1 January, in the priority order of the table below; a round is one build of each of the eight realisations, or of each of Thesis B's pre-selected cells. Rules: (1) when a VM frees up, the highest-priority waiting build starts; best-effort builds may use spare VMs but never hold back a guaranteed build, which takes the next free VM; (2) within a round Thesis A's cells start first, in rotated treatment order, then Thesis B's; (3) the shared resources follow the same list: Thesis A's DeepSeek key (Thesis A's builds and Thesis B's cells on A0's model) and the four local GPUs (Thesis B's Qwen in rounds 1 to 3, then Thesis A's Gemma anchor). Everything guaranteed is built by 1 January. Thesis B launches and monitors its own cells in this queue with their priority (addition F.9); Thesis A's 24 primary graphs and 16 anchor builds are never displaced. To check before F4: that SRW's queue holds a priority per cell; Thesis B's number of repetitions (fixed 21 November) sets the size of rounds 2 and 3.

| Priority | What | Builds | Status |
|---|---|---|---|
| 1 | Round 1: build 1 of Thesis A's 8 realisations, then Thesis B's A1, A2, A6 build 1 | 8 + 12 | guaranteed |
| 2 | Round 2, same pattern | 8 + 12 | guaranteed |
| 3 | Round 3, same pattern | 8 + 12 | guaranteed |
| 4 | Thesis A's anchors (GLM, Gemma), one build per realisation | 16 | guaranteed |
| 5 | Thesis B's A1, A2, A6 in RDF | 12 | guaranteed if the RDF go comes on 20 Nov |
| 6 | Thesis B's other seven configurations | up to 28 | best effort |
| 7 | Anchor builds 2 and 3 | 32 | best effort |

- **Change control.** Changes to shared components after F1 are agreed by both, dated and logged in section 7. No prompt, tool or schema changes during a run.
- **Run order and isolation.** Replication-major rounds: replication 1 of all eight realisations, then 2, then 3; cells start in rotated treatment order as VMs free up, PG and RDF concurrent; Thesis B's cells interleave in the same pattern; Thesis A's anchor cells after all primary cells (EX-12). Each cell runs from a pre-provisioned image with its container digest, with no outbound network except the model gateway (which logs every call, sub-model calls included, for the cost record) and the cell's own store; VM, workspace and graph store persist for the cell with a graph-state hash and snapshot after every unit; a crashed unit is retried once from the pre-unit snapshot, a VM crash resumes from the last snapshot with the resume logged, and a cell that cannot resume reruns under a new run id. The process-deliverable diagram and the stage figure of Thesis A are the figures of the common harness; Thesis B cites them and does not redraw them (BU-11).

## 5. Acknowledgement rules

1. Each thesis names the shared infrastructure and the other author's contribution in one sentence in Chapter 1, in the methods chapter where the shared setup is described, and, if the examination office requires it, in an addendum to the declaration of independent work. Template: *"The construction runs analysed here were executed once and shared with the companion thesis of [name] (in preparation), which evaluates the construction agents on the same runs. The division of contributions is documented in Appendix [X]."*
2. Each thesis describes the shared setup in its own words; no paragraph is copied between the theses.
3. Results of the other thesis are cited, never reproduced as one's own; the ownership table of section 3 may be reproduced in both.
4. Code authorship is the git history of the shared repository. Components listed in section 3 are described as the owner's work; help with another's component is acknowledged, not co-claimed.
5. This document is attached to both theses as an appendix; a short table from section 3 appears in Chapter 1 and the demarcation is discussed in the methods chapter of each thesis.
6. No statement about the similarity of the topics is needed; the theses are related by design and the supervisors know it.

## 6. Approval

| Step | Who | When |
|---|---|---|
| Both authors agree the content | Hall, Yagubi | meetings 24 Oct and 12 Nov 2026; Thesis B's written reply 9 Nov 2026 |
| Signed and dated by both | Hall, Yagubi | version 1.0 of 14 Nov 2026; dates beside the signatures in section 8 |
| Sent to the supervisors of both theses | Hall (A) to Prof. Fink and Prof. Bremm; Yagubi (B) with the F3 result | after signature |
| Question to the examination office whether a declaration beyond the standard one is required | Hall, Yagubi in copy | was due 6 Nov 2026; sent together with the signed agreement |
| Repository copy without matriculation numbers, approved by Yagubi as a pull request, merge commit tagged F1 | Hall, Yagubi | with the signature |

Draft e-mail to the Prüfungsamt Fb 2 (German): *"Sehr geehrte Damen und Herren, Herr Yagubi und ich schreiben zwei getrennt angemeldete Master-Arbeiten (Einzelarbeiten) mit unterschiedlichen Forschungsfragen, die dieselben Experimentläufe als gemeinsame Datengrundlage nutzen. Die Abgrenzung der Beiträge ist schriftlich dokumentiert und den Betreuern bekannt. Ist über die Eigenständigkeitserklärung hinaus eine gesonderte Erklärung oder ein Vermerk erforderlich? Mit freundlichen Grüßen, Niklas Hall."*

## 7. Open points and change log

**The 20 points as Thesis A posed them in Drafts 1 to 1h** (kept as the record of what was asked; the answers follow below the list):

1. Confirm the principle in section 2 and the disappearance test in both directions.
2. Ownership table (section 3): agree every row, in particular instrumentation (3.2) and operation (4.7) against Thesis B's exposé §4.3 and §4.5.
3. Which treatments and arms Thesis B's configurations cover (4.1).
4. Log fields that Thesis B needs beyond the shared minimum (4.3).
5. Hardware, storage location and repository access (4.7).
6. Freeze dates: proposed in 4.6 on 23 October from Thesis A's calendar; confirm.
7. Reference verification (3.3): Rachid as independent blind second verifier of the whole reference set, since Thesis B's output-quality layer needs the same reference; fallback the 15 per cent subsample of FR-16; or no involvement. Effort estimate for 100 per cent: three to four days in December.
8. Thesis B's supervisors, title and registration dates (section 1).
9. Who sends what to whom (section 6).
10. Benchmark jurisdictions and strata (4.4): two contrast municipalities beyond Frankfurt and the campus; confirm, since Thesis B's output-quality layer uses the same reference.
11. Calendar of the reference verification (3.3): calibration in the first week of December, full pass before 28 December; confirm that the effort fits Thesis B's own runs.
12. Shared log fields (section 5), to freeze at F2: per source unit and realisation a fatal-failure flag, validator rejections, retries until acceptance, abandoned units, wall-clock time, tokens or cost with the price table, the provider-returned model ID per call; per written assertion the source unit the agent was processing (BU-14, BU-16); confirm that Thesis B's instrumentation provides them. Extended 1 November (SC-14): the cost record of 4.3, exact model snapshots instead of aliases, token categories and call counts as the runtime reports them, wall-clock seconds; euro is computed by Thesis A from the price table frozen at F2 with its date and applied to every configuration, so the budget scenario's eligibility can be reported for Thesis B's configurations too (EV-13). Confirm that every configuration logs these.
13. Indicator names (section 5): the same indicator carries the same name and metric card in both theses; reconcile Thesis B's process-quality names with Thesis A's populatability components (TE-07) before the F2 log-field freeze.
14. Oracle querying runs (BU-19): Thesis A runs the shared querying configuration A0 against eight oracle graphs after the primary cells, 4 to 8 Jan 2027, with the same logging; confirm that these runs do not enter Thesis B's matrix and that A0 is available for them. The mutation-fixture executions (BU-23: agent queries re-run unchanged on mutant graphs, no agent involved) and the author's gold queries (BU-22) fall in the same window and are Thesis A's alone. The perturbation probe runs of EV-14 (graph and query probes on the outputs of the primary cells; two small schema-perturbation construction runs on 10 to 15 per cent of the development sources, both arms, one replication, A0) fall in the buffers 21 December to 1 January and 9 to 16 January and are Thesis A's alone.
15. Eval 2 second reader (EV-02): between the F2 freeze (13 November) and the pilot (16 November), Rachid reads Thesis A's GQM tables, metric cards and formulas against four checks (unit and denominator, no double counting, forward trace, no domain constant; BU-06); the outcome is logged in Thesis A's change log. Extended 1 November (SC-13, SC-14): the reading package also contains the four decision scenarios with their weight-justification paragraphs. Effort estimate half a day to one day; confirm.
16. Cost ledger (EV-10): both authors log their hours weekly by activity from now; run costs come from the shared log fields of point 12. Thesis A reports the ledger as feasibility evidence for the technique, including Rachid's hours on shared work (verification, instrumentation, reading). Confirm that these hours can be logged and shared.
17. Blind re-trace (EV-11): after the summative scoring (after 8 January, before 25 January 2027), Rachid re-traces a blind subsample of Thesis A's interpretability trace audit (reported aggregate to component to item to failure state to source and run) without seeing the author's trace; subsample size fixed by F4 with the audit sampling. Extended 1 November (EX-15): the re-trace covers 8 of the 40 audited aggregates; in addition Rachid blind-codes 20 per cent of each run-output audit, drawn from the author's sample without the author's verdict (160 matcher decisions, 240 supportedness assertions, 40 fixture verdicts; about 450 items in all), between 29 December and 23 January, after his reference pass. All items come from A0 cells, so none from a configuration he ran; every item is a canonicalised tuple with treatment and arm stripped. Agreement is reported as percent agreement and Krippendorff's alpha per slot, disagreements adjudicated and logged. Effort estimate about 12 hours; confirm.
18. Pre-selected configurations (EV-13): when Thesis B freezes its configuration list, both authors pre-select up to three configurations for maximal diversity (model family first, then tooling, then prompting), before any scored result exists; the selection and its rationale are recorded in this agreement. Those that pass the inclusion rule of 4.1 by F6 enter Thesis A's agent robustness check only, per arm, never pooled; A0 stays the sole baseline. Propose the list freeze together with A0 at F3 (13 November); confirm.
19. Scored outputs and the scoring protocol (SC-19): Thesis A computes no aggregate over any primary cell before its scoring protocol is hashed at F5 (28 December); until then run logs are read only for monitoring, failures and the cost record. Thesis B receives the run manifest, the log contract and the cost-record definitions, never the Scoring Protocol Specification; scored outputs for Thesis B's cells are delivered by Thesis A after F5 as results, together with the budget-scenario eligibility of each configuration in euro at the F2 price table. Confirm that Thesis B's timeline tolerates scored results only after 28 December.
20. Session-7 interface points (EX-05, EX-09 to EX-12, EX-14, EX-16, BU-11, BE-09, BE-11, BE-14, BE-16, BE-17, TE-10, FR-05; 4 to 8 November 2026): (a) query passes: pass 1 guaranteed for Thesis B's cells, passes 2 and 3 as capacity allows in the 4 to 8 January window (4.1); (b) the decoding, pass and manifest fields of 4.3 join the F2 log contract, field names fixed, values Thesis B's own; (c) Thesis A's two open-weight anchor families are chosen not to duplicate Thesis B's pre-selection of point 18, a coinciding cell is executed once and shared (4.1); (d) the drafting family named at F2 is excluded from every configuration of both theses (3.3); (e) replication-major interleaving, one task per unit, the workspace listing and the per-unit budget are part of the run-matrix contract (4.5, 4.7); (f) the canary bucket is part of the shared sample and every cell processes it (4.4); (g) the process-deliverable diagram and the stage figure are cited by Thesis B, and Rachid reads the stage figure blind in the pilot week (4.7, 3.3); (h) Thesis B's fixtures are built by Thesis A's adapters from the JSONL master (3.3); (i) the blind pass checks the archetype rule, the canonical answer set with witness and the predicted denotations (3.3); (j) Thesis A cites Thesis B's configuration list as its condition set K-R and reports Thesis B's use of the reference as a fact, never as validation of the dataset (TE-10, FR-05). Confirm (a) to (j).

**Answers of Thesis B (reply of 9 November 2026) and outcomes of the meeting of 12 November 2026.** All 20 points are accepted; point 3 deviates and points 7, 17 and 19 carry conditions. Points 4, 10, 12, 13, 18 and 20 were preliminary in the reply and are confirmed on 13 November together with A0.

| # | Point | Answer and outcome | Where it now stands |
|---|---|---|---|
| 1 | Principle and disappearance test | Yes. SRW with A0 can be deployed by Thesis B alone; fallback runner kgbench on a reduced sample, never in Thesis A's check | 4.7 |
| 2 | Ownership, instrumentation, operation | Yes. Thesis B owns A1 to A10, the event schema, its implementation as pull requests, its analysis code | 3.2, 4.7 |
| 3 | Treatments and arms | Yes, with deviation: pre-selected configurations on all four treatments in PG; the other seven on S4 (and S1) in PG only; RDF only for the pre-selected. **Agreed 12 Nov**, with the consequence for Thesis A's check and the amended fallback | 4.1 |
| 4 | Extra log fields | Yes; kgc-trace/1, final list 13 Nov; never changes the meaning of shared fields | 4.3 |
| 5 | Hardware, storage, access | Yes, with additions F.1, F.4, F.5, F.9 | 4.7 |
| 6 | Freeze dates | Yes; F3 freezes the field specification, non-semantic fixes until F4, logged (F.7); F1 moved to 13 Nov | 4.6 |
| 7 | Reference verification | Yes: target 100 per cent, minimum 15 per cent; codebook 28 Nov, calibration 1 to 4 Dec, blind sheets, blinding rule | 3.3 |
| 8 | Title, supervisors, dates | Supplied | 1 |
| 9 | Who sends what | Each author sends the signed agreement to his supervisors on 13 Nov | 6 |
| 10 | Jurisdictions and strata | Yes: Munich and Berlin; Prof. Fink's confirmation by Thesis A before F2 | 4.4 |
| 11 | Verification calendar | Yes; no overlap with Thesis B's pilot evaluation (16 to 21 Nov) | 3.3, 4.6 |
| 12 | Shared log fields, cost record | Yes, provided in kgc-trace/1; euro by Thesis A | 4.3 |
| 13 | Indicator names | Yes; mapping table by 13 Nov | 4.3 |
| 14 | Runs under A0 | Yes; Thesis B's cells built by 1 Jan; Thesis A's oracle, fixture, equal-budget and probe runs have priority 4 to 8 Jan and in both buffers | 4.1, 4.6 |
| 15 | Eval 2 second reader | Yes, 13 to 15 Nov, at most one day; package by 13 Nov | 3.3 |
| 16 | Cost ledger hours | Yes, weekly from 3 Nov, shared work only | 3.3 |
| 17 | Re-trace and audit coding | Yes, 29 Dec to 23 Jan, about 12 hours, batches with one worked example each | 3.3 |
| 18 | Pre-selected configurations | A1, A2, A6; fallback A4. Final 13 Nov | 4.1 |
| 19 | Scored outputs | Yes; Thesis B scores its own layers from 28 Dec without the specification | 3.3 |
| 20 | Session-7 points (a) to (j) | Yes to all. (a) was narrowed on 12 Nov: pass 1 is guaranteed for the three pre-selected configurations, best effort for the other seven | 4.1 |

**Thesis B's additions F.1 to F.9** (reply of 9 November 2026):

| # | Addition | Outcome | Where |
|---|---|---|---|
| F.1 | Own SRW login and raw-store access from F3; instrumentation as reviewed pull requests | Accepted | 4.7 |
| F.2 | Thesis A merges the instrumentation pull requests before the pilot; the pilot is the acceptance test of all fields | Accepted with a cut-off on Sun 15 Nov | 4.7, 4.1 |
| F.3 | At least one Thesis B configuration on S4-PG in the pilot | Accepted: A6 | 4.1 |
| F.4 | Thesis A's credits cover Thesis B's cells up to a cap fixed on 21 Nov | Accepted for cells on A0's model; cap from the pilot cost in euro at list price | 4.7 |
| F.5 | Either author can pull raw logs and exports in bulk | Accepted | 4.7 |
| F.6 | Codebook delivered by Thesis A by 28 Nov | Accepted | 3.3 |
| F.7 | F3 freezes the field specification; non-semantic fixes until F4, logged | Accepted; each fix is a row in the deviation table | 4.3, 4.6 |
| F.8 | An anchor coinciding with a pre-selected configuration runs once; one `run_id`, cost booked once | Accepted; no collision with the families named on 11 Nov | 4.1 |
| F.9 | Thesis B launches and monitors its own cells through SRW in the agreed order; Thesis A's primary cells are never displaced | **Agreed 12 Nov** with the one shared queue; the protection also covers the 16 anchor builds | 4.7 |

| Date | Change | Agreed by |
|---|---|---|
| 2026-10-23 | Draft 1 | Hall |
| 2026-10-23 | Draft 1a: dates in 4.6 and the run window, inclusion rule and fallback in 4.1, from the method decisions of 23 October | Hall |
| 2026-10-24 | Draft 1b: reference verification proposal in 3.1, 3.3 and open point 7, from BE-07 | Hall |
| 2026-10-24 | Draft 1c: jurisdictions and strata in 4.4, verification calendar in 3.3, pilot scope in 4.1, open points 10 and 11, from the method decisions of 24 October (BE-01 to BE-07, BE-18) | Hall |
| 2026-10-25 | Draft 1d: git tags at F4 and F5 and the deviation rule in 4.6 (BU-06); preservation sample and January supportedness subsample in 3.3 (BU-13, BU-16); open points 12 to 14 on shared log fields, indicator names and oracle runs (BU-14, BU-16, TE-07, BU-19) | Hall |
| 2026-10-26 | Draft 1e: open point 14 extended by fixture executions and gold queries (BU-22, BU-23); prompt-template rule in 4.5 (EX-07) | Hall |
| 2026-10-30 | Draft 1f: open points 15 to 18 on the Eval 2 second reader, the cost ledger, the blind re-trace and the pre-selected configurations (EV-02, EV-10, EV-11, EV-13); open point 14 extended by the probe runs (EV-14), from the method decisions of 29 and 30 October (EV-01 to EV-18) | Hall |
| 2026-11-01 | Draft 1g: cost record in 4.3 and open point 12 (SC-14); open point 15 extended by the scenario weight-justification paragraphs (SC-13, SC-14); open point 17 extended by the blind second coding of the run-output audits (EX-15); open point 19 on scored outputs after F5 and the scoring protocol (SC-19), from the method decisions of 31 October and 1 November (SC-02, SC-05 to SC-20, EX-15) | Hall |
| 2026-11-08 | Draft 1h: anchor cells and query passes in 4.1; decoding, pass and manifest fields in 4.3; unit definition and canary bucket in 4.4; decoding, per-unit task and the induction template in 4.5; run order and isolation in 4.7; blind-pass additions, drafting-family exclusion and fixtures in 3.3; open point 20, from the method decisions of 4 to 8 November (session 7: EX-05, EX-06, EX-09 to EX-14, EX-16, BU-09 to BU-12, BU-17, BE-08 to BE-17, TE-03, TE-05, TE-06, TE-08 to TE-10, FR-05, FR-08, FR-09, FR-11 to FR-13) | Hall |
| 2026-11-12 | Draft 1i: Thesis B's reply of 9 November folded in (registration facts in 1; ownership wording in 3.2; conditions on verification, Eval 2, audit coding, scored outputs and the ledger in 3.3; configurations A1 to A10 and Thesis B's cells in 4.1; kgc-trace/1, indicator names and cost in 4.3; additions F.1 to F.9 in 4.1, 4.3, 4.6 and 4.7; answers table in 7). Outcomes of the meeting of 12 November: Thesis B's run matrix with the consequence for Thesis A's check and the amended fallback (EX-04), the one shared build queue with its priority table (4.7), the query order with pass 1 guaranteed for the pre-selected configurations (4.1). Model names: A0 DeepSeek V4.1 Flash (EX-11 as amended on 11 November), anchors GLM-5.3-Flash and Gemma 4 31B, drafting family Claude Sonnet 5.5. Snapshot rule as it can be applied, F2 in two steps and the open A0 parameters, each marked to confirm on 13 November. F1 moved from 6 to 13 November | Hall, Yagubi (meeting 12 Nov 2026; signature 13 Nov 2026) |
| 2026-11-13 | Draft 1i, edited before the signature: A0 runs with thinking enabled (4.5; EX-10 as amended on 13 November). Checklist items 3 to 6, 9, 11, 18 and 23 confirmed by Hall as agreed on 12 November, so the markers on the pilot configuration (A6), the pull-request cut-off (15 November) and the cost cap are removed. New in 4.3: the schema file of the shared minimum, `shared-minimum-log/1`, with five departures from the field list, marked to confirm. New in 4.7: calls on Thesis A's DeepSeek key run in the provider's off-peak hours as far as the calendar allows (Thesis A's decision of 13 November to stay on the DeepSeek API, EX-11 as amended), marked to confirm. New in 4.4: Thesis A's decisions of 13 November on the composition of the sample (how the shares are counted, statutes in, statistics out, bucket sizes, pilot units for the numeric stratum, no campus regulations, operators' content only with consent). The local anchor is Gemma 4 26B A4B instead of Gemma 4 31B (4.1; EX-11 as amended). Six places remain marked to confirm at the signature | Hall (to be confirmed with Yagubi at the call of 13 Nov 2026) |
| 2026-11-14 | Version 1.0 for signature. Thesis B's written answers of 13 November (pull request 4): yes to the six points left open (log schema with its departures, snapshot rule, effort level high, three A0 numbers on 16 November, sample freeze in two steps, off-peak rule), with the condition on the off-peak threshold now in 4.7; no fallback model for A0 (4.5); cost clause accepted as written. Thesis B's message of 14 November: text accepted, `item_id` added to the insert attempt, `attempt_id` unchanged (4.2, 4.3). Field table in 4.3 brought in line with the agreed schema. F3 in two steps, 13 and 16 November (4.6). Dates corrected to what happened: field list and mapping table delivered 13 November, kgc-trace/1 version 1.1 by 15 November, configuration files frozen 16 November, Eval 2 package 14 November, Prof. Fink's confirmation requested. Signature block added (section 8) | Hall, Yagubi |

## 8. Approval of this copy

This is the text of record. Both authors approved it through the pull request that added this file; the merge commit carries the tag F1. The signed copy with the authors' matriculation numbers is kept outside the repository.
