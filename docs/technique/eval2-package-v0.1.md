# Eval 2 package: the technique on paper (v0.1 of 12 November 2026, corrected to 14 November 2026)

This is the schema-fitness evaluation technique of Thesis A written down before anything is run: what is measured, the requirements it has to meet, the stage model, one table of goals, questions and metrics per dimension, one card per metric, the formulas with the order of aggregation, and the four decision scenarios with the reasons for their weights. It is draft v0.1. Findings from the second reader and from the unit tests become change-log rows; version 1.0 is frozen on Monday 23 November 2026.

## 0. For the second reader

**What you are asked to do.** Read sections 2 to 7 once, at most one working day between 13 and 15 November, and check four things (BU-06, EV-02):

1. **Unit and denominator.** Every card names its unit of observation, its denominator and what happens when the denominator is empty or small.
2. **No double counting.** No lost item is charged to two dimensions, and no failure state appears in two headline rates.
3. **Forward trace.** Every requirement in section 2 leads to at least one card or procedure and to the evaluation episode that tests it; every card answers exactly one question in section 4 and names the requirements it serves.
4. **No domain constant.** No definition, formula or card names a waste-domain term or a database product. Domain and database enter only through the two binding tables (section 1.4). The scenarios in section 7 are the one deliberate exception: they are use contexts of the case and are labelled as such.

**How to report.** One row per finding: card or section id, check number (1 to 4), what is wrong, proposed change. "Nothing found" for a section is a valid entry. The rows go into the change log of the technique with your initials.

**What is not in this package.** The Scoring Protocol Specification (toggle grid, seeds, floors, dominance rule) stays with Thesis A (SC-19). There is no benchmark content and no result here. The requirement wording is revised once more when the literature review closes on 21 November; the scenario weights are provisional until 23 November.

**Two words that mean two things.** "F1 to F6" names the six failure states of an item in this document; the six freeze dates are always written "freeze F4 (23 Nov)". "S1 to S4" are the schema variants (treatments); the decision scenarios are U0 to U3 (U for use context; called S0 to S3 until 13 November 2026).

## 1. What is measured

### 1.1 Definition

Schema fitness is the degree to which a schema, realised in a data model, supports three things for a specified body of reference knowledge: representing it, having it populated from specified sources by a fixed construction agent, and having it retrieved by a fixed querying agent through structured queries. It is a vector under declared conditions, never one score and never a property of the schema alone (TE-01, SC-01):

    F(S | X, W, R, A_p, A_q, B) = (C, P, Q)

| Symbol | Meaning |
|---|---|
| S | Treatment: one schema variant. |
| R | Arm: the data model with its query language (two arms; database products are implementation choices and do not appear in the notation). |
| (S, R) | Realisation: one treatment realised in one arm. Coverage is a property of the realisation. |
| X | The frozen source sample: a hashed manifest of self-contained source units, each with a modality stratum, a jurisdiction, an authority class and a bucket. |
| W | The frozen question catalogue: competency questions, each with a canonical answer set and the ids of the reference assertions it requires. |
| A_p, A_q | The agent configuration that populates and the one that queries, each with its prompt template, decoding settings and tool set. |
| B | The budget: tool calls and tokens per source unit, attempts per question, timeouts. |
| K | A named condition set: fixed X, W, A_p, A_q and B. Every reported number carries its K and its arm (TE-10). |
| Cell | One realisation under one K in one replication. |
| C, P, Q | Coverage, populatability, queryability. The profile axes are C, P_func and Q_own (cards C.1, P.1, Q.1). |

### 1.2 Reference objects

- **Reference assertion.** One full proposition in schema-neutral canonical form: subject, predicate, object, qualifiers (scope, validity time, condition or exception, modality, and priority where a source documents it) and provenance (an anchor into a sampled source unit plus an authority record). A qualified rule is never split into plain triples (BE-04, BE-11).
- **Tier.** Each assertion is core, supporting or excluded by a written rule from the authority class of its source and three certainty flags (BE-06). Headline rates are computed on the core tier.
- **Competency question (CQ).** A question q with a canonical answer set V*(q) in the same neutral vocabulary, the set Req(q) of required assertion ids, an archetype, a complexity class and a cluster id (BE-14, BE-16).
- **Cluster.** The CQs that share one required-assertion set. Paraphrases of one question sit in one cluster (SC-07).
- **Input statement.** One atomic proposition annotated on a random sample of source units, independent of the reference (BU-13).
- **Inventory item.** One entry of a closed list with a defensible denominator (BE-13).

### 1.3 Instruments

- **Mapping.** One written mapping per realisation from canonical form into the schema, frozen with version 1.0. It decides, before any run, whether an assertion has an image (BU-08).
- **Exporter.** Per realisation, read-only: turns the populated graph back into canonical assertions with provenance pointers to graph elements. It is the inverse of the mapping (BU-07).
- **Matcher.** One deterministic matcher for all realisations, on canonical form only, in four ordered tiers: exact natural key, normalised lemma in the equivalence table, unit and spelling normalisation, human adjudication blind to cell and treatment. No similarity score, no embedding, no language model decides a match (BE-10).
- **Validator.** One shared validator applies the common rule set to canonical form, identically in both arms. Engine-native constraints are never the measuring instrument (BU-15, EX-02).
- **Loader.** The inverse of the exporter. It builds oracle graphs and mutation fixtures (BU-19, BU-23).
- **Answer mapping.** Per arm: turns a query result into canonical form so that it can be compared with V*(q) as an unordered set (BU-23).

### 1.4 The two binding tables

The technique is defined over abstract sets. A **Domain Binding** table supplies, for one domain: the target population and strata, jurisdictions, authority classes and the tier rule, the canonical predicate vocabulary and equivalence table, the closed lists, the phenomenon tags, the question families and archetypes, and the anchor types of the matcher. A **Formalism Binding** table supplies, per arm: the schema renderer, the validator rules, the exporter with its inverse, the query-language binding with the validity check, and the answer mapping (BU-05). Nothing in sections 2 to 6 depends on what is in either table.

## 2. Requirements for the technique

These are requirements for the technique, not for the schemas under test (BU-01). Origin tags: **E** empirical (the problem and the setting), **T** theoretical (kernel theories and method sources), **I** internal (consistency of the metric system). The source column is provisional until the literature review closes on 21 November.

| Id | The technique must ... | Origin | Source | Satisfied by | Tested in |
|---|---|---|---|---|---|
| R1 | measure coverage, populatability and queryability separately, each with defined components. | E | Research question; registration document §2, §4.4 | Three question tables (section 4); headline cards C.1, P.1, Q.1 with their diagnostic cards | Eval 2 (cards complete); Eval 3b (all cards computed) |
| R2 | report every value under declared, frozen conditions and hold construction and querying conditions constant across treatments. | E | Registration document §4.2, §4.6 | Notation and condition sets (1.1); frozen templates in which only the schema slot varies; cost record (P.10); run validity (X.4) | Eval 3a (pilot); Eval 3b |
| R3 | compute every measure for every treatment in both arms with one protocol, without treatment-specific code, parameters or normalisation. | E | Two-arm design; registration document §4.7 (applicability) | Canonical form, exporter, shared matcher and validator (1.3); natural proportions, no normalisation (6.6); binding tables (1.4) | Eval 3a; Eval 3b (share of planned measurement cells computed) |
| R4 | grade the reference by authority and certainty, keep the unweighted core figure primary and show the tiers. | E | Registration document §4.3, §4.4 | Tier rule (1.2); cards C.1, C.2, C.3 | Eval 2; Eval 3b |
| R5 | judge queryability on executed structured queries against the populated graph, with no language model in any verdict. | E | Registration document §4.6 (reader isolation) | Cards Q.1, Q.2; answer mapping and fixtures (1.3); matcher without a language model | Eval 2; fixture audit (X.3) |
| R6 | keep every component visible beneath any aggregate, allow a cross-dimension number only as a labelled scenario, and let every aggregate be traced to items. | E | Registration document §4.4, §4.6, §4.7 (interpretability, robustness) | Item table (3.3); headline rule (6.2); scenario layer (section 7) | Eval 2 (check 2); trace audit (X.3) |
| R7 | measure run-to-run variation of the agent and keep it apart from differences between treatments. | E | Registration document §4.2, §4.7 (repeatability) | Three replications per cell; order of aggregation (6.4); stability card P.8 | Eval 3b |
| R8 | be applicable by one person within the time and budget of the project, with effort and cost recorded. | E | Time and hardware limits; registration document §4.7 (feasibility) | Cost record (P.10); effort ledger; budget B (7.5) | Eval 3a (budget check); Eval 3b |
| R9 | show for each measure that it responds to the attribute it names. | T | Measurement validation (Kitchenham, Pfleeger and Fenton 1995; Cronbach and Meehl 1955) | Expected-property field of every card, implemented as unit tests on boundary graphs (section 5 preface); instrument audits (X.2, X.3) | Eval 2 (tests green before the pilot); rerun at freezes F4 and F5 |
| R10 | anchor every reference item in a sampled source unit, have it verified by a second coder, and report agreement per coding slot. | T | Competency-question tradition; benchmark construction and inter-annotator agreement | Reference objects (1.2); card X.1 | Benchmark freeze F5 (28 Dec) |
| R11 | exist as a versioned document that is frozen before the summative runs, with every later change logged with its effect. | T | Design-science evaluation (Venable, Pries-Heje and Baskerville 2016; Sonnenberg and vom Brocke 2012) | Change log from v0.1; freeze tags; deviation table | Eval 2 (this reading is logged); freezes F4 and F5 |
| R12 | attribute every lost item to exactly one stage, so that the three dimensions are separable by their denominators. | I | Stage model (BU-03) | One failure state per item per cell (3.2); eligibility rule (3.4); identities (6.3) | Eval 2 (check 2; decomposition tests) |
| R13 | state unit of observation, denominator, scale type, unit and a not-estimable rule for every measure, and use only operations the scale permits. | I | Units and denominators (BU-04); scale typing (BU-12, SC-05) | Card template (section 5 preface); not-estimable and small-denominator rule (6.5); "never add conditional rates" (6.2) | Eval 2 (check 1) |
| R14 | build the reference without any label, property name or structure taken from a treatment. | I | Schema-neutral reference (BE-04) | Canonical form and equivalence table (1.2, 1.3); surface forms from sources only | Eval 2; matcher audit (X.2) |
| R15 | define every measure without a domain or formalism constant. | I | Generality by binding (BU-05) | Binding tables (1.4) | Eval 2 (check 4) |

## 3. Stage model and item table

### 3.1 Six stages

A reference item travels through six stages. The figure is logical, not chronological; the order in which the harness works (construct, validate, match, query, judge) is a separate process figure (BU-03).

1. **Reference knowledge.** The anchored assertion or the CQ exists in the frozen reference.
2. **Representational capacity.** The mapping of the realisation has an image for the assertion.
3. **Population.** The agent writes the assertion from its source unit.
4. **Realised graph.** The assertion is present and correctly encoded, with all its qualifiers.
5. **Query formulation.** The agent returns a query that parses and executes.
6. **Correct retrieval.** The result matches the canonical answer set.

### 3.2 One state per item per cell

| State | Meaning | Applies to | Decided by | Evidence pointer |
|---|---|---|---|---|
| F1 | Not representable: the mapping has no image that preserves subject, predicate, object and every qualifier. A documented detour counts only if the exporter inverts it. | Assertions, CQs (inherited) | The frozen mapping, before any run | Mapping record with the rule applied |
| F2 | Representable but not extracted: no candidate at any anchor of the assertion. | Assertions, CQs (inherited) | Exporter and matcher | Matcher record |
| F3 | Extracted but wrongly encoded: a candidate exists at the anchor, but a value or a qualifier differs or is missing. | Assertions, CQs (inherited) | Exporter, matcher, validator | Matcher and validator record |
| F4 | No query: no query string returned within the budget. | CQs | Query log | Query log |
| F5 | Query invalid. Sub-codes: parse failure; runtime error (timeouts and schema violations as the engine reports them). | CQs | Parser and engine of the arm | Query log |
| F6 | Answer wrong: the query executes but the result does not match. Sub-codes: accidental correctness, memorised constant, empty without discrimination, boolean without witness. | CQs | Answer mapping, fixtures, AST and literal check | Answer-set check |
| Success | Present and correctly encoded (assertion); semantically correct at the final attempt (CQ). | Both | As above | As above |

Rules that hold everywhere:

- An assertion takes exactly one of F1, F2, F3 or success. A CQ takes exactly one of F1 to F6 or success.
- No partial credit. A qualified proposition populated without one of its qualifiers is F3, never a fraction (BE-11).
- Inventory items are a second item type. Each carries two independent facts: whether the mapping can hold it (F1 when it cannot, as for an assertion) and whether any sampled unit mentions it. An unmentioned item is marked **not in source**; it leaves the realised row and is charged to neither schema nor agent, and it still counts in the representable share (BE-13).

### 3.3 Item table

One table per cell. Every score and every subgroup result in the thesis is a group-by over it, which is what makes an aggregate traceable (R6) and a loss countable only once (R12).

| Column | Content |
|---|---|
| Item id, item type | Reference assertion, CQ, input statement or inventory item |
| Stratum, phenomenon tags, tier, jurisdiction, certainty flags | From the frozen reference |
| Failure state, sub-code, attempt index | Section 3.2; first and final attempt for CQs |
| Match tier | 1 to 4, or unmatched |
| Partial-presence flag | For an ineligible CQ: k of n required assertions present |
| Evidence pointer | Mapping record, validator record, matcher record, query log, answer-set check |

### 3.4 Eligibility

A CQ is **eligible** in a cell only when every assertion in Req(q) is in state success in that cell. Otherwise the CQ inherits the worst upstream state of its required assertions (F1 before F2 before F3) and carries the partial-presence flag. It stays in the item table and in the end-to-end denominator, and leaves the conditional one (BU-04). The eligible set is therefore a quantity of the cell, not of the catalogue.

### 3.5 The four views on one table

| View | Numerator | Denominator | Card |
|---|---|---|---|
| Coverage | Core assertions not in F1 | Core assertions | C.1 |
| Functional populatability | Core assertions in state success | Core assertions not in F1 | P.1 |
| Own-set queryability | Eligible CQs in state success | Eligible CQs | Q.1 |
| End-to-end answerability | CQs in state success | All CQs | Q.5 |

A missing fact appears as F1 in the first view, or as F2 or F3 in the second, and removes the CQs that need it from the denominator of the third. It is counted once.

### 3.6 Query funnel and failure states

| Funnel level | Gloss | Failure state when the level is not reached | Sub-codes |
|---|---|---|---|
| Produced | A query string is returned within the budget | F4 | none |
| Parses | Syntactic validity under the engine version named in the schema description | F5 | parse failure |
| Executes | Runs to completion without error within the timeout; an empty result or a boolean is a valid execution | F5 | runtime error |
| Semantically correct | Answer-set match on V*(q), plus passing fixture verdicts where fixtures exist | F6 | accidental correctness, memorised constant, empty without discrimination, boolean without witness |

The four levels are cumulative on one denominator, so a CQ cannot pass a level without passing the ones before it (SC-06, TE-09).

## 4. Goals, questions and metrics

One table per dimension and one for the measures that belong to no dimension (BU-02). Each question is answered by at least one card; each card answers exactly one question. "Role" says where the card is reported and is not part of the measure: **headline** = profile axis; **derived** = a row computed from headline states; **diagnostic** = reported with its own denominator, never an axis and never in a scenario score; **scenario reading** = replaces an axis inside one named scenario only.

### 4.1 Coverage

**Goal.** Characterise a realisation (S, R) with respect to its capacity to represent the graded reference knowledge, from the viewpoint of someone choosing among schema variants, in the context of the frozen reference and the frozen mappings. Coverage does not depend on the agent or on any run.

| Question | Card | Role |
|---|---|---|
| QC1. What share of the core reference assertions can the realisation hold without loss? | C.1 Coverage (C) | headline |
| QC2. What does the picture look like for the weaker evidence tier, and how large are the tiers? | C.2 Tier vector | diagnostic |
| QC3. How does coverage read when each assertion counts by the confidence in its source? | C.3 Authority-weighted coverage (C_w) | scenario reading |
| QC4. How much of the core reference is actually in the graph after a run? | C.4 Realised coverage (C_r) | derived |
| QC5. Can the qualifiers of qualified rules be held, and are they? | C.5 Qualifier completeness | diagnostic |
| QC6. How much of what the sampled sources state, beyond the reference, is kept? | C.6 Source-information preservation | diagnostic (secondary measure) |
| QC7. For closed lists with a defensible denominator: what share can the schema hold, and what share of the mentioned items is populated? | C.7 Closed-set domain completeness | diagnostic |

### 4.2 Populatability

**Goal.** Characterise how a fixed agent configuration A_p, under the frozen construction protocol, populates a realisation (S, R) from the sampled sources: correctly, conformantly, supportedly, stably and at what cost; from the viewpoint of someone who has to run the population; in the context of the frozen sample X and budget B. This is populatability in the empirical sense (TE-07).

| Question | Card | Role |
|---|---|---|
| QP1. Of what the realisation can hold, what share does the agent store correctly? | P.1 Functional populatability (P_func) | headline |
| QP2. Does the agent get through the source units at all? | P.2 Document-level population success | diagnostic |
| QP3. Does the stored graph obey rules that every realisation can obey? | P.3 Common-scope conformance | diagnostic |
| QP4. What did each treatment's own constraints demand, and were they met? | P.4 Treatment-scope conformance | diagnostic (table, never compared as a score) |
| QP5. Is the vocabulary used consistently? | P.5 Vocabulary coherence | diagnostic |
| QP6. Is what was stored at reference anchors right? | P.6 Reference precision | diagnostic (may be a scenario minimum requirement) |
| QP7. Is what was stored outside the reference grounded in its source unit? | P.7 Supportedness | diagnostic (may be a scenario minimum requirement) |
| QP8. Does repeating the run give the same content? | P.8 Run-to-run stability | diagnostic |
| QP9. How much did the agent have to fight the validator before a write was accepted? | P.9 Pre-commit friction | diagnostic, never scored |
| QP10. At what time and cost? | P.10 Resource efficiency | diagnostic (raw units beside the frontier; constraint in one scenario) |

### 4.3 Queryability

**Goal.** Characterise how a fixed querying agent A_q, under the frozen query protocol, retrieves by structured query the information that is verified present in the graph of a realisation; from the viewpoint of someone who will ask questions of the graph; in the context of the frozen catalogue W. This is conditional, agent-mediated queryability (TE-08).

| Question | Card | Role |
|---|---|---|
| QQ1. Of the questions whose facts are verified present, what share is answered correctly? | Q.1 Own-set queryability (Q_own) | headline |
| QQ2. At which step between question and answer do queries fail? | Q.2 Query funnel | diagnostic |
| QQ3. Do differences between cells hold on the same questions? | Q.3 Intersection queryability (Q_∩) | diagnostic (the comparison figure) |
| QQ4. How well is a realisation queried when its content is loaded deterministically instead of by the agent? | Q.4 Oracle queryability (Q_oracle) | diagnostic (schema-shape component) |
| QQ5. Does the whole chain from source to answer work? | Q.5 End-to-end answerability | diagnostic (application-facing; counterfactual single score) |
| QQ6. What share of the catalogue is askable in a cell at all? | Q.6 Eligibility rate | diagnostic |
| QQ7. Was a correct query possible at all? | Q.7 Gold-subset figures | diagnostic |
| QQ8. How much of a queryability result is explained by population, and how much by which questions survived? | Q.8 Difference diagnostics | diagnostic |
| QQ9. Does the result hold on question families never used in development? | Q.9 Leakage-controlled queryability (Q_own(held-out)) | diagnostic (printed beside the headline) |

### 4.4 Cross-cutting measures

**Goal.** Characterise the quality of the reference, of the instruments that assign states and of the runs themselves, from the viewpoint of a reader who has to decide how far to trust the three dimensions.

| Question | Card | Role |
|---|---|---|
| QX1. How reliable is the coding of the reference, and how much of it was verified twice? | X.1 Reference reliability | cross-cutting |
| QX2. Does the matcher assign presence correctly, and equally well for every realisation? | X.2 Matcher audit and match tiers | cross-cutting |
| QX3. Do the other instruments (supportedness coding, fixtures, loader, trace) work? | X.3 Instrument audits | cross-cutting |
| QX4. Were the runs valid and complete? | X.4 Run validity | cross-cutting |
| QX5. Is there evidence that the agent answered from memory instead of from the unit or the graph? | X.5 Leakage diagnostics | cross-cutting |

## 5. Metric cards

**Card fields** (BU-02, BU-12, SC-05; field provenance ISO/IEC 15939 and Kitchenham, Pfleeger and Fenton 1995): the question answered and the requirements served; attribute; unit of observation; denominator; function; not-estimable rule; instrument; scale type, domain, unit and polarity; expected properties. Indicators, thresholds, weights and aggregation are not on a card; they are in sections 6 and 7.

**Rules that apply to every card and are not repeated on each:**

- Empty denominator gives "n.e." with n = 0, never 0 and never 1. Below a denominator of 10 (items for assertion-based cards, clusters for question-based cards) per cell and stratum, the count is printed as "k of n" with no rate and no interval, is excluded from aggregation and is listed in the n.e. register (BU-04, SC-18).
- Every rate is printed with its denominator, per cell and per subgroup (modality stratum, jurisdiction, phenomenon tag, tier; for questions also family, archetype, complexity class and bucket).
- Canary units and canary CQs are outside every denominator except on card X.5. Assertions flagged "unresolved" and the CQs that require them are outside the denominators of the P and Q cards (BE-12).
- A void run is kept in the log and appears on no card except X.4.
- One frozen scoring code computes every card for every treatment and both arms.

**Expected properties are unit tests.** Each property is a seeded test on seven synthetic boundary graphs per arm, built through the real loader, exporter and item table (BU-12): **empty**; **full gold** (every reference assertion present and correct); **half gold** (every second assertion); **duplicated gold**; **fully hallucinated** (wrong candidates at the reference anchors and assertions with no basis); **wrong qualifiers** (gold with every qualifier wrong or missing); **text blob** (one string per source unit).

### 5.1 Coverage

#### C.1 Coverage (C), headline

- **Answers** QC1. **Serves** R1, R3, R4, R12, R14.
- **Attribute.** Capacity of the realisation to represent the core reference knowledge.
- **Unit of observation.** One core-tier reference assertion (a full proposition with all its qualifiers) per realisation.
- **Denominator.** All core-tier anchored assertions.
- **Function.** Share of the denominator whose state is not F1.
- **Instrument.** The frozen mapping of the realisation, applied per assertion and logged with the rule applied (F1 log). Decided before any run. For a schema that is itself induced, coverage is relative to the schema frozen for that arm and is stated as such.
- **Scale.** Ratio; [0, 1]; share of assertions; higher is better.
- **Expected properties.** Identical in every replication of a realisation. A mapping with no image gives 0; a mapping with an image for every assertion gives 1. Unchanged by the content of the graph: the same value on the full-gold, the fully hallucinated and the text-blob graph. Never below realised coverage (C.4). A treatment that stores canonical propositions as they are has coverage near 1 by construction.

#### C.2 Tier vector

- **Answers** QC2. **Serves** R4.
- **Attribute.** Capacity for the weaker evidence tier, and the size of each tier.
- **Unit of observation.** One supporting-tier assertion per realisation; one assertion per tier for the counts.
- **Denominator.** All supporting-tier anchored assertions.
- **Function.** Supporting coverage by the function of C.1 over the supporting tier, printed beside core coverage, the count of excluded assertions with their reasons, and the three tier sizes.
- **Instrument.** As C.1; the tier comes from the written grading rule frozen with the reference.
- **Scale.** Ratio; [0, 1] for the rate, non-negative integers for the counts; higher is better for the rate.
- **Expected properties.** Run-independent. Moving an assertion from one tier to another moves it between the denominators of C.1 and C.2 and never into both. The three tier sizes sum to the number of anchored assertions.

#### C.3 Authority-weighted coverage (C_w), scenario reading

- **Answers** QC3. **Serves** R4, R6.
- **Attribute.** Capacity when each assertion counts by the evidential confidence of its source class.
- **Unit of observation.** One core-tier or supporting-tier assertion per realisation.
- **Denominator.** The sum of the weights of all core and supporting assertions.
- **Function.** Sum of w(a) over the assertions not in F1, divided by the sum of w(a) over all, where w(a) is the weight of the authority class of the source of a.
- **Not estimable.** When the weight sum is 0.
- **Instrument.** As C.1, with one weight per authority class, author-set and declared with version 1.0: class A 1.0, class B 1.0, class C 0.5, class D 0.25. A weight expresses confidence in the evidence, never how much an assertion matters for a use (SC-08).
- **Scale.** Ratio; [0, 1]; weighted share; higher is better.
- **Expected properties.** Equal to the unweighted coverage over core and supporting together when all weights are equal. Unchanged when all weights are multiplied by one constant. Between the smallest and the largest class-wise coverage. Never combined with C.1 in one number.

#### C.4 Realised coverage (C_r), derived

- **Answers** QC4. **Serves** R1, R12.
- **Attribute.** Share of the core reference knowledge that is in the graph, correctly encoded, after a run.
- **Unit of observation.** One core-tier assertion per cell.
- **Denominator.** All core-tier anchored assertions.
- **Function.** Share of the denominator in state success. Identity: C_r = C × P_func.
- **Instrument.** Exporter and matcher (section 1.3).
- **Scale.** Ratio; [0, 1]; share of assertions; higher is better.
- **Expected properties.** Never above C. The identity is exact on the item table. Empty graph 0; full gold equals C; half gold equals half of C when the gold alternates over representable assertions. Every results table shows the decomposition of the loss into F1 against F2 plus F3, and the three shares F1, F2 plus F3 and success sum to 1.

#### C.5 Qualifier completeness

- **Answers** QC5. **Serves** R1, R12.
- **Attribute.** Whether the qualifiers of qualified propositions can be held (representable reading) and are held (realised reading).
- **Unit of observation.** One required qualifier of a core-tier assertion that carries at least one qualifier.
- **Denominator.** All required qualifiers on those assertions.
- **Function.** Representable qualifier completeness: share of the denominator with an image under the mapping. Realised qualifier completeness: share of the denominator matched in the cell.
- **Instrument.** Mapping for the representable reading; exporter and matcher for the realised reading.
- **Scale.** Ratio; [0, 1]; share of qualifiers; higher is better.
- **Expected properties.** The representable reading is run-independent. Realised never above representable. Full gold 1; wrong-qualifier graph: realised 0 and every affected assertion in F3. This card explains F1 and F3; it grants no credit, because the proposition is scored whole (section 3.2). The joint rate "held with all qualifiers" is obtained by counting the stricter state, never by multiplying this card into C.1.

#### C.6 Source-information preservation, secondary measure

- **Answers** QC6. **Serves** R1, R14.
- **Attribute.** How much of what the sampled source units state is kept, independently of what the reference selected.
- **Unit of observation.** One input statement: an atomic proposition with its qualifiers, annotated on a stratified random sample of source units drawn from the frozen manifest, with no authority filter.
- **Denominator.** Three readings on one sample: representational over all sampled statements ("representational preservation"); functional over the representable ones; realised over all sampled statements. Measured on the summative runs, and in the pilot in the same way. A separate side figure, "pooled preservation", pools all sampled propositions into one union; it is computed in the pilot only and is never evidence.
- **Function.** The state logic of section 3.2 applied to input statements: representational = share not in F1; functional = success over representable; realised = success over all; identity realised = representational × functional.
- **Instrument.** The same exporter and matcher as for the reference. The sample is pre-annotated with a pre-fill from a model family that is not a run agent, verified under the same codebook and second pass as the reference, and frozen with the benchmark.
- **Scale.** Ratio; [0, 1]; share of statements; higher is better.
- **Expected properties.** Representational preservation near 1 for a treatment that stores propositions as they are. Preservation and coverage have different denominators, so a gap between them is a finding about what the reference leaves out, not an inconsistency.
- **Sensitivity row.** A pooled variant (the de-duplicated union of source-verified extractions of all treatments on the pilot graphs) is computed once and printed as a sensitivity figure. It is never evidence and never recomputed on the summative runs.

#### C.7 Closed-set domain completeness

- **Answers** QC7. **Serves** R1, R15.
- **Attribute.** For closed lists with a defensible denominator: whether the schema can hold the items, and whether the items the sample mentions were populated.
- **Unit of observation.** One inventory item (an item type with its required attributes) per inventory and realisation; per cell for the realised row.
- **Denominator and function, three rows.** *Representable share* (the main figure): items whose type and required attributes have an image under the mapping, over all items of the inventory. *Realised over mentioned*: items with a matched agent-created instance, over the items that the frozen sample mentions. *Sample reach* (context only, never a score): mentioned items over all items, and realised items over all items.
- **Not estimable.** An item that no source unit mentions is marked "not in source" and leaves the realised row; whether it is representable is recorded independently and counts in the representable share. An inventory that the sample does not mention at all has no realised row. Everything outside the closed lists is not estimable, because the domain is open.
- **Instrument.** Inventories compiled by script from the official lists named in the Domain Binding, with retrieval date and hash; a deterministic mention scan of the frozen sample, run once; mapping and matcher.
- **Scale.** Ratio; [0, 1]; share of items; higher is better.
- **Expected properties.** The representable share is constant per treatment and independent of the sample and of the run. No agent is charged for an item its sources did not contain. An inventory declared capacity-only has no realised row.

### 5.2 Populatability

#### P.1 Functional populatability (P_func), headline

- **Answers** QP1. **Serves** R1, R2, R7, R12.
- **Attribute.** Degree to which the fixed agent configuration stores, correctly encoded, what the realisation can hold.
- **Unit of observation.** One representable core-tier assertion per cell.
- **Denominator.** Core-tier assertions not in F1 for the realisation.
- **Function.** Share of the denominator in state success. The misses are F2 and F3.
- **Not estimable.** When no core assertion is representable (C = 0).
- **Instrument.** Exporter and four-tier matcher on canonical form; validator record. Presence is never decided by a querying agent or a language model.
- **Scale.** Ratio; [0, 1]; share of assertions; higher is better.
- **Expected properties.** Empty graph 0; full gold 1; half gold exactly 0.5 with exact denominators; duplicated gold 1 with no inflation, and the same value after a second export; fully hallucinated 0; wrong qualifiers 0 with an F3 share of 1 when every representable core assertion carries a qualifier (an assertion without a qualifier cannot be made wrong this way and stays a success); text blob 0. The shares of F2, F3 and success over the representable assertions sum to 1.

#### P.2 Document-level population success

- **Answers** QP2. **Serves** R1, R2.
- **Attribute.** Whether the agent processes source units without fatal failure.
- **Unit of observation.** One source unit of the frozen sample per cell.
- **Denominator.** All source units of the cell, per modality stratum.
- **Function.** Share of units without the fatal-failure flag. Abandoned units are listed by reason code: budget exhausted, crashed twice, resumed cell.
- **Instrument.** Harness log.
- **Scale.** Ratio; [0, 1]; share of units; higher is better.
- **Expected properties.** 1 when no unit carries the flag, 0 when all do. Independent of what was written for the units that succeeded.

#### P.3 Common-scope conformance

- **Answers** QP3. **Serves** R1, R3.
- **Attribute.** Conformance of the stored graph to rules that every realisation can satisfy.
- **Unit of observation.** One exported canonical assertion per cell.
- **Denominator.** All exported assertions of the cell, per modality stratum of the originating source unit.
- **Function.** Share of the denominator that passes every rule of the common scope; the violation count per rule is printed beside it.
- **Instrument.** The shared validator on the canonical export, identical for all realisations and both arms. The validator is built by the companion thesis (agreement, section 3.2); the rule list below is part of this technique. Common scope: subject, predicate and object well formed; predicate and type mapped or mappable under the equivalence table; no dangling reference; datatypes and units parse; qualifiers well formed; provenance pointer present; no duplicate assertion. A rule that some realisation cannot satisfy structurally is removed, not relaxed.
- **Scale.** Ratio; [0, 1]; share of assertions; higher is better.
- **Expected properties.** Empty export: n.e. Full gold 1. Duplicated gold below 1, with the duplicate rule as the only violated rule. Unchanged by a second export. Not trivially 1 for any realisation in the pilot.

#### P.4 Treatment-scope conformance, diagnostic table

- **Answers** QP4. **Serves** R1.
- **Attribute.** What the declared constraints of each treatment demanded and whether the committed graph meets them.
- **Unit of observation.** One applicable constraint of a realisation; one graph element checked against it.
- **Denominator.** Per constraint: the elements to which it applies.
- **Function.** A table per realisation: the constraint inventory (count by kind and strength: label and type, required property, datatype, uniqueness, cardinality, endpoint, closed value list) and the violations per applicable constraint after commit.
- **Not estimable.** "n.a." where a realisation declares no constraint.
- **Instrument.** The declared constraints of the realisation, checked on the committed graph.
- **Scale.** Counts (absolute) and per-constraint ratios in [0, 1]; a table, never compared across treatments as a score and never part of a scenario or of the dominance analysis.
- **Expected properties.** "n.a." by construction for a treatment without declared constraints. A graph that satisfies every declared constraint has zero violations in every row.

#### P.5 Vocabulary coherence

- **Answers** QP5. **Serves** R1, R14.
- **Attribute.** How consistently the agent used the vocabulary, measured against the frozen equivalence table.
- **Unit of observation and denominator, four rows.** (a) Distinct predicates per canonical predicate, over the canonical predicates that occur. (b) Distinct types or labels per canonical concept, over the canonical concepts that occur. (c) Unmapped predicates and types, over all distinct predicates and types used. (d) Object literals that cannot be normalised to an anchor, over all object literals of anchor-typed slots.
- **Function.** (a) and (b) are means of counts; (c) and (d) are shares.
- **Instrument.** Canonical export and equivalence table.
- **Scale.** (a), (b): ratio; [1, ∞); count per concept; lower is better. (c), (d): ratio; [0, 1]; share; lower is better.
- **Expected properties.** On the full-gold graph in canonical vocabulary (a) = (b) = 1 and (c) = (d) = 0. Empty export: n.e. Duplicating assertions changes none of the four rows.

#### P.6 Reference precision

- **Answers** QP6. **Serves** R1, R12.
- **Attribute.** Correctness of what the agent stored at reference anchors.
- **Unit of observation.** One core-tier assertion with a candidate at its anchor, per cell.
- **Denominator.** Assertions in state success plus assertions in F3.
- **Function.** Success over success plus F3.
- **Instrument.** Exporter and matcher.
- **Scale.** Ratio; [0, 1]; share of assertions; higher is better.
- **Expected properties.** Full gold 1; wrong qualifiers 0 when every core assertion carries a qualifier; fully hallucinated 0 (the boundary graph places wrong candidates at the reference anchors); empty graph n.e.; duplicated gold 1.

#### P.7 Supportedness

- **Answers** QP7. **Serves** R1, R5, R9.
- **Attribute.** Whether what the agent stored outside the reference is grounded in the source unit it was processing.
- **Unit of observation.** One exported assertion that matches no reference item, drawn into a stratified random sample of equal size per realisation (by modality stratum of the originating unit, seeded draw after the benchmark freeze).
- **Denominator.** All sampled assertions of the realisation.
- **Function.** Unsupported rate = share coded contradicted, no basis in unit, or unanchored (no log pointer to a source unit). Printed beside it: supported explicit, supported interpreted, and undecidable. The five shares sum to 1.
- **Not estimable.** A realisation with no non-reference output.
- **Instrument.** Hand coding against the source unit found through the harness log pointer, independent of any provenance the schema itself stores; no language model; a second coder codes 20 % blind and agreement is reported per slot (X.3).
- **Scale.** Ratio; [0, 1]; share of sampled assertions; lower is better for the unsupported rate. Wilson 95 % interval.
- **Expected properties.** Fully hallucinated graph: unsupported rate 1. An assertion matching either side of an unresolved conflict is coded supported.

#### P.8 Run-to-run stability, card family with three rows

- **Answers** QP8. **Serves** R7.
- **Attribute.** Whether repeating the run under identical conditions gives the same content.
- **Row (a), construction.** Unit of observation: one source unit per cell triple (the three replications of a realisation). Per unit, the set of canonicalised assertions each replication exported for it (reference and non-reference; the qualifiers are part of an assertion's identity). Function: mean pairwise Jaccard over the three replication pairs, averaged over units; and the stable-core share, the assertions present in all three replications over the three-way union. Denominator: the units for which at least one replication exported something. Beside it: the standard deviation of each axis over the three replications.
- **Row (b), querying.** Unit of observation: one CQ per replication graph, asked in three passes. Function: verdict agreement across passes; share of CQs with any flip; share of passes with identical canonicalised query text. Denominator: the CQs asked in all completed passes; per cluster, archetype and arm.
- **Row (c), induction drift**, only for a treatment that is itself produced by a stochastic procedure. Unit: one further seeded induction per arm. Function: type and property counts, pairwise Jaccard of canonicalised names, share of the frozen vocabulary that recurs in every induction.
- **Not estimable.** Row (a) for a realisation with fewer than three replications. A unit for which no replication exported anything leaves the denominator of row (a).
- **Instrument.** Canonical export of each replication; query log of each pass.
- **Scale.** Ratio; [0, 1]; higher is more stable. Never part of an axis, a map, a dominance test or a scenario score.
- **Expected properties.** Three identical replications give 1; disjoint sets give 0; the value does not depend on the order of the replications; duplicated assertions do not change it. For treatments whose canonical identity depends on the mapping and the equivalence table, that dependence is stated on the card and not corrected.

#### P.9 Pre-commit friction, diagnostic, never scored

- **Answers** QP9. **Serves** R1, R2.
- **Attribute.** The effort the agent spends against the shared validator before a write is accepted.
- **Unit of observation.** One write attempt; one source unit.
- **Denominator and function, three rows.** Rejected write attempts over all write attempts; retries until acceptance per accepted write; abandoned units over all units. Each also per source unit and per realisation.
- **Instrument.** The log of the shared validator on canonical form (the validator is built by the companion thesis; agreement, section 3.2). Engine-native errors are never counted here, so the figure does not depend on how strict a database product is.
- **Scale.** Counts (absolute) and shares in [0, 1]; lower means less friction. Reported, never scored.
- **Expected properties.** Zero rejections by construction for a configuration whose validator only logs. A unit accepted at the first attempt contributes zero retries.

#### P.10 Resource efficiency

- **Answers** QP10. **Serves** R2, R8.
- **Attribute.** Time and resources the population consumed.
- **Unit of observation.** One model call, summed per source unit and per cell.
- **Function and denominators.** Wall-clock seconds, tokens by category (input, cached input, output, reasoning, as the runtime reports them) and euro, each per source unit (denominator: units processed) and per confirmed-correct assertion (denominator: assertions in state success).
- **Not estimable.** Per confirmed-correct assertion when the success count is 0.
- **Instrument.** The cost record per call: model identifier and version evidence, token counts, number of calls, wall-clock; euro = tokens × the price table frozen with the sample, applied to every run whatever was actually paid.
- **Scale.** Ratio; [0, ∞); seconds, tokens, euro; lower is better. Raw units only: never normalised, never aggregated, never an axis.
- **Expected properties.** Doubling every price doubles the euro figures and leaves seconds and tokens unchanged. Zero successes give "n.e.", not 0 and not infinity.

### 5.3 Queryability

Every rate in this section is a **cluster macro-average**: the mean over clusters of the within-cluster share over the CQs counted in the denominator. The CQ micro-average is printed as a secondary row with the cluster-size distribution. A cluster with no CQ in the denominator leaves it and is counted in the n.e. register (SC-07).

#### Q.1 Own-set queryability (Q_own), headline

- **Answers** QQ1. **Serves** R1, R5, R12.
- **Attribute.** Degree to which the fixed querying agent retrieves, by a structured query on the populated graph, information that is verified present.
- **Unit of observation.** One eligible CQ per replication graph and pass.
- **Denominator.** The eligible CQs of the cell (section 3.4), in clusters.
- **Function.** Share of the denominator that is semantically correct at the final attempt. Accidental correctness and the other F6 sub-codes are failures. A CQ with no produced query stays in the denominator and fails.
- **Instrument.** The frozen query template and schema description; the query is executed on the populated graph; the answer mapping of the arm; answer-set match against V*(q) as an unordered set, with witnesses for booleans; fixture verdicts where fixtures exist; the AST and literal check. No language model reads or judges an answer.
- **Scale.** Ratio; [0, 1]; share of eligible CQs; higher is better.
- **Expected properties.** On the full-gold graph with the gold queries: 1. On the empty graph: n.e., because no CQ is eligible. Equal to level 4 of the funnel (Q.2) at the final attempt. An empty answer is correct only for a CQ whose canonical answer set is empty and only if the query returns an item once that item is inserted.

#### Q.2 Query funnel

- **Answers** QQ2. **Serves** R1, R6, R12.
- **Attribute.** The step at which a query fails.
- **Unit of observation.** One eligible CQ per replication graph and pass, at the first and at the final attempt.
- **Denominator.** As Q.1, the same for all four levels.
- **Function.** Four cumulative rates: produced, parses, executes, semantically correct (section 3.6), at the first and at the final attempt; repair gain = final minus first, per level. The F6 sub-codes are printed as shares of F6, never as levels.
- **Instrument.** Query log; parser and engine of the arm; answer-set check.
- **Scale.** Ratio; [0, 1]; share of eligible CQs; higher is better. No weights and no mean over levels.
- **Expected properties.** The four rates never increase from one level to the next. The final attempt is the last attempt and is the one scored (SC-06). A revision can break a query that worked, so final may be below first; a negative repair gain is printed, not clipped. A CQ that fails a level fails all later ones.

#### Q.3 Intersection queryability (Q_∩)

- **Answers** QQ3. **Serves** R1, R3.
- **Attribute.** Queryability on the questions that every cell of a stated comparison can be asked. It is the figure of every direct comparison of two schemas (paired contrasts). The ranking of schemas is decided on own-set queryability (Q.1), with this figure as a check: where the two disagree, the result is labelled "eligibility-driven".
- **Unit of observation.** One CQ that is eligible in every cell of the comparison.
- **Denominator.** That intersection, in clusters; its count is printed with every figure. Flagged "weak intersection" when it has fewer than 10 clusters or fewer than half the clusters of the smallest own eligible set.
- **Function.** As Q.1, on the intersection.
- **Instrument.** As Q.1.
- **Scale.** Ratio; [0, 1]; higher is better.
- **Expected properties.** Equal to Q_own in every cell when all cells of the comparison have the same eligible set. The intersection is never larger than any own set.

#### Q.4 Oracle queryability (Q_oracle)

- **Answers** QQ4. **Serves** R1, R9.
- **Attribute.** The schema-shape component of queryability: how well a realisation is queried when its content does not depend on the populating agent.
- **Unit of observation.** One CQ whose required assertions are all in the oracle core (the core tier minus the assertions in F1 for the realisation), asked on an oracle graph.
- **Denominator.** Those CQs, in clusters; the same fixed set for every replication of a realisation.
- **Function.** As Q.1, on the oracle graph.
- **Not estimable.** Dropped by rule, with a logged deviation, if the loaders do not pass the round-trip test by the benchmark freeze.
- **Instrument.** The loader of the realisation; the same agent configuration, template and passes as for the populated graphs.
- **Scale.** Ratio; [0, 1]; higher is better. Never a profile axis and never in a scenario score.
- **Expected properties.** Round trip: exporting a loaded item returns the item, for every item, in both arms.

#### Q.5 End-to-end answerability

- **Answers** QQ5. **Serves** R1, R6.
- **Attribute.** Whether the whole chain from source to answer works for a question.
- **Unit of observation.** One CQ of the frozen catalogue per replication graph and pass.
- **Denominator.** All CQs of the catalogue. Ineligible CQs stay in it with their inherited state.
- **Function.** Share of the denominator in state success. Identity, printed with both factors and their counts: end-to-end = eligibility rate (Q.6) × Q_own.
- **Instrument.** Item table.
- **Scale.** Ratio; [0, 1]; share of CQs; higher is better. Never in a scenario score or the dominance analysis: it already contains coverage and populatability.
- **Expected properties.** Never above the eligibility rate. The identity is exact. Empty graph 0. The product C × P_func × Q_own is shown once as an approximation with its deviation from the measured figure, and is never used as a result.

#### Q.6 Eligibility rate

- **Answers** QQ6. **Serves** R12.
- **Attribute.** The share of the catalogue that can be asked in a cell at all.
- **Unit of observation.** One CQ per cell.
- **Denominator.** All CQs of the catalogue.
- **Function.** Share of the denominator that is eligible. Each ineligible CQ carries its inherited state and "k of n required assertions present".
- **Instrument.** Item table.
- **Scale.** Ratio; [0, 1]; higher is better.
- **Expected properties.** Full gold: 1 when coverage is 1, otherwise the share of CQs none of whose required assertions is in F1; empty graph 0. A CQ is eligible exactly when all its required assertions are in state success.

#### Q.7 Gold-subset figures

- **Answers** QQ7. **Serves** R5, R9.
- **Attribute.** Whether a correct query exists, and how the agent performs where one does.
- **Unit of observation.** One CQ of the gold subset: a seeded draw of one CQ per cluster, stratified by archetype and complexity class, per realisation.
- **Denominator and function, three rows.** *Impossibility rate*: eligible subset CQs for which no gold query could be written within the time box, over the eligible subset CQs. *Queryability given answerability* (Q_agent): semantically correct over the eligible subset CQs that have a gold query. *Oracle-vocabulary existence*, for a treatment without a declared vocabulary: subset CQs with a gold query in the canonical vocabulary of the oracle graph, over the subset CQs.
- **Instrument.** Gold queries written by the author on the oracle graphs under a fixed time box with all attempts logged, before any agent query output is seen; each gold query is run on its mutation fixtures and must return the predicted answer.
- **Scale.** Ratio; [0, 1]; lower is better for the impossibility rate, higher for the other two.
- **Expected properties.** The impossibility rate and the share of eligible subset CQs with a gold query sum to 1. An agent answer that is correct on a CQ recorded as having no gold query is logged, and that gold-query record is reviewed. The structural complexity of the gold queries (pattern count, hop depth, filter count, aggregation) is printed as a baseline per archetype and is not a score.

#### Q.8 Difference diagnostics

- **Answers** QQ8. **Serves** R6, R12.
- **Attribute.** How much of a queryability result is explained by population and by the questions that survived.
- **Unit of observation.** One cell.
- **Function, two rows.** *Population-induced queryability loss* = Q_oracle minus Q_own on the CQs both figures share. *Survivorship effect* = Q_own minus Q_∩ for a stated comparison, printed with the archetype mix of the own eligible set.
- **Denominator.** The shared CQs, in clusters, printed with each difference.
- **Not estimable.** The first row when Q_oracle is dropped.
- **Scale.** Interval; [−1, 1]; difference of shares. Diagnostic only; never a dimension.
- **Expected properties.** Both differences are 0 when the populated graph equals the oracle graph and all cells have the same eligible set.

#### Q.9 Leakage-controlled queryability (Q_own(held-out))

- **Answers** QQ9. **Serves** R2, R9.
- **Attribute.** Queryability on question families that were never used in development.
- **Unit of observation.** One eligible CQ of the held-out bucket.
- **Denominator.** The eligible held-out CQs of the cell, in clusters.
- **Function.** As Q.1. Printed beside the headline; the gap between development and held-out families is itself reported.
- **Instrument.** As Q.1; the bucket is a field of the frozen catalogue.
- **Scale.** Ratio; [0, 1]; higher is better.
- **Expected properties.** Equal to Q_own when the catalogue contains only held-out families.

### 5.4 Cross-cutting

#### X.1 Reference reliability

- **Answers** QX1. **Serves** R10.
- **Attribute.** Reliability of the coding of the reference.
- **Unit of observation.** One reference item coded independently by both coders, per codebook slot (accept or reject, each phenomenon tag, each certainty flag, tier, CQ archetype).
- **Denominator.** The doubly coded items per slot and stratum. The verification share has all reference items as its denominator.
- **Function.** Percent agreement and Krippendorff's alpha per slot; verification share = doubly coded items over all items. No single global coefficient.
- **Not estimable.** Alpha for a slot in which both coders use only one value.
- **Instrument.** Codebook; blind second pass without any run output; adjudication log.
- **Scale.** Percent agreement: ratio; [0, 1]. Alpha: interval; at most 1. Higher is better.
- **Expected properties.** Identical codings give agreement 1 and alpha 1. Swapping the coders changes nothing.

#### X.2 Matcher audit and match tiers

- **Answers** QX2. **Serves** R3, R9, R14.
- **Attribute.** Whether the matcher assigns presence correctly, and equally well for every realisation.
- **Unit of observation.** One matcher decision in a stratified audit sample of equal size per realisation, half matched items and half unmatched reference items, hand-checked blind to the verdict, with treatment and arm stripped.
- **Denominator.** Audited matched items (precision); audited reference items that are truly present (recall). For the tier shares: all matched items of the cell.
- **Function.** Matcher precision and recall per realisation with Wilson 95 % intervals; share of matched items per match tier 1 to 4 and the unmatched share; precision per tier.
- **Instrument.** Audit protocol; item table (match-tier column).
- **Scale.** Ratio; [0, 1]; higher is better for precision and recall.
- **Expected properties.** On the full-gold boundary graph every item matches at tier 1 or 2 and recall is 1. The tier shares and the unmatched share sum to 1.

#### X.3 Instrument audits

- **Answers** QX3. **Serves** R6, R9.
- **Attribute.** Whether the remaining instruments do what the cards assume.
- **Unit of observation and denominator, four rows.** *Fixture verdicts*: one scorer verdict in an audit sample per arm, half on mutant and half on original graphs; share that a human confirms. *Loader round trip*: one loaded item; number of items whose export differs from the item, over all items. *Trace*: one reported aggregate in a stratified sample over the reporting layers, arms and treatments; share traced from aggregate to component to item to failure state to source unit and run without manual lookup. *Second coding*: one audited item coded twice; percent agreement and alpha per slot on 20 % of every human audit.
- **Instrument.** Audit protocol; item table with evidence pointers.
- **Scale.** Ratio; [0, 1]; higher is better, except the round-trip difference count (absolute; 0 expected).
- **Expected properties.** Round-trip difference 0. Every sampled aggregate traces.

#### X.4 Run validity

- **Answers** QX4. **Serves** R2, R7, R11.
- **Attribute.** Whether the runs behind the numbers were valid and complete.
- **Unit of observation.** One run, one cell, one source unit or one model call, depending on the row.
- **Function, rows of counts with their totals.** Void runs and their reruns; cells resumed from a snapshot with the resume count; units retried; abandoned units by reason; calls whose returned model identifier or fingerprint differs from the first call of the run; completed share of the query passes per cell; deviations by action class.
- **Instrument.** Harness log; deviation table.
- **Scale.** Counts (absolute) and shares in [0, 1].
- **Expected properties.** Each cell has exactly one valid run identifier per replication. A void run contributes to no other card.

#### X.5 Leakage diagnostics

- **Answers** QX5. **Serves** R2, R5, R9.
- **Attribute.** Evidence that the agent answered from memory instead of from the source unit or the graph.
- **Unit of observation.** One final query (flag row); one canary assertion or canary CQ and its matched public counterpart (canary rows).
- **Denominator and function.** *Flagged share*: final queries that return a constant or literal, reference no schema element of the realisation or contain no graph pattern, over all final queries; per cell and family. *Canary, construction side*: canary P_func beside the P_func of the matched public items, and the share of canary assertions coded "corrected to public knowledge". *Canary, query side*: canary Q beside the Q of the matched public CQs, with Wilson intervals.
- **Instrument.** AST and literal check of the query-language binding; canary units, hand-written and inserted in the sample under neutral names; supportedness codebook.
- **Scale.** Ratio; [0, 1]; lower is better for the flagged share. A canary deficit beyond the intervals is reported as detected leakage for that family.
- **Expected properties.** A flagged query with a correct answer is F6 with the sub-code "memorised constant", never success. Canary items enter no other card.

## 6. Formulas and order of aggregation, stated once

### 6.1 Sets and states

For a realisation ρ = (S, R), a cell c (ρ under a condition set K in replication i) and the frozen reference:

- A = the core-tier anchored assertions; A_sup = the supporting tier.
- st(a, ρ) ∈ {F1, representable}, decided by the mapping; st(a, c) ∈ {F1, F2, F3, success} after a run.
- Rep(ρ) = {a ∈ A : st(a, ρ) ≠ F1}; Succ(c) = {a ∈ A : st(a, c) = success}.
- W = all non-canary CQs; Req(q) ⊆ A the required assertions of q; Elig(c) = {q ∈ W : Req(q) ⊆ Succ(c)}.
- corr(q, c, p) = 1 if q is semantically correct at the final attempt in pass p of cell c, else 0.
- 𝒦 = the clusters; for a set of CQs D, 𝒦(D) = the clusters with at least one CQ in D.

### 6.2 The three axes

    C(ρ)       = |Rep(ρ)| / |A|
    P_func(c)  = |Succ(c)| / |Rep(ρ)|
    Q_own(c,p) = (1 / |𝒦(Elig(c))|) · Σ over k in 𝒦(Elig(c)) of
                 ( Σ over q in k ∩ Elig(c) of corr(q,c,p) ) / |k ∩ Elig(c)|

These three are the headline cards and the only profile axes. Each is the stage rate of the item table that chains with the other two (SC-10). Every other card has its own denominator and is never added to an axis. Rates with different denominators are multiplied along the chain or shown side by side; they are never added and never averaged.

Q_∩ and Q_oracle use the function of Q_own with Elig(c) replaced by the intersection of the eligible sets of a stated comparison, and by the CQs whose required assertions lie in the oracle core, respectively.

### 6.3 Identities printed in the results tables

    C_r(c)        = |Succ(c)| / |A|            = C(ρ) × P_func(c)
    1             = share F1 + share (F2 + F3) + share success       (over A)
    end-to-end(c) = eligibility rate(c) × Q_own(c)                   (at CQ level, per pass)
    produced ≥ parses ≥ executes ≥ semantically correct              (one denominator)

The identities are arithmetic checks on the item table. They are not models: no reported result is obtained by multiplying rates.

### 6.4 Order of aggregation

1. **Item.** One state per item per cell (section 3.2).
2. **Cluster.** For every question-based rate: the within-cluster share first, then the mean over clusters.
3. **Pass.** A replication's value of a question-based rate is the mean over its completed query passes (up to three); the first-pass value is printed beside it and the completed share per cell is reported (EX-09).
4. **Replication.** Each replication's rate is computed on its own denominator. The reported axis value of a realisation is the arithmetic mean of the three replication rates, with the minimum and the maximum beside it. Counts are never pooled across replications (SC-17).
5. **Above the axis.** Scenario scores, weight maps and dominance are computed from the replication-mean axes. Per-replication axes, scores and rankings are computed as well and shown for spread.
6. **Never pooled.** Nothing is pooled across the two arms, and nothing across condition sets.
7. **Subgroups.** A subgroup figure is the same computation on the subgroup's rows of the item table, under the same small-denominator rule.

Why means first: with a geometric scenario score, averaging per-run scores would let run noise pull the value down, and one failed run could trigger the zero rule of section 7.1.

### 6.5 Not estimable, small denominators, intervals

- Empty denominator: "n.e.", never 0 or 1. Below 10 (items or clusters) per cell and stratum: "k of n", no rate, no interval, not aggregated, listed in the n.e. register.
- An n.e. component is excluded, never imputed.
- Interval of a single-cell rate: Wilson score, 95 %.
- Interval of a treatment aggregate: percentile bootstrap that resamples source units, each carrying its assertions and CQs, within each replication, then averages over replications. Replications are never resampled; their spread is the run-noise band. Every interval names its resampling unit and the number of units (EV-16, SC-18).

### 6.6 Scale and normalisation

Effectiveness components are ratio-scale shares on their natural [0, 1] with a true zero; cost components are seconds, tokens and euro in raw units. There is no treatment-specific normalisation, no min–max across treatments and no z-score in any reported result (SC-05). The absolute scale is fixed by two tests: on the empty graph every run-dependent rate is 0 or n.e.; on the full-gold graph every conditional rate (functional populatability, reference precision, own-set queryability) is 1, while realised coverage, the eligibility rate and end-to-end answerability equal what the mapping allows and reach 1 only at coverage 1.

### 6.7 Cost record

Per model call: the model identifier with its version evidence, tokens by category, wall-clock seconds, pass id. Euro = tokens × the price table frozen with the sample, at list price, whatever was actually paid. The euro figure is a dated linear transform of tokens per model and is valid for comparison inside this study only (SC-14).

## 7. Decision scenarios and the reasons for their weights

### 7.1 What a scenario is

A scenario is a use context expressed as a weight vector on the three profile axes, optional minimum requirements and an optional reading of one axis. A scenario never changes a measurement: one profile per arm serves every scenario (SC-08, SC-14). A cross-dimension number exists only here and always carries its scenario label.

    V_s(t) = C(t)^w_C · P_func(t)^w_P · Q_own(t)^w_Q,   with w_C + w_P + w_Q = 1 and all w ≥ 0

- **Weighted geometric mean** (SC-12). A collapse on one axis cannot be bought back by the other two. This is fixed before any data, with the UNDP Human Development Index of 2010 as precedent. The arithmetic mean is computed only as a robustness toggle.
- **Weights are elasticities** (SC-13). A weight states how much an equal proportional loss on that axis hurts in the use context. "w_Q = 0.6 and w_C = 0.2" reads: a 10 % proportional loss of queryability hurts three times as much as a 10 % proportional loss of coverage. Weights are multiples of 0.1, set by the author from the use context before any score exists, and locked with version 1.0. They are author-set and not derived from a source or from data; each scenario paragraph gives the argument. The three use-context scenarios give the leading axis 0.6 and the other two 0.2 each. Why 0.6: on the 0.1 grid, 0.4 is nearly equal weights and 0.8 nearly ignores two axes, so 0.6 is the one value that states a clear priority and keeps the other two axes in play. The whole weight simplex is analysed as the robustness check, so no single vector carries a conclusion.
- **Zero rule.** An axis at exactly 0 gives the score 0 and the label "collapsed on axis x"; collapsed treatments are not ranked among themselves.
- **Minimum requirement.** A scenario may state an axis or card, a threshold and a reason. It is applied as a filter before ranking. A treatment that fails is "not eligible under scenario s" with the failed requirement; it keeps its profile and is never scored 0. There is no general veto threshold.
- **Weakest dimension.** Printed for every treatment in every scenario.

Scenarios U1 to U3 are use contexts of the case and therefore belong to the Domain Binding; the mechanism above is domain-free.

### 7.2 Scenario U0, equal weights

**Weights** C 1/3, P 1/3, Q 1/3. **Requirement** none. **Reading** standard.

No use context is assumed: an equal proportional loss on any axis hurts equally. This is the centre of the weight simplex and the reference point for a reader who wants one number. Under equal weights the ranking equals the ranking by the product of the three stage rates, so scenario U0 says little that the end-to-end rate does not; what the profile adds is where a treatment loses and how the ranking moves with the use.

### 7.3 Scenario U1, citizen disposal routing in one city

**Weights** C 0.2, P 0.2, Q 0.6. **Requirement** none. **Reading** standard; the row for the municipal routing family is printed beneath the ranking.

A resident or a member of the campus asks where an item goes, through an application that queries the graph. The questions are a narrow set that is asked again and again. What the user experiences is the answer: a fact that is stored but cannot be retrieved is as useless as a missing one, and it is asked for thousands of times. A gap in rarely needed rule areas, or a population step that needs more correction, is paid once by the operator. Hence a 10 % proportional loss of queryability is taken to hurt three times as much as the same loss of coverage or of populatability.

### 7.4 Scenario U2, regulatory lookup on binding rules

**Weights** C 0.6, P 0.2, Q 0.2. **Requirement** coverage of the core assertions from authority classes A and B (binding law and binding municipal or institutional rules) equals 1. **Reading** the coverage axis is read as authority-weighted coverage C_w (card C.3).

An officer or a compliance user looks up what the binding rules say. A rule the schema cannot hold cannot be repaired by better extraction or better queries; it needs a different schema. A missed extraction can be corrected by hand for a bounded body of law, and a lookup user can tolerate a slower or an expert-written query. Hence a 10 % proportional loss of coverage is taken to hurt three times as much as the same loss on either other axis, and a schema that cannot hold every binding rule is not a candidate at all. Coverage counts by confidence in the evidence, because in this use a rule from a statute and a remark from a flyer do not carry the same assurance.

**Authority-class weights for C_w (proposal, to be declared with version 1.0):** A 1.0, B 1.0, C 0.5, D 0.25. They express confidence only: binding federal or state law and binding municipal rules are equally certain evidence for their own scope; official non-binding guidance half as certain; derivative material a quarter.

### 7.5 Scenario U3, bulk ingestion of product catalogues

**Weights** C 0.2, P 0.6, Q 0.2. **Requirements** (1) cost per 1,000 source units at the frozen price table does not exceed the budget B, evaluated per agent family; (2) the unsupported rate (card P.7) stays below an error ceiling that is stated with its reason in version 1.0 (open until 23 November). **Reading** standard.

An operator loads large product catalogues continuously. Nobody reviews single records, so what the agent stores wrongly, or not at all, stays that way; and content with no basis in the source must not enter. The records are uniform, so the capacity of the schema is rarely the limit, and the questions asked later are simple lookups. Hence a 10 % proportional loss of populatability is taken to hurt three times as much as the same loss of coverage or of queryability. Cost is a constraint, never a criterion that can be traded against quality.

**Budget B** is the run allowance of the project per primary cell: the reserved run budget for the 24 primary cells divided by 24, stated in tokens of the primary agent and in euro at the frozen price table, per cell and per 1,000 source units. It is fixed with version 1.0 from the pilot costs and does not depend on any result. If, on pilot costs scaled to the sample, all four treatments of an arm pass or all fail, B is moved once before the freeze to the nearest value that separates, and the change is logged.

### 7.6 What the scenarios are used for

One ranking per scenario and arm; one weight map per scenario and arm showing which treatment is first in which part of the weight space; and one worked schema decision, drawn from scenario U1 or U3 and named with version 1.0, compared with three counterfactual outputs: the single end-to-end score, coverage alone, and the equal-weight mean.
