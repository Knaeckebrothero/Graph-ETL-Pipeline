# Prior work and starting point

What the earlier fessi semesters built, what actually ran, and what the theses can reuse. This was compiled on 19 October 2026 from a review of the ETL documentation against the code, the reports and decks, the schema history, the source research and the material of earlier semesters. Where the reports and the code or files disagree, this summary follows the code and files.

**Bottom line.** The theses start from inputs and readers, not from results.

- **What exists:** readers for seven structured sources, one schema that actually runs (v5.0 Cypher), a 324-row candidate source catalogue, a few local PDF and DOCX documents, and 160 old chatbot questions with gold answers.
- **The record is weaker than the reports suggest:**
  - the source research is LLM output that nobody verified;
  - WiSe 2025/26 never evaluated answers, only whether the pipelines produced valid output;
  - only structured sources populated anything: the rules layer never held a node, and the infrastructure layer only facilities;
  - six relationship vocabularies coexist;
  - several docs report work as complete that the code does not contain.
- **No earlier graph is reused.** Each took about a day to build, and rebuilding graphs is what the theses study.

## Semesters

| Phase | When | What was built | What carried over |
|---|---|---|---|
| "Müll-Meister" | SoSe 2024 | Flask + Chroma RAG chatbot with GPT-4o mini and CLIP | The four bin DOCX files (April 2024) |
| Projekt Fessie | WiSe 2024/25, Prof. Fink. Submitted 31 March and 29 April 2025 | Three groups: backend (G1), pipelining (G2), frontend (G3). G1 and G3 evaluated the chatbot | G2's data folder (Abfall-ABC files, facility map, persona files) and G1's question sets |
| Projekt DS | SoSe 2025 | Angular 19 PWA on a mock backend | Gruppe 2's AVV workbook (June 2025) and Gruppe 3's Abfall-ABC workbook (May 2025); no code |
| Knowledge graph | WiSe 2025/26, submitted 28 February 2026 | Schema v4.0 spec (5 Dec 2025); first ETL (9–10 Dec); source research (10–17 Jan); most importers and the LLM ETL, committed 26 Feb | This repository; tag `wise2526-submission` |
| Afterwards | March–May 2026 | Barcode lookup in the chatbot; a k3s redeployment plan (never started) | Chatbot branch `feature/barcode-scanner`; `deploy/history/` |

Almost all of the WiSe 2025/26 code arrived in two commits on 26 February 2026, so Git cannot date its development.

## What is in this folder

| Folder | Content |
|---|---|
| [`reports/`](reports/) | The graded Data Engineering and Data Mining reports (PDF, 28 Feb 2026) with their Markdown sources, section drafts and the outline |
| [`design/`](design/) | Implementation docs of WiSe 2025/26: ETL plan, AI-ETL workflow, dictionary baseline, Open Food Facts import, BMEcat import, AVV classification, graph import agent |
| [`presentations/`](presentations/) | Decks: basic presentation, data and schema overview, status update, the pitch to Prof. Fink |
| [`figures/`](figures/) | Query screenshots with their Cypher, and chatbot UI screenshots |
| [`source-provenance/`](source-provenance/) | Two write-ups from the Teams folder on which source columns were used and which sources were left out |

Not copied here, still in the WiSe 2025/26 Teams folder: the decks as PPTX with speaker notes, Word drafts, 19 schema papers, the reading list "Finks Quellen" and a RAG course archive.

## Import code: designed, built, ran

| Stage | Designed in | Implemented | Ran |
|---|---|---|---|
| Five `WasteStream` nodes | `design/dictionary_baseline.md` ("DONE" via `init_db.py --schema`) | No: the script creates constraints only | Yes, by code that is lost; rebuildable from `WASTE_STREAM_REFERENCE` in `src/llm_etl/schemas.py` (uid = first 16 hex digits of `sha256(name)`) |
| Facilities, Abfall-ABC | `design/etl_implementation.md` | Yes | December 2025 only |
| Abfall-ABC on the dictionary (pre-promotion, provenance) | `design/dictionary_baseline.md` ("COMPLETE") | **No**; it exists only in the doc | No |
| Dictionary parse, import, classify | `design/dictionary_baseline.md` | Yes; promotion requires `is_physical` **and** `disposal_relevant` | Yes; Groq llama-3.1-8b, about $0.30 |
| Materials, Open Food Facts, Open Beauty Facts, Ecolabel | `design/open_food_facts_import.md`, `design/bmecat_import.md` | Yes | Yes |
| OpenGTINDB | `design/bmecat_import.md` | Yes | No |
| BMEcat | `design/bmecat_import.md` | **No code at all** | No |
| AVV codes and classification | `design/avv_classification.md` | Yes: chapter → subgroup → code, validated at each step | 500 items, alphabetically first; 499 valid codes, mostly catch-alls |
| LLM pipeline, orchestrator, loaders | `design/etl_implementation.md` ("Complete"), `design/ai_etl_workflow.md` | Partly: the orchestrator plans without an LLM, validation does not gate loading, the generic loader writes nodes only | Never on a corpus |
| PDF/DOCX extraction | README "Planned" | No: PDFs are read as raw bytes | No |
| Graph import agent | `design/graph_import_agent.md` | Not started; every component "Pending" | No |

**Inconsistencies that would bias any measurement:**
- four ID policies: `sha256(name)` case-sensitive, `sha256(lower(strip(name)))` in the LLM loader, `sha256(ean)`, and the natural AVV code. The ETL and LLM loader therefore collide on the `WasteItem` name constraint;
- merge rules differ by importer: Open Food Facts and Open Beauty Facts overwrite on match (last writer wins), Ecolabel only adds its own fields;
- missing values are stored as `""` or `[]`, not NULL as documented;
- provenance exists at dataset level only (five `Source` nodes), and not at all for facilities, Abfall-ABC or AVV codes;
- no run ID, model ID or token log anywhere;
- "the schema is the prompt" is false in practice: in the default JSON mode the schema never reaches the model.

**Worth reusing:** the per-source readers (with a separate writer per schema under test); the per-field coverage profiling in `design/open_food_facts_import.md`; the batched UNWIND writer with `--dry-run` and statistics; hierarchical constrained choice validated against the allowed set (the AVV pattern); the import-agent design in `design/graph_import_agent.md`; `schema_to_prompt_hint` as a schema serialiser; the documented provider failures (Groq tool calling, the JSON keyword, small-model key drift) as threats to validity.

**Avoid:** mixed ID rules, last-writer-wins updates, empty strings for missing values, status claims without run artefacts, `ORDER BY name LIMIT n` as a sample, promoting a 98k-word dictionary into the label being tested, and network-only sources without a frozen snapshot.

## Questions and evaluation

**WiSe 2025/26 measured pipelines, not answers.** There was no question set, gold standard or accuracy figure. The AVV "99.8 %" means 499 of 500 items got a *valid* code, mostly the catch-alls 20 01 28 and 20 01 99, not a correct one. The "90 % vs 60 %" accuracy figure in the Data Engineering report comes from an external study (da Cruz et al. 2025). The team's own criteria (coverage, rule depth, location accuracy) were never measured, and its January question to the supervisor, "Welche Evaluation ist gewünscht?", has no recorded answer.

**WiSe 2024/25** evaluated a vector-RAG chatbot. Gruppe 1's question sets are the seeds in [`questions/`](../../questions/README.md); see there for their quality. Methods worth reusing:
- G1's test-case JSON with a structured gold field (route, facility, AVV code) scored by exact match;
- reporting failures instead of cleaning them out (G1's cleaning removed 17 of 18 document-grounded questions, so its scores rest on templated items);
- G3's LLM-as-judge with a negative control built by rotating answers across questions (every control mean ≤ 22.9), to be run with randomised order and a judge from another model family;
- a generator separate from the judge, and scoring the factual answer apart from its wording.

## Claims in the reports against the record

Cite the reports for intent and design, and the code and files for outcomes.

| Claim | Where | Record |
|---|---|---|
| 548 Abfall-ABC items, 1,067 routes and 38 facilities in the graph | DE p. 31; DM p. 28 | Only in the December 2025 graph, not in the submitted dumps |
| 67,912 WasteItems promoted | DE p. 6, 26 | 98,449 after a second run; the printed query omits `disposal_relevant`, which the code requires |
| ~400 viable sources; ~550 on a rubric; 68 evaluated | DM p. 5, 13; DE p. 22 | 324 catalogue rows, about 256 real sources, none verified |
| Barcode scanner "fully integrated" | DE p. 16, 32 | Never committed until October 2026 |
| The schema "supports all anticipated data categories without modification" | DE p. 6, 21, 32 | Never exercised: the rules layer is empty |
| Every node records its origin | DE p. 22 | Only products, dictionary entries and materials do |
| A node-type list in the prompt is enough for Cypher | DE p. 18 | Untested; this is exactly what the queryability condition tests |
| Agent mode prevents near-duplicates; 2–3 dry runs suffice | DM p. 25, 30–31 | No dry-run logs; the agent never ran on a corpus |

## Reuse and citation

- The earlier material is unpublished coursework without a licence. Cite it as unpublished project documentation by the authors on its title pages. The WiSe 2025/26 reports list four authors: Riel, Hashemi, Rao and Cefariello.
- Ask Prof. Fink before reusing other groups' question sets, code or results verbatim, and credit SoSe 2025 Gruppe 2 for the AVV workbook.
- Keep the Post Direkt/Sinus persona data out entirely, and check the scope of the FES NDA (signed 10 November 2025) before publishing FES-derived content.
- Label the LLM-generated source research as such.

## Open points

- Who produced `sources/raw/fes-abfall-abc/Abfall-ABC_new.csv`, and how?
- SoSe 2025 Gruppe 2's report on how the AVV workbook was built exists only as a PDF on the laptop; read it.
- Scope of the FES NDA.
- Prof. Fink's permission to reuse the earlier groups' material.
