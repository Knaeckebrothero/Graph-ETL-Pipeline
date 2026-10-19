# AVV Classification: WasteItem → AVVCode

## Goal

Classify every `WasteItem` node in the knowledge graph with the appropriate 6-digit AVV code
(Abfallverzeichnis-Verordnung / European Waste Catalogue) using the LLM Pipeline.

The AVV is the legal classification system for waste in Germany. Each waste type gets a 6-digit code
that determines handling, transport, and disposal requirements.

## AVV Code Structure

The AVV is a 3-level hierarchy:

| Level | Format | Count | Example |
|-------|--------|-------|---------|
| 1 — Chapter | `XX` | 20 | `20` — Siedlungsabfälle (Municipal Waste) |
| 2 — Subgroup | `XX XX` | ~113 | `20 01` — Getrennt gesammelte Fraktionen |
| 3 — Specific type | `XX XX XX` | ~1,505 | `20 01 01` — Papier und Pappe |

Codes ending with `*` indicate hazardous waste (gefährlicher Abfall).
Codes ending with `99` are catch-all categories ("Abfälle a.n.g." — waste not otherwise specified).

**Data source:** `data/Abgabe_Gruppe2_AVV_mit_AbfallABC.csv` (1,505 Level 3 codes, semicolon-delimited).

## What We Built

### 1. AVV Code Importer (`src/etl/avv_codes.py`)

Structured ETL (no LLM) that parses the AVV CSV and creates Neo4j nodes:

- Creates `AVVCode` nodes at all 3 levels with properties: `code`, `name`, `level`, `is_hazardous`, `status`
- Creates `HAS_PARENT` relationships: `(Level3)-[:HAS_PARENT]->(Level2)-[:HAS_PARENT]->(Level1)`
- MERGE-based, idempotent
- Registered in `src/etl/__init__.py`

```bash
python -m src.etl.avv_codes           # import all codes
python -m src.etl.avv_codes --dry-run # preview only
```

**Result:** 22 chapters, 113 subgroups, 1,505 specific types imported.

Note: The CSV contains chapters 21 and 22 beyond the standard 20 — these are custom
university-specific codes from the FRA-UAS data (e.g., Lötpaste, Gasflaschen, Heftordner).

### 2. Classification Script (`src/etl/avv_classification.py`)

Uses 3-step hierarchical classification with `ExtractionChain.extract_single()` to
classify each WasteItem against the AVV catalog. Each item requires 3 small LLM calls
instead of 1 large one:

1. **Chapter** — pick from 20 AVV chapters (~272 tokens context)
2. **Subgroup** — pick from ~5-30 subgroups within that chapter
3. **Specific code** — pick from ~5-15 codes within that subgroup

```bash
python -m src.etl.avv_classification --limit 5 --dry-run -v   # test 5 items
python -m src.etl.avv_classification --skip-classified         # only unclassified
python -m src.etl.avv_classification --delay 2                 # custom rate limit
```

### 3. Pipeline Fixes for Groq Compatibility

Two fixes were needed to make the Pipeline work with Groq as the LLM provider:

**Fix 1 — `structured_output_method` parameter** (`src/llm_etl/extraction_chain.py`, `src/llm_etl/pipeline.py`):

Groq's tool use (function calling) validation rejects valid JSON that the model generates correctly.
The model outputs `<function=AVVClassificationList> {...}` and Groq's own API rejects it.

Solution: Added `structured_output_method` parameter (default: `"json_mode"`) to `ExtractionChain`
and `Pipeline`. This uses `response_format: {"type": "json_object"}` instead of tool calling,
bypassing Groq's broken function call validation.

**Fix 2 — "json" keyword requirement** (`src/llm_etl/extraction_chain.py`):

Groq requires the literal word "json" to appear somewhere in the message content when
`response_format: {"type": "json_object"}` is set. LangChain's `with_structured_output(method="json_mode")`
doesn't add this automatically.

Solution: Added "and return them as JSON" to the system prompt in `ExtractionChain.DEFAULT_SYSTEM_PROMPT`.

## Issues Encountered

### Issue 1: Groq Function Calling Broken (400 Bad Request)

**Error:** `"Failed to call a function. Please adjust your prompt."` with `tool_use_failed` code.

**Root cause:** Groq's API rejects its own model's tool call output. The `failed_generation` field
shows perfectly valid JSON, but Groq's validation layer can't parse it back.

**Impact:** All extraction calls fail with 400 errors on every retry.

**Resolution:** Switch from `function_calling` to `json_mode` structured output method.
See Fix 1 above.

### Issue 2: Groq "json" Keyword Requirement (400 Bad Request)

**Error:** `"'messages' must contain the word 'json' in some form, to use 'response_format' of type 'json_object'."`

**Root cause:** Groq enforces that the word "json" appears in the prompt text when JSON mode is enabled.
LangChain doesn't inject this automatically.

**Resolution:** Added "and return them as JSON" to system prompt. See Fix 2 above.

### Issue 3: 98K WasteItem Nodes Are Dictionary Entries

**Symptom:** Script reported 9,845 batches (98,449 items) instead of the expected ~548.

**Root cause:** The dictionary classifier had promoted all `DictionaryEntry` nodes to `WasteItem`.
All 98,449 nodes carry both labels (`:WasteItem:DictionaryEntry`). There are 0 "original"
WasteItem nodes (from the Abfall-ABC CSV). The items include words like "(eingeschossiges) Haus",
"(nahe) Verwandtschaft", "(modisches) Zubehör" — clearly not waste items.

**Impact:** 98K items × 10 per batch × ~20K tokens context = ~197M tokens total.
On Groq's 250K tokens/min limit, this would take ~13 hours.

**Current status:** Need to either re-import original waste items from `Abfall-ABC_new.csv`
or filter the classification query to exclude dictionary-promoted entries.

### Issue 4: Rate Limiting (429 Too Many Requests)

**Error:** `429 Too Many Requests` with `x-ratelimit-remaining-tokens: 17284`.

**Root cause:** Each LLM call sends the full 1,505-code AVV reference as context (~20K tokens).
Groq's rate limit is 250K tokens/min, so we hit it after ~12 calls.

**Resolution:** Hierarchical 3-step classification (see below). Each call is ~300-500 tokens,
well under any rate limit. The `--delay` flag (default 2s) provides additional safety margin.

### Issue 5: 8B Model Too Small for Classification (0 Entities Extracted)

**Symptom:** Successful 200 OK responses but "Extracted 0 AVVClassification entities" on every batch.
Some batches also fail with `"max completion tokens reached before generating a valid document"`.

**Root cause:** `llama-3.1-8b-instant` (8B parameters) cannot handle:
- 20K tokens of AVV context
- 10 German waste item names
- Structured JSON output with reasoning

The model either returns empty `{"items": []}` or echoes back input words as raw JSON
(e.g., `{"Abbausystems", "Abbauverlust", ...}` — not even valid JSON, just a list of words).

**Resolution:** Hierarchical 3-step classification. Each call has ~300 tokens of context,
which `llama-3.1-8b-instant` handles without issues.

### Issue 6: 8B Model Ignores Pydantic Field Names in JSON Mode

**Symptom:** LLM returns `{"AVV_Chapter": "20"}` instead of `{"chapter_code": "20", "reasoning": "..."}`.
All 3 retries fail with the same field name mismatch.

**Root cause:** In `json_mode`, LangChain's `with_structured_output()` injects the schema but
`llama-3.1-8b-instant` ignores the Pydantic field names and invents its own JSON keys.

**Resolution:** Added explicit JSON format instructions and examples directly in each prompt:
```
Return JSON with exactly these keys: "chapter_code" (2-digit string) and "reasoning" (brief explanation).
Example: {"chapter_code": "20", "reasoning": "Municipal household waste"}
```
This forces the model to use the correct field names.

## Implemented Approach: Hierarchical 3-Step Classification

Instead of stuffing all 1,505 codes into a single LLM call, each item is classified in 3 steps:

| Step | Schema | Context size | Question |
|------|--------|-------------|----------|
| 1. Chapter | `ChapterClassification` | 22 codes (~272 tokens) | Which of the 20 AVV chapters? |
| 2. Subgroup | `SubgroupClassification` | ~5-30 codes | Which subgroup within that chapter? |
| 3. Specific code | `AVVClassification` | ~5-15 codes | Which 6-digit code within that subgroup? |

**How it works:**
- Uses `ExtractionChain.extract_single()` directly (not Pipeline)
- AVV hierarchy loaded from `parse_avv_csv()` in `src/etl/avv_codes.py`
- At each step, context is dynamically filtered to only show valid children of the previous step's result
- Validation at each step: returned code must exist in the filtered set
- If any step fails or returns an invalid code, the item is skipped

**Benefits:**
- Tiny context per call (~300-500 tokens total instead of 20K)
- Works with small models (`llama-3.1-8b-instant`)
- 3 cheap calls per item instead of 1 expensive call
- No rate limit issues on Groq (3 × ~500 tokens = ~1.5K tokens/item)
- Hierarchical consistency guaranteed by validation

**Trade-off:** 3× more API calls per item, but each is 40× smaller. Net token usage is lower.

**CLI flags:**
```
--dry-run       Classify but don't write relationships
--limit N       Max items to classify (0 = all)
--delay D       Seconds between items (default: 2.0)
--skip-classified  Skip items with existing CLASSIFIED_AS
--verbose       Show each classification step
```

## Test Runs

### Smoke Test (10 items, dry run)

```bash
python -m src.etl.avv_classification --limit 10 --dry-run --delay 1
```

**Result:** 10/10 classified, 0 failures.

- Model: `llama-3.1-8b-instant` via Groq
- ~300ms per LLM call, ~1s per item (3 calls + overhead)
- No rate limit issues (each call ~300-500 tokens)

### First Batch Run (500 items, 2026-02-27)

```bash
python -m src.etl.avv_classification --limit 500 --delay 1 --skip-classified
```

**Result:** 499/500 classified, 1 failure, 499 relationships written to Neo4j.

| Metric | Value |
|--------|-------|
| Items queried | 500 |
| Classified | 499 (99.8%) |
| Failed | 1 (`A-Kohle`) |
| Hazardous | 8 |
| Relationships created | 499 |
| Runtime | ~18 minutes |

**Chapter distribution:**

| Chapter | Name | Items |
|---------|------|-------|
| 20 | Siedlungsabfälle (Haushaltsabfälle) | 490 |
| 02 | Abfälle aus Landwirtschaft, Gartenbau, Nahrungsmittel | 9 |

**Query results:**
```cypher
-- All classified items
MATCH (w:WasteItem)-[r:CLASSIFIED_AS]->(a:AVVCode)
RETURN w.name, a.code, a.name, r.is_hazardous, r.reasoning
ORDER BY w.name LIMIT 50

-- Hazardous items only
MATCH (w:WasteItem)-[r:CLASSIFIED_AS]->(a:AVVCode)
WHERE r.is_hazardous = true
RETURN w.name, a.code, r.reasoning

-- By chapter
MATCH (w:WasteItem)-[:CLASSIFIED_AS]->(a:AVVCode)
RETURN substring(a.code, 0, 2) AS chapter, count(w) AS items
ORDER BY items DESC
```

**Observations:**
- Pipeline mechanics work correctly — 99.8% success rate, no rate limit issues, no parsing failures.
- Classification quality is poor for many items because the current WasteItem nodes are
  dictionary entries promoted by the dictionary classifier (e.g., "Diamant", "Verwandtschaft",
  "0,2-Liter-Flasche"). These are not actual waste items, which explains the heavy bias toward
  catch-all codes like 200128 (Farben/Druckfarben/Klebstoffe) and 200199 (sonstige Fraktionen).
- The 8 hazardous classifications and 9 agriculture-chapter items suggest the model does
  differentiate when the item name carries real waste semantics.
- Quality will improve significantly when run on real waste items from `Abfall-ABC_new.csv`
  instead of dictionary-promoted entries.
