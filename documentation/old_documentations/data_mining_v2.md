# Data Mining Documentation (v2)

> Fessi — LLM-based Knowledge Extraction for a Waste Disposal Knowledge Graph

---

## 1. Introduction (shared across both docs)

- Project context: "Fessi" — a university waste disposal chatbot that answers questions like "Where do I throw away paint cans?"
- Problem: waste disposal knowledge is scattered across CSVs, JSONs, PDFs, legal statutes, and municipal documents — no single queryable source of truth
- Why a knowledge graph (Neo4j) over relational or document stores
  - Waste disposal logic is inherently graph-shaped: items → rules → conditions → streams/facilities
  - Variable-depth traversals ("What can I bring to Wertstoffhof Nord?") are natural graph queries, painful as SQL joins
- Why Graph-RAG over vector-RAG — the "Paint Can" problem
  - Vector search retrieves documents *about* paint cans but can't reason about state (liquid → Schadstoffannahme, dry → Restabfalltonne)
  - Graph-RAG traverses decision trees, not document chunks
- Project scope: two ETL pipelines (structured + LLM-based) that transform raw data into a Neo4j knowledge graph, plus a dictionary baseline for entity anchoring
- Two-module split: this document covers the data mining aspects (LLM extraction, classification, orchestration); the companion document covers the data engineering aspects (sources, schema, pipelines, infrastructure)

---

## 2. Related Work

### 2.1 Graph-RAG vs Vector-RAG

- Standard vector-RAG retrieves document chunks by similarity — works for simple factual lookups
- Fails on conditional logic: "How do I dispose of a paint can?" requires knowing the *state*, not just retrieving a passage
- Graph-RAG traverses decision trees modeled as graph paths: `(WasteItem)-[:HAS_RULE]->(DisposalRule)-[:HAS_CONDITION]->(Condition)-[:IF_TRUE]->(WasteStream)`
- The "Paint Can" problem as the motivating example for the architecture

### 2.2 Ontology-First vs Extraction-First Knowledge Graph Construction

- 2025 study comparing approaches for RAG-backed question answering
- Ontology-first KGs achieved 90% answer accuracy vs. 60% for vector-RAG baselines
- Key finding: aligning a KG with an ontology from structured sources matches performance of ontologies derived from dynamic text, at lower computational cost
- Implication: seed the graph with structured dictionary data before running LLM extraction

### 2.3 Structured Output from LLMs

- `with_structured_output()` (LangChain) / function calling (OpenAI API) — constrain LLM output to match a schema
- Pydantic models as output schemas: field names and descriptions serve as implicit prompts
- Eliminates post-processing regex/parsing; output is validated at generation time

### 2.4 LangGraph for Stateful Orchestration

- LangGraph: library for building stateful, multi-step LLM workflows as directed graphs
- Nodes = processing steps, edges = transitions (including conditional routing)
- Enables loop-based extraction with shared state

---

## 3. Document Analysis

### 3.1 Source Document Inventory

| Document | Type | Content | Target Entity Types |
|----------|------|---------|---------------------|
| Abfallsatzung (8. Anderungssatzung) | PDF | Legal waste disposal statute | DisposalRule, Condition |
| was_kommt_wo_hinein_de | PDF | Container sorting guide | WasteItem, WasteStream |
| Auf dem Weg zur Zero Waste City | PDF | Policy document | general context |
| Restabfalltonne.docx | DOCX | Residual waste bin docs | WasteStream, WasteItem |
| Altpapiertonne.docx | DOCX | Paper bin docs | WasteStream, WasteItem |
| Verpackungstonne.docx | DOCX | Packaging bin docs | WasteStream, WasteItem |
| Alltagscontainer.docx | DOCX | Everyday container guide | WasteStream, WasteItem |

### 3.2 Document Characteristics

- **Language**: German, with domain-specific waste management vocabulary
- **Conditional rules**: "wenn...dann" patterns, "falls...sonst" branching, arrow notation (→)
- **Legal formatting**: numbered paragraphs (§), nested subclauses, definitions sections
- **Tables**: yes/no lists per bin type, facility opening hours
- **Mixed content**: same document contains both factual lists and conditional rules

### 3.3 Entity Detection via Keyword Heuristics

The orchestrator detects likely entity types using domain-specific German keywords:

| Entity Type | Keywords |
|-------------|----------|
| WasteItem | "abfall", "mull", "entsorgung", "wertstoff", "recycling" |
| Facility | "wertstoffhof", "annahmestelle", "offnungszeiten", "adresse" |
| WasteStream | "tonne", "biotonne", "gelbe tonne", "leerung" |
| DisposalRule | "wenn", "falls", "flussig", "trocken", "→" |

Detection is keyword-frequency-based: a document mentioning "Offnungszeiten" and "Adresse" multiple times likely contains facility information.

### 3.4 Chunking Strategies

| Strategy | Split Method | Best For |
|----------|-------------|----------|
| Paragraph (default) | Double newline | General documents |
| Section | Headers (# or ALL CAPS) | Structured documents with headings |
| Sentence | Sentence boundaries | Dense text without clear paragraph breaks |
| Fixed-size | Character count with overlap | Large documents, uniform processing |

Default: paragraph strategy with 2000 character max chunk size.

---

## 4. Extraction Architecture

### 4.1 Two-Tier Design

```
┌──────────────────────────────────────────────────────┐
│                   ORCHESTRATOR                        │
│             (LangGraph State Machine)                 │
│   Planning, schema awareness, task delegation         │
│                                                       │
│   Analyze → Plan → Extract (loop) → Validate         │
└───────────────────────┬──────────────────────────────┘
                        │ delegates
                        ▼
┌──────────────────────────────────────────────────────┐
│                 EXTRACTION CHAIN                      │
│        (Stateless, reusable, schema-driven)           │
│                                                       │
│   Input:  text chunk + Pydantic schema + context      │
│   Output: List[Entity] matching the schema            │
└──────────────────────────────────────────────────────┘
```

### 4.2 Design Principle: "The Schema IS the Prompt"

- No complex prompt engineering for each entity type
- Pydantic model field names and `Field(description=...)` guide the LLM
- Swap the schema to extract a different entity type — same chain, same code
- Example: passing `WasteItemExtract` vs `DisposalRuleExtract` to the same chain produces different entity types without changing any prompts

### 4.3 Why Two Tiers

- **Separation of concerns**: the Orchestrator handles intelligence (what to extract, in what order, from which chunks), the Extraction Chain handles execution (call the LLM, validate output, retry)
- The chain is stateless and reusable — can be called independently for simple extraction tasks
- The orchestrator adds planning, context management, and validation around the chain

---

## 5. Schema-Driven Extraction

### 5.1 The Six Pydantic Extraction Schemas

| Schema | Key Fields | Purpose |
|--------|-----------|---------|
| `WasteItemExtract` | name, synonyms, disposal_targets, conditions | Waste items with disposal routes |
| `FacilityExtract` | name, address, opening_hours, contact, accepted_items | Disposal facilities |
| `WasteStreamExtract` | name, english_name, lid_color, emptying_frequency, available_sizes | Bin types with metadata |
| `DisposalRuleExtract` | waste_item, condition, if_true_target, if_false_target | Conditional disposal logic |
| `RelationshipExtract` | source_type, source_name, relationship, target_type, target_name | Cross-entity relationships |
| `SourceExtract` | name, file_type, file_path, url | Provenance metadata |

### 5.2 How Field Descriptions Guide LLM Output

```python
class WasteItemExtract(BaseModel):
    name: str = Field(description="Name of the waste item in German")
    synonyms: list[str] = Field(default=[], description="Alternative names")
    disposal_targets: list[str] = Field(description="Facility or WasteStream names")
    conditions: list[str] = Field(default=[], description="Conditional disposal notes")
```

The LLM sees these descriptions as part of the output schema constraint. "Name of the waste item in German" implicitly instructs the model to extract German-language names, without writing a separate prompt.

### 5.3 Dynamic Schema Dispatch

The `SCHEMA_REGISTRY` maps string names to Pydantic classes:

```python
SCHEMA_REGISTRY = {
    "WasteItem": WasteItemExtract,
    "Facility": FacilityExtract,
    "WasteStream": WasteStreamExtract,
    "DisposalRule": DisposalRuleExtract,
    "Relationship": RelationshipExtract,
    "Source": SourceExtract,
}
```

This enables the orchestrator to dynamically select schemas based on document analysis, without hardcoding entity types.

### 5.4 Retry Logic

- Default: 2 retries on malformed output
- LLM output is validated against the Pydantic schema; if validation fails, the chain retries with the same input
- Prevents pipeline failures from occasional LLM formatting errors

---

## 6. Orchestration Pipeline (LangGraph)

### 6.1 State Machine

```
Entry → analyze_document → create_extraction_plan → run_extraction ↻ → validate_results → End
                                                      (loops until
                                                       all steps done)
```

### 6.2 State Definition

```python
class OrchestratorState(TypedDict):
    document: str
    file_path: str
    document_stats: dict
    detected_entity_types: list[str]
    chunks: list[str]
    extraction_plan: list[dict]
    current_step_index: int
    extracted_entities: list[dict]
    validation_summary: dict
    db_gaps: list[str]
    messages: list[str]
```

### 6.3 Node Descriptions

**`analyze_document`**
- Computes document statistics (word count, paragraphs)
- Detects entity types via keyword heuristics (see Section 3.3)
- Chunks the document using the selected strategy
- Output: detected_entity_types, chunks, document_stats

**`create_extraction_plan`**
- Creates an ordered list of extraction steps (schema + chunk pairs)
- Queries database for existing entities as context (helps LLM match against known names)
- Prioritizes WasteItem extraction first (most common entity type)
- Output: extraction_plan

**`run_extraction`**
- Iterates through the extraction plan step by step
- Delegates each step to the ExtractionChain with the appropriate schema and chunk
- Collects extracted entities into the shared state
- Conditional edge: loops back to itself until all steps are complete
- Output: extracted_entities (accumulated)

**`validate_results`**
- Checks required fields (every entity must have a name)
- Validates references (disposal targets should exist in DB)
- Detects duplicates (entity already in database)
- Identifies gaps (new entities not yet in DB)
- Output: validation_summary, db_gaps

### 6.4 Conditional Edges and Loop Termination

The `run_extraction` node uses a conditional edge:
- If `current_step_index < len(extraction_plan)`: loop back to `run_extraction`
- If all steps complete: proceed to `validate_results`

This enables processing documents of arbitrary size without hardcoding the number of extraction steps.

---

## 7. Dictionary Baseline — LLM Classification

> The data pipeline (download, parse, filter, import) is covered in the Data Engineering documentation. This section covers the LLM classification step.

### 7.1 The Case Against Bottom-Up Entity Creation

Five structural problems when letting the LLM create nodes on-the-fly during extraction:

1. **Duplicate explosion**: "Schokolade", "Schokoriegel", "chocolate bar" become three separate nodes
2. **Inconsistent granularity**: No way to decide if "Batterie" and "Knopfzelle" are synonyms or parent-child without a taxonomy
3. **Destructive deduplication**: Post-hoc merging risks losing relationship edges and breaking provenance
4. **Limited recall**: Graph vocabulary bounded by whatever terms appeared in processed documents
5. **Competing LLM tasks**: The extraction model must simultaneously understand content AND resolve entities

### 7.2 Top-Down Approach: Separate Vocabulary from Knowledge

- Seed the graph with structured dictionary data (149k German nouns) before running LLM extraction
- **Vocabulary building** (deterministic, no LLM needed): creates the noun inventory — covered in DE doc
- **Knowledge attachment** (LLM pipeline): processes unstructured sources and attaches disposal rules to existing anchor nodes
- The LLM focuses only on what it's good at — understanding unstructured text — without the burden of entity resolution

### 7.3 Classification Task

After dictionary import, a small model classifies each concept cluster:

| Field | Question | Type |
|-------|----------|------|
| `is_physical` | Can this noun refer to a physical object? | boolean |
| `disposal_relevant` | Could someone plausibly need to dispose of this? | boolean |
| `category` | Broad waste category | enum (food, electronics, chemicals, packaging, textiles, furniture, construction, hazardous, other) |

### 7.4 Model Selection & Cost

- Small models sufficient for classification: Llama 8B, GPT-OSS-20B
- Inference via Groq API for speed and cost
- Cost estimate: under $1 for ~80k concept clusters
- Total dictionary baseline cost (including cleanup pass): under $5

### 7.5 Two-Label Promotion

```
DictionaryEntry {is_physical: true}   →   SET d:WasteItem   →   queryable as both labels
DictionaryEntry {is_physical: false}  →   stays DictionaryEntry only
```

### 7.6 Chatbot Response States

The two-label strategy enables four distinct chatbot responses:

| State | Agent Response |
|-------|---------------|
| `:WasteItem` with disposal edges | "Dispose of Schokolade in Biotonne" |
| `:WasteItem` without disposal edges | "I know this is a waste item but I don't have disposal info yet" |
| `:DictionaryEntry` only | "This isn't something you'd dispose of at the university" |
| No match | "I don't recognize this term" |

---

## 8. EAN Barcode Pipeline (Data Mining)

> The data pipeline (multi-API resolver, bulk Parquet import, material taxonomy, product ETL) is covered in the Data Engineering documentation. This section covers the barcode scanning, data models, and planned LLM-based product matching.

### 8.1 Barcode Image Scanning

**Module:** `src/barcode/scanner.py`

The BarcodeScanner uses pyzbar (a Python wrapper around the ZBar library) to extract barcode numbers from images:

1. Load image via Pillow
2. Detect and decode barcode regions (supports EAN-13, EAN-8, UPC-A, Code128, QR, etc.)
3. Return structured `ScanResult` with barcode value, format, and confidence

This is a computer vision preprocessing step — no LLM involved. The extracted barcode number is then passed to the BarcodeResolver for product lookup.

### 8.2 Multi-API Fallback Resolution

**Module:** `src/barcode/resolver.py`

The BarcodeResolver implements a cascading fallback pattern, querying APIs in sequence until a product is found:

1. **Open Food Facts** — ~4.29M food products, full packaging decomposition
2. **Open Beauty Facts** — ~13K cosmetics/personal care, same API schema
3. **UPCitemdb** — ~686M products (all categories), name/brand/category only
4. (Optional) **OpenGTINDB** — ~50K German products, German names, bitwise flags

The resolver returns a unified `ProductInfo` regardless of which API responded. The `source_api` field tracks which source provided the data — relevant for provenance and for understanding data completeness (OFF/OBF provide packaging decomposition, UPCitemdb does not).

### 8.3 Pydantic Data Models

```python
class PackagingComponentInfo(BaseModel):
    material: str                 # e.g., "en:clear-glass"
    shape: str                    # e.g., "en:jar"
    recycling: str | None         # e.g., "en:recycle-in-glass-bin"
    number_of_units: int          # e.g., 1
    weight_measured: float | None # Weight in grams
    food_contact: bool | None     # Whether component contacts food

class ProductInfo(BaseModel):
    barcode: str
    found: bool
    source_api: str               # "openfoodfacts", "openbeautyfacts", "upcitemdb", "opengtindb"
    product_name: str
    product_name_de: str          # German name if available
    brands: str
    categories_tags: list[str]
    packaging_components: list[PackagingComponentInfo]
    packaging_materials_tags: list[str]  # Material codes — key for waste routing
    ecoscore_grade: str
    nutriscore_grade: str
    image_url: str
    lookup_timestamp: str         # ISO 8601
```

Follows the same "schema IS the prompt" principle used in the extraction chain — Pydantic models define the structure, validation is automatic. The models normalize responses from four different APIs into one consistent format.

### 8.4 Planned: LLM-based Product → WasteItem Matching

Currently, the `IS_A` link between a `Product` and a `WasteItem` (e.g., Nutella IS_A chocolate spread) is not automatically resolved. Planned approach (see `docs/graph_import_agent.md`):

**Phase 1 — Category-based bulk matching**
- Open Food Facts provides `categories_tags` (e.g., `en:chocolate-spreads`, `en:hazelnut-spreads`)
- Rule-based mapping of OFF categories to existing WasteItem nodes
- Scales to the full 4.29M product corpus without LLM cost

**Phase 2 — LLM agent-based enrichment**
- For products without category matches, use an LLM agent with graph search tools
- Agent tools: search graph by name/category, update product edges, create new WasteItem if needed
- Input: product name, brands, categories
- Output: matched WasteItem UID or "no match"
- Leverages the dictionary baseline: the LLM searches against ~30-60k known WasteItem nodes rather than creating new ones

**Phase 3 — Novel query pattern**
- User asks: "How do I dispose of Nutella?"
- Graph traversal: `Product → HAS_COMPONENT → PackagingComponent → MADE_OF → Material → DEFAULT_STREAM → WasteStream`
- Returns per-component disposal instructions instead of a single answer

### 8.5 The Nutella Example as Applied "Paint Can" Problem

The barcode pipeline makes the "Paint Can" problem concrete and user-facing:

| Component | Material | Disposal Route |
|-----------|----------|---------------|
| Glass jar (210g) | Clear glass | Altglascontainer |
| Plastic lid (8g) | PP-5 Polypropylene | Verpackungstonne |
| Paper label | Paper | Altpapiertonne |

Same product, three different bins. Vector-RAG would retrieve a passage about Nutella; Graph-RAG traverses the actual component → material → stream paths.

At scale (4.29M products, 555K components), this graph structure provides disposal routing for real-world products that no document-based system could match.

---

## 9. Validation & Provenance

### 9.1 Entity Validation

| Check | Entity Types | Description |
|-------|-------------|-------------|
| Required fields | All | Name must be present and non-empty |
| Disposal targets | WasteItem | Referenced targets should exist in database |
| Duplicate detection | All | Warns if entity with same name/UID exists |
| Reference validation | DisposalRule | Checks that referenced WasteItem exists |

### 9.2 Gap Analysis

The validation stage compares extracted entities against the database:
- **New entities**: extracted items not yet in DB (candidates for insertion)
- **Existing entities**: already in DB (candidates for enrichment, not duplication)
- **Missing references**: extracted relationships pointing to non-existent nodes

### 9.3 Provenance Model

Every LLM-extracted entity is linked to its source document:

```
(:WasteItem {name: "Altol"})
    -[:DERIVED_FROM]->
(:Source {name: "Abfallsatzung_2024.pdf", file_type: "pdf", import_date: datetime()})
```

This enables:
- Tracing any piece of knowledge back to its source
- Auditing which documents contributed which entities
- Re-extracting from updated source documents

### 9.4 Enrichment Strategy

For entities that already exist in the database, the loader updates properties without overwriting existing data:

```cypher
SET f.address = CASE WHEN $address <> '' THEN $address ELSE f.address END
```

This prevents partial extractions from erasing previously imported data.

---

## 10. Results & Evaluation

### 10.1Structured ETL Baseline

> Full structured ETL results are in the Data Engineering documentation.

| Metric | Value |
|--------|-------|
| WasteItem nodes | 548 |
| Facility nodes | 38 |
| WasteStream nodes | 5 |
| Total relationships | 1,067 |

### 10.2LLM Pipeline Evaluation

- TODO: demonstration of extraction on sample documents
- TODO: entity detection accuracy (precision/recall on annotated test set)
- TODO: extraction quality assessment (correct vs. hallucinated entities)

### 10.3Coverage Analysis

- TODO: what percentage of waste items in source documents are captured
- TODO: what percentage of disposal rules are modeled as graph paths
- TODO: comparison of graph coverage before and after LLM extraction

### 10.4Cost Breakdown

| Component | Model | Estimated Cost |
|-----------|-------|---------------|
| Dictionary classification (~80k clusters) | Llama 8B via Groq | < $1 |
| Dictionary cleanup pass | GPT-OSS-20B via Groq | ~$1-2 |
| Document extraction (per document) | Configurable (local or API) | Variable |
| **Total dictionary baseline** | | **< $5** |

### 10.5Limitations and Future Work

- TODO: current limitations (entity types not yet modeled, documents not yet processed, conditional logic coverage gaps)
- TODO: planned enhancements (multi-model comparison, active learning, incremental updates, batch processing)
- TODO: scalability considerations (graph size projections, query performance)
