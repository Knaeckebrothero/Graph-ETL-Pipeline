# Graph Import Agent — Architecture Proposal

Generic agent pipeline for enriching the waste knowledge graph from heterogeneous product data sources (Open Food Facts, BMEcat, etc.).

## Motivation

Hardcoding ETL per data source doesn't scale. Instead, a single agent reasons about mapping *any* product record to existing WasteItems in the graph — handling semantic equivalence ("Corny Schoko" → WasteItem "Schokolade"), attribute enrichment, and provenance tracking.

**Economics:** `gpt-oss-20b` on Groq at ~$0.10–0.20/M input tokens makes 10M records feasible for single-digit dollars.

## Core Pattern: Agent-as-Mapper

1. The agent receives a product record (Open Food Facts entry, BMEcat product, etc.)
2. It uses graph tools to search for the best-matching WasteItem
3. It compares existing attributes against the new record
4. It either enriches the WasteItem with new information, or skips if nothing is new
5. All mutations are idempotent (`MERGE` semantics) and tracked via `DERIVED_FROM` provenance

The same agent + toolset works for any data source — only the system prompt and input adapter change.

## Two-Phase Matching

Running a full agent loop for each of 10M records is wasteful. Most Open Food Facts records map to the same ~50–100 WasteItems. The pipeline uses two phases:

### Phase 1 — Bulk Classification (structured output, no tools)

Group records by product category (`categories_tags` in OFF, commodity class in BMEcat). Run a single structured LLM call per *category* to establish the mapping:

```
Input:  category = "Chocolate bars", candidate WasteItems = [Schokolade, Süßwaren, ...]
Output: { match_uid: "abc123", confidence: 0.95, mapping_rule: "All chocolate bars → Schokolade" }
```

Then apply the mapping deterministically to all products in that category. This turns 10M agent calls into ~5K LLM calls + 10M cheap lookups.

### Phase 2 — Agent Enrichment (tool-using agent, complex cases only)

Invoke the full agent loop only for:
- Records that don't fit any existing category mapping
- Low-confidence matches from Phase 1
- Records carrying genuinely new attributes worth reviewing

## Agent Tool Definitions

The agent is only as good as its tools:

| Tool | Purpose |
|------|---------|
| `search_waste_items(query, limit)` | Full-text / fuzzy search on WasteItem names |
| `get_waste_item(uid)` | Fetch all properties + relationships of a WasteItem |
| `get_waste_streams()` | List available WasteStreams for routing decisions |
| `update_waste_item(uid, properties)` | Add/update properties (MERGE semantics) |
| `create_waste_item(name, stream, ...)` | Create new WasteItem only if no match exists |
| `link_product(product_id, waste_item_uid, source)` | Create provenance link back to source record |
| `skip(reason)` | Explicitly pass with logged reason — gives the agent a clean way to say "nothing new here" |

## Pipeline Architecture

```
┌─────────────────────────────────────────────────┐
│  Source Adapter (OFF / BMEcat / CSV / ...)       │
│  Normalizes each source to a common Record type  │
└──────────────────┬──────────────────────────────┘
                   │ Record
        ┌──────────▼──────────┐
        │  Category Grouper    │  ← batch by product type
        └──────────┬──────────┘
                   │
    ┌──────────────▼──────────────┐
    │  Classifier (structured LLM) │  ← 1 call per category
    │  "Corny Schoko" → Schokolade │
    └──────────────┬──────────────┘
                   │ { match_uid, new_props, confidence }
          ┌────────┴────────┐
          │                 │
     high confidence    low confidence / new item
          │                 │
    ┌─────▼─────┐    ┌─────▼──────┐
    │ Bulk MERGE │    │ Agent Loop  │  ← tools for complex cases
    │ (no agent) │    │ (with tools)│
    └───────────┘    └────────────┘
```

## Source Adapters

Each data source gets a thin adapter that normalizes records into a common shape:

```python
@dataclass
class ProductRecord:
    source: str              # "open_food_facts", "bmecat", ...
    source_id: str           # original ID in the source dataset
    name: str                # product name
    category: str            # product category (for grouping)
    attributes: dict         # source-specific attributes (packaging, materials, etc.)
```

### Open Food Facts

Key fields for waste classification:
- `categories_tags` — product category hierarchy
- `packaging_tags` / `packaging_materials_tags` — packaging materials (most useful for waste stream routing)
- `brands`, `product_name` — for display / search

### BMEcat

Key fields:
- `COMMODITY_CLASS` — product classification
- `DESCRIPTION_SHORT` / `DESCRIPTION_LONG` — product description
- Material/composition attributes vary by catalog

## Resilience at Scale

At 10M records:

- **Batching with checkpoints** — track processed record IDs (offset marker or set). Resume from last checkpoint on failure.
- **Dead-letter logging** — records the agent can't classify go to a separate JSONL file for human review.
- **Rate limiting** — respect Groq RPM/TPM limits. Configurable delay between calls (like the existing `--delay` flag in dictionary classifier).
- **Async parallelism** — run N agents concurrently with thread-safe Neo4j sessions.
- **Deduplication** — multiple products may map to the same WasteItem. All mutations use `MERGE` to handle this safely.

## Reusability

The same pipeline works for any new data source:

1. Write a source adapter (normalize to `ProductRecord`)
2. Write a system prompt tailored to the source's domain
3. Run the pipeline

The graph tools, two-phase matching logic, checkpointing, and error handling are all shared infrastructure.

## Existing Infrastructure — Reusable Components

Codebase audit of what's already built and directly reusable.

### Database Layer (`src/db/`)

- **`neo4j_db` singleton** — connection pooling, `query()`, `session()` context manager. Use as-is.

### LLM Infrastructure (`src/llm_etl/`)

| Module | Reusable Component |
|--------|--------------------|
| `config.py` | `get_llm()` — already supports Groq (`LLM_PROVIDER=groq`), temperature control, multi-provider switching |
| `extraction_chain.py` | Structured output extraction with retries, context injection |
| `schemas.py` | Pydantic models, `SCHEMA_REGISTRY` for dynamic dispatch, `WasteStreamType` enum (all 5 Frankfurt bins) |
| `loader.py` | `EntityLoader` — provenance tracking (`DERIVED_FROM`), enrichment-vs-creation logic, dry-run mode |
| `tools/db_tools.py` | `query_db()`, `get_existing_entities()`, `find_gaps()`, `validate_extraction()` |

### Dictionary Classifier (`src/etl/dictionary_classifier.py`) — Closest Precedent

This is the reference implementation. It already does batched LLM classification at scale:

- **5-pass pipeline:** load → LLM classify (batched) → inherit → write → promote
- **Resume support:** queries already-classified UIDs from Neo4j, skips them on restart
- **Rate limiting:** exponential backoff (15s / 30s / 60s on HTTP 429)
- **Structured output:** `llm.with_structured_output(BatchClassificationResult)`
- **Configurable delay:** `--delay` flag for free Groq tier rate limits

```python
# Pattern: batched classification with resume
already_classified = _get_classified_uids(session)
to_classify = [c for c in items if c["uid"] not in already_classified]

for batch in _batched(to_classify, batch_size=25):
    result = structured_llm.invoke(messages)
    time.sleep(delay)
```

### Dictionary Importer (`src/etl/dictionary_importer.py`)

- **`_batched()` generator** — chunked processing for any list
- **Multi-pass batched `UNWIND`** — 100x faster than individual MERGEs (handles 424k nodes + 589k edges)
- **Source node creation** + provenance edge pass

```python
# Pattern: batched UNWIND for bulk writes
for batch in _batched(entries, 500):
    session.run("""
        UNWIND $entries AS e
        MERGE (n {uid: e.uid})
        SET n.name = e.name, ...
    """, entries=batch)
```

### Barcode Module (`src/barcode/`)

- **`BarcodeResolver.lookup(ean)`** — Open Food Facts API client, returns `ProductInfo` Pydantic model
- **`packaging_materials_tags`** — flat list of material codes (e.g. `en:clear-glass`)
- **`packaging_components`** — structured per-component breakdown with material, shape, recycling hint, weight (from OFF `packagings` array)
- **`PackagingComponentInfo`** model — Pydantic model for each physical component
- No API key required, handles timeouts/errors
- Currently single-lookup only (no batch/search endpoint)

### Common Patterns (all ETL modules)

| Pattern | Used In |
|---------|---------|
| `generate_uid()` — deterministic SHA256[:16] | `waste_items.py`, `facilities.py`, `loader.py` |
| Dry-run mode (`--dry-run` flag) | All ETL modules |
| MERGE-based idempotency | All database writes |
| Stats dict returned from every import | All ETL modules |
| Provenance via `DERIVED_FROM` edges | `loader.py`, `dictionary_importer.py` |

## What Needs to Be Built

| Component | Description | Status |
|-----------|-------------|--------|
| **Schema: Product layer** | Product, PackagingComponent, Material constraints in `schema.cql` | **Done** — `schema.cql` v5.0 |
| **Material seed data** | 90 OFF material tags → WasteStream mappings with IS_A hierarchy | **Done** — `data/materials.json` + `src/etl/materials.py` |
| **BarcodeResolver update** | Fetch per-component `packagings` array from OFF API | **Done** — `PackagingComponentInfo` model, live-tested with Nutella |
| **Product importer ETL** | Create Product → PackagingComponent → Material graph from OFF data | Pending |
| **Source adapters** | Normalize OFF/BMEcat records into common `ProductRecord` | Pending |
| **Category grouper** | Batch products by `categories_tags` for Phase 1 bulk classification | Pending |
| **Similarity search** | Find matching WasteItem for a product (text search or embeddings) | Pending |
| **Agent tool definitions** | `search_waste_items`, `update_waste_item`, `skip`, etc. | Pending |
| **Bulk OFF fetcher** | Paginated Open Food Facts search/dump client | Pending |
| **Checkpoint tracker** | Persistent offset tracking for 10M-record runs | Pending |
| **Pipeline orchestrator** | Ties Phase 1 (bulk) and Phase 2 (agent) together | Pending |

## Research Findings: Product Decomposition & Schema Design

Based on a deep research survey of product ontologies, waste management knowledge graphs, and the Open Food Facts API.

### Key Insight: OFF Already Provides Per-Component Decomposition

The Open Food Facts API returns a `packagings` array with **structured per-component breakdown**. Our current `BarcodeResolver` only fetches the flat `packaging_materials_tags` list, but the full data is available.

Example — Nutella (3017620422003):

| Component | Material | Shape | Recycling Hint | Weight |
|-----------|----------|-------|----------------|--------|
| Glass jar | `en:clear-glass` | `en:pot` | `en:recycle-in-glass-bin` | 199.94g |
| Plastic lid | `en:pp-5-polypropylene` | `en:lid` | `en:recycle-in-sorting-bin` | 6.8g |
| Cardboard | `en:non-corrugated-cardboard` | `en:backing` | — | ~2.2g |

This means the agent doesn't need to *infer* decomposition — the data source already tells us "glass jar + plastic lid + cardboard label." The agent's job is to map each component's material tag to the correct WasteStream.

### Ontology Precedents

The `Product → Component → Material` pattern is well-established:

- **CEON (Circular Economy Ontology Network)** from Linkoping University defines `Product -[hasProductComponent]-> ProductComponent` and `Material -[hasMaterialComponent]-> MaterialComponent` as separate decomposition layers. ([liusemweb.github.io/CEON](https://liusemweb.github.io/CEON/))
- **Material Passport Ontology** (Seddiqui et al., 2024) validates a three-tier `Product → Component → Material` hierarchy across manufacturing use cases.
- **Neo4j BOM (Bill of Materials) pattern** uses `HAS_PART` with quantity properties for recursive composition trees — standard in manufacturing graphs.

### Waste-Specific Ontologies

- **ewc-onto** — OWL ontology of the European Waste Catalogue (990 classes, CC BY 4.0). Maps directly to our `AVVCode` node type. Available on [Zenodo](https://zenodo.org/records/15348556).
- **Sinha & Coudert (INRIA)** — OWL ontology for automated waste sorting using material composition percentages. Models N-ary material relations (e.g., "Glass_30" = ">30% glass") to trigger sorting rules. ([hal-00924427](https://inria.hal.science/hal-00924427))
- **No existing system** combines barcode lookup → knowledge graph → per-component waste routing. This would be novel.

### Schema Extension: Product Layer (Implemented)

New node types and relationships added to `src/db/schema.cql` (v5.0):

```
New Layer — Products:
  Product              — commercial product with barcode (e.g., "Nutella 400g")
  PackagingComponent   — physical part of a product (e.g., "glass jar", "plastic lid")
  Material             — material taxonomy node (e.g., "clear-glass", "pp-5-polypropylene")

New Relationships:
  (Product)-[:HAS_COMPONENT {qty, weight_g, shape}]->(PackagingComponent)
  (PackagingComponent)-[:MADE_OF]->(Material)
  (Material)-[:DEFAULT_STREAM]->(WasteStream)
  (Material)-[:IS_A]->(Material)                    — material taxonomy hierarchy
  (Material)-[:CLASSIFIED_AS]->(AVVCode)            — AVV Chapter 15 packaging codes
  (Product)-[:IS_A]->(WasteItem)                    — links product to waste concept
  (PackagingComponent)-[:HAS_RULE]->(DisposalRule)  — component-level conditions
  (Product)-[:DERIVED_FROM]->(Source)               — provenance
```

**Graph pattern for Nutella:**

```
(Product: "Nutella 400g", barcode: "3017620422003")
  ├─[:HAS_COMPONENT]─>(PackagingComponent: "pot", 199.94g)
  │                      └─[:MADE_OF]─>(Material: "clear-glass")
  │                                       └─[:DEFAULT_STREAM]─>(WasteStream: "Altglascontainer")
  ├─[:HAS_COMPONENT]─>(PackagingComponent: "lid", 6.8g)
  │                      └─[:MADE_OF]─>(Material: "pp-5-polypropylene")
  │                                       ├─[:DEFAULT_STREAM]─>(WasteStream: "Verpackungstonne")
  │                                       └─[:IS_A]─>(Material: "plastic")
  ├─[:HAS_COMPONENT]─>(PackagingComponent: "backing", ~2.2g)
  │                      └─[:MADE_OF]─>(Material: "non-corrugated-cardboard")
  │                                       └─[:DEFAULT_STREAM]─>(WasteStream: "Altpapiertonne")
  ├─[:IS_A]─>(WasteItem: "Schokolade")
  └─[:DERIVED_FROM]─>(Source: "openfoodfacts")
```

**Query: "How do I dispose of Nutella?"**

```cypher
MATCH (p:Product)-[:HAS_COMPONENT]->(c:PackagingComponent)
      -[:MADE_OF]->(m:Material)-[:DEFAULT_STREAM]->(s:WasteStream)
WHERE p.name =~ '(?i).*nutella.*' OR p.barcode = $barcode
RETURN c.shape AS component, m.name AS material, s.name AS bin, c.weight_g AS weight
ORDER BY c.weight_g DESC
```

### Material Nodes as Shared Reference Data (Implemented)

Seed data in `data/materials.json` (90 materials), importer in `src/etl/materials.py`. Material nodes are a small taxonomy that millions of PackagingComponents point into:

```
(Material: "plastic")
  ├─(Material: "pp-5-polypropylene")──[:DEFAULT_STREAM]──>(WasteStream: "Verpackungstonne")
  ├─(Material: "pet-1")──[:DEFAULT_STREAM]──>(WasteStream: "Verpackungstonne")
  └─(Material: "hdpe-2")──[:DEFAULT_STREAM]──>(WasteStream: "Verpackungstonne")

(Material: "glass")
  ├─(Material: "clear-glass")──[:DEFAULT_STREAM]──>(WasteStream: "Altglascontainer")
  ├─(Material: "green-glass")──[:DEFAULT_STREAM]──>(WasteStream: "Altglascontainer")
  └─(Material: "brown-glass")──[:DEFAULT_STREAM]──>(WasteStream: "Altglascontainer")
```

The `IS_A` hierarchy enables fallback: if a specific material has no explicit mapping, traverse up to the parent and use its default stream.

### Updated Schema Layers (Implemented in schema.cql v5.0)

```
Layer 0 — Vocabulary:      DictionaryEntry (promoted to WasteItem after LLM classification)
Layer 1 — Core Entities:   WasteItem, WasteStream, AVVCode
Layer 2 — Products:        Product, PackagingComponent, Material
Layer 3 — Logic & Rules:   DisposalRule, Condition, Instruction, Tip
Layer 4 — Infrastructure:  Container, Room, Building, Facility, Source
```

### Material Importer (Implemented)

`src/etl/materials.py` — multi-pass import of the material taxonomy:

```bash
python -m src.etl.materials              # Import materials into Neo4j
python -m src.etl.materials --dry-run    # Preview without changes
python -m src.etl.materials -v           # Verbose output
```

Seed data: `data/materials.json` — 90 materials from OFF, manually mapped to WasteStreams.

| Pass | Operation | Count |
|------|-----------|-------|
| 1 | Source node (provenance) | 1 |
| 2 | Material nodes (`MERGE`) | 90 |
| 3 | `IS_A` hierarchy edges | 76 |
| 4 | `DEFAULT_STREAM` → WasteStream edges | 89 |
| 5 | `DERIVED_FROM` → Source edges | 90 |

Stream distribution:

| WasteStream | Materials |
|-------------|-----------|
| Verpackungstonne | 58 |
| Altpapiertonne | 12 |
| Altglascontainer | 9 |
| Restabfalltonne | 9 |
| Biotonne | 1 |
| (none — inert gas) | 1 |

### Additional Schema Improvements Identified

**Material ↔ AVVCode links:** AVV Chapter 15 has dedicated codes for packaging materials:
- 15 01 01 — Paper/cardboard packaging
- 15 01 02 — Plastic packaging
- 15 01 07 — Glass packaging

Connect `(Material)-[:CLASSIFIED_AS]->(AVVCode)` for regulatory queries.

**Component-level conditions:** A glass jar with food residue routes differently than a clean one. Allow `(PackagingComponent)-[:HAS_RULE]->(DisposalRule)` with conditions like "Is it cleaned?" → IF_TRUE: Altglascontainer, IF_FALSE: Restabfalltonne.

**Material taxonomy via `IS_A`:** Enables fallback routing when a specific material (e.g., `en:pet-1`) has no direct mapping — traverse up to parent `en:plastic`.

### BarcodeResolver Update (Implemented)

`BarcodeResolver` now fetches the `packagings` array and returns structured per-component data:

- `packagings` added to `OFF_FIELDS` in `src/barcode/resolver.py`
- `PackagingComponentInfo` Pydantic model added to `src/barcode/models.py` (fields: material, shape, recycling, number_of_units, weight_measured, food_contact)
- `packaging_components` list added to `ProductInfo`

```python
from src.barcode import BarcodeResolver
r = BarcodeResolver()
p = r.lookup("3017620422003")
for c in p.packaging_components:
    print(f"{c.shape}: {c.material} ({c.weight_measured}g) → {c.recycling}")
# en:pot: en:clear-glass (199.94g) → en:recycle-in-glass-bin
# en:lid: en:pp-5-polypropylene (6.8g) → en:recycle-in-sorting-bin
# en:backing: en:non-corrugated-cardboard (1.8g) → en:recycle-in-sorting-bin
# en:seal: xx:82-c-pap (0.4g) → en:recycle-in-sorting-bin
```

### References

- [CEON — Circular Economy Ontology Network](https://liusemweb.github.io/CEON/)
- [ewc-onto — European Waste Catalogue OWL Ontology](https://zenodo.org/records/15348556)
- [Sinha & Coudert — OWL Ontology for Waste Sorting](https://inria.hal.science/hal-00924427)
- [Material Passport Ontology (Seddiqui et al., 2024)](https://www.researchgate.net/publication/385081535)
- [Neo4j Configurable BOM Pattern](https://neo4j.com/developer/industry-use-cases/manufacturing/product-design-and-engineering/configurable-bom/)
- [CircuGraphRAG — Graph-RAG for Circular Economy (2025)](https://arxiv.org/html/2506.04252v1)
- [Waste-to-Resource Knowledge Graph — Nature Communications (2025)](https://www.nature.com/articles/s41467-025-66599-7)
- [Open Food Facts API Documentation](https://openfoodfacts.github.io/openfoodfacts-server/api/)
- [Open Food Facts Dataset on Hugging Face](https://huggingface.co/datasets/openfoodfacts/product-database)

## Resolved Design Decisions

Answers to the original open questions, informed by the research above.

**1. Confidence threshold (Phase 1 vs Phase 2):**
Keep it simple — Phase 1 returns a match or `null`. If `null`, goes to Phase 2. No numeric scoring needed initially. Add scoring later if the null rate is too high.

**2. New WasteItem creation policy:**
Start conservative: enrich only. The existing WasteItems from Abfall-ABC + promoted DictionaryEntry nodes cover most categories. Unmatched products go to a dead-letter JSONL file for human review. Open up creation later if needed.

**3. Product vs WasteItem modeling:**
Products are **not** WasteItems — they decompose into components. New `Product`, `PackagingComponent`, and `Material` node types (see schema extension above). Products link to WasteItems via `[:IS_A]` ("Nutella" IS_A "Schokolade"). Each component routes independently to a WasteStream via its Material.

**4. Search strategy:**
Start with Neo4j full-text index on `WasteItem.name` + `WasteItem.synonyms`. The Material taxonomy (`~200 nodes`) can be searched by exact tag match. Add embedding-based similarity later if text search proves insufficient for fuzzy matching.

## Remaining Open Questions

- **ProductModel deduplication**: Should products with identical packaging structures (e.g., two brands of 400g chocolate in identical jars) share a `ProductModel` node? Could reduce 10M products to ~2M distinct models. Decide after seeing real data distribution.
- **Food content as component**: Should the food itself (chocolate, jam, etc.) be a PackagingComponent? It routes to Biotonne, but it's not "packaging." Could use a separate `[:CONTAINS]->(FoodContent)` relationship, or just rely on the `Product -[:IS_A]-> WasteItem` link for the food portion.
- **ewc-onto import**: Import the 990-class European Waste Catalogue ontology to enrich AVVCode nodes? Low effort, high value for regulatory queries. Needs a small ETL script.
