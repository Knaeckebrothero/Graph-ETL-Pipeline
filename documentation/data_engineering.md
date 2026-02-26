# Data Engineering Documentation

> Fessi — University Waste Disposal Chatbot Knowledge Graph

---

## 1. Introduction

- Problem statement: a university waste disposal chatbot needs structured, queryable knowledge — not just documents, but relationships between waste items, disposal rules, facilities, and bin types
- Why a knowledge graph (Neo4j) over a relational database or document store
  - Waste disposal logic is inherently graph-shaped: items connect to rules, rules branch on conditions, conditions route to streams or facilities
  - Relational joins become unwieldy for variable-depth traversals (e.g., "What can I bring to Wertstoffhof Nord?")
  - Document stores can't model conditional logic natively
- Why Graph-RAG over vector-RAG — the "Paint Can" problem
  - Vector similarity search retrieves documents *about* paint cans, but can't reason about state: liquid paint goes to Schadstoffannahme, dry/empty cans go to Verpackungstonne
  - Graph-RAG traverses decision trees, not document chunks
  - Standard RAG returns "top-k similar passages" — Graph-RAG returns "the correct disposal path for this specific condition"
- Project scope: ETL pipelines that transform raw data (CSV, JSON, PDF, DOCX, dictionary databases) into a Neo4j knowledge graph for Graph-RAG retrieval

---

## 2. Data Sources

### 2.1 Structured Sources (Imported)

#### `disposal_map_db.json`
- 38 waste disposal facilities in Frankfurt
- JSON structure: UUID keys mapping to arrays of facility objects
- Fields: name, address, opening_hours, contact, additional_info, link
- Quality: some duplicate UUIDs for the same facility (handled by deduplication)

#### `Abfall-ABC_new.csv`
- 548 waste items with disposal routes
- Columns: Abfallart (waste item name), Entsorgungsweg (disposal target), Adresse, Offnungszeiten, Kontakt
- Quality issues: multiline cells, concatenated facility names, typos, embedded notes/comments, tab characters, synonym inconsistencies
- Language: German

### 2.2 Unstructured Sources (For LLM ETL)

- PDFs: Abfallsatzung (legal waste statute), container guides (was_kommt_wo_hinein), Zero Waste City document
- DOCX: Bin type documentation (Restabfalltonne, Altpapiertonne, Verpackungstonne, Alltagscontainer)
- Characteristics: German legal language, tables, conditional rules ("wenn...dann"), mixed formatting

### 2.3 Dictionary Sources (For Baseline)

| Source | Content | Scale |
|--------|---------|-------|
| Kaikki.org (Wiktextract) | German noun inventory from Wiktionary | 149,849 word senses |
| OdeNet (Open German WordNet) | Synset-based taxonomy with hypernym/hyponym trees | ~36,000 synsets |
| Open English WordNet | English-side taxonomy, interlingual links | ~120,000 synsets |
| OpenThesaurus | German synonym clusters | 280,000+ synonyms |

### 2.4 Data Quality Assessment

- TODO: completeness per source (which fields are populated, which are missing)
- TODO: consistency issues (naming conventions, format differences across sources)
- TODO: coverage gaps (what the sources collectively don't cover)

---

## 3. Graph Schema Design

### 3.1 Three-Layer Ontology (v4.0)

The knowledge graph follows a three-layer architecture defined in `src/db/schema.cql`:

**Layer 1 — Core Entities (Begriffs-Ebene)**
- `WasteItem`: The central node (e.g., "Banana Peel", "Paint Can")
- `WasteStream`: Target waste fraction / bin type (e.g., "Restabfalltonne", "Biotonne")
- `AVVCode`: Legal classification per Abfallverzeichnis-Verordnung (European Waste Catalogue), 6-digit hierarchical codes

**Layer 2 — Logic & Rules (Regel-Ebene)**
- `DisposalRule`: Wrapper for conditional disposal logic
- `Condition`: Decision node (e.g., "Is it liquid?")
- `Instruction`: Imperative commands (e.g., "Don't flush")
- `Tip`: Helpful hints (sorting_hint, exception, best_practice)

**Layer 3 — Infrastructure (Infrastruktur-Ebene)**
- `Facility`: External disposal locations (e.g., "Wertstoffhof Nord")
- `Container`: Physical bins
- `Room`, `Building`: Campus locations
- `Source`: Provenance tracking for AI-extracted data

### 3.2 Key Relationship Types

| Relationship | From | To | Semantics |
|-------------|------|-----|-----------|
| `DISPOSED_AT` | WasteItem | Facility | Direct disposal location |
| `DISPOSED_IN` | WasteItem | WasteStream | Bin type routing |
| `HAS_RULE` | WasteItem | DisposalRule | Conditional disposal logic |
| `HAS_CONDITION` | DisposalRule | Condition | Decision point |
| `IF_TRUE` / `IF_FALSE` | Condition | WasteStream or DisposalRule | Decision tree branches |
| `ROUTES_TO` | DisposalRule | WasteStream | Rule outcome |
| `CLASSIFIED_AS` | WasteItem | AVVCode | Legal classification |
| `HAS_PARENT` | AVVCode | AVVCode | AVV hierarchy |
| `DERIVED_FROM` | any | Source | Provenance tracking |
| `COLLECTED_IN` | WasteStream | Container | Physical bin mapping |
| `LOCATED_IN` / `PART_OF` | Container/Room | Room/Building | Campus topology |

### 3.3 Constraints and Indexes

- Uniqueness constraints on `uid` and `name` for all core entities
- Indexes on frequently queried properties (AVV level, hazardous flag, tip type, location name)
- Full constraint definitions in `src/db/schema.cql`

### 3.4 Schema Extension: Dictionary Baseline

New node label and relationships added for the dictionary import:

- `DictionaryEntry`: All dictionary-sourced nodes; promoted to `:WasteItem` after LLM classification
- `SYNONYM_OF`: Same-language synonym link between DictionaryEntry nodes
- `TRANSLATION_OF`: Cross-language link (English → German)
- `IS_A`: Taxonomic hierarchy (hyponym → hypernym)

The two-label promotion strategy:
```
(:DictionaryEntry)                    — abstract noun, not disposable
(:DictionaryEntry:WasteItem)          — promoted: physical, disposable item
```

---

## 4. Structured ETL Pipeline

### 4.1 Architecture Overview

Two-phase pipeline with execution order dependency: facilities must be imported before waste items (waste items link to existing facility nodes via `MATCH`).

```
Phase 1: Facilities    →    Phase 2: Waste Items
(creates Facility           (creates WasteItem + WasteStream nodes,
 nodes)                      links to existing Facilities)
```

Both phases share common patterns:
- Deterministic UID generation via SHA256 hash of canonical name (16-char hex)
- `MERGE`-based idempotent ingestion (safe to re-run)
- CLI with `--dry-run` and `--verbose` flags
- Run via `python -m src.etl.<module>`

### 4.2 Phase 1 — Facilities Import

**Module:** `src/etl/facilities.py`
**Input:** `data/disposal_map_db.json`
**Output:** 38 Facility nodes

Key implementation details:
- JSON structure uses UUIDs as keys; multiple UUIDs can reference the same facility by name
- Deduplication: groups entries by facility name, merges the most complete record (most non-empty fields)
- MERGE query with ON CREATE SET (initial properties) and ON MATCH SET (updated_at timestamp)
- Properties: uid, name, address, opening_hours, contact, additional_info, link, created_at, updated_at

### 4.3 Phase 2 — Waste Items Import

**Module:** `src/etl/waste_items.py`
**Input:** `data/Abfall-ABC_new.csv`
**Output:** 548 WasteItem nodes, 5 WasteStream nodes, 1,067 relationships

#### Data Normalization Challenges

The CSV `Entsorgungsweg` column contains messy, inconsistent data requiring extensive cleanup:

| Challenge | Solution | Example |
|-----------|----------|---------|
| Multiline cells | Split by `\n`, process each line | "Wertstoffhof\nSchadstoffmobil" → 2 targets |
| Concatenated names | 25+ regex patterns for known facilities | "WertstoffhofSchadstoffmobil" → 2 targets |
| Typos and tab chars | `FACILITY_NAME_MAP` dictionary | "Restmulltonne" → "Restabfalltonne" |
| Embedded notes | `is_valid_facility_name()` filter | "Laut FES: ..." → filtered out |
| Synonym inconsistency | Canonical name mapping | "Restmulltonne" → "Restabfalltonne" |

#### WasteStream vs Facility Classification

The `WASTE_STREAMS` set defines five bin types that become `WasteStream` nodes (not Facilities):

| WasteStream | Lid Color | Purpose |
|-------------|-----------|---------|
| Restabfalltonne | gray/black | Residual waste |
| Biotonne | brown | Organic waste |
| Verpackungstonne | yellow | Packaging |
| Altpapiertonne | blue/green | Paper/cardboard |
| Altglascontainer | various | Glass |

The `classify_target()` function routes each parsed disposal target to either `DISPOSED_IN → WasteStream` or `DISPOSED_AT → Facility`.

#### Results

| Metric | Value |
|--------|-------|
| WasteItem nodes | 548 |
| WasteStream nodes | 5 |
| DISPOSED_AT relationships | ~935 |
| DISPOSED_IN relationships | ~132 |
| Total relationships | 1,067 |

---

## 5. Dictionary Baseline Pipeline

### 5.1 Motivation

The ontology-first approach addresses five structural problems with bottom-up graph construction:

1. **Duplicate explosion** — Without anchors, "Schokolade", "Schokoriegel", "chocolate bar" become three separate nodes
2. **Inconsistent granularity** — No way to decide if "Batterie" and "Knopfzelle" are synonyms, siblings, or parent-child
3. **Destructive deduplication** — Merging nodes after the fact risks losing edges and breaking provenance
4. **Limited recall** — Graph vocabulary bounded by whatever terms appeared in processed documents
5. **Competing LLM tasks** — Extraction model must simultaneously extract knowledge and resolve entities

### 5.2 Data Sources

- **Kaikki.org**: 149,849 German noun senses from Wiktionary (JSONL, CC-BY-SA 3.0)
- **OdeNet**: ~36,000 German WordNet synsets with hypernym/hyponym trees (CC-BY-SA 4.0)
- **Open English WordNet**: ~120,000 synsets for English translations (MIT)
- **OpenThesaurus**: 280,000+ German synonyms for colloquial coverage (CC-BY-SA 4.0)

### 5.3 Pipeline Phases

```
Phase 0: Download & Cache    →    Phase 1: Parse & Filter    →    Phase 2: LLM Classify
(~3 GB, local)                    (~150k nouns → concept          (is_physical, category
                                   clusters via OdeNet)             via Groq, ~$0.50)
        ↓
Phase 3: Neo4j Import        →    Phase 3b: Promotion
(MERGE pattern, provenance)        (DictionaryEntry → WasteItem
                                    for is_physical=true)
```

**Phase 0** — Download Kaikki.org JSONL, OdeNet/OEWN via `wn` package, OpenThesaurus text dump. All cached in `data/dictionaries/`.

**Phase 1** — Filter to nouns, extract per entry (word, senses, synonyms, translations, hypernyms), cross-reference with OdeNet synsets, enrich from OpenThesaurus, group into concept clusters.

**Phase 2** — LLM classification via Groq (Llama 8B): `is_physical`, `disposal_relevant`, `category`. Cost: under $1.

**Phase 3** — Neo4j import using MERGE pattern. Creates DictionaryEntry nodes, SYNONYM_OF/TRANSLATION_OF/IS_A edges, Source provenance.

**Phase 3b** — Promote DictionaryEntry nodes where `is_physical=true` to `:WasteItem` label.

### 5.4 Expected Scale

| Metric | Estimate |
|--------|----------|
| Concept clusters | ~80-120k |
| After is_physical filter | ~30-60k WasteItem nodes |
| Synonym edges | ~100-200k |
| Translation edges | ~40-80k |
| IS_A (hypernym) edges | ~30-50k |
| **Total** | **~50-100k nodes, ~200-400k edges** |

### 5.5 Integration with Existing Pipeline

New execution order with dictionary baseline as Phase 0:

```
Phase 0:  Dictionary import         (DictionaryEntry nodes)
Phase 0b: LLM classification        (promote to WasteItem)
Phase 1:  Facilities import          (Facility nodes)
Phase 2:  Waste items CSV import     (links to existing DictionaryEntry:WasteItem nodes)
Phase 3:  LLM extraction pipeline    (attaches disposal rules from unstructured sources)
```

---

## 6. LLM-based ETL Pipeline (Architecture)

### 6.1 Overview

Two-tier architecture for extracting structured data from unstructured German documents:

```
Orchestrator (LangGraph State Machine)
    → Extraction Chain (stateless, schema-driven)
        → Entity Loader (idempotent insertion + provenance)
            → Neo4j
```

### 6.2 Data Engineering Aspects

- **Chunking**: Documents split into processable chunks (paragraph, section, sentence, or fixed-size strategies). Default: paragraph with 2000 char max.
- **Entity Loader**: All inserts use `MERGE` for idempotent, re-runnable imports. Existing nodes are enriched (properties updated via `CASE WHEN` pattern), not duplicated.
- **Provenance**: Every extracted entity links to a `Source` node via `DERIVED_FROM`. Source nodes track filename, file type, path, and import date.
- **Dry-run mode**: All ETL modules support `--dry-run` to preview changes without database writes.

### 6.3 Enrichment Strategy

For entities that already exist in the database (e.g., Facilities from Phase 1):

```cypher
MATCH (f:Facility {uid: $uid})
SET f.address = CASE WHEN $address <> '' THEN $address ELSE f.address END,
    f.opening_hours = CASE WHEN $hours <> '' THEN $hours ELSE f.opening_hours END
```

This preserves existing data when the new extraction has empty fields.

---

## 7. Infrastructure

### 7.1 Container Runtime

- Podman (rootless) with podman-compose
- Compose file: `docker/podman-compose.yml`
- Neo4j 5 Community Edition with APOC plugin

### 7.2 Database Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `NEO4J_URI` | `bolt://localhost:7687` | Bolt protocol URI |
| `NEO4J_USER` | `neo4j` | Database username |
| `NEO4J_PASSWORD` | `neo4j_dev` | Database password |

### 7.3 LLM Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `LLM_PROVIDER` | `openai` | Provider: `openai` or `groq` |
| `LLM_API_KEY` | `""` | API key |
| `LLM_BASE_URL` | `""` | OpenAI-compatible endpoint override |
| `LLM_MODEL` | `gpt-4o` | Model name |
| `GROQ_API_KEY` | `""` | Groq API key (fallback to LLM_API_KEY) |

### 7.4 Python Environment

- Python 3.12+ (venv uses 3.13)
- Dependencies: `requirements.txt`
- All modules run via `python -m src.<module>` (sys.path resolution)

### 7.5 Reproducing the Stack

```bash
# 1. Start Neo4j
podman-compose -f docker/podman-compose.yml up -d

# 2. Install dependencies
pip install -r requirements.txt

# 3. Run structured ETL (order matters)
python -m src.etl.facilities
python -m src.etl.waste_items

# 4. (Optional) Run LLM ETL
python -m src.llm_etl.orchestrate --input data/document.txt
python -m src.llm_etl.load --input extracted.json
```

---

## 8. Data Quality & Validation

### 8.1 Validation Queries

```cypher
-- Node counts by type
MATCH (n) RETURN labels(n)[0] AS label, count(*) AS count ORDER BY count DESC;

-- Orphan WasteItems (no disposal route)
MATCH (w:WasteItem) WHERE NOT (w)-[:DISPOSED_IN|DISPOSED_AT]->() RETURN w.name;

-- Items per WasteStream
MATCH (w:WasteItem)-[:DISPOSED_IN]->(s:WasteStream)
RETURN s.name AS stream, count(w) AS items ORDER BY items DESC;

-- Most connected facilities
MATCH (f:Facility)<-[:DISPOSED_AT]-(w:WasteItem)
RETURN f.name AS facility, count(w) AS items ORDER BY items DESC LIMIT 10;

-- Provenance audit
MATCH (n)-[:DERIVED_FROM]->(s:Source)
RETURN s.name AS source, labels(n)[0] AS type, count(*) AS count ORDER BY count DESC;

-- Facilities missing addresses
MATCH (f:Facility) WHERE f.address IS NULL RETURN f.name;
```

### 8.2 Quality Metrics

- TODO: WasteStream distribution analysis (items per bin type)
- TODO: Facility connectivity (items per facility, facilities with no items)
- TODO: Orphan node count over time
- TODO: Provenance coverage (% of nodes with DERIVED_FROM edges)

---

## 9. Results

### 9.1 Current Graph State

| Node Type | Count |
|-----------|-------|
| Facility | 38 |
| WasteItem | 548 |
| WasteStream | 5 |
| **Total** | **591 nodes** |

| Relationship Type | Count |
|-------------------|-------|
| DISPOSED_AT | ~935 |
| DISPOSED_IN | ~132 |
| **Total** | **1,067 relationships** |

### 9.2 WasteStream Distribution

| Stream | Items |
|--------|-------|
| Restabfalltonne | 87 |
| Biotonne | 16 |
| Verpackungstonne | 13 |
| Altpapiertonne | 9 |
| Verpackungstonne (Gelbe Tonne) | 7 |

### 9.3 Projected State After Dictionary Baseline

| Metric | Current | Projected |
|--------|---------|-----------|
| Nodes | 591 | ~50,000-100,000 |
| Relationships | 1,067 | ~200,000-400,000 |
| WasteItem coverage | 548 items | ~30,000-60,000 items |
| Synonym coverage | none | ~100,000-200,000 edges |
| Multilingual support | none | English translations |
