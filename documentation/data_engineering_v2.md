# Data Engineering Documentation (v2)

> Fessi — University Waste Disposal Chatbot Knowledge Graph

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
- Two-module split: this document covers the data engineering aspects (sources, schema, pipelines, infrastructure); the companion document covers the data mining aspects (LLM extraction, classification, orchestration)

---

## 2. Data Sources & Quality

### 2.1 Source Inventory

| Source | Format | Content | Records |
|--------|--------|---------|---------|
| `disposal_map_db.json` | JSON | 38 waste disposal facilities in Frankfurt | 38 facilities |
| `Abfall-ABC_new.csv` | CSV | Waste items with disposal routes | 548 items |
| Kaikki.org (Wiktextract) | JSONL | German noun inventory from Wiktionary | 149,849 senses |
| OdeNet | WordNet XML | German synsets with hypernym/hyponym trees | ~36,000 synsets |
| Open English WordNet | WordNet XML | English-side taxonomy, interlingual links | ~120,000 synsets |
| OpenThesaurus | Text dump | German synonym clusters | 280,000+ synonyms |
| PDFs (Abfallsatzung, container guides, etc.) | PDF | Legal statutes, sorting guides, policy docs | 5 documents |
| DOCX (bin type documentation) | DOCX | Per-bin-type waste lists and rules | 4 documents |

### 2.2 Structured Source Analysis

#### `disposal_map_db.json`
- JSON structure: UUID keys mapping to arrays of facility objects
- Fields: name, address, opening_hours, contact, additional_info, link
- Quality issues: duplicate UUIDs referencing the same facility (handled by deduplication logic)

#### `Abfall-ABC_new.csv`
- Columns: Abfallart (waste item name), Entsorgungsweg (disposal target), Adresse, Offnungszeiten, Kontakt
- Quality issues: multiline cells, concatenated facility names, typos, embedded notes/comments, tab characters, synonym inconsistencies
- Language: German throughout

### 2.3 Dictionary Sources

- Kaikki.org: CC-BY-SA 3.0, ~3 GB raw download, JSONL format with one entry per word sense
- OdeNet: CC-BY-SA 4.0, accessed via Python `wn` package
- Open English WordNet: MIT license, cross-referenced with OdeNet for translations
- OpenThesaurus: CC-BY-SA 4.0, plain text synonym groups

### 2.4 Data Quality Assessment

- Completeness: which fields are populated vs. missing per source
- Consistency: naming conventions, format differences across sources
- Coverage gaps: what the sources collectively don't cover (e.g., campus-specific locations, seasonal items)

---

## 3. Graph Schema Design

### 3.1 Three-Layer Ontology (v4.0)

Defined in `src/db/schema.cql`:

**Layer 1 — Core Entities (Begriffs-Ebene)**
- `WasteItem`: Central node (e.g., "Bananenschale", "Farbdose")
- `WasteStream`: Target waste fraction / bin type (e.g., "Restabfalltonne", "Biotonne")
- `AVVCode`: Legal classification per Abfallverzeichnis-Verordnung, 6-digit hierarchical codes
- `Product`: EAN-identified products mapped to waste categories

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
| `IF_TRUE` / `IF_FALSE` | Condition | WasteStream/DisposalRule | Decision tree branches |
| `ROUTES_TO` | DisposalRule | WasteStream | Rule outcome |
| `CLASSIFIED_AS` | WasteItem | AVVCode | Legal classification |
| `HAS_PARENT` | AVVCode | AVVCode | AVV hierarchy |
| `IS_A` | Product | WasteItem | Product → waste category |
| `HAS_PACKAGING` | Product | WasteItem | Product packaging waste |
| `SYNONYM_OF` | DictionaryEntry | DictionaryEntry | Same-language synonym |
| `TRANSLATION_OF` | DictionaryEntry | DictionaryEntry | Cross-language link |
| `DERIVED_FROM` | any | Source | Provenance tracking |
| `COLLECTED_IN` | WasteStream | Container | Physical bin mapping |
| `LOCATED_IN` / `PART_OF` | Container/Room | Room/Building | Campus topology |

### 3.3 Constraints and Indexes

- Uniqueness constraints on `uid` and `name` for all core entities
- Indexes on frequently queried properties (AVV level, hazardous flag, tip type, location name)
- Full constraint definitions in `src/db/schema.cql`

### 3.4 Schema Extension: Dictionary Baseline

- `DictionaryEntry` node label for all dictionary-sourced nodes
- Two-label promotion strategy: `(:DictionaryEntry)` → `(:DictionaryEntry:WasteItem)` after LLM classification
- Enables four distinct chatbot response states (known item with route, known item without route, non-disposable noun, unknown term)

---

## 4. Structured ETL Pipeline

### 4.1 Architecture Overview

Two-phase pipeline with execution order dependency: facilities must be imported before waste items.

```
Phase 1: Facilities    →    Phase 2: Waste Items
(creates Facility           (creates WasteItem + WasteStream nodes,
 nodes from JSON)            links to existing Facilities from CSV)
```

Shared patterns across both phases:
- Deterministic UID generation via SHA256 hash of canonical name (16-char hex)
- `MERGE`-based idempotent ingestion (safe to re-run)
- CLI with `--dry-run` and `--verbose` flags
- Run via `python -m src.etl.<module>`

### 4.2 Phase 1 — Facilities Import

**Module:** `src/etl/facilities.py`
**Input:** `data/disposal_map_db.json`
**Output:** 38 Facility nodes

- Deduplication: groups entries by facility name, selects the most complete record (most non-empty fields)
- MERGE with ON CREATE SET / ON MATCH SET pattern
- Properties: uid, name, address, opening_hours, contact, additional_info, link, created_at, updated_at

### 4.3 Phase 2 — Waste Items Import

**Module:** `src/etl/waste_items.py`
**Input:** `data/Abfall-ABC_new.csv`
**Output:** 548 WasteItem nodes, 5 WasteStream nodes, 1,067 relationships

#### Data Normalization

| Challenge | Solution | Example |
|-----------|----------|---------|
| Multiline cells | Split by `\n`, process each line | "Wertstoffhof\nSchadstoffmobil" → 2 targets |
| Concatenated names | 25+ regex patterns | "WertstoffhofSchadstoffmobil" → 2 targets |
| Typos and tab chars | `FACILITY_NAME_MAP` dictionary | "Restmulltonne" → "Restabfalltonne" |
| Embedded notes | `is_valid_facility_name()` filter | "Laut FES: ..." → filtered out |
| Synonym inconsistency | Canonical name mapping | "Restmulltonne" → "Restabfalltonne" |

#### WasteStream vs Facility Classification

Five bin types classified as WasteStream nodes (not Facilities):

| WasteStream | Lid Color | Purpose |
|-------------|-----------|---------|
| Restabfalltonne | gray/black | Residual waste |
| Biotonne | brown | Organic waste |
| Verpackungstonne | yellow | Packaging |
| Altpapiertonne | blue/green | Paper/cardboard |
| Altglascontainer | various | Glass |

`classify_target()` routes each parsed disposal target to either `DISPOSED_IN → WasteStream` or `DISPOSED_AT → Facility`.

---

## 5. Dictionary Baseline Pipeline (Data Engineering)

> The LLM classification step of this pipeline is covered in the Data Mining documentation.

### 5.1 Motivation

The ontology-first approach addresses five structural problems with bottom-up graph construction:

1. **Duplicate explosion** — Without anchors, "Schokolade", "Schokoriegel", "chocolate bar" become three separate nodes
2. **Inconsistent granularity** — No way to decide if "Batterie" and "Knopfzelle" are synonyms, siblings, or parent-child
3. **Destructive deduplication** — Merging nodes after the fact risks losing edges and breaking provenance
4. **Limited recall** — Graph vocabulary bounded by whatever terms appeared in processed documents
5. **Competing LLM tasks** — Extraction model must simultaneously extract knowledge and resolve entities

### 5.2 Pipeline Phases (Data Engineering Scope)

```
Phase 0: Download & Cache    →    Phase 1: Parse & Filter    →    [Phase 2: LLM Classify → see DM doc]
(~3 GB, local cache)              (~150k nouns → concept
                                   clusters via OdeNet)
        ↓
Phase 3: Neo4j Import        →    Phase 3b: Promotion
(MERGE pattern, provenance)        (DictionaryEntry → WasteItem
                                    for is_physical=true)
```

**Phase 0 — Download & Cache**
- Kaikki.org JSONL (~3 GB), cached in `data/dictionaries/`
- OdeNet and Open English WordNet via Python `wn` package
- OpenThesaurus text dump

**Phase 1 — Parse & Filter**
- Filter to German nouns only
- Extract per entry: word, senses, synonyms, translations, hypernyms
- Cross-reference with OdeNet synsets for taxonomic structure
- Enrich from OpenThesaurus for colloquial synonyms
- Group into concept clusters

**Phase 3 — Neo4j Import**
- MERGE pattern for idempotent import
- Creates DictionaryEntry nodes with properties: word, language, senses, is_physical, category
- Creates edges: SYNONYM_OF, TRANSLATION_OF, IS_A
- Source provenance nodes for each dictionary source

**Phase 3b — Label Promotion**
- Cypher query: `MATCH (d:DictionaryEntry) WHERE d.is_physical = true SET d:WasteItem`
- Enables dual-label querying

### 5.3 Expected Scale

| Metric | Estimate |
|--------|----------|
| Concept clusters | ~80-120k |
| After is_physical filter | ~30-60k WasteItem nodes |
| Synonym edges | ~100-200k |
| Translation edges | ~40-80k |
| IS_A (hypernym) edges | ~30-50k |
| **Total** | **~50-100k nodes, ~200-400k edges** |

### 5.4 Integration with Structured ETL

Full execution order with dictionary baseline:

```
Phase 0:  Dictionary import         (DictionaryEntry nodes)
Phase 0b: LLM classification        (promote to WasteItem)  → see DM doc
Phase 1:  Facilities import          (Facility nodes)
Phase 2:  Waste items CSV import     (links to existing DictionaryEntry:WasteItem)
Phase 3:  LLM extraction pipeline    (attaches disposal rules)  → see DM doc
```

---

## 6. Infrastructure

### 6.1 Container Runtime

- Podman (rootless) with podman-compose
- Compose file: `docker/podman-compose.yml`
- Neo4j 5 Community Edition with APOC plugin

### 6.2 Database Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `NEO4J_URI` | `bolt://localhost:7687` | Bolt protocol URI |
| `NEO4J_USER` | `neo4j` | Database username |
| `NEO4J_PASSWORD` | `neo4j_dev` | Database password |

### 6.3 LLM Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `LLM_PROVIDER` | `openai` | Provider: `openai` or `groq` |
| `LLM_API_KEY` | `""` | API key |
| `LLM_BASE_URL` | `""` | OpenAI-compatible endpoint override |
| `LLM_MODEL` | `gpt-4o` | Model name |
| `GROQ_API_KEY` | `""` | Groq API key |

### 6.4 Python Environment

- Python 3.12+
- Dependencies: `requirements.txt`
- All modules run via `python -m src.<module>`

### 6.5 Reproducing the Stack

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

## 7. Data Quality & Validation

### 7.1 Validation Queries

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

### 7.2 Quality Metrics

- WasteStream distribution analysis (items per bin type)
- Facility connectivity (items per facility, facilities with no items)
- Orphan node count
- Provenance coverage (% of nodes with DERIVED_FROM edges)

---

## 8. Results

### 8.1 Current Graph State

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

### 8.2 WasteStream Distribution

| Stream | Items |
|--------|-------|
| Restabfalltonne | 87 |
| Biotonne | 16 |
| Verpackungstonne | 13 |
| Altpapiertonne | 9 |
| Altglascontainer | 7 |

### 8.3 Projected State After Dictionary Baseline

| Metric | Current | Projected |
|--------|---------|-----------|
| Nodes | 591 | ~50,000-100,000 |
| Relationships | 1,067 | ~200,000-400,000 |
| WasteItem coverage | 548 items | ~30,000-60,000 items |
| Synonym coverage | none | ~100,000-200,000 edges |
| Multilingual support | none | English translations |
