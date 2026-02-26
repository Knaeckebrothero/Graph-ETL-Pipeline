# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

ETL pipeline for the "Fessi" university waste disposal chatbot. Converts raw data (Excel/CSV/JSON) and unstructured documents into a Neo4j knowledge graph for Graph-RAG.

**Why Graph-RAG?** Standard vector search fails on conditional logic (the "Paint Can" problem: liquid paint → Chemical Waste, empty/dry cans → regular trash). Graph-RAG models decision trees, not just documents.

## Tech Stack

- **Language:** Python 3.12+
- **Database:** Neo4j 5 Community
- **Container Runtime:** Podman
- **LLM Framework:** LangChain + LangGraph (for AI-based extraction)

## Commands

```bash
# Start Neo4j container
podman-compose -f docker/podman-compose.yml up -d

# Install dependencies
pip install -r requirements.txt

# Database management
python src/scripts/init_db.py           # Verify connection and show stats
python src/scripts/init_db.py --reset   # Clear all data (requires confirmation)
python src/scripts/init_db.py --stats   # Show stats only

# Run structured ETL imports (order matters: facilities first, then waste items)
python -m src.etl.facilities            # Phase 1: Import Facility nodes
python -m src.etl.waste_items           # Phase 2: Import WasteItem nodes + relationships

# Dry-run mode (no database changes)
python -m src.etl.facilities --dry-run
python -m src.etl.waste_items --dry-run

# LLM-based extraction (for unstructured documents)
python -m src.llm_etl.extract --schema WasteItem --input data/doc.txt
python -m src.llm_etl.orchestrate --input data/doc.txt --dry-run
python -m src.llm_etl.load --input extracted.json --source-name "doc.txt"

# Verbose output
python -m src.etl.facilities -v
python -m src.llm_etl.orchestrate --input doc.txt --verbose
```

## Architecture

### Schema Layers (Ontology v4.0)

Three-layer graph structure defined in `src/db/schema.cql`:

1. **Core Entities:** `WasteItem`, `WasteStream`, `AVVCode`, `Product`
2. **Logic & Rules:** `DisposalRule`, `Condition`, `Instruction`, `Tip`
3. **Infrastructure:** `Container`, `Room`, `Building`, `Facility`, `Source`

Key relationships:
- `(Product)-[:IS_A]->(WasteItem)` - EAN product maps to waste category
- `(Product)-[:HAS_PACKAGING]->(WasteItem)` - product packaging waste category
- `(WasteItem)-[:DISPOSED_AT]->(Facility)` - direct disposal location
- `(WasteItem)-[:DISPOSED_IN]->(WasteStream)` - bin type routing
- `(WasteItem)-[:HAS_RULE]->(DisposalRule)-[:ROUTES_TO]->(WasteStream)` - conditional logic
- `(Condition)-[:IF_TRUE|IF_FALSE]->(WasteStream)` - decision trees
- `(*)-[:DERIVED_FROM]->(Source)` - provenance tracking for AI-extracted data

### Two ETL Pipelines

**1. Structured ETL (`src/etl/`)** - For CSV/JSON with known schemas:
```
src/etl/
├── facilities.py    # Phase 1: Facility import from disposal_map_db.json
├── waste_items.py   # Phase 2: WasteItem import from Abfall-ABC_new.csv
└── waste_streams.py # Phase 3A: WasteStream enrichment (pending)
```

**2. LLM-based ETL (`src/llm_etl/`)** - For unstructured documents (PDFs, DOCX):
```
src/llm_etl/
├── extraction_chain.py  # Core: LLM structured output extraction
├── orchestrator.py      # LangGraph state machine for document processing
├── loader.py            # Database loader with provenance tracking
├── schemas.py           # Pydantic extraction schemas
├── extract.py           # CLI: direct extraction
├── orchestrate.py       # CLI: orchestrated extraction
├── load.py              # CLI: database loading
└── tools/               # Document/DB analysis tools
```

The orchestrator flow: `Analyze → Plan → Extract → Validate → Load`

**Import order dependency:** `facilities.py` must run before `waste_items.py` (waste items link to existing facilities).

### Data Sources

Located in `data/`:
- `disposal_map_db.json` - Facility names, addresses, opening hours
- `Abfall-ABC_new.csv` - Waste items and disposal routes
- `AVV_Katalog.xlsx` - Legal AVV codes and hazard levels (planned)

## Code Patterns

**Database access:** Use the singleton instance, not direct instantiation:
```python
from src.db import neo4j_db

results = neo4j_db.query("MATCH (n:WasteItem) RETURN n LIMIT 10")

with neo4j_db.session() as session:
    session.run("MERGE (w:WasteItem {uid: $uid})", uid="item-001")
```

**Idempotent ingestion:** Always use `MERGE` queries to allow re-running ETL safely.

**UID generation:** Use deterministic SHA256 hash of canonical name (16 chars):
```python
def generate_uid(name: str) -> str:
    return hashlib.sha256(name.encode()).hexdigest()[:16]
```

**LLM extraction:** Use ExtractionChain for schema-driven extraction:
```python
from src.llm_etl import ExtractionChain, WasteItemExtract

chain = ExtractionChain()
items = chain.extract(text="...", output_schema=WasteItemExtract)
```

**Config:** Environment variables loaded via `src/config.py` from `.env` file. LLM config requires `LLM_API_KEY` and `LLM_BASE_URL`.

## Validation Queries

```cypher
-- Count nodes by type
MATCH (n) RETURN labels(n)[0] AS label, count(*) AS count ORDER BY count DESC;

-- Check WasteItems with no disposal route
MATCH (w:WasteItem) WHERE NOT (w)-[:DISPOSED_IN|DISPOSED_AT]->() RETURN w.name;

-- Items per WasteStream
MATCH (w:WasteItem)-[:DISPOSED_IN]->(s:WasteStream)
RETURN s.name AS stream, count(w) AS items ORDER BY items DESC;

-- Count entities per source (provenance)
MATCH (n)-[:DERIVED_FROM]->(s:Source)
RETURN s.name AS source, labels(n)[0] AS type, count(*) AS count ORDER BY count DESC;
```
