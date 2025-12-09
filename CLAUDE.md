# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

ETL pipeline for the "Fessi" university waste disposal chatbot. Converts raw data (Excel/PDF) into a Neo4j knowledge graph for Graph-RAG.

## Tech Stack

- **Language:** Python 3.10+
- **Database:** Neo4j 5 Community (with APOC plugin)
- **LLM Integration:** LangChain + OpenAI for logic extraction
- **Container Runtime:** Podman (Fedora Linux environment)

## Commands

```bash
# Start Neo4j container
podman-compose -f docker/podman-compose.yml up -d

# Install dependencies
pip install -r requirements.txt

# Verify database connection and show stats
python src/scripts/init_db.py

# Reset database (clears all data, requires confirmation)
python src/scripts/init_db.py --reset

# Show stats only
python src/scripts/init_db.py --stats

# Verbose output
python src/scripts/init_db.py -v
```

## Architecture

### Why Graph-RAG?

Standard vector search fails on conditional logic. The "Paint Can" problem:
- "Paint cans go to Chemical Waste" (liquid paint)
- "Paint cans go to regular trash" (empty/dry cans)

Graph-RAG models decision trees, not just documents.

### Schema Layers (Ontology v4.0)

Three-layer graph structure defined in `src/db/schema.cql`:

1. **Core Entities:** `WasteItem`, `WasteStream`, `AVVCode`
2. **Logic & Rules:** `DisposalRule`, `Condition`, `Instruction`
3. **Infrastructure:** `Container`, `Room`, `Building`, `Facility`

Key relationships:
- `(WasteItem)-[:HAS_RULE]->(DisposalRule)-[:ROUTES_TO]->(WasteStream)`
- `(Condition)-[:IF_TRUE|IF_FALSE]->(WasteStream)` for decision trees
- `(Container)-[:LOCATED_IN]->(Room)-[:PART_OF]->(Building)`

### Data Sources

Located in `data/`:
- `Abfall_ABC.xlsx` - Items, synonyms, notes
- `AVV_Katalog.xlsx` - Legal codes, hazard levels
- `Campus_Master.csv` - Buildings, rooms, container locations

## Code Patterns

**Database access:** Use the singleton instance, not direct instantiation:
```python
from src.db import neo4j_db

results = neo4j_db.query("MATCH (n:WasteItem) RETURN n LIMIT 10")

with neo4j_db.session() as session:
    session.run("MERGE (w:WasteItem {uid: $uid})", uid="item-001")
```

**Idempotent ingestion:** Always use `MERGE` queries for graph data to allow re-running ETL safely.

**Config:** Environment variables loaded via `src/config.py` from `.env` file.
