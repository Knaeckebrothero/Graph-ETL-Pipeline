# Fessi Graph ETL Pipeline

ETL pipeline for constructing a Neo4j knowledge graph to power the "Fessi" university waste disposal chatbot. Uses Graph-RAG to model decision logic rather than just retrieving text segments.

## Table of Contents

- [Overview](#overview)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
- [Pipeline API](#pipeline-api)
- [Project Structure](#project-structure)
- [Schema](#schema)
- [Data Sources](#data-sources)
- [License](#license)
- [Contact](#contact)

## Overview

### Why Graph-RAG?

Standard vector search retrieves conflicting documents without understanding context. For example, asking "How do I dispose of a paint can?" might return:

- "Paint cans must be disposed of in Chemical Waste" (liquid paint)
- "Paint cans can be put in regular trash" (empty/dry cans)

A knowledge graph models **decision logic**, not just text:

```
WasteItem(Paint) → Condition(Form?) → [Liquid] → Chemical Waste
                                    → [Dry]    → Regular Trash
```

### Current Status

| Metric | Count |
|--------|-------|
| Facility nodes | 38 |
| WasteItem nodes | 548 |
| WasteStream nodes | 5 |
| Relationships | 1,067 |
| **Total nodes** | **591** |

## Installation

### Prerequisites

- Python 3.12+
- Podman (or Docker)
- Neo4j 5 Community (via container)

### Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/Knaeckebrothero/Graph-ETL-Pipeline.git
   cd Graph-ETL-Pipeline
   ```

2. **Start Neo4j container**
   ```bash
   podman-compose -f docker/podman-compose.yml up -d
   ```
   The Neo4j browser will be available at http://localhost:7474

3. **Configure environment**
   ```bash
   cp .env.example .env
   ```

4. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

5. **Verify connection**
   ```bash
   python src/scripts/init_db.py
   ```

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `NEO4J_URI` | `bolt://localhost:7687` | Neo4j Bolt protocol URI |
| `NEO4J_USER` | `neo4j` | Database username |
| `NEO4J_PASSWORD` | `neo4j_dev` | Database password |

### LLM Configuration (for AI extraction)

| Variable | Default | Description |
|----------|---------|-------------|
| `LLM_PROVIDER` | `openai` | LLM provider (`openai` or `groq`) |
| `LLM_MODEL` | `gpt-4o` | Model identifier |
| `LLM_API_KEY` | | API key for the provider |
| `LLM_BASE_URL` | | OpenAI-compatible endpoint override (for local llama.cpp, etc.) |

### Default Credentials

- **URI**: `bolt://localhost:7687`
- **User**: `neo4j`
- **Password**: `neo4j_dev`

## Usage

### ETL Pipeline

Import order matters: facilities must be imported before waste items.

| Phase | Module | Status | Description |
|-------|--------|--------|-------------|
| 1 | `facilities.py` | Done | Import Facility nodes from JSON |
| 2 | `waste_items.py` | Done | Import WasteItem nodes and relationships |
| 3A | `waste_streams.py` | Pending | Enrich WasteStream nodes with metadata |
| 3B | `facilities.py --enrich` | Pending | Enrich Facility nodes with addresses/hours |

```bash
# Phase 1: Import facilities (creates nodes with name + uid)
python -m src.etl.facilities

# Phase 2: Import waste items (requires Phase 1)
python -m src.etl.waste_items

# Dry-run mode (no database changes)
python -m src.etl.facilities --dry-run
python -m src.etl.waste_items --dry-run

# Verbose output
python -m src.etl.facilities -v
python -m src.etl.waste_items -v
```

### Database Management

```bash
# Check connection and show stats
python src/scripts/init_db.py

# Clear all data (with confirmation)
python src/scripts/init_db.py --reset

# Show stats only
python src/scripts/init_db.py --stats

# Verbose output
python src/scripts/init_db.py -v
```

### Programmatic Access

```python
from src.db import neo4j_db

# Run queries
results = neo4j_db.query("MATCH (n:WasteItem) RETURN n.name LIMIT 10")

# Use session context manager
with neo4j_db.session() as session:
    session.run("MERGE (w:WasteItem {uid: $uid})", uid="item-001")

# Get database stats
stats = neo4j_db.get_stats()
print(f"Nodes: {stats['total_nodes']}, Relationships: {stats['relationship_count']}")
```

## Pipeline API

The `Pipeline` class is the unified entry point for LLM-based extraction. It takes text + prompt + settings and returns structured entities with optional Neo4j insertion. Two modes are available:

| Mode | Description | LLM Calls | Use Case |
|------|-------------|-----------|----------|
| `chain` | Single structured-output call | 1 | Bulk processing, clean text |
| `agent` | ReAct loop exploring the graph, then extraction | N+1 | Complex text, needs graph context |

### Quick Start

```python
from src.llm_etl import Pipeline, WasteItemExtract, SourceExtract

# Create pipeline
pipeline = Pipeline(
    schema=WasteItemExtract,
    prompt="Extract waste items with disposal routes",
    mode="chain",
    load_to_db=True,
    source=SourceExtract(name="guide.pdf", file_type="pdf"),
)

# Run on each text passage
result = pipeline.run("Altöl muss bei der Schadstoffannahme abgegeben werden.")

print(result.entities)      # [WasteItemExtract(name='Altöl', ...)]
print(result.load_result)   # LoadResult(entities_created=1, ...)
```

### Agent Mode

When extraction benefits from understanding what's already in the graph:

```python
pipeline = Pipeline(
    schema=WasteItemExtract,
    prompt="Extract waste items, match disposal targets to known facilities",
    mode="agent",
    max_iterations=5,
    load_to_db=True,
    source=SourceExtract(name="brochure.pdf", file_type="pdf"),
)

for page_text in pdf_pages:
    result = pipeline.run(page_text)
    print(result.messages)  # Shows agent tool calls and extraction trace
```

### Custom Schemas

Define your own Pydantic schema — the `GenericEntityLoader` handles DB insertion automatically:

```python
from pydantic import BaseModel, Field

class ChemicalExtract(BaseModel):
    name: str = Field(description="Chemical name")
    cas_number: str = Field(default="", description="CAS registry number")
    hazard_class: str = Field(default="", description="GHS hazard class")

pipeline = Pipeline(
    schema=ChemicalExtract,
    prompt="Extract chemical substances with hazard info",
    load_to_db=True,
    source=SourceExtract(name="safety_sheet.pdf", file_type="pdf"),
)

result = pipeline.run(text)
# Entities persisted as :ChemicalExtract nodes in Neo4j
```

### Pipeline Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `schema` | *required* | Pydantic model class defining what to extract |
| `prompt` | `""` | Extraction instructions for the LLM |
| `mode` | `"chain"` | `"chain"` or `"agent"` |
| `max_iterations` | `10` | Max agent tool-calling rounds (agent mode) |
| `max_retries` | `2` | Structured output retry count |
| `temperature` | `0.0` | LLM sampling temperature |
| `load_to_db` | `False` | Persist entities to Neo4j |
| `dry_run` | `False` | Simulate DB writes |
| `source` | `None` | `SourceExtract` for provenance tracking |
| `context` | `""` | Static context passed to every `run()` call |

See `notebooks/pipeline_example.ipynb` for a full walkthrough.

## Project Structure

```
.
├── src/
│   ├── config.py                 # Environment configuration
│   ├── db/
│   │   ├── neo4j_db.py           # Neo4j database singleton
│   │   └── schema.cql            # Graph schema (constraints, indexes)
│   ├── etl/                      # Structured ETL (CSV/JSON)
│   │   ├── facilities.py         # Phase 1: Facility import
│   │   ├── waste_items.py        # Phase 2: WasteItem import
│   │   ├── materials.py          # Material taxonomy import
│   │   └── dictionary_classifier.py  # Batched LLM classification
│   ├── llm_etl/                  # LLM-based ETL (unstructured docs)
│   │   ├── pipeline.py           # Pipeline: unified entry point
│   │   ├── extraction_chain.py   # Schema-driven LLM extraction
│   │   ├── orchestrator.py       # LangGraph document processor
│   │   ├── loader.py             # Entity loader with provenance
│   │   ├── generic_loader.py     # Fallback loader for custom schemas
│   │   ├── schemas.py            # Pydantic extraction schemas
│   │   ├── config.py             # LLM provider configuration
│   │   └── tools/                # Agent tools (DB, doc, web)
│   ├── barcode/                  # Product barcode lookup (OFF API)
│   └── scripts/
│       └── init_db.py            # Database initialization CLI
├── notebooks/
│   └── pipeline_example.ipynb    # Pipeline usage walkthrough
├── data/
│   ├── disposal_map_db.json      # Facility names, addresses, hours
│   └── Abfall-ABC_new.csv        # Waste items and disposal routes
├── docs/                         # Technical documentation
├── docker/
│   └── podman-compose.yml        # Container setup
├── requirements.txt
└── .env.example
```

## Schema

Three-layer ontology (v4.0) defined in `src/db/schema.cql`:

| Layer | Nodes | Purpose |
|-------|-------|---------|
| Core | `WasteItem`, `WasteStream`, `AVVCode` | What is the item? What bin type? |
| Rules | `DisposalRule`, `Condition`, `Instruction`, `Tip` | How do we decide? |
| Infrastructure | `Container`, `Room`, `Building`, `Facility`, `Source` | Where is the bin? Data provenance? |

### Key Relationships

```
(WasteItem)-[:DISPOSED_AT]->(Facility)       # Direct disposal location
(WasteItem)-[:DISPOSED_IN]->(WasteStream)    # Bin type routing
(WasteItem)-[:HAS_RULE]->(DisposalRule)      # Conditional logic
(Condition)-[:IF_TRUE|IF_FALSE]->(...)       # Decision trees
```

## Data Sources

| File | Description | Status |
|------|-------------|--------|
| `disposal_map_db.json` | Facility names, addresses, opening hours | Imported |
| `Abfall-ABC_new.csv` | Waste items and disposal routes | Imported |
| `*.docx` (bin guides) | Bin type metadata (colors, sizes) | Planned |
| `*.pdf` (Abfallsatzung) | Legal disposal rules | Planned |

## License

This project is licensed under the terms of the Creative Commons Attribution 4.0 International License (CC BY 4.0) and the All Rights Reserved License. See the [LICENSE](LICENSE.txt) file for details.

## Contact

[Github](https://github.com/Knaeckebrothero) | [Mail](mailto:OverlyGenericAddress@pm.me)
