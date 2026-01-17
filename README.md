# Fessi Graph ETL Pipeline

ETL pipeline for constructing a Neo4j knowledge graph to power the "Fessi" university waste disposal chatbot. Uses Graph-RAG to model decision logic rather than just retrieving text segments.

## Table of Contents

- [Overview](#overview)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)
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

## Project Structure

```
.
├── src/
│   ├── config.py                 # Environment configuration
│   ├── db/
│   │   ├── __init__.py
│   │   ├── neo4j_db.py           # Neo4j database singleton
│   │   └── schema.cql            # Graph schema (constraints, indexes)
│   ├── etl/
│   │   ├── __init__.py
│   │   ├── facilities.py         # Phase 1: Facility import
│   │   ├── waste_items.py        # Phase 2: WasteItem import
│   │   └── waste_streams.py      # Phase 3A: WasteStream enrichment (pending)
│   └── scripts/
│       └── init_db.py            # Database initialization CLI
├── data/
│   ├── disposal_map_db.json      # Facility names, addresses, hours
│   ├── Abfall-ABC_new.csv        # Waste items and disposal routes
│   ├── *.docx                    # Bin type documentation (future)
│   └── *.pdf                     # Legal rules (future)
├── docker/
│   └── podman-compose.yml        # Container setup
├── requirements.txt
├── .env.example
└── etl_implementation.md         # Detailed ETL documentation
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
