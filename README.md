# Fessi Graph ETL Pipeline

ETL pipeline for constructing a Neo4j knowledge graph to power the "Fessi" university waste disposal chatbot.

## Why Graph-RAG?

Standard vector search retrieves conflicting documents without understanding context. For example, asking "How do I dispose of a paint can?" might return:

- "Paint cans must be disposed of in Chemical Waste" (liquid paint)
- "Paint cans can be put in regular trash" (empty/dry cans)

A knowledge graph models **decision logic**, not just text:

```
WasteItem(Paint) → Condition(Form?) → [Liquid] → Chemical Waste
                                    → [Dry]    → Regular Trash
```

## Quick Start

### 1. Start Neo4j Container

```bash
podman-compose -f docker/podman-compose.yml up -d
```

The Neo4j browser will be available at http://localhost:7474

### 2. Configure Environment

```bash
cp .env.example .env
# Edit .env if using different credentials
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Verify Connection

```bash
python src/scripts/init_db.py
```

## Usage

### Database Class

```python
from src.db import neo4j_db

# Check connection
if neo4j_db.is_connected():
    print("Connected!")

# Run queries
results = neo4j_db.query("MATCH (n) RETURN n LIMIT 10")

# Use session context manager
with neo4j_db.session() as session:
    result = session.run("CREATE (n:Test {name: 'test'}) RETURN n")

# Get database stats
stats = neo4j_db.get_stats()
print(f"Nodes: {stats['total_nodes']}")
print(f"Relationships: {stats['relationship_count']}")

# Cleanup
neo4j_db.close()
```

### Initialization Script

```bash
# Check connection and show stats
python src/scripts/init_db.py

# Clear all data (with confirmation)
python src/scripts/init_db.py --reset

# Show stats only
python src/scripts/init_db.py --stats

# Use custom credentials
python src/scripts/init_db.py --uri bolt://remote:7687 --user admin --password secret

# Verbose output
python src/scripts/init_db.py -v
```

## Schema Overview

Three-layer ontology (v4.0):

| Layer | Nodes | Purpose |
|-------|-------|---------|
| Concept | `WasteItem`, `AVVCode` | What is the item? |
| Rule | `DisposalRule`, `Condition`, `Instruction` | How do we decide? |
| Infrastructure | `Container`, `Room`, `Building` | Where is the bin? |

## Project Structure

```
.
├── src/
│   ├── config.py              # Environment configuration
│   ├── db/
│   │   ├── __init__.py
│   │   └── neo4j_db.py        # Neo4j database class
│   └── scripts/
│       └── init_db.py         # Database initialization CLI
├── docker/
│   └── podman-compose.yml     # Container setup
├── data/                      # Data sources (not tracked)
├── requirements.txt           # Python dependencies
└── .env.example               # Environment template
```

## Data Sources

- `Abfall_ABC.xlsx` - Waste items, synonyms, disposal notes
- `AVV_Katalog.xlsx` - Official legal codes and hazard levels
- `Campus_Master.csv` - Building/room/container locations

## Default Credentials

- **URI**: bolt://localhost:7687
- **User**: neo4j
- **Password**: neo4j_dev

## License

This project is licensed under the terms of the Creative Commons Attribution 4.0 International License (CC BY 4.0) and the All Rights Reserved License. See the [LICENSE](LICENSE.txt) file for details.

## Contact

[Github](https://github.com/Knaeckebrothero) | [Mail](mailto:OverlyGenericAddress@pm.me)
