# AI ETL Workflow

This document describes the LLM-based ETL pipeline for extracting structured data from unstructured documents and loading it into the Neo4j knowledge graph.

---

## Overview

The AI ETL pipeline transforms unstructured documents (PDFs, DOCX, text files, web content) into structured graph data. It uses a 120B thinking model to intelligently extract entities like waste items, facilities, disposal rules, and waste streams from German waste management documents.

**Key Capabilities:**
- Schema-driven extraction using Pydantic models
- Intelligent document analysis and chunking
- Automatic entity type detection
- Database validation and gap analysis
- Provenance tracking for all extracted data
- Enrichment of existing database nodes

---

## Architecture

The pipeline uses a two-tier architecture:

```
┌─────────────────────────────────────────────────────────────────┐
│                      ORCHESTRATOR                                │
│            (LangGraph State Machine)                             │
│                                                                  │
│  ┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────┐     │
│  │ Analyze  │ → │   Plan   │ → │ Extract  │ → │ Validate │     │
│  │ Document │   │ Strategy │   │ Entities │   │ Results  │     │
│  └──────────┘   └──────────┘   └──────────┘   └──────────┘     │
│                                      │                           │
│                                      ▼                           │
│                           ┌──────────────────┐                  │
│                           │ Extraction Chain │                  │
│                           │  (per chunk)     │                  │
│                           └──────────────────┘                  │
└─────────────────────────────────────────────────────────────────┘
                               │
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                       ENTITY LOADER                              │
│                                                                  │
│  • Creates Source nodes for provenance                          │
│  • MERGE pattern for idempotent insertion                       │
│  • Enriches existing nodes (Facility, WasteStream)              │
│  • Creates DERIVED_FROM relationships                           │
└─────────────────────────────────────────────────────────────────┘
                               │
                               ▼
                        ┌─────────────┐
                        │   Neo4j     │
                        │  Database   │
                        └─────────────┘
```

---

## Components

### 1. Extraction Chain (`extraction_chain.py`)

The workhorse component. A stateless, reusable chain that extracts entities from text using LLM structured output.

**How it works:**
1. Takes a text chunk and a Pydantic schema
2. Sends to LLM with schema as the output format
3. Parses and validates the structured response
4. Returns list of extracted entities

**Supported schemas:**
| Schema | Purpose |
|--------|---------|
| `WasteItemExtract` | Waste items with disposal targets |
| `FacilityExtract` | Disposal facilities with addresses/hours |
| `WasteStreamExtract` | Bin types with colors/sizes |
| `DisposalRuleExtract` | Conditional disposal rules |
| `RelationshipExtract` | Entity relationships |
| `SourceExtract` | Source document metadata |

### 2. Orchestrator (`orchestrator.py`)

The intelligence layer. A LangGraph state machine that manages the full extraction workflow.

**Processing stages:**

| Stage | Description |
|-------|-------------|
| **Analyze** | Compute document stats, detect entity types via keyword heuristics |
| **Plan** | Create extraction plan (which schemas, which chunks) |
| **Extract** | Run extraction chain on each chunk per schema |
| **Validate** | Check extracted entities against database constraints |

**Entity type detection:**
The orchestrator uses German keyword heuristics to detect likely entity types:

- **WasteItem**: "abfall", "müll", "entsorgung", "wertstoff", "recycling"
- **Facility**: "wertstoffhof", "annahmestelle", "öffnungszeiten", "adresse"
- **WasteStream**: "tonne", "biotonne", "gelbe tonne", "leerung"
- **DisposalRule**: "wenn", "falls", "flüssig", "trocken", "→"

### 3. Entity Loader (`loader.py`)

Handles database insertion with provenance tracking.

**Features:**
- **MERGE pattern**: All inserts use MERGE for idempotent, re-runnable imports
- **Provenance**: Every entity links to its source via `DERIVED_FROM`
- **Enrichment**: Existing nodes are updated, not duplicated
- **Deterministic UIDs**: SHA256 hash of canonical names (16 chars)

**Load actions:**
| Action | Meaning |
|--------|---------|
| `created` | New entity inserted |
| `updated` | Existing entity modified |
| `enriched` | Existing entity properties added |
| `skipped` | Entity not loaded (error or unsupported) |

---

## Data Flow

```
                    Document
                       │
                       ▼
              ┌────────────────┐
              │  Orchestrator  │
              │    analyze     │
              └───────┬────────┘
                      │
         ┌────────────┴────────────┐
         │  Detected: WasteItem,   │
         │  Facility, DisposalRule │
         └────────────┬────────────┘
                      │
              ┌───────┴───────┐
              │  Chunk into   │
              │  paragraphs   │
              └───────┬───────┘
                      │
         ┌────────────┴────────────┐
         ▼            ▼            ▼
    ┌─────────┐  ┌─────────┐  ┌─────────┐
    │ Chunk 1 │  │ Chunk 2 │  │ Chunk 3 │
    └────┬────┘  └────┬────┘  └────┬────┘
         │            │            │
         ▼            ▼            ▼
    ┌─────────────────────────────────┐
    │     Extraction Chain (LLM)      │
    │   Schema: WasteItemExtract      │
    └─────────────────────────────────┘
         │            │            │
         ▼            ▼            ▼
    ┌─────────┐  ┌─────────┐  ┌─────────┐
    │ Items   │  │ Items   │  │ Items   │
    └────┬────┘  └────┬────┘  └────┬────┘
         │            │            │
         └────────────┼────────────┘
                      │
              ┌───────┴───────┐
              │   Validate    │
              │ against DB    │
              └───────┬───────┘
                      │
              ┌───────┴───────┐
              │ Entity Loader │
              │  + Provenance │
              └───────┬───────┘
                      │
                      ▼
                   Neo4j
```

---

## Provenance Tracking

Every extracted entity is linked to its source document:

```cypher
// Source node structure
(:Source {
  uid: "abc123...",
  name: "Abfallsatzung_2024.pdf",
  file_type: "pdf",
  file_path: "/data/sources/Abfallsatzung_2024.pdf",
  import_date: datetime()
})

// Entity linked to source
(:WasteItem {name: "Altöl"})-[:DERIVED_FROM]->(:Source {name: "..."})
```

Query provenance:
```cypher
// Find all entities from a source
MATCH (n)-[:DERIVED_FROM]->(s:Source {name: "Abfallsatzung_2024.pdf"})
RETURN labels(n)[0] AS type, n.name AS name;

// Count entities per source
MATCH (n)-[:DERIVED_FROM]->(s:Source)
RETURN s.name AS source, labels(n)[0] AS type, count(*) AS count
ORDER BY count DESC;
```

---

## Usage

### Prerequisites

1. **Environment variables** (`.env`):
```bash
# LLM API configuration
LLM_API_KEY=your-api-key
LLM_BASE_URL=https://your-server:443/v1  # OpenAI-compatible endpoint

# Neo4j connection
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=your-password
```

2. **Dependencies**:
```bash
pip install -r requirements.txt
```

3. **Neo4j running**:
```bash
podman-compose -f docker/podman-compose.yml up -d
```

### CLI Commands

#### Direct Extraction (Phase 5A)

Extract entities from text with a specific schema:

```bash
# Extract waste items from a text file
python -m src.llm_etl.extract \
    --schema WasteItem \
    --input data/waste_info.txt \
    --output extracted.json

# Extract from inline text
python -m src.llm_etl.extract \
    --schema DisposalRule \
    --text "Farbreste: flüssig → Schadstoffannahme, eingetrocknet → Restmüll"

# List available schemas
python -m src.llm_etl.extract --list-schemas
```

#### Orchestrated Extraction (Phase 5C)

Process a full document with automatic entity detection:

```bash
# Analyze and extract (dry-run)
python -m src.llm_etl.orchestrate \
    --input data/document.txt \
    --dry-run

# Full extraction with output
python -m src.llm_etl.orchestrate \
    --input data/Abfallsatzung.pdf \
    --output extracted.json \
    --verbose

# Process inline text
python -m src.llm_etl.orchestrate \
    --text "Altöl muss bei der Schadstoffannahme abgegeben werden..."
```

#### Database Loading (Phase 5D)

Load extracted entities into Neo4j:

```bash
# Dry-run load (no database changes)
python -m src.llm_etl.load \
    --input extracted.json \
    --dry-run

# Load with provenance tracking
python -m src.llm_etl.load \
    --input extracted.json \
    --source-name "Abfallsatzung_2024.pdf" \
    --source-type pdf

# Full pipeline: extract + load
python -m src.llm_etl.load \
    --file data/document.txt \
    --source-name "document.txt"
```

### Python API

```python
from src.llm_etl import (
    ExtractionChain,
    Orchestrator,
    EntityLoader,
    SourceExtract,
    WasteItemExtract,
)

# Direct extraction
chain = ExtractionChain()
items = chain.extract(
    text="Altbatterien können beim Schadstoffmobil abgegeben werden.",
    output_schema=WasteItemExtract,
    extraction_prompt="Extract waste items with disposal locations",
)

# Orchestrated extraction
orchestrator = Orchestrator()
result = orchestrator.process_file("data/document.txt")

print(f"Extracted {len(result.extracted_entities)} entities")
print(f"Validation: {result.validation_summary}")

# Load into database
source = SourceExtract(
    name="document.txt",
    file_type="txt",
    file_path="data/document.txt",
)

loader = EntityLoader()
load_result = loader.load(
    entities=[WasteItemExtract(**e["entity"]) for e in result.extracted_entities],
    source=source,
    dry_run=False,
)

print(f"Created: {load_result.entities_created}")
print(f"Enriched: {load_result.entities_enriched}")
```

---

## Chunking Strategies

The orchestrator supports multiple chunking strategies:

| Strategy | Description | Best For |
|----------|-------------|----------|
| `PARAGRAPH` | Split on double newlines | General documents |
| `SECTION` | Split on headers (# or ALL CAPS) | Structured documents |
| `SENTENCE` | Split on sentence boundaries | Dense text |
| `FIXED_SIZE` | Fixed character chunks with overlap | Large documents |

Default: `PARAGRAPH` with 2000 char max chunk size.

---

## Enrichment Strategy

When loading entities that already exist in the database:

**Facilities:**
```cypher
MATCH (f:Facility {uid: $uid})
SET f.address = CASE WHEN $address <> '' THEN $address ELSE f.address END,
    f.opening_hours = CASE WHEN $opening_hours <> '' THEN $opening_hours ELSE f.opening_hours END,
    ...
```

**WasteStreams:**
```cypher
MATCH (s:WasteStream {uid: $uid})
SET s.lid_color = CASE WHEN $lid_color <> '' THEN $lid_color ELSE s.lid_color END,
    s.english_name = CASE WHEN $english_name <> '' THEN $english_name ELSE s.english_name END,
    ...
```

This ensures:
- Existing properties are preserved if new value is empty
- New properties are added from extraction
- No data loss from partial extractions

---

## Validation

The pipeline validates extracted entities against database constraints:

| Check | Entity Types | Description |
|-------|--------------|-------------|
| Required fields | All | Name must be present |
| Disposal targets | WasteItem | Targets should exist in DB |
| Duplicate detection | All | Warns if entity exists |
| Reference validation | DisposalRule | Checks WasteItem exists |

Validation results:
```json
{
  "valid": true,
  "warnings": ["Disposal target 'Recyclinghof' not found in database"],
  "errors": []
}
```

---

## File Structure

```
src/llm_etl/
├── __init__.py           # Package exports
├── config.py             # LLM client configuration
├── schemas.py            # Pydantic extraction schemas
├── extraction_chain.py   # Core extraction logic
├── extract.py            # CLI: direct extraction
├── orchestrator.py       # LangGraph state machine
├── orchestrate.py        # CLI: orchestrated extraction
├── loader.py             # Database loader with provenance
├── load.py               # CLI: database loading
└── tools/
    ├── __init__.py
    ├── db_tools.py       # Database query tools
    ├── doc_tools.py      # Document analysis tools
    └── web_tools.py      # Web search (placeholder)
```

---

## Troubleshooting

### Common Issues

**LLM connection error:**
```
Error: Could not connect to LLM endpoint
```
- Check `LLM_BASE_URL` and `LLM_API_KEY` in `.env`
- Verify the LLM server is running

**Neo4j connection error:**
```
Error: Couldn't connect to localhost:7687
```
- Start Neo4j: `podman-compose -f docker/podman-compose.yml up -d`
- Check `NEO4J_URI`, `NEO4J_USER`, `NEO4J_PASSWORD` in `.env`

**No entities extracted:**
- Document may not contain recognizable entity patterns
- Try with `--verbose` flag to see detection details
- Check if document is in German (keywords are German)

**Validation warnings:**
- Disposal targets not found: Run facility import first
- Entity already exists: Normal for re-runs (uses MERGE)

### Debug Mode

```bash
# Verbose logging
python -m src.llm_etl.orchestrate --input doc.txt --verbose

# Show processing messages in output
python -m src.llm_etl.orchestrate --input doc.txt --show-messages
```
