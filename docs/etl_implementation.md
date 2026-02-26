# ETL Implementation Plan

This document outlines the step-by-step implementation plan for importing data into the Neo4j knowledge graph.

---

## Phase 1: Facility Import (`disposal_map_db.json`) ✅ COMPLETE

**Source:** `data/disposal_map_db.json`
**Target:** `Facility` nodes
**Status:** ✅ Completed (38 facilities imported)

### Data Structure
```json
{
  "uuid-key": [
    {
      "name": "Wertstoffhof Nord",
      "address": "Max-Holder-Str. 29 60437 Frankfurt am Main",
      "opening_hours": "Mo. - Sa. 8.00 - 16.50 Uhr",
      "contact": "",
      "additional_info": "Heiligabend, Silvester und Karsamstag geschlossen",
      "link": ""
    }
  ]
}
```

### Implementation Details
- **File:** `src/etl/facilities.py`
- **Key Functions:**
  - `generate_uid(name)` - Deterministic SHA256 hash (16 chars)
  - `load_facilities(filepath)` - Loads JSON, deduplicates by name, merges most complete entries
  - `import_facilities(filepath, dry_run)` - Main import with MERGE pattern

### Run Command
```bash
python -m src.etl.facilities [--dry-run] [-v]
```

### Results
| Metric | Value |
|--------|-------|
| Facilities loaded | 38 |
| Unique facilities | 38 |

---

## Phase 2: Waste Items Import (`Abfall-ABC_new.csv`) ✅ COMPLETE

**Source:** `data/Abfall-ABC_new.csv`
**Target:** `WasteItem` nodes, `WasteStream` nodes, relationships
**Status:** ✅ Completed (548 items, 5 streams, 1,067 relationships)

### Data Structure
| Column | Description | Maps To |
|--------|-------------|---------|
| `Abfallart` | Waste item name | `WasteItem.name` |
| `Entsorgungsweg` | Disposal method(s) | `WasteStream` / `Facility` |
| `Adresse` | Address(es) | *(not used - already in Facility)* |
| `Öffnungszeiten` | Opening hours | *(not used - already in Facility)* |
| `Kontakt` | Contact info | *(not used - already in Facility)* |

### Implementation Details
- **File:** `src/etl/waste_items.py`
- **Key Functions:**
  - `is_valid_facility_name(name)` - Filters out notes/hints/comments
  - `parse_disposal_targets(text)` - Extracts facility names from multiline cells
  - `extract_facilities_from_concat(text)` - Pattern-based extraction from concatenated strings
  - `classify_target(name)` - Determines if target is `WasteStream` or `Facility`
  - `import_waste_items(filepath, dry_run)` - Main import

### Data Normalization
The CSV contains messy data requiring cleanup:

| Issue | Solution |
|-------|----------|
| Multiline cells with newlines | Split by `\n` and process each line |
| Concatenated facility names | Pattern-based regex extraction |
| Notes like "Laut FES:", "Hinweis" | Filtered via `is_valid_facility_name()` |
| Typos ("Fachhandel / Herstelle") | `FACILITY_NAME_MAP` normalization |
| Tab characters in names | Normalized in mapping |
| Synonyms ("Restmülltonne") | Mapped to canonical names |

### WasteStream Classification
These bin types are created as `WasteStream` nodes (not Facilities):
- `Restabfalltonne` (gray bin - residual waste)
- `Biotonne` (brown bin - organic waste)
- `Verpackungstonne` / `Verpackungstonne (Gelbe Tonne)` (yellow bin - packaging)
- `Altpapiertonne` (green bin - paper)

### Run Command
```bash
python -m src.etl.waste_items [--dry-run] [-v]
```

### Results
| Metric | Value |
|--------|-------|
| WasteItem nodes | 548 |
| WasteStream nodes | 5 |
| DISPOSED_AT relationships | ~935 |
| DISPOSED_IN relationships | ~132 |
| **Total relationships** | **1,067** |

### WasteStream Distribution
| Stream | Items |
|--------|-------|
| Restabfalltonne | 87 |
| Biotonne | 16 |
| Verpackungstonne | 13 |
| Altpapiertonne | 9 |
| Verpackungstonne (Gelbe Tonne) | 7 |

---

## Current Database State

```
Node Counts:
  Facility:    38
  WasteItem:   548
  WasteStream: 5
  ─────────────────
  Total:       591 nodes

Relationship Count: 1,067
```

---

## File Structure

```
src/etl/
├── __init__.py      # Exports: import_facilities, import_waste_items
├── facilities.py    # ✅ Phase 1: Facility import
└── waste_items.py   # ✅ Phase 2: WasteItem import
```

---

## Running the ETL

```bash
# Phase 1: Import facilities (creates nodes with name + uid only)
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

---

## Validation Queries

After import, verify data integrity:

```cypher
// Count nodes by type
MATCH (n) RETURN labels(n)[0] AS label, count(*) AS count ORDER BY count DESC;

// Check WasteItems with no disposal route
MATCH (w:WasteItem) WHERE NOT (w)-[:DISPOSED_IN|DISPOSED_AT]->() RETURN w.name;

// List facilities
MATCH (f:Facility) RETURN f.name, f.address LIMIT 20;

// Sample disposal paths
MATCH (w:WasteItem)-[r]->(target)
RETURN w.name, type(r), labels(target)[0], target.name
LIMIT 20;

// Items per WasteStream
MATCH (w:WasteItem)-[:DISPOSED_IN]->(s:WasteStream)
RETURN s.name AS stream, count(w) AS items
ORDER BY items DESC;

// Most connected facilities
MATCH (f:Facility)<-[:DISPOSED_AT]-(w:WasteItem)
RETURN f.name AS facility, count(w) AS items
ORDER BY items DESC
LIMIT 10;

// Check WasteStream enrichment (after LLM ETL)
MATCH (s:WasteStream)
RETURN s.name, s.lid_color, s.english_name, s.emptying_frequency;

// Check Facility enrichment (after LLM ETL)
MATCH (f:Facility)
WHERE f.address IS NOT NULL
RETURN f.name, f.address, f.opening_hours
LIMIT 10;

// Facilities still missing addresses
MATCH (f:Facility)
WHERE f.address IS NULL
RETURN f.name;

// Check provenance tracking (after LLM ETL)
MATCH (n)-[:DERIVED_FROM]->(s:Source)
RETURN s.name AS source, labels(n)[0] AS node_type, count(*) AS count
ORDER BY count DESC;
```

---

## Phase 5: LLM-based ETL Pipeline

**Status:** ✅ Complete (Phases 5A-5D implemented)
**Rationale:** Remaining data sources (PDFs, DOCX, web-scraped content) are unstructured and require intelligent extraction. Traditional parsing is fragile. With 96GB VRAM and a 120B thinking model available, an LLM-based approach is more effective than complex rule-based parsers.

### Implementation Status
| Phase | Component | Status |
|-------|-----------|--------|
| 5A | Extraction Chain | ✅ Complete |
| 5B | Pydantic Schemas | ✅ Complete |
| 5C | Orchestrator Agent | ✅ Complete |
| 5D | Database Loader | ✅ Complete |

### Architecture Overview

Two-tier extraction system: a reusable **Extraction Chain** (workhorse) and an **Orchestrator Agent** (intelligence layer).

```
┌─────────────────────────────────────────────────────────────────┐
│                    ORCHESTRATOR AGENT                           │
│  (Planning, schema awareness, task delegation, gap analysis)    │
│                                                                 │
│  • Analyzes full documents                                      │
│  • Creates extraction plan                                      │
│  • Identifies entity types present                              │
│  • Delegates to extraction chains                               │
│  • Cross-references with existing DB                            │
│  • Optional: web search for enrichment                          │
└─────────────────────────┬───────────────────────────────────────┘
                          │ delegates
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                    EXTRACTION CHAIN                             │
│  (Stateless, reusable, schema-driven structured output)         │
│                                                                 │
│  Input:                                                         │
│    • text_chunk: str                                            │
│    • output_schema: Pydantic model (dynamic)                    │
│    • context: Optional[str] (related entities, hints)           │
│                                                                 │
│  Output:                                                        │
│    • List[Entity] matching the provided schema                  │
└─────────────────────────────────────────────────────────────────┘
```

### Model Configuration

- **Endpoint:** OpenAI-compatible (llama.cpp server on port 443)
- **Model:** OpenAI OSS 120B thinking model (agentic tasks)
- **Configuration:** Environment variables for API key and base URL, overriding default OpenAI client when base URL is set

```bash
# .env configuration
LLM_API_KEY=your-api-key
LLM_BASE_URL=https://your-server:443/v1  # When set, overrides OpenAI default
```

### Phase 5A: Extraction Chain (Foundation)

The extraction chain is a generic, stateless component that can extract any entity type by swapping the output schema.

**Core Concept:** The schema IS the prompt. Pass a Pydantic model, LangChain's structured output handles the rest. Same chain extracts `WasteItem`, `DisposalRule`, or `Condition` - just swap the schema.

#### Interface Design
```python
class ExtractionChain:
    """Generic extraction chain with dynamic schema support."""

    def extract(
        self,
        text: str,
        output_schema: Type[BaseModel],  # Dynamic - WasteItem, Facility, Rule, etc.
        extraction_prompt: str,           # What to look for
        context: str = ""                 # Related entities, hints from DB
    ) -> list[BaseModel]:
        """Extract entities matching the provided schema from text."""
        ...
```

#### Example Usage
```python
# Extract waste items from a text chunk
items = chain.extract(
    text="Altöl muss bei der Schadstoffannahme abgegeben werden...",
    output_schema=WasteItemExtract,
    extraction_prompt="Extract all waste items mentioned with their disposal routes",
    context="Known facilities: Wertstoffhof Nord, Schadstoffmobil..."
)

# Same chain, different schema - extract disposal rules
rules = chain.extract(
    text="Farbreste: flüssig → Schadstoffannahme, eingetrocknet → Restmüll",
    output_schema=DisposalRuleExtract,
    extraction_prompt="Extract conditional disposal rules (if X then Y)",
    context=""
)
```

#### Implementation Details
- **File:** `src/llm_etl/extraction_chain.py`
- **Dependencies:** LangChain, Pydantic
- **Validation:** Retry logic for malformed outputs
- **CLI:** `python -m src.llm_etl.extract --schema WasteItem --input chunk.txt`

### Phase 5B: Pydantic Schema Definitions

Pydantic models mirroring the Neo4j schema for structured LLM output.

**File:** `src/llm_etl/schemas.py`

```python
from pydantic import BaseModel, Field

class WasteItemExtract(BaseModel):
    """Extracted waste item with disposal information."""
    name: str = Field(description="Name of the waste item in German")
    synonyms: list[str] = Field(default=[], description="Alternative names")
    disposal_targets: list[str] = Field(description="Facility or WasteStream names")
    conditions: list[str] = Field(default=[], description="Conditional disposal notes")

class DisposalRuleExtract(BaseModel):
    """Conditional disposal rule (the 'Paint Can' problem)."""
    waste_item: str = Field(description="The waste item this rule applies to")
    condition: str = Field(description="The condition (e.g., 'liquid', 'dry')")
    if_true_target: str = Field(description="Disposal target if condition is true")
    if_false_target: str = Field(default="", description="Disposal target if condition is false")

class FacilityExtract(BaseModel):
    """Extracted facility information."""
    name: str = Field(description="Official facility name")
    address: str = Field(default="", description="Street address")
    opening_hours: str = Field(default="", description="Operating hours")
    contact: str = Field(default="", description="Contact information")
    additional_info: str = Field(default="", description="Holiday closures, special notes")
    link: str = Field(default="", description="Website URL")
    accepted_items: list[str] = Field(default=[], description="Waste types accepted")

class WasteStreamExtract(BaseModel):
    """Extracted waste stream (bin type) with metadata."""
    name: str = Field(description="German name of the bin type")
    english_name: str = Field(default="", description="English translation")
    lid_color: str = Field(default="", description="Lid color (e.g., 'yellow', 'brown')")
    body_color: str = Field(default="", description="Body/container color")
    description: str = Field(default="", description="What goes in this bin")
    emptying_frequency: str = Field(default="", description="How often emptied")
    available_sizes: list[str] = Field(default=[], description="Available container sizes")

class RelationshipExtract(BaseModel):
    """Extracted relationship between entities."""
    source_type: str = Field(description="Source node label (e.g., 'WasteItem')")
    source_name: str = Field(description="Source node name")
    relationship: str = Field(description="Relationship type (e.g., 'DISPOSED_AT')")
    target_type: str = Field(description="Target node label")
    target_name: str = Field(description="Target node name")

class SourceExtract(BaseModel):
    """Source document for provenance tracking."""
    name: str = Field(description="Filename or document title")
    file_type: str = Field(description="File type (pdf, docx, csv, json, web)")
    file_path: str = Field(default="", description="Path to source file")
    url: str = Field(default="", description="Source URL if web-scraped")
```

#### WasteStream Reference Data (Frankfurt FES)
The LLM should extract/verify against these known bin types:

| German Name | Lid Color | Body Color | English Name | Sizes |
|-------------|-----------|------------|--------------|-------|
| Restabfalltonne | Gray/Black | Gray/Black | Residual Waste | 80, 120, 240, 770L; 1.1, 2.5, 5m³ |
| Verpackungstonne | Yellow | Gray/Black | Packaging | 120, 240, 770L |
| Altpapiertonne | Blue/Green | Gray | Paper/Cardboard | 120, 240, 770L |
| Biotonne | Brown | Brown | Organic Waste | 60, 120, 240L |
| Altglascontainer | Various | Blue | Glass | Public containers only |

### Phase 5C: Orchestrator Agent (Intelligence Layer)

LangGraph-based agent for complex document processing. This is where the 120B thinking model shines.

**Capabilities:**
1. **Document Analysis** - "What entity types are likely in this document?"
2. **Chunking Strategy** - "How should I split this? By section? By table?"
3. **Extraction Planning** - "Run WasteItem extraction on pages 5-12, then DisposalRule on pages 13-20"
4. **Gap Analysis** - "Compare extracted items against DB, identify missing relationships"
5. **Enrichment** - "Web search for AVV codes for these chemical items"

**File:** `src/llm_etl/orchestrator.py`

#### Agent Tools
| Tool | Purpose |
|------|---------|
| `extract_entities` | Invoke extraction chain with specific schema |
| `query_db` | Query Neo4j for existing entities, find gaps |
| `web_search` | Search for additional information (AVV codes, etc.) |
| `chunk_document` | Split document using various strategies |
| `validate_extraction` | Check extracted data against DB constraints |

#### State Machine (LangGraph)
```python
from langgraph.graph import StateGraph

class OrchestratorState(TypedDict):
    document: str
    document_type: str
    extraction_plan: list[dict]
    extracted_entities: list[BaseModel]
    db_gaps: list[str]
    current_step: str

# Nodes
graph = StateGraph(OrchestratorState)
graph.add_node("analyze_document", analyze_document)
graph.add_node("create_plan", create_extraction_plan)
graph.add_node("extract", run_extraction_chain)
graph.add_node("validate", validate_against_db)
graph.add_node("enrich", web_search_enrichment)
graph.add_node("human_review", request_human_approval)  # Optional checkpoint
```

### Phase 5D: Database Integration

Loader component to insert extracted entities into Neo4j with provenance tracking.

**File:** `src/llm_etl/loader.py`

```python
class EntityLoader:
    """Load extracted entities into Neo4j with provenance."""

    def load(
        self,
        entities: list[BaseModel],
        source: SourceExtract,  # For provenance tracking
        dry_run: bool = False
    ) -> LoadResult:
        """Insert entities using MERGE pattern with source linking."""
        ...
```

#### Provenance Tracking
Every loaded entity gets linked to its source document:

```cypher
// Create Source node
MERGE (s:Source {uid: $source_uid})
SET s.name = $filename,
    s.type = $file_type,
    s.file_path = $path,
    s.import_date = datetime()

// Link extracted entity to source
MATCH (e {uid: $entity_uid})
MERGE (e)-[:DERIVED_FROM]->(s)
```

#### Enrichment Strategy
For existing nodes (Facilities, WasteStreams), the loader uses MATCH + SET:

```cypher
// Enrich existing Facility
MATCH (f:Facility {name: $name})
SET f.address = $address,
    f.opening_hours = $opening_hours,
    f.contact = $contact,
    f.additional_info = $additional_info,
    f.link = $link

// Enrich existing WasteStream
MATCH (s:WasteStream {name: $name})
SET s.lid_color = $lid_color,
    s.body_color = $body_color,
    s.english_name = $english_name,
    s.description = $description,
    s.emptying_frequency = $frequency,
    s.available_sizes = $sizes
```

### File Structure

```
src/llm_etl/
├── __init__.py           # ✅ Exports: ExtractionChain, Orchestrator, EntityLoader
├── config.py             # ✅ OpenAI client setup with base URL override
├── schemas.py            # ✅ Pydantic models for extraction
├── extraction_chain.py   # ✅ Phase 5A: Generic extraction chain
├── extract.py            # ✅ CLI entry point for extraction
├── orchestrator.py       # ✅ Phase 5C: LangGraph agent
├── orchestrate.py        # ✅ CLI entry point for orchestration
├── loader.py             # ✅ Phase 5D: Neo4j insertion with provenance
├── load.py               # ✅ CLI entry point for loading
└── tools/                # Agent tools
    ├── __init__.py       # ✅ Tool exports
    ├── db_tools.py       # ✅ query_db, validate_extraction, find_gaps
    ├── doc_tools.py      # ✅ chunk_document, estimate_entity_types
    └── web_tools.py      # ✅ web_search placeholder
```

### Run Commands

```bash
# Phase 5A: Direct extraction with specific schema
python -m src.llm_etl.extract \
    --schema WasteItem \
    --input data/chunk.txt \
    --output extracted.json

# Phase 5C: Full orchestrator on a document
python -m src.llm_etl.orchestrate \
    --input data/document.txt \
    --dry-run

# Phase 5D: Load extracted entities into Neo4j
python -m src.llm_etl.load \
    --input extracted.json \
    --dry-run

# Full pipeline: orchestrate + load
python -m src.llm_etl.load \
    --file data/document.txt \
    --source-name "document.txt" \
    --source-type txt

# Load with custom source provenance
python -m src.llm_etl.load \
    --input extracted.json \
    --source-name "Abfallsatzung_2024.pdf" \
    --source-type pdf \
    --source-path "/data/sources/Abfallsatzung_2024.pdf"
```

### Implementation Phases

| Phase | Component | Description | Dependencies |
|-------|-----------|-------------|--------------|
| 5A | Extraction Chain | Generic schema-driven extraction | LangChain, Pydantic |
| 5B | Schema Definitions | Pydantic models for all entity types | Phase 5A |
| 5C | Orchestrator Agent | LangGraph planning and delegation | Phase 5A, 5B |
| 5D | Database Loader | Neo4j insertion with provenance | Phase 5A, 5B |

### Target Data Sources

| Data Source | Purpose | Extraction Strategy |
|-------------|---------|---------------------|
| `disposal_map_db.json` | Facility enrichment (addresses, hours, contacts) | FacilityExtract → enrich existing nodes |
| DOCX container guides | WasteStream metadata, "what goes in" lists | WasteStreamExtract, WasteItemExtract |
| PDF documents (Abfallsatzung) | Legal disposal rules | DisposalRuleExtract |
| Web-scraped content | Additional waste items, facilities | WasteItemExtract, FacilityExtract |
| AVV_Katalog.xlsx | Legal codes, hazard levels | AVVCodeExtract (structured, may not need LLM) |

### Design Principles

1. **Schema is the prompt** - Pydantic models drive extraction, not complex prompts
2. **Stateless chain, stateful agent** - Chain is reusable; agent manages workflow
3. **Idempotent insertion** - Always use MERGE, safe to re-run
4. **Provenance tracking** - Link all extracted data to source documents
5. **Human-in-the-loop** - Optional approval checkpoints for critical data
6. **Model-agnostic** - Easy to swap models by changing config

---

## Future Enhancements

| Enhancement | Description |
|-------------|-------------|
| Multi-model comparison | Run same extraction with different models, compare quality |
| Active learning | Flag low-confidence extractions for human review |
| Incremental updates | Detect changed source documents, re-extract only diffs |
| RAG-enhanced extraction | Use existing DB entities as context for better extraction |
| Batch processing | Process multiple documents in parallel |

**Hardware:** 96GB VRAM homelab with OpenAI OSS 120B model available for development and production runs.
