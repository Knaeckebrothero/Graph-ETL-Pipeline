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

## Phase 3: Waste Streams & Containers (PENDING)

**Source:** Container description documents + schema knowledge
**Target:** Enhanced `WasteStream` nodes, `Container` nodes
**Status:** 🔲 Pending

### Standard Bin Types (from Frankfurt FES)
| Stream Name | Container Color | German Name |
|-------------|-----------------|-------------|
| Residual Waste | Gray lid | Restabfalltonne |
| Packaging | Yellow lid | Verpackungstonne |
| Paper | Green lid | Altpapiertonne |
| Bio Waste | Brown lid | Biotonne |
| Glass | Various | Altglascontainer |

### Implementation Steps
1. Create `src/etl/waste_streams.py`
2. Enhance existing `WasteStream` nodes with properties (color, description)
3. Create `Container` nodes with physical attributes
4. Link `(WasteStream)-[:COLLECTED_IN]->(Container)`

### Cypher Pattern
```cypher
// Enhance WasteStream with properties
MATCH (s:WasteStream {name: $name})
SET s.color = $color,
    s.description = $description,
    s.english_name = $english_name

// Create Container and link
MERGE (c:Container {uid: $container_uid})
SET c.type = $type,
    c.lid_color = $lid_color
MERGE (s)-[:COLLECTED_IN]->(c)
```

---

## Phase 4: Source Provenance (PENDING)

**Target:** `Source` nodes for data lineage
**Status:** 🔲 Pending

### Implementation Steps
1. Create `Source` nodes for each imported file
2. Link imported nodes via `[:DERIVED_FROM]` relationships

### Cypher Pattern
```cypher
MERGE (s:Source {uid: $source_uid})
SET s.name = $filename,
    s.type = $file_type,
    s.file_path = $path,
    s.import_date = datetime()

// Link to imported data
MATCH (w:WasteItem {uid: $item_uid})
MERGE (w)-[:DERIVED_FROM]->(s)
```

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
├── waste_items.py   # ✅ Phase 2: WasteItem import
└── waste_streams.py # 🔲 Phase 3: WasteStream/Container setup
```

---

## Running the ETL

```bash
# Phase 1: Import facilities
python -m src.etl.facilities

# Phase 2: Import waste items (requires Phase 1)
python -m src.etl.waste_items

# Phase 3: Setup waste streams (pending)
python -m src.etl.waste_streams

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
```

---

## Future Enhancements (LLM-based)

These require LLM processing and are planned for later phases:

| Data Source | Purpose | LLM Task |
|-------------|---------|----------|
| PDF documents (Abfallsatzung) | Extract disposal rules | Rule extraction, condition parsing |
| DOCX container guides | "What goes in" lists | Yes/No classification |
| Waste item synonyms | Better RAG matching | Synonym generation |
| Entity normalization | Clean facility names | Named entity recognition |

**Note:** User has 96GB VRAM homelab with OpenAI OSS 120B model available.
