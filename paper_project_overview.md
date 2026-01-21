# Project Overview: LLMs for Data Preparation
## Context Document for University Paper (Data Engineering & Data Mining)

---

## 1. Project Summary

**Project Name:** Fessi Graph-ETL Pipeline
**Purpose:** Transform raw waste disposal data into a Neo4j knowledge graph for a university chatbot
**University:** Frankfurt University of Applied Sciences (FRA-UAS)

### The Core Problem
Standard vector-based RAG (Retrieval-Augmented Generation) fails on **conditional logic**. This is the "Paint Can Problem":

- Query: "How do I dispose of a paint can?"
- Vector search returns conflicting documents:
  - "Liquid paint → Hazardous Waste"
  - "Empty/dry cans → Regular Trash"
- The user is confused: *Which answer is correct?*

### The Solution: Graph-RAG with LLM-Extracted Decision Trees
Instead of flat text retrieval, we build a **knowledge graph** that models decision trees:

```
WasteItem(Paint Can)
├─ Condition: "Is it liquid?"
│  ├─ IF TRUE → Chemical Waste Facility
│  └─ IF FALSE → Regular Trash Bin
```

This enables the chatbot to ask clarifying questions and provide the correct disposal route.

---

## 2. Data Sources

### 2.1 Structured Data (Traditional ETL)

| File | Format | Records | Content |
|------|--------|---------|---------|
| `disposal_map_db.json` | JSON | 38 facilities | Disposal facility names, addresses, opening hours |
| `Abfall-ABC_new.csv` | CSV | 548 items | German waste catalog with disposal routes |

**Data Quality Issues Encountered:**
- Concatenated facility names without delimiters ("Wertstoffhof NordWertstoffhof West")
- Typos and synonyms ("Restmülltonne" vs. "Restabfalltonne")
- Mixed notes and data in same fields
- Multiline cell values

### 2.2 Unstructured Data (LLM-based ETL Target)

| File | Format | Content | Challenge |
|------|--------|---------|-----------|
| `Abfallsatzung.pdf` | PDF | Official waste statute (legal rules) | Multi-column layout, legal German |
| `was_kommt_wo_hinein_de.pdf` | PDF | Visual bin sorting guide | Mixed text and graphics |
| `Restabfalltonne.docx` | DOCX | Container-specific rules | Embedded tables, semi-structured lists |
| `Alltagscontainer.docx` | DOCX | Everyday waste guidance | Implicit conditional logic in prose |

**Why Traditional Parsing Fails:**
- No clear delimiters between conditions
- Rules embedded in narrative German text
- Multiple disposal targets per item
- Context-dependent exceptions

---

## 3. The Two-Tier ETL Architecture

### Tier 1: Traditional ETL (for Structured Data)
Standard Python-based extraction for clean CSV/JSON files.

**Pipeline:**
```
JSON/CSV → Parse → Normalize → Deduplicate → MERGE into Neo4j
```

**Results:**
- 38 Facility nodes
- 548 WasteItem nodes
- 5 WasteStream nodes
- 1,067 relationships

### Tier 2: LLM-based ETL (for Unstructured Data)
AI-powered extraction for PDFs, DOCX, and free text.

**Pipeline:**
```
Document → Analyze → Plan → Extract (LLM) → Validate → Load with Provenance
```

---

## 4. LLM-based ETL: How It Works

### 4.1 The Key Innovation: "Schema Is The Prompt"

Instead of writing complex extraction prompts, we define **Pydantic schemas** that automatically become extraction instructions:

```python
class WasteItemExtract(BaseModel):
    name: str = Field(description="Name of the waste item in German")
    synonyms: list[str] = Field(description="Alternative names")
    disposal_targets: list[str] = Field(description="Facility or bin names")
    conditions: list[str] = Field(description="Conditional disposal notes")
```

The LLM receives the schema as context and returns structured JSON matching it.

### 4.2 Handling Conditional Logic (The Paint Can Problem)

**Example Input Text (German):**
```
Farbreste:
Flüssige Farbreste müssen bei der Schadstoffannahme abgegeben werden.
Eingetrocknete Farbreste können als Restmüll behandelt werden.
```

**LLM Extracts:**
```python
DisposalRuleExtract(
    waste_item="Farbreste (Paint Residue)",
    condition="Is it liquid?",
    if_true_target="Schadstoffannahme (Hazardous Waste)",
    if_false_target="Restabfalltonne (Regular Waste)"
)
```

This becomes a decision tree in the graph, not flat text.

### 4.3 The Four-Phase Extraction Process

| Phase | Component | Purpose |
|-------|-----------|---------|
| **Analyze** | Orchestrator | Detect entity types, compute document stats |
| **Plan** | Orchestrator | Choose chunking strategy, create extraction plan |
| **Extract** | Extraction Chain | Send chunks to LLM with schema, parse JSON |
| **Load** | Entity Loader | MERGE into Neo4j with provenance tracking |

### 4.4 Provenance Tracking

Every LLM-extracted entity is linked to its source:
```
(WasteItem)-[:DERIVED_FROM]->(Source {
    name: "Abfallsatzung.pdf",
    extraction_date: "2024-12-20",
    model_id: "gpt-4"
})
```

This enables:
- Data lineage auditing
- Quality assessment per source
- Iterative refinement

---

## 5. The Knowledge Graph Schema

Three-layer ontology modeling the waste disposal domain:

### Layer 1: Core Entities
- `WasteItem` - What is being disposed (e.g., "Banana Peel", "Paint Can")
- `WasteStream` - Bin types (e.g., "Biotonne", "Restabfalltonne")
- `AVVCode` - Legal waste classification codes

### Layer 2: Logic & Rules
- `DisposalRule` - Conditional disposal logic
- `Condition` - Decision nodes ("Is it liquid?")
- `Instruction` - Imperative guidance ("Do not mix with...")

### Layer 3: Infrastructure
- `Facility` - Physical disposal locations
- `Container`, `Room`, `Building` - Location hierarchy
- `Source` - Data provenance (for LLM-extracted data)

### Key Relationships
```
(WasteItem)-[:DISPOSED_AT]->(Facility)      # Direct location
(WasteItem)-[:DISPOSED_IN]->(WasteStream)   # Bin type
(WasteItem)-[:HAS_RULE]->(DisposalRule)     # Conditional logic
(Condition)-[:IF_TRUE]->(WasteStream)       # Decision tree edges
(Condition)-[:IF_FALSE]->(WasteStream)
```

---

## 6. Why LLMs for Data Preparation?

### 6.1 Traditional ETL Limitations
| Challenge | Traditional Approach | Limitation |
|-----------|---------------------|------------|
| Unstructured text | Regex patterns | Brittle, language-specific |
| Conditional logic | Manual rule extraction | Doesn't scale |
| German language | Language-specific parsers | Domain vocabulary gaps |
| Schema evolution | Code changes | Slow iteration |

### 6.2 LLM-based ETL Advantages
| Challenge | LLM Approach | Benefit |
|-----------|-------------|---------|
| Unstructured text | Natural language understanding | Handles prose, tables, lists |
| Conditional logic | Semantic extraction | Captures implicit rules |
| German language | Multilingual models | Domain vocabulary built-in |
| Schema evolution | Change Pydantic model | Immediate effect on extraction |

### 6.3 The Hybrid Approach
We use **both** tiers:
- **Traditional ETL** for clean, structured data (CSV, JSON)
- **LLM-based ETL** for unstructured documents (PDF, DOCX)

This is pragmatic: LLM extraction is slower and costs tokens, so we only use it where necessary.

---

## 7. Technology Stack

| Component | Technology |
|-----------|------------|
| Database | Neo4j 5 (Graph Database) |
| Language | Python 3.12+ |
| LLM Framework | LangChain + LangGraph |
| Data Validation | Pydantic v2 |
| Container Runtime | Podman |
| LLM API | OpenAI-compatible (local or cloud) |

---

## 8. Current Status

### Completed
- Traditional ETL for facilities and waste items
- Neo4j schema with constraints and indexes
- LLM extraction chain (stateless, schema-driven)
- Orchestrator agent (LangGraph state machine)
- Entity loader with provenance tracking

### In Progress / Pending
- Legal document extraction (Abfallsatzung PDF)
- Container guide extraction (DOCX files)
- AVV code hierarchy (Excel + LLM enrichment)
- Facility enrichment via web scraping

---

## 9. Key Takeaways for Paper

1. **LLMs solve real limitations** of traditional data preparation for unstructured text
2. **Schema-driven extraction** makes LLMs reusable across entity types
3. **Conditional logic** is the key differentiator for Graph-RAG vs. Vector-RAG
4. **Hybrid pipelines** (traditional + LLM) are practical and cost-effective
5. **Provenance tracking** is essential for AI-extracted data quality and compliance

---

## 10. Potential Paper Sections

Based on this project, your paper could cover:

1. **Introduction**: The data preparation challenge for knowledge graphs
2. **Background**: Traditional ETL vs. LLM-based extraction
3. **Methodology**: Schema-driven extraction with Pydantic + LangChain
4. **Case Study**: The "Paint Can Problem" and conditional logic extraction
5. **Implementation**: Two-tier ETL architecture
6. **Evaluation**: Quality comparison (structured vs. LLM-extracted data)
7. **Discussion**: When to use LLMs for data preparation
8. **Conclusion**: Practical recommendations for hybrid pipelines

---

*This document serves as project context for research. For detailed code, see the source files in `src/etl/` and `src/llm_etl/`.*
