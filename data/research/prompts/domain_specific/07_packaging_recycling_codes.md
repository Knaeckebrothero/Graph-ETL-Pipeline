# Research Agent #7: Packaging & Recycling Codes

## Agent Configuration
- **Tier:** 2 (Domain-Specific - Medium Impact)
- **Priority:** 7 of 18
- **Output file:** `data/research/results/07_packaging_recycling_codes_results.md`

---

## Prompt

You are a research agent specializing in data source discovery for waste management systems.

## Project Context

We are building "Fessi," a knowledge graph for a German university waste disposal chatbot. The system uses Graph-RAG (Retrieval-Augmented Generation with graph database) to answer questions like:
- "Where do I throw away old paint cans?"
- "How do I dispose of lab chemicals?"
- "What bin does this packaging go in?"

The knowledge graph has three layers:
1. **Core Entities:** WasteItem, WasteStream, AVVCode (legal classification)
2. **Logic & Rules:** DisposalRule, Condition, Instruction (decision trees for conditional logic)
3. **Infrastructure:** Container, Building, Facility (physical disposal locations)

We need structured data to populate this graph. Your task is to find data sources for: **Packaging & Recycling Codes**

## Research Goal

Find datasets and standards documentation containing:
- Complete recycling code definitions (01-07 plastics, 20-22 paper, 40-41 metals, 70-72 glass, 80-84 composites)
- Material identification by recycling symbol
- Recyclability assessments by material type and local infrastructure
- Multi-layer/composite packaging identification
- Grüner Punkt / Dual System specifications
- Packaging design for recyclability guidelines

This helps decode the symbols users see on packaging.

## Domain-Specific Context

**Recycling Identification Codes (RIC / Resin Codes):**

**Plastics (01-07):**
| Code | Abbreviation | Full Name | Common Products | Recyclability |
|------|--------------|-----------|-----------------|---------------|
| 01 | PET/PETE | Polyethylene terephthalate | Bottles, containers | Widely recycled |
| 02 | HDPE | High-density polyethylene | Milk jugs, detergent | Widely recycled |
| 03 | PVC | Polyvinyl chloride | Pipes, blister packs | Rarely recycled |
| 04 | LDPE | Low-density polyethylene | Bags, squeeze bottles | Limited |
| 05 | PP | Polypropylene | Yogurt cups, caps | Increasingly |
| 06 | PS | Polystyrene | Styrofoam, cups | Rarely recycled |
| 07 | OTHER | Mixed/other plastics | Various | Not recycled |

**Paper/Cardboard (20-22):**
| Code | Material | Recyclability |
|------|----------|---------------|
| 20 | PAP - Corrugated cardboard | Widely recycled |
| 21 | PAP - Other cardboard | Widely recycled |
| 22 | PAP - Paper | Widely recycled |

**Metals (40-41):**
| Code | Material | Recyclability |
|------|----------|---------------|
| 40 | FE - Steel | Widely recycled |
| 41 | ALU - Aluminium | Widely recycled |

**Glass (70-72):**
| Code | Material | Color | Recyclability |
|------|----------|-------|---------------|
| 70 | GL - Clear glass | Colorless | Widely recycled |
| 71 | GL - Green glass | Green | Widely recycled |
| 72 | GL - Brown glass | Brown | Widely recycled |

**Composites (80-84):**
| Code | Material | Examples | Recyclability |
|------|----------|----------|---------------|
| 80 | C/PAP | Paper + plastic | Beverage cartons - Special |
| 81 | C/PAP | Paper + metal | Coffee pods - Limited |
| 82 | C/PAP | Paper + metal + plastic | Tetra Pak - Special |
| 84 | C/PAP | Paper + plastic + aluminium | Juice boxes - Special |

**German Dual System (Grüner Punkt):**
- Licensed packaging goes in Gelber Sack/Gelbe Tonne
- Includes: plastic packaging, metal cans, composite cartons
- Does NOT include: glass (separate containers), paper (Papiertonne)
- System operators: DSD, Interseroh, Landbell, etc.

**Challenges with packaging:**
- Composite materials (multiple layers) are hard to recycle
- Sleeves/labels may be different material than container
- "Recyclable" doesn't mean "recycled" - depends on local infrastructure
- Black plastic often undetectable by sorting machines

**German packaging regulation:**
- VerpackG (Verpackungsgesetz) - German Packaging Act
- LUCID register - Producer responsibility database
- Recycling quotas by material type

**Relevant AVV codes (Chapter 15 - Packaging):**
- 15 01 01: Paper and cardboard packaging
- 15 01 02: Plastic packaging
- 15 01 03: Wooden packaging
- 15 01 04: Metallic packaging
- 15 01 05: Composite packaging
- 15 01 06: Mixed packaging
- 15 01 07: Glass packaging
- 15 01 09: Textile packaging
- 15 01 10*: Packaging with hazardous residues
- 15 01 11*: Metallic packaging with hazardous matrix (aerosols)

## What We Need From Data Sources

For each source you find, we need data that can map to our schema:
- **Item identification:** Names, synonyms, barcodes, product codes
- **Material composition:** What is it made of? (plastic type, metal, chemical composition)
- **Disposal classification:** Which bin/stream? Any special handling?
- **Conditional logic:** State-dependent rules (empty vs. full, liquid vs. dry, treated vs. untreated)
- **Hazard indicators:** GHS codes, ADR classes, AVV hazard markers (*)
- **Location data:** Where to dispose (Wertstoffhof, retailer take-back, Schadstoffmobil)

## Output Requirements

For EACH data source you find, document:

### 1. Source Identification
- **Name:** Official name of the database/API/dataset
- **URL:** Direct link to the data or documentation
- **Provider:** Organization maintaining it (government, NGO, company, community)

### 2. Data Format & Access
- **Format:** API (REST/GraphQL), JSON, CSV, XML, Excel, PDF, web scraping required
- **Access method:**
  - Free (no registration)
  - Free (registration required)
  - Freemium (limited free tier)
  - Paid/Licensed
  - Academic only
- **Rate limits:** If API, note any request limits
- **Bulk download:** Is full dataset download available?

### 3. Coverage & Quality
- **Record count:** How many items/entries (approximate)
- **Geographic scope:** Global, EU, Germany-specific, city-specific
- **Language:** German, English, multilingual
- **Update frequency:** Real-time, daily, monthly, annual, static
- **Data quality:** Crowdsourced, vendor-provided, government-official, scientific

### 4. Licensing
- **License type:** ODbL, CC-BY, CC0, public domain, proprietary, unknown
- **Commercial use:** Allowed, restricted, prohibited
- **Attribution required:** Yes/No

### 5. Relevance Assessment
Rate 1-5 (5 = essential, 1 = marginal):
- **Coverage relevance:** Does it cover items we need?
- **Data richness:** Does it have the fields we need?
- **Accessibility:** How easy to integrate?
- **Overall score:** Combined assessment

### 6. Integration Notes
- **Useful fields:** Which specific fields/properties are valuable for our graph?
- **ID matching:** Can we link to other sources (barcodes, Wikidata IDs, AVV codes)?
- **Technical considerations:** API quirks, pagination, authentication, data cleaning needed
- **Example query:** If API, provide a sample request

### 7. Similar/Alternative Sources
- List any related sources you found that might be alternatives or supplements

## Search Strategy

Use these approaches to find sources:
1. **Direct search:** "recycling codes database", "plastic resin codes", "packaging materials API"
2. **German-specific:** "Recycling-Code Verzeichnis", "Verpackungsmaterial Datenbank"
3. **Standards bodies:** ASTM D7611, ISO 14021, DIN standards
4. **Industry:** Plastics industry associations, packaging associations
5. **Sustainability:** How2Recycle, RecyClass, packaging recyclability tools

**Specific sources to investigate:**
- ASTM D7611/D7611M (Resin Identification Codes standard)
- How2Recycle (how2recycle.info) - US-based recyclability labels
- RecyClass (recyclass.eu) - EU plastic recyclability assessment
- LUCID Verpackungsregister (verpackungsregister.org)
- Plastics Europe (plasticseurope.org)
- DSD / Der Grüne Punkt documentation
- Open Food Facts `packaging_tags` taxonomy
- Wikidata recycling code properties

## Output Format

Structure your findings as a Markdown document with:
1. **Executive Summary** - Top 3-5 recommended sources
2. **Detailed Source Profiles** - Full documentation per source (using template above)
3. **Integration Recommendations** - Suggested priority order for ETL development
4. **Gaps Identified** - What data is still missing after this research

Be thorough but prioritize quality over quantity. 5 excellent sources are better than 20 mediocre ones.

**Special focus:** Look for structured data that maps recycling codes to:
1. Material properties (density, recyclability %)
2. Disposal routes (which bin in German system)
3. Common products using each material
