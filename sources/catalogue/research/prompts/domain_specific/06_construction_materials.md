# Research Agent #6: Construction Materials (Bauschutt)

## Agent Configuration
- **Tier:** 2 (Domain-Specific - Medium Impact)
- **Priority:** 6 of 18
- **Output file:** `data/research/results/06_construction_materials_results.md`

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

We need structured data to populate this graph. Your task is to find data sources for: **Construction Materials (Bauschutt)**

## Research Goal

Find APIs and datasets containing:
- Building material types and compositions (concrete, brick, wood, insulation, glass, metals)
- Material density data (critical for weight estimation from visual volume)
- Contamination indicators (asbestos, tar, lead paint, treated wood)
- Life Cycle Assessment (LCA) data including end-of-life scenarios
- Environmental Product Declarations (EPDs)
- Recycling potential classifications (pure mineral vs. mixed construction waste)

This is relevant for university campus renovations and construction projects.

## Domain-Specific Context

**Key distinction in German construction waste:**

| Category | German Term | Description | Disposal Cost |
|----------|-------------|-------------|---------------|
| Pure mineral rubble | **Bauschutt** | Concrete, brick, tiles, stone only | Cheap, recyclable |
| Mixed construction waste | **Baumischabfall** | Rubble + wood + plastic + wallpaper + metals | Expensive, limited recycling |
| Hazardous construction waste | **Gefährlicher Bauabfall** | Asbestos, tar, PCBs, lead paint | Very expensive, special disposal |

**Common construction materials:**
- **Mineral:** Concrete, brick, tiles, ceramite, natural stone, morite
- **Gypsum:** Drywall (Rigips), plaster - must be separated (sulfate issues in landfill)
- **Wood:** Untreated vs. treated (see Altholz categories A1-A4)
- **Insulation:** Mineral wool, styrofoam (EPS/XPS), polyurethane foam
- **Metals:** Steel, aluminum, copper (valuable scrap)
- **Glass:** Window glass (different from container glass!)
- **Roofing:** Tiles, bitumen, tar paper (some hazardous)

**Contamination concerns:**
- **Asbestos:** Pre-1993 buildings, requires certified removal
- **PCBs:** Sealants, paints in 1960s-80s buildings
- **PAHs (tar):** Old roofing, road materials
- **Lead paint:** Pre-1970s buildings
- **Wood preservatives:** CCA, creosote (railway sleepers, utility poles)

**Relevant AVV codes (Chapter 17 - Construction/Demolition):**
- 17 01 01: Concrete
- 17 01 02: Bricks
- 17 01 03: Tiles and ceramics
- 17 01 07: Mixed concrete, bricks, tiles (non-hazardous)
- 17 02 01: Wood (untreated)
- 17 02 02: Glass
- 17 02 03: Plastics
- 17 02 04*: Glass, plastic, wood with hazardous substances
- 17 03 01*: Bituminous mixtures containing coal tar (hazardous)
- 17 04 01-07: Metals (copper, aluminum, lead, zinc, iron, tin, mixed)
- 17 05 03*: Soil containing hazardous substances
- 17 06 01*: Insulation with asbestos
- 17 06 03*: Other insulation with hazardous substances
- 17 06 04: Insulation materials (non-hazardous)
- 17 08 01*: Gypsum contaminated with hazardous substances
- 17 09 03*: Other construction waste with hazardous substances

**Density reference (for volume-to-weight estimation):**
```
Material              Density (kg/m³)
────────────────────────────────────
Concrete              2,400
Brick                 1,800
Tiles/Ceramics        2,000
Drywall (gypsum)        800
Mineral wool            100
EPS/Styrofoam            25
Glass                 2,500
Steel                 7,800
Soil/Earth            1,500
```

**Known sources to investigate:**
- ÖKOBAUDAT (German building materials LCA database)
- OpenEPD / EC3 (Environmental Product Declarations)
- Materials Project (scientific material properties)
- MakeItFrom.com (engineering properties)

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
1. **Direct search:** "construction materials database", "building materials API", "LCA database"
2. **German-specific:** "Baustoffe Datenbank", "ÖKOBAUDAT", "Bauschutt Klassifikation"
3. **Sustainability:** EPD databases, circular economy platforms, material passports
4. **Scientific:** Materials science databases, engineering property databases
5. **Industry:** Construction industry associations, recycling associations

**Specific sources to investigate:**
- ÖKOBAUDAT (oekobaudat.de) - German government LCA database
- OpenEPD / EC3 (buildingtransparency.org) - Environmental declarations
- Madaster (material passport platform)
- Materials Project (materialsproject.org) - Scientific properties
- MakeItFrom.com - Engineering properties comparison
- DGNB (German sustainable building council)
- BNB (Bewertungssystem Nachhaltiges Bauen)

## Output Format

Structure your findings as a Markdown document with:
1. **Executive Summary** - Top 3-5 recommended sources
2. **Detailed Source Profiles** - Full documentation per source (using template above)
3. **Integration Recommendations** - Suggested priority order for ETL development
4. **Gaps Identified** - What data is still missing after this research

Be thorough but prioritize quality over quantity. 5 excellent sources are better than 20 mediocre ones.
