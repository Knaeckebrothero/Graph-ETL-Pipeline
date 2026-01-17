# Research Agent #11: Material Properties

## Agent Configuration
- **Tier:** 3 (Enrichment - Nice to Have)
- **Priority:** 11 of 18
- **Output file:** `data/research/results/11_material_properties_results.md`

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

We need structured data to populate this graph. Your task is to find data sources for: **Material Properties**

## Research Goal

Find APIs and datasets containing:
- Density data (critical for weight estimation from visual volume)
- Flammability and autoignition temperatures
- Toxicity indicators (LD50, LC50, exposure limits)
- Biodegradability and environmental persistence
- Chemical compatibility (what materials react with each other)
- Thermal properties (melting point, decomposition temperature)
- Electrical conductivity (relevant for battery/electronics safety)

These properties help estimate disposal costs, identify hazards, and provide safety guidance.

## Domain-Specific Context

**Why material properties matter for waste disposal:**

1. **Weight estimation:** Disposal costs are often by weight. If a user shows a pile of rubble, knowing density helps estimate cost:
   - Concrete: 2,400 kg/m³ → Heavy, expensive
   - Styrofoam: 25 kg/m³ → Light, cheap by weight but bulky

2. **Safety identification:** Properties reveal hazards not obvious from appearance:
   - Flash point < 23°C → Highly flammable, special handling
   - Autoignition temp → Fire risk during storage
   - Water reactivity → Don't get wet!

3. **Disposal route determination:**
   - Biodegradable → Potentially compostable (Biomüll)
   - Persistent chemicals → Hazardous waste (Schadstoffmobil)
   - Reactive materials → Special handling, segregation rules

**Key properties and their relevance:**

| Property | Unit | Relevance |
|----------|------|-----------|
| Density | kg/m³ | Weight estimation, disposal cost |
| Flash point | °C | Flammability classification |
| Autoignition temp | °C | Fire risk in storage |
| Melting point | °C | Processing, incineration |
| Boiling point | °C | Vapor hazards |
| Solubility | g/L | Water contamination risk |
| pH | - | Corrosivity |
| LD50 (oral) | mg/kg | Toxicity classification |
| LC50 (inhalation) | mg/L | Inhalation hazard |
| Biodegradability | % | Environmental persistence |
| Bioaccumulation | factor | Food chain concerns |
| Electrical conductivity | S/m | Short circuit risk |

**Density reference table (common materials):**
```
Material              Density (kg/m³)
────────────────────────────────────────
Water                 1,000
Concrete              2,400
Brick                 1,800
Glass                 2,500
Steel                 7,800
Aluminum              2,700
Copper                8,900
Lead                  11,300
Wood (average)          600
Paper/Cardboard         700
PET plastic           1,380
HDPE plastic            950
PVC plastic           1,400
Polystyrene (solid)   1,050
Styrofoam (EPS)          25
Soil/Earth            1,500
Sand                  1,600
Textiles (loose)        200
```

**Flammability classification (flash point):**
- < 23°C: Highly flammable (GHS02, H224/H225)
- 23-60°C: Flammable (H226)
- > 60°C: Combustible
- > 93°C: Generally not classified as flammable

**Known data sources to investigate:**
- PubChem (physical properties for chemicals)
- NIST Chemistry WebBook (thermochemical data)
- Materials Project (scientific material properties)
- MakeItFrom.com (engineering comparisons)
- MatWeb (material datasheets)
- Open Materials Database

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
1. **Direct search:** "material properties database API", "density database", "physical properties API"
2. **Scientific:** NIST, materials science databases, chemistry databases
3. **Engineering:** Material selection databases, CAD material libraries
4. **Safety:** Toxicity databases, MSDS/SDS databases
5. **Environmental:** Biodegradability databases, environmental fate databases

**Specific sources to investigate:**
- PubChem (pubchem.ncbi.nlm.nih.gov) - Physical properties section
- NIST Chemistry WebBook (webbook.nist.gov)
- Materials Project (materialsproject.org)
- MakeItFrom.com (makeitfrom.com)
- MatWeb (matweb.com)
- Open Materials Database (openmaterialsdb.se)
- Granta EduPack (academic material selection)
- EPA EPI Suite (environmental properties)
- ECHA (physical-chemical properties)

## Output Format

Structure your findings as a Markdown document with:
1. **Executive Summary** - Top 3-5 recommended sources
2. **Detailed Source Profiles** - Full documentation per source (using template above)
3. **Integration Recommendations** - Suggested priority order for ETL development
4. **Gaps Identified** - What data is still missing after this research

Be thorough but prioritize quality over quantity. 5 excellent sources are better than 20 mediocre ones.

**Special focus:** Density data is the highest priority - this enables weight estimation from volume, which is critical for disposal cost calculations.
