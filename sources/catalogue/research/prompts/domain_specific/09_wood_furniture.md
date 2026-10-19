# Research Agent #9: Wood & Furniture (Altholz)

## Agent Configuration
- **Tier:** 2 (Domain-Specific - Medium Impact)
- **Priority:** 9 of 18
- **Output file:** `data/research/results/09_wood_furniture_results.md`

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

We need structured data to populate this graph. Your task is to find data sources for: **Wood & Furniture (Altholz)**

## Research Goal

Find APIs and datasets containing:
- Wood type classifications (solid wood, MDF, chipboard, plywood, OSB)
- Treatment indicators (untreated, painted, coated, preserved)
- German Altholz categories (A I through A IV + PCB)
- Contamination identification (wood preservatives, halogenated coatings)
- Composite materials (wood + metal, wood + fabric, wood + glass)
- Furniture component breakdown

Critical for campus disposal - old furniture, pallets, and construction wood are common.

## Domain-Specific Context

**German Altholz Categories (Altholzverordnung):**

| Category | Description | Examples | Recycling | Energy Recovery |
|----------|-------------|----------|-----------|-----------------|
| **A I** | Untreated, mechanically processed only | Pallets, crates, solid wood furniture, untreated construction timber | YES (unrestricted) | All facilities |
| **A II** | Glued, painted, coated, varnished (no halogen, no preservatives) | Painted furniture, laminated panels, MDF, chipboard, plywood, veneered boards | YES (after treatment) | Facilities >1MW |
| **A III** | Halogen-organic coatings (no wood preservatives) | PVC-coated chipboard, some laminate flooring | NO | Facilities >1MW with exhaust cleaning |
| **A IV** | Wood preservative treated | Railway sleepers, utility poles, fence posts, outdoor playground equipment, vineyard poles | **NO** | Facilities >1MW with special exhaust |
| **PCB** | PCB-contaminated | Old insulation panels from pre-1989 buildings | **NO** | Hazardous waste disposal |

**Wood Types:**
- **Solid wood (Massivholz):** Oak, pine, beech, birch, spruce, etc.
- **Engineered wood:**
  - MDF (Medium Density Fiberboard) - fine fibers, smooth surface
  - HDF (High Density Fiberboard) - denser than MDF
  - Chipboard/Particleboard (Spanplatte) - wood chips + resin
  - Plywood (Sperrholz) - thin layers glued cross-grain
  - OSB (Oriented Strand Board) - large strands, visible texture
  - Blockboard - solid wood core with veneer

**Common Wood Preservatives (make wood A IV!):**
- CCA (Chromated Copper Arsenate) - greenish tint, pre-2004
- Creosote (coal tar) - dark, oily, railway sleepers
- PCP (Pentachlorophenol) - banned in EU, legacy use
- Lindane - banned, legacy use
- Boron compounds - less toxic, still A IV

**How to Identify Treatment:**
- Green/blue-green tint → likely CCA preserved
- Dark oily appearance, strong smell → creosote
- Outdoor ground-contact use → assume preserved
- Paint, varnish, lacquer → at least A II
- Laminate surface → at least A II
- Unknown → treat as A II minimum

**Furniture Complexity:**
Furniture often has multiple components:
- Wood frame (may be solid or engineered)
- Metal hardware (hinges, screws, rails)
- Glass (shelves, doors)
- Fabric/leather upholstery
- Foam padding
- Plastic components

**Disposal routes:**
- **A I (pure untreated):** Wertstoffhof free, chipboard production
- **A II (coated):** Wertstoffhof, energy recovery
- **A III/A IV:** Wertstoffhof with fees, special treatment
- **Furniture (Sperrmüll):** Scheduled bulky waste pickup or Wertstoffhof

**Relevant AVV Codes:**

Construction/Demolition Wood (Chapter 17):
- 17 02 01: Wood (untreated)
- 17 02 04*: Glass, plastic, wood with hazardous substances

Municipal/Furniture Wood (Chapter 20):
- 20 01 37*: Wood containing hazardous substances
- 20 01 38: Wood not containing hazardous substances
- 20 03 07: Bulky waste (Sperrmüll - includes furniture)

Industrial Wood (Chapter 03):
- 03 01 04*: Sawdust, shavings with hazardous substances
- 03 01 05: Other sawdust, shavings (non-hazardous)

Packaging (Chapter 15):
- 15 01 03: Wooden packaging (pallets, crates)

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
1. **Direct search:** "wood waste classification", "furniture materials database", "Altholz categories"
2. **German-specific:** "Altholzverordnung", "Altholz Kategorien", "Holzabfall Klassifizierung"
3. **Industry:** Wood recycling associations, furniture industry data
4. **Sustainability:** PEFC, FSC certification databases (for wood types)
5. **Building materials:** Wood product databases, engineered wood specifications

**Specific sources to investigate:**
- Altholzverordnung (gesetze-im-internet.de) - Legal text
- Bundesverband Altholz e.V. (altholzverband.de) - Industry association
- ÖKOBAUDAT - Wood product LCA data
- PEFC/FSC databases - Certified wood products
- European Panel Federation - Engineered wood data
- Furniture industry associations (VDM, etc.)
- Wikidata wood material properties

## Output Format

Structure your findings as a Markdown document with:
1. **Executive Summary** - Top 3-5 recommended sources
2. **Detailed Source Profiles** - Full documentation per source (using template above)
3. **Integration Recommendations** - Suggested priority order for ETL development
4. **Gaps Identified** - What data is still missing after this research

Be thorough but prioritize quality over quantity. 5 excellent sources are better than 20 mediocre ones.

**Special focus:** The A I/A II/A III/A IV classification is critical for German disposal. Look for any structured data that helps identify which category wood items fall into.
