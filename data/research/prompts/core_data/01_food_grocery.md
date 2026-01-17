# Research Agent #1: Food & Grocery Products

## Agent Configuration
- **Tier:** 1 (Core Data - High Impact)
- **Priority:** 1 of 18
- **Output file:** `data/research/results/01_food_grocery_results.md`

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

We need structured data to populate this graph. Your task is to find data sources for: **Food & Grocery Products**

## Research Goal

Find APIs and datasets containing:
- Product identification (barcodes/EAN/GTIN, product names, brands, categories)
- Packaging materials (plastic types, glass, metal, cardboard, composites)
- Recycling codes and disposal instructions
- Organic content indicators (for Biomüll separation)
- Eco-scores or environmental impact ratings

Focus on sources with good coverage of products sold in Germany and the EU.

## Domain-Specific Context

**Key context for food/grocery waste:**
- Packaging is the main disposal concern (the food itself typically goes to Biomüll)
- German recycling codes follow international standards:
  - 01-07: Plastics (PET, HDPE, PVC, LDPE, PP, PS, Other)
  - 20-22: Paper/cardboard
  - 40-41: Metals (steel, aluminum)
  - 70-72: Glass (clear, green, brown)
  - 80-84: Composites (paper+plastic, paper+metal, Tetra Pak)
- "Grüner Punkt" / Dual System for packaging (Gelber Sack/Gelbe Tonne)
- Composite packaging is challenging (e.g., Tetra Pak = paper+plastic+aluminum)

**Known source to build upon:**
- Open Food Facts (openfoodfacts.org) - Has `packaging_tags` and `packaging_materials_tags` fields
- Investigate: Open Pet Food Facts, Open Beauty Facts (same schema, different products)

**Relevant AVV codes:**
- Chapter 15: Verpackungsabfall (packaging waste)
- Chapter 20: Siedlungsabfälle (municipal waste)
- 15 01 01: Paper/cardboard packaging
- 15 01 02: Plastic packaging
- 15 01 04: Metal packaging
- 15 01 07: Glass packaging
- 20 01 08: Biodegradable kitchen waste

**German terminology:**
- Gelber Sack/Gelbe Tonne = Yellow bag/bin (packaging)
- Biomüll/Biotonne = Organic waste bin
- Restmüll = Residual waste
- Papiertonne = Paper bin
- Altglas = Glass recycling (color-separated)

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
1. **Direct search:** "food product database API", "grocery barcode database", "packaging materials dataset"
2. **German-specific:** "Lebensmittel Datenbank", "Verpackung Recycling Daten"
3. **Open data portals:** data.europa.eu, govdata.de
4. **Industry:** GS1 (barcode standards), packaging industry associations
5. **Sustainability:** Eco-label databases, environmental product declarations

**Specific sources to investigate:**
- Open Food Facts and sibling projects
- GS1 GTIN/barcode databases
- Nutri-Score / Eco-Score data sources
- German trade association data (BVE, HDE)
- EU food product registries

## Output Format

Structure your findings as a Markdown document with:
1. **Executive Summary** - Top 3-5 recommended sources
2. **Detailed Source Profiles** - Full documentation per source (using template above)
3. **Integration Recommendations** - Suggested priority order for ETL development
4. **Gaps Identified** - What data is still missing after this research

Be thorough but prioritize quality over quantity. 5 excellent sources are better than 20 mediocre ones.
