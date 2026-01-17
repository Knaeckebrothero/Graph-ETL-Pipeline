# Research Agent #4: German Municipal Waste Rules

## Agent Configuration
- **Tier:** 1 (Core Data - High Impact)
- **Priority:** 4 of 18
- **Output file:** `data/research/results/04_german_municipal_rules_results.md`

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

We need structured data to populate this graph. Your task is to find data sources for: **German Municipal Waste Rules**

## Research Goal

Find APIs, datasets, and scrapable sources containing:
- Abfall-ABC / waste disposal guides (item → bin mappings)
- Municipal waste collection schedules and systems
- Wertstoffhof locations and accepted materials
- Schadstoffmobil schedules and accepted items
- Local variations in disposal rules between cities
- White-label waste app APIs (MyMüll, AbfallPlus, etc.)

This is essential for accurate, location-specific disposal advice.

## Domain-Specific Context

**German waste separation system:**
- **Restmüll** (black/gray bin): Non-recyclable household waste
- **Biomüll/Biotonne** (brown bin): Organic/compostable waste
- **Papiertonne** (blue bin): Paper and cardboard
- **Gelber Sack/Gelbe Tonne** (yellow): Packaging with Grüner Punkt
- **Altglas** (containers): Glass sorted by color (clear, green, brown)
- **Sperrmüll**: Bulky waste (furniture, mattresses) - scheduled pickup
- **Wertstoffhof**: Recycling center for special items
- **Schadstoffmobil**: Mobile collection for hazardous household waste

**Why local rules matter:**
- Each municipality (Landkreis/Stadt) has its own waste company
- Rules vary: some accept cooking oil in Biomüll, others don't
- Collection schedules differ
- Wertstoffhof acceptance criteria vary
- Some items are free, others cost money

**Major waste management companies/systems:**
- BSR (Berliner Stadtreinigung) - Berlin
- AWM (Abfallwirtschaftsbetrieb München) - Munich
- SRH (Stadtreinigung Hamburg) - Hamburg
- AWS (Abfallwirtschaft Stuttgart) - Stuttgart
- FES (Frankfurter Entsorgungs- und Service GmbH) - Frankfurt
- AWB Köln - Cologne
- AWISTA - Düsseldorf

**White-label app providers (APIs may exist):**
- MyMüll.de - Used by many municipalities
- Abfall+ / AbfallPlus (abfall.io) - Another major provider
- Müllmax - Some municipalities
- A.R.T. apps - Regional

**Abfall-ABC structure:**
Most cities publish an "Abfall-ABC" or "Abfalllexikon" - an alphabetical list of items with disposal instructions. These typically include:
- Item name (sometimes with synonyms)
- Target bin/disposal route
- Special instructions
- Fees (if applicable)

**Known Abfall-ABC URLs:**
- Berlin BSR: bsr.de/abfall-abc-20563.php
- Munich AWM: awm-muenchen.de/abfallarten
- Hamburg SRH: stadtreinigung.hamburg/abfall-abc/
- Cologne AWB: awbkoeln.de/abfalllexikon/
- Frankfurt FES: fes-frankfurt.de/leistungen/entsorgung/abfalllexikon/

**Open source projects:**
- HACS Waste Collection Schedule (Home Assistant integration)
- Various GitHub projects scraping waste calendars

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
1. **Direct search:** "Abfall API", "waste disposal Germany API", "Müll App API"
2. **German-specific:** "Abfall-ABC Daten", "Abfallkalender API", "Wertstoffhof Standorte"
3. **Municipal websites:** Check major cities' waste company sites
4. **App reverse engineering:** Investigate MyMüll, AbfallPlus app APIs
5. **Open source:** GitHub repos for waste calendar integrations
6. **Open data portals:** govdata.de, city open data portals

**Specific sources to investigate:**
- MyMüll.de (underlying API)
- AbfallPlus / Abfall.io
- HACS Waste Collection Schedule GitHub
- BSR Berlin (largest city, often sets patterns)
- Individual city Abfall-ABC pages (scraping potential)
- OpenStreetMap waste infrastructure tags
- Govdata.de waste-related datasets

## Output Format

Structure your findings as a Markdown document with:
1. **Executive Summary** - Top 3-5 recommended sources
2. **Detailed Source Profiles** - Full documentation per source (using template above)
3. **Integration Recommendations** - Suggested priority order for ETL development
4. **Gaps Identified** - What data is still missing after this research

Be thorough but prioritize quality over quantity. 5 excellent sources are better than 20 mediocre ones.

**Special focus:** Pay attention to whether APIs are documented vs. undocumented, and whether scraping would be legally/technically feasible for Abfall-ABC pages.
