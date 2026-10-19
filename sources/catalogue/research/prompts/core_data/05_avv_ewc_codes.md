# Research Agent #5: AVV/EWC Legal Codes

## Agent Configuration
- **Tier:** 1 (Core Data - High Impact)
- **Priority:** 5 of 18
- **Output file:** `data/research/results/05_avv_ewc_codes_results.md`

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

We need structured data to populate this graph. Your task is to find data sources for: **AVV/EWC Legal Codes**

## Research Goal

Find datasets and documentation containing:
- Complete AVV (Abfallverzeichnis-Verordnung) code list with descriptions
- Hazard classifications (codes marked with *)
- EWC (European Waste Catalogue) equivalents
- Code hierarchy (chapter → subgroup → specific waste)
- Cross-references between AVV codes and waste streams
- German and English descriptions

This is the legal foundation for waste classification in Germany/EU.

## Domain-Specific Context

**What is AVV?**
The Abfallverzeichnis-Verordnung (AVV) is the German implementation of the European Waste Catalogue (EWC). It provides a standardized 6-digit coding system for all waste types.

**AVV Code Structure:**
```
XX        = Chapter (20 total, based on waste origin/industry)
XX XX     = Subgroup (process or industry-specific)
XX XX XX  = Specific waste type
XX XX XX* = Hazardous waste (asterisk marker)
```

**The 20 AVV Chapters:**
- 01: Mining and quarrying waste
- 02: Agriculture, horticulture, food production waste
- 03: Wood processing, paper production waste
- 04: Leather, fur, textile industry waste
- 05: Petroleum refining, coal pyrolysis waste
- 06: Inorganic chemical processes waste
- 07: Organic chemical processes waste
- 08: Coatings, adhesives, sealants, printing inks waste
- 09: Photographic industry waste
- 10: Thermal processes waste
- 11: Chemical surface treatment, metal coating waste
- 12: Mechanical shaping, surface treatment waste
- 13: Oil waste (except edible oils)
- 14: Organic solvents, coolants waste
- 15: Packaging waste, absorbents, wiping cloths, filter materials
- 16: Waste not otherwise specified
- 17: Construction and demolition waste
- 18: Healthcare and veterinary waste
- 19: Waste/wastewater treatment facility waste
- 20: Municipal waste (household and similar)

**Mirror entries:**
Some waste types have "mirror entries" - two codes for the same material depending on whether it's hazardous:
- Example: 08 01 11* (paint with solvents - hazardous) vs. 08 01 12 (paint without solvents - non-hazardous)

**Legal sources:**
- German law: gesetze-im-internet.de/avv/
- EU Decision: 2000/532/EC (European Waste Catalogue)
- Updates via Commission Decision 2014/955/EU

**University-relevant chapters:**
- Chapter 06, 07: Lab chemical waste
- Chapter 08: Paints, inks
- Chapter 15: Packaging
- Chapter 16: Batteries, electronics, misc
- Chapter 17: Construction (campus renovation)
- Chapter 18: Medical/bio lab waste
- Chapter 20: Office/cafeteria waste

**Data we need:**
- All ~800+ AVV codes with German descriptions
- English translations (EWC)
- Hazard markers
- Parent-child hierarchy
- Disposal route indicators (if available)

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
1. **Direct search:** "AVV Codes download", "Abfallverzeichnis Excel", "European Waste Catalogue database"
2. **German-specific:** "Abfallschlüssel Verzeichnis", "AVV Katalog"
3. **Legal sources:** gesetze-im-internet.de, EUR-Lex
4. **Government:** Umweltbundesamt, LAGA, BMU
5. **Industry tools:** Waste management software, compliance tools
6. **EU portals:** European Environment Agency, data.europa.eu

**Specific sources to investigate:**
- gesetze-im-internet.de/avv/ (official German law text)
- EUR-Lex Decision 2000/532/EC (EU source)
- Umweltbundesamt AVV resources
- LAGA (Länderarbeitsgemeinschaft Abfall)
- CSG EWC Code Tool (UK, but same codes)
- Eurostat waste statistics (uses EWC)
- Commercial AVV lookup tools (may reveal data structure)

**Also look for:**
- Excel/CSV exports of the full code list
- Structured data versions (JSON, XML)
- APIs from environmental agencies
- Open data portal versions
- GitHub repositories with parsed AVV data

## Output Format

Structure your findings as a Markdown document with:
1. **Executive Summary** - Top 3-5 recommended sources
2. **Detailed Source Profiles** - Full documentation per source (using template above)
3. **Integration Recommendations** - Suggested priority order for ETL development
4. **Gaps Identified** - What data is still missing after this research

Be thorough but prioritize quality over quantity. 5 excellent sources are better than 20 mediocre ones.

**Special note:** The official legal text is authoritative but may be in PDF/HTML format that needs parsing. Look for pre-parsed versions or APIs that provide structured access.
