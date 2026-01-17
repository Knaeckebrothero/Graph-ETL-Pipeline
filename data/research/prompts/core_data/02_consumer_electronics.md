# Research Agent #2: Consumer Electronics & E-Waste

## Agent Configuration
- **Tier:** 1 (Core Data - High Impact)
- **Priority:** 2 of 18
- **Output file:** `data/research/results/02_consumer_electronics_results.md`

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

We need structured data to populate this graph. Your task is to find data sources for: **Consumer Electronics & E-Waste**

## Research Goal

Find APIs and datasets containing:
- Product categories and specifications (laptops, phones, appliances, etc.)
- Material composition (plastic types, metals, glass, PCBs)
- Battery types and chemistry (Li-ion, Li-Po, NiMH, NiCd, Lead-acid)
- WEEE categories (EU 6-category system)
- Hazardous components (mercury, lead, cadmium, flame retardants)
- Energy efficiency labels and RoHS/REACH compliance data

Focus on sources that can help identify what electronics contain and how they should be disposed.

## Domain-Specific Context

**WEEE Categories (EU Directive 2012/19/EU):**
1. Temperature exchange equipment (fridges, freezers, AC units)
2. Screens and monitors (TVs, laptops, tablets)
3. Lamps (fluorescent tubes, LEDs, CFLs)
4. Large equipment (washing machines, dishwashers, PV panels >50cm)
5. Small equipment (vacuums, toasters, microwaves, electronics <50cm)
6. Small IT and telecom (phones, routers, keyboards, mice)

**Hazardous components in electronics:**
- Mercury: LCD backlights (older), fluorescent lamps, thermostats
- Lead: CRT glass, solder (pre-RoHS), batteries
- Cadmium: NiCd batteries, older CRT phosphors
- Lithium: Li-ion/Li-Po batteries (fire risk!)
- Flame retardants: Circuit boards, plastic housings (BFRs)
- Refrigerants: CFCs/HFCs in cooling equipment

**Battery disposal rules:**
- All batteries must be collected separately (BattG - Batteriegesetz)
- Retailers selling batteries must accept returns
- Li-ion batteries = fire risk, special handling
- Lead-acid batteries = deposit system (Pfand)

**Relevant AVV codes:**
- 16 02 11*: Equipment with CFCs (hazardous)
- 16 02 13*: Equipment with hazardous components (e.g., CRTs, mercury)
- 16 02 14: Equipment without hazardous components
- 20 01 21*: Fluorescent tubes (mercury - hazardous)
- 20 01 23*: Equipment containing CFCs
- 20 01 35*: E-waste with hazardous components
- 20 01 36: E-waste without hazardous components
- 16 06 01*: Lead batteries
- 16 06 02*: NiCd batteries
- 16 06 03*: Mercury batteries
- 16 06 04: Alkaline batteries
- 16 06 05: Other batteries

**German terminology:**
- Elektroschrott / Elektroaltgeräte = E-waste
- Wertstoffhof = Recycling center (accepts e-waste)
- Stiftung EAR = German WEEE registration authority
- Altgeräte-Rücknahme = Appliance take-back (retailers)

**Known sources to investigate:**
- Icecat (product datasheets with specs)
- iFixit (repair guides, teardowns showing components)
- Open Products Facts (non-food product database)
- Stiftung EAR registry

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
1. **Direct search:** "electronics product database API", "e-waste classification", "WEEE product database"
2. **German-specific:** "Elektrogeräte Datenbank", "Elektroschrott Kategorien"
3. **Industry:** Manufacturer datasheets, recycler databases
4. **Regulatory:** WEEE registers, RoHS compliance databases, Stiftung EAR
5. **Repair community:** iFixit, repair.eu, component databases

**Specific sources to investigate:**
- Icecat Open Catalog
- iFixit API/database
- Open Products Facts
- Stiftung EAR (German WEEE authority)
- EPREL (EU Energy Label database)
- UL Environment (product sustainability)
- TCO Certified product database

## Output Format

Structure your findings as a Markdown document with:
1. **Executive Summary** - Top 3-5 recommended sources
2. **Detailed Source Profiles** - Full documentation per source (using template above)
3. **Integration Recommendations** - Suggested priority order for ETL development
4. **Gaps Identified** - What data is still missing after this research

Be thorough but prioritize quality over quantity. 5 excellent sources are better than 20 mediocre ones.
