# Research Agent #15: Disassembly Instructions

## Agent Configuration
- **Tier:** 3 (Enrichment - Nice to Have)
- **Priority:** 15 of 18
- **Output file:** `data/research/results/15_disassembly_instructions_results.md`

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

We need structured data to populate this graph. Your task is to find data sources for: **Disassembly Instructions**

## Research Goal

Find APIs and datasets containing:
- Product teardown guides and disassembly steps
- Component identification (what's inside the product)
- Hazardous component locations (batteries, mercury switches, CFCs)
- Material recovery instructions
- Tools required for safe disassembly
- Manufacturer recycling/disposal documentation

This helps users properly prepare items for disposal (e.g., "remove battery before recycling laptop").

## Domain-Specific Context

**Why disassembly matters for waste:**

Many products contain components that must be separated before disposal:
- **Batteries** - Fire hazard, must be removed from electronics
- **Mercury** - Found in old thermostats, some lamps, switches
- **Refrigerants** - CFCs/HFCs in fridges, AC units (ozone depletion)
- **Toner/Ink** - Should be removed from printers (recyclable separately)
- **Data storage** - Hard drives may need data destruction
- **Valuable materials** - Copper, gold, silver can be recovered

**Common disassembly requirements:**

| Product | Component to Remove | Reason | Disposal Route |
|---------|---------------------|--------|----------------|
| Smartphone | Battery (if possible) | Fire risk | Battery collection |
| Laptop | Battery | Fire risk, Li-ion | Battery collection |
| Printer | Toner cartridge | Recyclable separately | Retailer take-back |
| Old thermostat | Mercury ampule | Toxic | Schadstoffmobil |
| Smoke detector | Radioactive element (Am-241) | Radioactive | Return to manufacturer |
| Refrigerator | Refrigerant | Ozone depleting | Professional removal |
| CRT monitor/TV | Entire unit | Lead in glass | Wertstoffhof (don't break) |
| Fluorescent tube | Entire tube | Mercury vapor | Wertstoffhof (intact) |
| Microwave | Magnetron | High voltage capacitor | Don't disassemble |

**iFixit as primary source:**
iFixit.com hosts 100,000+ repair guides with:
- Step-by-step teardown photos
- Component identification
- Tools required
- Difficulty ratings
- Repairability scores

**WEEE Directive requirements:**
- Article 15 requires producers to provide "treatment information"
- Recyclers must have access to component/material information
- Some manufacturers publish this (Apple, Dell, HP)

**Product categories needing disassembly guidance:**
1. **Consumer electronics** - Phones, tablets, laptops, cameras
2. **Appliances** - Fridges, washing machines, microwaves
3. **IT equipment** - Computers, servers, printers
4. **Lighting** - Fluorescent tubes, CFLs, LED bulbs
5. **Heating/cooling** - Thermostats, AC units, heaters
6. **Safety devices** - Smoke detectors, fire extinguishers
7. **Medical devices** - Thermometers, blood pressure monitors

**Safety warnings:**
- High voltage capacitors in microwaves, CRT monitors
- Pressurized containers (fire extinguishers, aerosols)
- Sharp edges, broken glass
- Chemical exposure (toner, refrigerants)
- Radioactive materials (smoke detectors, some exit signs)

**Data we need per product:**
1. Product identification (model, category)
2. List of components
3. Hazardous components flagged
4. Disassembly steps (optional, can link to external)
5. Tools required
6. Safety warnings
7. Disposal route per component

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
1. **Direct search:** "product teardown database", "disassembly guide API", "repair manual database"
2. **Repair community:** iFixit, repair.eu, repair cafes
3. **Manufacturer:** Apple, Samsung, Dell recycling/repair documentation
4. **Regulatory:** WEEE treatment information, producer responsibility databases
5. **E-waste:** E-waste recycler databases, material recovery guides

**Specific sources to investigate:**
- iFixit (ifixit.com) - Repair guides, teardowns, repairability scores
- iFixit API (if available)
- Repair.eu (European repair network)
- Manufacturer environmental reports (Apple, Samsung, Dell, HP, Lenovo)
- WEEE producer treatment information
- Stiftung EAR (German WEEE registry)
- YouTube teardown channels (data extraction potential)
- Instructables (diy guides)
- WikiHow repair guides

## Output Format

Structure your findings as a Markdown document with:
1. **Executive Summary** - Top 3-5 recommended sources
2. **Detailed Source Profiles** - Full documentation per source (using template above)
3. **Integration Recommendations** - Suggested priority order for ETL development
4. **Gaps Identified** - What data is still missing after this research

Be thorough but prioritize quality over quantity. 5 excellent sources are better than 20 mediocre ones.

**Special focus:**
1. iFixit is the primary source - investigate API access and data structure
2. Look for structured component lists (not just videos/text)
3. Identify sources that flag hazardous components specifically
4. Note which manufacturers provide good recycling documentation
