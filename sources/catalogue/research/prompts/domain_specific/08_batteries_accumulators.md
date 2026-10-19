# Research Agent #8: Batteries & Accumulators

## Agent Configuration
- **Tier:** 2 (Domain-Specific - Medium Impact)
- **Priority:** 8 of 18
- **Output file:** `data/research/results/08_batteries_accumulators_results.md`

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

We need structured data to populate this graph. Your task is to find data sources for: **Batteries & Accumulators**

## Research Goal

Find APIs and datasets containing:
- Battery chemistry types and classifications (Li-ion, Li-Po, NiMH, NiCd, Lead-acid, Alkaline, etc.)
- Hazard levels and fire risk indicators
- Size categories (button cell, cylindrical, prismatic, pouch, vehicle battery)
- Collection requirements by type
- UN transport numbers for dangerous goods
- Producer responsibility / take-back obligations
- Battery identification (voltage, capacity, chemistry indicators)

Batteries are critical due to fire safety concerns (especially lithium) and heavy metal content.

## Domain-Specific Context

**Battery Chemistry Types:**

| Chemistry | Common Names | Hazards | Collection Point |
|-----------|--------------|---------|------------------|
| **Lithium-ion (Li-ion)** | Rechargeable, phones, laptops, e-bikes | FIRE RISK, thermal runaway | Wertstoffhof, electronics retailers |
| **Lithium Polymer (Li-Po)** | Thin batteries, drones, tablets | FIRE RISK (puncture sensitive) | Wertstoffhof, electronics retailers |
| **Lithium metal** | Primary (non-rechargeable), button cells | FIRE RISK, reactive | Supermarket boxes, Wertstoffhof |
| **Nickel-Metal Hydride (NiMH)** | AA/AAA rechargeable, older devices | Low toxicity | Supermarket boxes, retailers |
| **Nickel-Cadmium (NiCd)** | Power tools (older), emergency lighting | CADMIUM (toxic) | Electronics retailers, Wertstoffhof |
| **Lead-acid** | Car batteries, UPS | LEAD, ACID (corrosive) | Retailers (Pfand system), garages |
| **Alkaline** | AA, AAA, C, D, 9V (primary) | Low (contains zinc/manganese) | Supermarket collection boxes |
| **Zinc-Carbon** | Cheap primary cells | Low | Supermarket collection boxes |
| **Silver Oxide** | Button cells, watches | Contains silver | Supermarket boxes |
| **Zinc-Air** | Hearing aid batteries | Low | Supermarket boxes, pharmacies |
| **Mercury (legacy)** | Old button cells (pre-2015 EU ban) | MERCURY (highly toxic) | Schadstoffmobil |

**Battery Size Categories:**
- Button/coin cells (CR2032, LR44, SR626)
- Cylindrical (AA, AAA, 18650, 21700)
- 9V block (PP3)
- Prismatic (phones, laptops)
- Pouch cells (tablets, wearables)
- Vehicle starter batteries (SLI)
- Traction batteries (EVs, e-bikes, forklifts)
- Industrial stationary (UPS, telecom)

**German Battery Law (BattG - Batteriegesetz):**
- Producers must register with Stiftung EAR
- Retailers selling >32kg batteries/year MUST accept returns
- All battery types must be collected separately (never Restmüll!)
- Lead-acid batteries: Pfand (deposit) system

**EU Battery Regulation 2023/1542 (new!):**
- Replaces old Directive 2006/66/EC
- New categories: Portable, LMT (light means of transport), SLI, Industrial, EV
- QR code requirements for battery passports (coming 2027)
- Recycling efficiency targets by chemistry

**Fire Safety - Lithium Batteries:**
- Damaged Li-ion batteries can catch fire spontaneously
- Must be stored in fireproof containers
- Tape terminals before disposal
- Never crush, puncture, or expose to water
- Special collection bins at some Wertstoffhöfe

**UN Numbers for Transport (ADR):**
| UN Number | Description |
|-----------|-------------|
| UN3090 | Lithium metal batteries (standalone) |
| UN3091 | Lithium metal batteries in/with equipment |
| UN3480 | Lithium ion batteries (standalone) |
| UN3481 | Lithium ion batteries in/with equipment |
| UN2794 | Lead-acid batteries, wet, filled |
| UN2795 | Lead-acid batteries, wet, filled with acid |
| UN3028 | Dry batteries with potassium hydroxide |

**Relevant AVV Codes:**
- 16 06 01*: Lead batteries (hazardous)
- 16 06 02*: Ni-Cd batteries (hazardous)
- 16 06 03*: Mercury-containing batteries (hazardous)
- 16 06 04: Alkaline batteries
- 16 06 05: Other batteries and accumulators
- 16 06 06*: Separately collected battery electrolyte (hazardous)
- 20 01 33*: Batteries from municipal waste containing hazardous materials
- 20 01 34: Batteries from municipal waste (non-hazardous)

**German collection systems:**
- GRS Batterien (grs-batterien.de) - Main collection system
- Stiftung EAR - Producer registration
- Retailers: All must accept portable batteries
- Wertstoffhof: Accepts all types including large/industrial

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
1. **Direct search:** "battery chemistry database", "battery types classification", "battery disposal guide"
2. **German-specific:** "Batteriegesetz", "GRS Batterien", "Batterie Entsorgung"
3. **Regulatory:** EU Battery Regulation, UN dangerous goods, ADR
4. **Industry:** Battery manufacturers associations, recycling associations
5. **Scientific:** Battery research databases, material safety databases

**Specific sources to investigate:**
- GRS Batterien (grs-batterien.de) - German collection system
- Stiftung EAR (stiftung-ear.de) - Producer registry
- EU Battery Regulation 2023/1542 documentation
- ADR dangerous goods classification
- BatteryUniversity.com - Technical battery information
- Wikidata battery type properties
- IEC battery standards (IEC 60086, IEC 61960)
- EPBA (European Portable Battery Association)

## Output Format

Structure your findings as a Markdown document with:
1. **Executive Summary** - Top 3-5 recommended sources
2. **Detailed Source Profiles** - Full documentation per source (using template above)
3. **Integration Recommendations** - Suggested priority order for ETL development
4. **Gaps Identified** - What data is still missing after this research

Be thorough but prioritize quality over quantity. 5 excellent sources are better than 20 mediocre ones.

**Special focus:** Fire safety information for lithium batteries is critical - look for sources that clearly identify which battery types pose fire risks.
