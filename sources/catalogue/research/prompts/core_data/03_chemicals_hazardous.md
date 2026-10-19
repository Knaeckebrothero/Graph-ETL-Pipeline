# Research Agent #3: Chemicals & Hazardous Materials

## Agent Configuration
- **Tier:** 1 (Core Data - High Impact)
- **Priority:** 3 of 18
- **Output file:** `data/research/results/03_chemicals_hazardous_results.md`

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

We need structured data to populate this graph. Your task is to find data sources for: **Chemicals & Hazardous Materials**

## Research Goal

Find APIs and datasets containing:
- Chemical names and synonyms (including German names)
- CAS numbers, EC numbers, UN numbers
- GHS hazard classifications (pictograms, H-statements, P-statements)
- Safety Data Sheet (SDS) Section 13 disposal information
- Physical properties (flammability, reactivity, toxicity)
- Incompatible materials (what not to mix)
- ADR transport classifications

This is critical for a university context where lab chemicals are common waste items.

## Domain-Specific Context

**GHS (Globally Harmonized System) Hazard Classes:**
- Physical hazards: Explosives, Flammable, Oxidizers, Gases, Corrosive to metals
- Health hazards: Acute toxicity, Skin/eye irritation, Sensitization, Carcinogenicity, etc.
- Environmental hazards: Aquatic toxicity

**GHS Pictograms (9 symbols):**
- GHS01: Exploding bomb (explosives)
- GHS02: Flame (flammables)
- GHS03: Flame over circle (oxidizers)
- GHS04: Gas cylinder (compressed gases)
- GHS05: Corrosion (corrosives)
- GHS06: Skull and crossbones (acute toxicity)
- GHS07: Exclamation mark (irritant, sensitizer)
- GHS08: Health hazard (carcinogen, mutagen, etc.)
- GHS09: Environment (aquatic toxicity)

**H-Statements (Hazard):** H200-H400 series codes describing specific hazards
**P-Statements (Precautionary):** P100-P500 series codes for handling/disposal

**ADR Classes (Transport of Dangerous Goods):**
- Class 1: Explosives
- Class 2: Gases
- Class 3: Flammable liquids
- Class 4: Flammable solids, spontaneous combustion, water-reactive
- Class 5: Oxidizers, organic peroxides
- Class 6: Toxic and infectious substances
- Class 7: Radioactive material
- Class 8: Corrosives
- Class 9: Miscellaneous (lithium batteries, asbestos, etc.)

**Relevant AVV codes (hazardous marked with *):**
- Chapter 06: Inorganic chemical process waste
- Chapter 07: Organic chemical process waste
- Chapter 14: Organic solvents, coolants
- 06 01 01* - 06 01 06*: Acids (sulfuric, nitric, hydrochloric, etc.)
- 06 02 01* - 06 02 05*: Bases (sodium hydroxide, ammonia, etc.)
- 07 01 01* - 07 07 99*: Various organic chemical wastes
- 14 06 01* - 14 06 05*: Solvents (halogenated, non-halogenated)

**University lab context:**
- Common lab chemicals: acids, bases, solvents, reagents
- Disposal typically via central collection (Sammelstelle)
- Segregation rules (don't mix acids/bases, halogenated/non-halogenated)
- Small quantities vs. bulk disposal

**German terminology:**
- Gefahrstoff = Hazardous substance
- Sicherheitsdatenblatt (SDB) = Safety Data Sheet (SDS)
- Schadstoffmobil = Mobile hazardous waste collection
- Sammelstelle = Collection point
- GESTIS = German hazardous substance database (IFA/DGUV)

**Known sources to investigate:**
- PubChem (NIH) - largest free chemical database
- ECHA (EU chemicals agency) - REACH/CLP data
- GESTIS (German IFA) - German hazardous substances
- ChemSpider (RSC)
- Sigma-Aldrich/Merck SDS library

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
1. **Direct search:** "chemical database API", "GHS classification database", "SDS database"
2. **German-specific:** "Gefahrstoff Datenbank", "Chemikalien Deutschland"
3. **Scientific:** PubChem, ChemSpider, chemical registries
4. **Regulatory:** ECHA, EPA, national chemical inventories
5. **Academic:** University chemical safety databases
6. **Industry:** Supplier SDS libraries, chemical distributors

**Specific sources to investigate:**
- PubChem (pubchem.ncbi.nlm.nih.gov)
- ECHA C&L Inventory (echa.europa.eu)
- GESTIS Substance Database (gestis.dguv.de)
- ChemSpider (chemspider.com)
- NIST Chemistry WebBook
- Sigma-Aldrich/Merck SDS database
- EPA CompTox Dashboard
- WHO INCHEM

## Output Format

Structure your findings as a Markdown document with:
1. **Executive Summary** - Top 3-5 recommended sources
2. **Detailed Source Profiles** - Full documentation per source (using template above)
3. **Integration Recommendations** - Suggested priority order for ETL development
4. **Gaps Identified** - What data is still missing after this research

Be thorough but prioritize quality over quantity. 5 excellent sources are better than 20 mediocre ones.
