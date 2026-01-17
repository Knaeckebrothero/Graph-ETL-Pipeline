# Research Agent #17: Transport/ADR Regulations

## Agent Configuration
- **Tier:** 4 (Advanced Features)
- **Priority:** 17 of 18
- **Output file:** `data/research/results/17_transport_adr_regulations_results.md`

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

We need structured data to populate this graph. Your task is to find data sources for: **Transport/ADR Regulations**

## Research Goal

Find APIs and datasets containing:
- UN numbers for dangerous goods
- ADR (road transport) classifications
- Proper shipping names
- Packaging requirements and approved containers
- Labeling requirements (placards, marks)
- Quantity limits (Kleinmengenregelung for private transport)
- Documentation requirements
- Exemptions for small quantities

This helps users safely transport waste to disposal facilities.

## Domain-Specific Context

**What is ADR?**
ADR = Accord européen relatif au transport international des marchandises Dangereuses par Route (European Agreement concerning the International Carriage of Dangerous Goods by Road)

Updated every 2 years (ADR 2025 is current).

**ADR Classes:**

| Class | Description | Examples |
|-------|-------------|----------|
| 1 | Explosives | Fireworks, ammunition |
| 2 | Gases | Aerosol cans, propane, CO2 |
| 3 | Flammable liquids | Petrol, solvents, paints |
| 4.1 | Flammable solids | Matches, sulfur |
| 4.2 | Spontaneously combustible | Oily rags, some metal powders |
| 4.3 | Water-reactive | Sodium, lithium metal |
| 5.1 | Oxidizing substances | Bleach, pool chemicals |
| 5.2 | Organic peroxides | Some adhesives, catalysts |
| 6.1 | Toxic substances | Pesticides, some chemicals |
| 6.2 | Infectious substances | Medical waste |
| 7 | Radioactive material | Smoke detectors (Am-241) |
| 8 | Corrosive substances | Acids, bases, batteries |
| 9 | Miscellaneous | Lithium batteries, asbestos, dry ice |

**UN Numbers (examples relevant to waste):**

| UN # | Name | Class | Common Waste Form |
|------|------|-------|-------------------|
| UN1133 | Adhesives | 3 | Old glue, contact cement |
| UN1170 | Ethanol | 3 | Lab alcohol, hand sanitizer |
| UN1193 | Ethyl methyl ketone | 3 | Nail polish remover |
| UN1263 | Paint | 3 | Leftover paint |
| UN1593 | Dichloromethane | 6.1 | Paint stripper |
| UN1760 | Corrosive liquid | 8 | Drain cleaner, acids |
| UN1824 | Sodium hydroxide solution | 8 | Drain cleaner (lye) |
| UN1950 | Aerosols | 2 | Spray cans |
| UN2794 | Lead-acid batteries | 8 | Car batteries |
| UN2795 | Lead-acid batteries (filled) | 8 | Car batteries |
| UN3077 | Environmentally hazardous solid | 9 | Some pesticides |
| UN3082 | Environmentally hazardous liquid | 9 | Some chemicals |
| UN3090 | Lithium metal batteries | 9 | Button cells |
| UN3091 | Lithium metal batteries in equipment | 9 | Devices with Li batteries |
| UN3480 | Lithium ion batteries | 9 | Phone/laptop batteries |
| UN3481 | Lithium ion batteries in equipment | 9 | Old laptops, phones |

**Kleinmengenregelung (Small Quantity Exemption):**
Private individuals can transport limited quantities without full ADR compliance:
- Per transport unit (car): Limited Quantities (LQ) allowed
- Labeling simplified
- No dangerous goods license needed
- Packaging must still be safe

**Key data points per substance:**
1. UN number
2. Proper shipping name (official name)
3. Class and subsidiary risks
4. Packing group (I = high danger, II = medium, III = low)
5. Labels required
6. Special provisions
7. Limited quantities allowed
8. Packaging instructions
9. Transport category (for calculating total limits)

**Why this matters for our chatbot:**
- User: "Can I drive old paint cans to the Wertstoffhof?"
- Bot: "Yes, but seal containers, transport upright, limit X liters per trip"

**German-specific considerations:**
- GGVSEB (Gefahrgutverordnung Straße, Eisenbahn und Binnenschifffahrt)
- DGUV guidelines for workplace transport
- BG (Berufsgenossenschaft) regulations

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
1. **Direct search:** "UN number database", "ADR dangerous goods list", "hazardous materials transport"
2. **Official:** UNECE (ADR maintainer), national transport authorities
3. **German-specific:** BAM (Bundesanstalt für Materialforschung), DGUV
4. **Industry:** Dangerous goods software, logistics companies
5. **Safety:** Chemical safety databases with transport info

**Specific sources to investigate:**
- UNECE ADR (unece.org/transport/dangerous-goods) - Official source
- ADR-Tool (adr-tool.com) - Web lookup tool
- GESTIS (transport information section)
- PubChem (transport classification section)
- ECHA (transport classification)
- ERICards (Emergency Response Intervention Cards)
- CHEMSAFE (BAM database)
- DG Lookup tools (various commercial)
- EPA DOT database (US, but UN numbers are international)

## Output Format

Structure your findings as a Markdown document with:
1. **Executive Summary** - Top 3-5 recommended sources
2. **Detailed Source Profiles** - Full documentation per source (using template above)
3. **Integration Recommendations** - Suggested priority order for ETL development
4. **Gaps Identified** - What data is still missing after this research

Be thorough but prioritize quality over quantity. 5 excellent sources are better than 20 mediocre ones.

**Special focus:**
1. Find sources with structured/searchable UN number data (not just PDF)
2. Look for small quantity exemption information
3. Note sources with German-language content
4. Identify cross-references to GHS/CAS numbers for linking
