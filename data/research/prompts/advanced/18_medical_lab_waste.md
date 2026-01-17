# Research Agent #18: Medical & Lab Waste

## Agent Configuration
- **Tier:** 4 (Advanced Features)
- **Priority:** 18 of 18
- **Output file:** `data/research/results/18_medical_lab_waste_results.md`

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

We need structured data to populate this graph. Your task is to find data sources for: **Medical & Lab Waste**

## Research Goal

Find APIs and datasets containing:
- Medical waste categories and handling requirements
- Laboratory waste classifications (biological, chemical, radioactive)
- Sharps disposal requirements
- Infectious waste identification and treatment
- Pharmaceutical waste disposal
- Cytotoxic/cytostatic drug waste handling
- Decontamination procedures
- Packaging and labeling requirements

This is highly relevant for a university with health sciences, biology, and chemistry programs.

## Domain-Specific Context

**Medical Waste Categories (LAGA M18 - German Federal Guidelines):**

| Category | German Term | Description | Examples | Disposal |
|----------|-------------|-------------|----------|----------|
| AS 18 01 01 | Spitze/scharfe Gegenstände | Sharps | Needles, scalpels, broken glass | Puncture-resistant containers |
| AS 18 01 02 | Körperteile/Organe | Body parts | Tissue, organs | Special incineration |
| AS 18 01 03* | Infektiöse Abfälle | Infectious waste | Cultures, contaminated materials | Autoclaving or incineration |
| AS 18 01 04 | Nichtinfektiöse Abfälle | Non-infectious | Bandages, gloves, gowns | Restmüll (if not contaminated) |
| AS 18 01 06* | Zytostatika | Cytotoxic drugs | Chemo waste, contaminated materials | High-temp incineration |
| AS 18 01 08* | Arzneimittel (gefährlich) | Hazardous medicines | Cytotoxic, controlled substances | Schadstoffmobil or pharmacy |
| AS 18 01 09 | Arzneimittel | Non-hazardous medicines | Expired pills, syrups | Pharmacy or Restmüll (varies) |

**University Lab Waste Categories:**

### Biological Waste
| Type | Risk Level | Handling |
|------|-----------|----------|
| BSL-1 organisms | Low | Autoclave before disposal |
| BSL-2 organisms | Moderate | Autoclave, special collection |
| BSL-3 organisms | High | On-site decontamination required |
| GMO waste | Variable | Inactivation required (autoclaving, chemical) |
| Animal carcasses | Variable | Special collection, incineration |
| Cell cultures | Low-Moderate | Autoclave or chemical inactivation |

### Chemical Lab Waste
| Type | Handling |
|------|----------|
| Organic solvents (halogenated) | Separate collection, special disposal |
| Organic solvents (non-halogenated) | Separate collection |
| Acids | Neutralization or separate collection |
| Bases | Neutralization or separate collection |
| Heavy metal solutions | Separate collection |
| Oxidizers | Separate collection, no mixing |
| Peroxides | Age-limited, special handling |

### Radioactive Waste (universities with licenses)
| Type | Half-life | Handling |
|------|-----------|----------|
| Short-lived (<100 days) | Days-weeks | Decay storage, then regular disposal |
| Long-lived | Years+ | Licensed disposal facility |
| Mixed (radioactive + chemical) | Variable | Most complex, special procedures |

**Relevant German Regulations:**
- **LAGA M18:** Vollzugshilfe zur Entsorgung von Abfällen aus Einrichtungen des Gesundheitsdienstes
- **BioStoffV:** Biostoffverordnung (biological agents regulation)
- **GefStoffV:** Gefahrstoffverordnung (hazardous substances)
- **StrlSchV:** Strahlenschutzverordnung (radiation protection)
- **AMG:** Arzneimittelgesetz (pharmaceutical law)
- **GenTG:** Gentechnikgesetz (genetic engineering)
- **TRBA:** Technische Regeln für Biologische Arbeitsstoffe

**Relevant AVV Codes (Chapter 18):**

Healthcare waste:
```
18 01 01    Spitze/scharfe Gegenstände (sharps)
18 01 02    Körperteile und Organe
18 01 03*   Infektiöse Abfälle
18 01 04    Nichtinfektiöse Abfälle
18 01 06*   Zytostatika/zytotoxische Arzneimittel
18 01 07    Zytostatika (nicht gefährlich)
18 01 08*   Arzneimittel (gefährlich)
18 01 09    Arzneimittel (nicht gefährlich)
18 01 10*   Amalgamabfälle
```

Veterinary waste (similar structure):
```
18 02 01    Spitze/scharfe Gegenstände
18 02 02*   Infektiöse Abfälle
18 02 03    Nichtinfektiöse Abfälle
18 02 05*   Zytostatika
18 02 06    Zytostatika (nicht gefährlich)
18 02 07*   Arzneimittel (gefährlich)
18 02 08    Arzneimittel (nicht gefährlich)
```

**University-specific considerations:**
- Teaching labs (lower risk, standardized experiments)
- Research labs (variable risk, novel materials)
- Health clinics on campus
- Animal facilities
- Central chemical stores
- Radiation safety office (Strahlenschutzbeauftragter)

**Container requirements:**
- **Yellow containers:** Sharps (puncture-resistant, closable)
- **Red bags/containers:** Infectious waste (autoclaved before collection)
- **Blue containers:** Cytotoxic waste (clearly labeled)
- **Standard waste bags:** Non-infectious medical waste

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
1. **Direct search:** "medical waste classification", "laboratory waste disposal database", "healthcare waste guidelines"
2. **German-specific:** "LAGA M18", "medizinische Abfälle", "Laborabfall Entsorgung"
3. **International:** WHO healthcare waste guidelines, UN guidelines
4. **Scientific:** Biosafety databases, laboratory safety resources
5. **Regulatory:** RKI (Robert Koch Institut), BfArM (medicines), BfS (radiation)

**Specific sources to investigate:**
- LAGA M18 (laga-online.de) - German federal healthcare waste guidelines
- RKI (rki.de) - Robert Koch Institute infection control
- WHO Healthcare Waste Guidelines (who.int)
- DGUV (dguv.de) - Lab safety guidelines
- TRBA documents (BAuA)
- PubChem (pharmaceutical data with disposal info)
- DrugBank (pharmaceutical database)
- BfArM (bfarm.de) - German medicines authority
- ABDA (pharmacy association) - Medicine disposal info
- University biosafety manuals (many are public)

## Output Format

Structure your findings as a Markdown document with:
1. **Executive Summary** - Top 3-5 recommended sources
2. **Detailed Source Profiles** - Full documentation per source (using template above)
3. **Integration Recommendations** - Suggested priority order for ETL development
4. **Gaps Identified** - What data is still missing after this research

Be thorough but prioritize quality over quantity. 5 excellent sources are better than 20 mediocre ones.

**Special focus:**
1. German regulatory sources (LAGA M18 is critical)
2. University lab safety resources
3. Pharmaceutical disposal databases
4. Infectious waste classification criteria
5. Note any sources with structured data (vs. PDF guidelines)
