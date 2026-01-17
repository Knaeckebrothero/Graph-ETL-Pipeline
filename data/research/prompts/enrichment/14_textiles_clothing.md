# Research Agent #14: Textiles & Clothing

## Agent Configuration
- **Tier:** 3 (Enrichment - Nice to Have)
- **Priority:** 14 of 18
- **Output file:** `data/research/results/14_textiles_clothing_results.md`

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

We need structured data to populate this graph. Your task is to find data sources for: **Textiles & Clothing**

## Research Goal

Find APIs and datasets containing:
- Fiber composition (cotton, polyester, wool, blends)
- Care label codes and symbols
- Recyclability by fiber type
- Condition assessment (reusable vs. waste)
- Textile categories (clothing, household textiles, technical textiles)
- Brand sustainability/take-back programs

Textiles are increasingly important in waste management due to EU regulations on textile waste.

## Domain-Specific Context

**Textile Fiber Types:**

**Natural Fibers (plant-based):**
| Fiber | Properties | Recyclability |
|-------|------------|---------------|
| Cotton | Breathable, absorbent | Mechanically recyclable, biodegradable |
| Linen (Flax) | Strong, cool | Recyclable, biodegradable |
| Hemp | Durable, eco-friendly | Recyclable, biodegradable |
| Jute | Coarse, cheap | Limited use, biodegradable |

**Natural Fibers (animal-based):**
| Fiber | Properties | Recyclability |
|-------|------------|---------------|
| Wool | Warm, resilient | Mechanically recyclable |
| Silk | Luxurious, delicate | Limited recycling |
| Cashmere | Soft, expensive | Limited recycling |
| Leather | Durable, not a fiber | Special recycling |

**Synthetic Fibers (petroleum-based):**
| Fiber | Properties | Recyclability |
|-------|------------|---------------|
| Polyester (PET) | Durable, wrinkle-resistant | Chemically recyclable to PET |
| Nylon (Polyamide) | Strong, elastic | Chemically recyclable |
| Acrylic | Wool-like, cheap | Difficult to recycle |
| Spandex/Elastane | Stretchy | Not recyclable |
| Polypropylene | Moisture-wicking | Limited recycling |

**Blends (the problem):**
- Cotton/Polyester blends are extremely common (60/40, 50/50)
- Blends require chemical separation or are downcycled
- Pure materials are much easier to recycle

**Care Label Symbols (ISO 3758):**
Care labels contain fiber content (legally required) and care instructions.
- Fiber content: "60% Cotton, 40% Polyester"
- Washing, bleaching, drying, ironing, professional care symbols

**Textile Disposal Routes:**

| Condition | Route | Notes |
|-----------|-------|-------|
| Wearable, good condition | Altkleidercontainer, donation | Second-hand market |
| Damaged but clean | Altkleidercontainer | Recycled to rags, insulation |
| Wet, moldy, contaminated | Restmüll | Cannot be recycled |
| Work uniforms with logos | Some retailers take back | Or remove logos first |
| Shoes (pairs) | Altkleidercontainer | Tie together |
| Single shoes, broken | Restmüll | No recycling value |

**EU Textile Strategy (2022):**
- Extended Producer Responsibility for textiles by 2025
- Separate collection mandatory by 2025
- Eco-design requirements coming
- Digital Product Passport for textiles planned

**Relevant AVV Codes:**
- 20 01 10: Clothes
- 20 01 11: Textiles
- 04 02 09: Waste from composite materials (textiles + plastic)
- 04 02 21: Waste from unprocessed textile fibers
- 04 02 22: Waste from processed textile fibers
- 15 01 09: Textile packaging
- 19 12 08: Textiles from waste treatment

**German textile collection:**
- Altkleidercontainer (street containers) - charities, commercial collectors
- Wertstoffhof - some accept textiles
- Retailer take-back - H&M, Zara, etc. have programs
- DRK, Caritas, Oxfam shops

**NIST NIR-SORT (new 2025):**
National Institute of Standards and Technology released molecular fingerprint data for automated textile sorting in 2025.

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
1. **Direct search:** "textile fiber database", "clothing materials API", "fabric composition"
2. **Standards:** ISO 2076 (fiber names), ISO 3758 (care labeling), ASTM textile standards
3. **Sustainability:** Fashion sustainability databases, textile recycling research
4. **Industry:** Textile industry associations, fashion brands with transparency
5. **Scientific:** Textile research databases, fiber identification

**Specific sources to investigate:**
- NIST NIR-SORT (2025 molecular fingerprint data)
- ISO 2076 textile fiber generic names
- Open Apparel Registry (fashion supply chain)
- Fashion Revolution Transparency Index
- Textile Exchange (fiber data)
- OEKO-TEX certification database
- GOTS (Global Organic Textile Standard)
- Higg Index (sustainability)
- Good On You (brand ratings)
- Wikidata textile fiber properties

## Output Format

Structure your findings as a Markdown document with:
1. **Executive Summary** - Top 3-5 recommended sources
2. **Detailed Source Profiles** - Full documentation per source (using template above)
3. **Integration Recommendations** - Suggested priority order for ETL development
4. **Gaps Identified** - What data is still missing after this research

Be thorough but prioritize quality over quantity. 5 excellent sources are better than 20 mediocre ones.

**Special focus:**
1. Fiber composition databases - what materials are items made of?
2. Recyclability by fiber type - which fibers can be recycled and how?
3. Care label decoding - mapping symbols/codes to fiber content
