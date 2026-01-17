# Research Agent #12: Universal Taxonomies

## Agent Configuration
- **Tier:** 3 (Enrichment - Nice to Have)
- **Priority:** 12 of 18
- **Output file:** `data/research/results/12_universal_taxonomies_results.md`

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

We need structured data to populate this graph. Your task is to find data sources for: **Universal Taxonomies**

## Research Goal

Find standardized classification systems containing:
- Product category hierarchies (what type of thing is this?)
- Material taxonomies (what is it made of?)
- Cross-reference mappings between systems
- Standardized product identifiers
- Industry classification codes

These taxonomies help map user queries to known waste categories and enable data integration across sources.

## Domain-Specific Context

**Why taxonomies matter:**
When a user says "I have an old printer," we need to:
1. Recognize "printer" as a product category
2. Map it to "office equipment" → "electronics" → "WEEE Category 6"
3. Identify typical materials (plastic, metal, PCB, toner)
4. Determine disposal route (Wertstoffhof, electronics retailer)

Taxonomies provide the "brain" that connects everyday terms to disposal logic.

**Key Classification Systems:**

### UNSPSC (United Nations Standard Products and Services Code)
- **Coverage:** 157,000+ product/service categories
- **Structure:** 4-level hierarchy (Segment → Family → Class → Commodity)
- **Example:**
  ```
  43 = Information Technology
    43 21 = Computer Equipment
      43 21 21 = Computer printers
        43 21 21 05 = Laser printers
  ```
- **Use case:** Map product names to categories → derive material/disposal info

### GPC (GS1 Global Product Classification)
- **Coverage:** Global, barcode-linked
- **Structure:** Segment → Family → Class → Brick → Attributes
- **Maintained by:** GS1 (the barcode people)
- **Use case:** Link barcodes to product categories

### ECLASS
- **Coverage:** 45,000+ classes, German industry standard
- **Structure:** Hierarchical with detailed properties
- **Used by:** Siemens, BMW, SAP, German government (Kaufhaus des Bundes)
- **Properties include:** REACH/RoHS compliance, hazardous materials
- **Use case:** Industrial products with safety/material properties

### HS Codes (Harmonized System)
- **Coverage:** International trade classification
- **Structure:** 6-digit codes (countries add more)
- **Maintained by:** World Customs Organization
- **Use case:** Material identification from trade descriptions

### CPC (Central Product Classification)
- **Coverage:** UN statistical classification
- **Structure:** Hierarchical product taxonomy
- **Use case:** Statistical reference, cross-mapping

### NACE/WZ (Economic Activity Classification)
- **Coverage:** Industries, not products
- **Structure:** EU standard (NACE), German version (WZ)
- **Use case:** Identify waste-generating industries

**Taxonomy Integration Strategy:**
```
User Query: "old laser printer"
       │
       ▼
┌─────────────────────────────┐
│  NLP Entity Recognition     │
│  "laser printer" detected   │
└─────────────────────────────┘
       │
       ▼
┌─────────────────────────────┐
│  UNSPSC Lookup              │
│  43-21-21-05 = Laser printer│
└─────────────────────────────┘
       │
       ▼
┌─────────────────────────────┐
│  Category Mapping           │
│  → Electronics              │
│  → WEEE Category 6          │
│  → Contains: toner, PCB     │
└─────────────────────────────┘
       │
       ▼
┌─────────────────────────────┐
│  Disposal Logic             │
│  → Wertstoffhof (free)      │
│  → Electronics retailer     │
│  → Remove toner first       │
└─────────────────────────────┘
```

**Cross-reference value:**
Many systems map to each other:
- UNSPSC ↔ GPC (product categories)
- HS ↔ UNSPSC (trade to product)
- ECLASS ↔ UNSPSC (German industry)
- Wikidata links to most systems

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
1. **Direct search:** "product classification system", "product taxonomy database", "UNSPSC download"
2. **Standards bodies:** UN, GS1, ISO, DIN
3. **E-procurement:** Government procurement classifications
4. **Industry:** Trade associations, B2B data standards
5. **Open data:** Wikidata, DBpedia, schema.org

**Specific sources to investigate:**
- UNSPSC (unspsc.org, undp.org) - Free PDF/Excel download
- GPC Browser (gpc-browser.gs1.org) - Free browsing
- ECLASS (eclass.eu) - Licensed, but search is free
- HS Codes (wcoomd.org, zolltarifnummern.de)
- Wikidata taxonomy properties (P31, P279, P361)
- Schema.org Product types
- Amazon Product Taxonomy (browse tree)
- eBay Category API
- Google Product Taxonomy

## Output Format

Structure your findings as a Markdown document with:
1. **Executive Summary** - Top 3-5 recommended sources
2. **Detailed Source Profiles** - Full documentation per source (using template above)
3. **Integration Recommendations** - Suggested priority order for ETL development
4. **Gaps Identified** - What data is still missing after this research

Be thorough but prioritize quality over quantity. 5 excellent sources are better than 20 mediocre ones.

**Special focus:**
1. UNSPSC is likely most valuable - find the best way to download/access the full hierarchy
2. Look for cross-reference tables between systems
3. Note which systems are free vs. licensed
