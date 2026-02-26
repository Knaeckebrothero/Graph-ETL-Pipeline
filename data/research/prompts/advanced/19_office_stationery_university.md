# Research Agent #19: Office Supplies, Stationery & University Products

## Agent Configuration
- **Tier:** 1 (Core Data - High Impact)
- **Priority:** 19 of 19
- **Output file:** `data/research/results/19_office_stationery_university_results.md`

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

We need structured data to populate this graph. Your task is to find data sources for: **Office Supplies, Stationery & University Products**

## Research Goal

Find APIs, datasets, and product catalogs containing:
- Product identification for office/stationery items (names, EAN barcodes, manufacturer part numbers, UNSPSC/eCl@ss codes)
- Material composition (wood, plastic type, metal, ink/toner chemistry, rubber, adhesives)
- Packaging information (blister packs, cardboard, shrink wrap)
- Disposal routes and take-back programs (especially toner/ink cartridges)
- BMEcat-format catalogs or equivalent structured product feeds from office supply distributors
- University or public-sector procurement catalogs with structured product data

This is a critical gap: food products are well-covered by Open Food Facts, and electronics by Icecat, but **no equivalent open database exists for everyday office and stationery products.** These items make up a large fraction of university waste.

## Domain-Specific Context

**Why this category matters for universities:**
Office supplies are the most common non-food waste items on a university campus. Students and staff routinely need to dispose of:
- Pens, pencils, markers, highlighters (plastic + ink/lead/pigment)
- Paper (printer paper, notebooks, sticky notes, envelopes)
- Binders, folders, plastic sleeves (mixed plastic + metal rings)
- Toner and ink cartridges (plastic housing + chemical toner/ink powder)
- Tape, glue sticks, correction fluid (adhesive chemicals)
- Staplers, scissors, paper clips (metal, mixed materials)
- Whiteboards, whiteboard markers (plastic + solvent-based ink)
- Presentation materials (poster boards, laminated paper, foam boards)
- USB drives, peripherals (overlap with electronics — covered separately)

**The "BMEcat problem":**
BMEcat is an XML-based product catalog exchange format widely used in German B2B procurement. It is a **format standard, not a data source** — you need to find suppliers or aggregators that publish catalogs in BMEcat or equivalent formats. Universities typically procure through framework contracts (Rahmenverträge) with distributors like Lyreco, Schäfer Shop, Staples/Office Depot (now Lyreco), or Mercateo. Their product catalogs are the richest source of structured office product data, but access varies.

**Material complexity — conditional disposal logic:**

| Item | Material(s) | Disposal Route | Condition |
|------|-------------|----------------|-----------|
| Wooden pencil | Wood, graphite, paint, rubber (eraser), metal (ferrule) | Restabfalltonne | Always |
| Ballpoint pen | Polypropylene housing, ink (oil-based), metal tip | Verpackungstonne OR Restabfalltonne | Depends on local rules |
| Toner cartridge | ABS/PS plastic, iron oxide toner, OPC drum | Retailer take-back / manufacturer return | Never in regular bins |
| Ring binder | Polypropylene cover, metal rings, paper labels | Separate: metal → Wertstoffhof, plastic → Verpackungstonne | Must disassemble |
| Sticky notes | Paper + acrylate adhesive | Altpapiertonne | If adhesive strip is minor |
| Correction fluid | Solvent (methylcyclohexane/ethanol), titanium dioxide | Schadstoffmobil | If liquid; Restabfalltonne if dried out |
| Laminated paper | Paper + plastic film | Restabfalltonne | Cannot be recycled (composite) |

**Toner and ink cartridges — special case:**
- Germany has producer take-back obligations (§10 VerpackG for packaging, plus voluntary take-back)
- Major programs: HP Planet Partners, Canon Return Program, Brother Return, Xerox Green World Alliance
- Third-party recyclers: Interseroh, Gelbe Tonne Plus (some regions)
- Refill/remanufacture industry (Rebuilt-Toner) — extends product life
- AVV 08 03 17* (hazardous waste ink/toner) vs. 08 03 18 (non-hazardous) — depends on chemical composition

**Relevant AVV codes:**
- 20 01 01: Paper and cardboard
- 20 01 39: Plastics (non-packaging)
- 20 01 40: Metals (non-packaging)
- 15 01 01: Paper/cardboard packaging
- 15 01 02: Plastic packaging
- 15 01 06: Mixed packaging
- 08 03 12*: Waste ink containing dangerous substances
- 08 03 13: Waste ink (non-hazardous)
- 08 03 17*: Waste printing toner containing dangerous substances
- 08 03 18: Waste printing toner (non-hazardous)
- 20 03 01: Mixed municipal waste (Restabfall catch-all)

**German terminology:**
- Büromaterial / Bürobedarf = Office supplies
- Schreibwaren = Stationery (writing instruments)
- Druckerzubehör = Printer accessories (toner, ink, drums)
- Toner-Rücknahme = Toner take-back
- Hochschulbeschaffung = University procurement
- Rahmenvertrag = Framework contract (how universities buy in bulk)
- Vergabestelle = Procurement office
- Blauer Engel = Blue Angel eco-label (certifies environmentally friendly products)
- Wiederaufbereitung = Remanufacturing (rebuilt toner)

**Known classification systems for this domain:**
- **UNSPSC Segment 44** = Office Equipment, Accessories, and Supplies
  - Family 4412 = Office Supplies
  - Class 441217 = Writing Instruments
  - Commodity 44121706 = Wooden Pencils
- **UNSPSC Segment 14** = Paper Materials and Products
- **eCl@ss 24** = Office Materials (Büromaterial)
- **GPC Segment 10005000** = Stationery/Office Machinery/Occasion Supplies

**What already exists in the project:**
- The barcode module (`src/barcode/`) can look up products via Open Food Facts — but OFF has virtually no office supply coverage
- Open Products Facts (openproductsfacts.org) exists as a sibling project but has very sparse data for stationery
- UNSPSC and eCl@ss are researched as taxonomy systems (Agent #12) but not as product data sources
- The graph_import_agent architecture (`docs/graph_import_agent.md`) lists BMEcat as a planned source adapter but has no actual data or URLs

## What We Need From Data Sources

For each source you find, we need data that can map to our schema:
- **Item identification:** Names, synonyms, barcodes, manufacturer part numbers, UNSPSC/eCl@ss codes
- **Material composition:** What is it made of? (plastic type, wood, metal, ink/toner chemistry, adhesives)
- **Disposal classification:** Which bin/stream? Any special handling? Take-back programs?
- **Conditional logic:** State-dependent rules (empty vs. full cartridge, dried-out vs. liquid correction fluid)
- **Hazard indicators:** Solvent content, toner dust classification, GHS codes on ink/correction fluid
- **Eco-labels:** Blauer Engel, EU Ecolabel, FSC/PEFC (paper), Nordic Swan
- **Packaging data:** How the product itself is packaged (blister packs, cardboard, etc.)

## Output Requirements

For EACH data source you find, document:

### 1. Source Identification
- **Name:** Official name of the database/API/dataset
- **URL:** Direct link to the data or documentation
- **Provider:** Organization maintaining it (government, NGO, company, community)

### 2. Data Format & Access
- **Format:** API (REST/GraphQL), JSON, CSV, XML (BMEcat/ETIM), Excel, PDF, web scraping required
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
- **ID matching:** Can we link to other sources (barcodes, UNSPSC codes, eCl@ss IDs, Wikidata)?
- **Technical considerations:** API quirks, pagination, authentication, BMEcat version differences, data cleaning needed
- **Example query:** If API, provide a sample request

### 7. Similar/Alternative Sources
- List any related sources you found that might be alternatives or supplements

## Search Strategy

Use these approaches to find sources:

1. **Product catalogs (primary):**
   - "BMEcat catalog download", "BMEcat Büromaterial", "BMEcat office supplies XML"
   - "Lyreco product data API", "Schäfer Shop product feed", "Mercateo API"
   - "office supply product catalog API", "stationery product database"

2. **Open data / community:**
   - "Open Products Facts office supplies", "Wikidata office supplies"
   - "office products open data", "stationery barcode database"
   - openproductsfacts.org coverage for stationery

3. **German-specific:**
   - "Bürobedarf Datenbank", "Büromaterial Produktdaten"
   - "Kaufhaus des Bundes Produktkatalog" (federal procurement portal)
   - "Hochschulbeschaffung Rahmenvertrag Büromaterial"
   - "Blauer Engel Produktdatenbank Büro" (eco-labeled office products)

4. **Procurement and classification:**
   - "UNSPSC segment 44 download", "eCl@ss 24 Büromaterial"
   - "GPC stationery classification"
   - "university procurement catalog structured data"
   - German e-procurement portals (cosinex, Vergabe24)

5. **Toner/ink specific:**
   - "toner cartridge database", "printer cartridge compatibility database"
   - "HP Planet Partners API", "Canon cartridge return program data"
   - "rebuilt toner database", "Interseroh toner recycling"
   - "printer consumables product data"

6. **Industry associations:**
   - PBS (Papier-, Büro- und Schreibwaren) industry association
   - German Stationery Trade Association
   - European Writing Instruments Manufacturers Association (EWIMA)
   - Office product trade fairs (Insights-X, Paperworld)

7. **Eco-labels and sustainability:**
   - "Blauer Engel Büroartikel", "EU Ecolabel office products"
   - "FSC certified paper database", "PEFC paper products"
   - "Recycling-Fähigkeit Büromaterial"

**Specific sources to investigate:**
- Mercateo / Unite (B2B marketplace with product data)
- Lyreco / Staples catalog data
- Schäfer Shop / OTTO Office product feeds
- Kaufhaus des Bundes (federal procurement, may have UNSPSC-coded catalogs)
- Blauer Engel product database (blauer-engel.de/produktwelt)
- Open Products Facts (openproductsfacts.org)
- Wikidata — office supply items (P31 = "writing instrument", etc.)
- eCl@ss free search (eclass.eu)
- EHI Retail Institute (retail product data standards)
- GS1 Germany (GTIN/barcode registry for stationery)

## Output Format

Structure your findings as a Markdown document with:
1. **Executive Summary** - Top 3-5 recommended sources
2. **Detailed Source Profiles** - Full documentation per source (using template above)
3. **Integration Recommendations** - Suggested priority order for ETL development
4. **Gaps Identified** - What data is still missing after this research

Be thorough but prioritize quality over quantity. 5 excellent sources are better than 20 mediocre ones.

**Special focus areas:**
1. **BMEcat catalogs** — Can we actually get one? Who publishes them openly? Is there a demo/sample catalog we can use for development?
2. **Toner/ink cartridge data** — This is the highest-value subset (hazardous waste rules, take-back programs, high disposal complexity). Find the best structured source.
3. **Blauer Engel database** — This is a German government-backed eco-label with a public product database. Check if it has API access or bulk download and what fields are available.
4. **University procurement portals** — Do any German Hochschulen publish their procurement catalogs as open data?
5. **Material composition** — The hardest data to find. Most catalogs have product names and prices but NOT what the product is made of. Identify which sources actually have material/composition data.
