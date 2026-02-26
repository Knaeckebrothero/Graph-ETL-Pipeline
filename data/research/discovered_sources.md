# Discovered Data Sources

Consolidated list of data sources identified from research reports. This is an actionable quick-reference for ETL implementation.

**Last Updated:** 2026-02-26

---

## Report 01: Food & Grocery Products

### Primary Sources

| Source | URL | Format | License | Relevance | Coverage |
|--------|-----|--------|---------|-----------|----------|
| **Open Food Facts** | https://world.openfoodfacts.org/data | JSON API, MongoDB Dump, JSONL | ODbL | 5/5 | 3M+ global, 800k+ German |
| **ZSVR Mindeststandard** | https://www.verpackungsregister.org | PDF | Free | 5/5 | German recyclability rules |
| **BSR Abfall-ABC** | https://daten.berlin.de/datensaetze/bsr-recyclingh-fe-und-abfall-abc | JSON, CSV | CC BY | 4/5 | Berlin waste categories |
| **Blue Angel (Blauer Engel)** | https://www.blauer-engel.de/en/trade/e-commerce | CSV/JSON | Free (registration) | 3/5 | Eco-certified products |

### Secondary/Verification Sources

| Source | URL | Purpose |
|--------|-----|---------|
| GS1 GEPIR | https://www.gs1us.org/tools/gs1-company-database-gepir | GTIN verification, brand lookup |
| LUCID Register | https://www.verpackungsregister.org/en/foundation-authority/public-registers | Producer compliance check |
| dm-drogerie markt | (website scraping) | Cosmetics, cleaning agents |

### Key Integration Notes

**Open Food Facts:**
- Use MongoDB dump for bulk load (avoid API rate limits: 100 req/min)
- Filter: `countries_tags:en:germany`
- Key fields: `code`, `packaging_tags`, `ecoscore_grade`, `ecoscore_data.adjustments.packaging`
- Taxonomy normalization needed: `en:plastic-bottle` → `Material:Plastic + Shape:Bottle`

**BSR Abfall-ABC:**
- Gold standard for German waste categories
- Use for ontology construction (WasteCategory class hierarchy)
- Note: Berlin uses Wertstofftonne (accepts non-packaging plastics) vs. other cities using Gelber Sack

**ZSVR:**
- Requires manual knowledge engineering (PDF → DisposalRule nodes)
- Key rules: composite thresholds (>5% non-paper), black plastics detection

---

## Report 02: Consumer Electronics

### Primary Sources

| Source | URL | Format | License | Relevance | Coverage |
|--------|-----|--------|---------|-----------|----------|
| **Stiftung EAR** | https://www.stiftung-ear.de/en/lists/ | CSV | Public | 5/5 | German ElektroG taxonomy, producer registry |
| **Icecat Open Catalog** | https://icecat.biz/ | XML/JSON API | Free tier | 5/5 | IT/CE product specs (dimensions, battery) |
| **EPREL** | https://eprel.ec.europa.eu/ | JSON API | Public (QES for bulk) | 4/5 | Monitors, TVs, lighting, appliances |
| **ECHA SCIP Database** | https://echa.europa.eu/scip-database | IUCLID (XML) | Public | 4/5 | Hazardous substances (SVHC) in products |
| **iFixit API** | https://www.ifixit.com/api/2.0/doc | JSON API | Free | 4/5 | Teardowns, battery removability, components |
| **LAGA Mitteilung 31** | https://www.laga-online.de/ | PDF (encode as rules) | Public | 5/5 | AVV waste code logic for electronics |

### Secondary/Verification Sources

| Source | URL | Purpose |
|--------|-----|---------|
| Open Products Facts | https://world.openproductsfacts.org/ | Fallback, packaging info |
| Open Repair Alliance (ORDS) | https://standard.openrepair.org/ | Repair statistics, failure modes |
| EPBA Guidelines | https://www.epbaeurope.net/ | Battery chemistry codes, marking |
| TCO Certified | https://industry.tcocertified.com/gtin-api/ | IT sustainability certification |
| GS1 Digital Link | https://www.gs1.org/ | Future: Digital Product Passport |

### Key Integration Notes

**Stiftung EAR:**
- Foundational source for "Logic & Rules" layer
- Defines Sammelgruppen (collection groups 1-6) based on dimensions and device type
- B2B vs B2C distinction critical for university context
- Publishes First Treatment Facilities (EBA) list

**Icecat:**
- Primary source for physical attributes required by ElektroG
- Key fields: `Height`, `Width`, `Depth`, `Battery technology`
- Use bulk XML download + Live API hybrid approach
- Mapping: `Battery technology` → `hasBatteryChemistry`, `Display diagonal` → `screenSize`

**EPREL:**
- "Truth Source" for Category 2 (Screens >100cm²) and Category 3 (Lamps)
- Contains `visibleScreenArea`, `lightSourceTechnology`
- Requires QES (Qualified Electronic Seal) for bulk API access
- Apply as "Public Institution" or "Researcher"

**iFixit:**
- Proxy for battery removability (BattG compliance)
- Logic: IF difficulty="Low/Moderate" → "Remove battery"; IF "Very Hard" → "Dispose whole unit"
- Use `parts` array to set flags: `hasRemovableBattery`, `containsGlass`

**LAGA Mitteilung 31:**
- Cannot be downloaded as database - must encode as SHACL/SWRL rules
- Conditional logic: Component + State → AVV Code
- Example: `IF Part=PCB AND HasCapacitor(>25mm) → 16 02 15* (Hazardous)`

**ECHA SCIP:**
- Links products to REACH chemical hazards
- Strategy: Extract "Risk Map" for CE categories, don't query real-time
- Map Candidate List Substance → AVV Hazard Trigger

### ElektroG Sammelgruppen (Collection Groups)

| Group | Description | Size Criterion |
|-------|-------------|----------------|
| 1 | Heat exchangers (fridges, AC) | - |
| 2 | Screens/monitors | Surface >100cm² |
| 3 | Lamps | - |
| 4 | Large equipment | Any dimension >50cm |
| 5 | Small equipment | No dimension >50cm |
| 6 | Small IT/telecom | No dimension >50cm |

### API Quick Reference

```bash
# Icecat - Get product by GTIN or Brand+Code
curl "https://live.icecat.biz/api?lang=de&ProductCode=XYZ&Brand=Samsung"

# Open Products Facts - Get product by barcode
curl "https://world.openproductsfacts.org/api/v2/product/[barcode].json"

# iFixit - Get teardown guide
curl "https://www.ifixit.com/api/2.0/guides/[id]"
```

---

## Report 03: Chemicals & Hazardous

### Primary Sources: Chemical Identity

| Source | URL | Format | License | Relevance | Coverage |
|--------|-----|--------|---------|-----------|----------|
| **PubChem PUG REST** | https://pubchem.ncbi.nlm.nih.gov/rest/pug | JSON API | Open (5 req/s) | 5/5 | 111M+ compounds, GHS hazards, synonyms |
| **CAS Common Chemistry** | https://commonchemistry.cas.org/ | JSON API | Free (no bulk) | 4/5 | 500k substances, CAS validation |
| **GESTIS Substance DB** | https://gestis-database.dguv.de/ | Web (no API) | Link only | 5/5 | 8,800 substances, storage classes, TRGS |

### Primary Sources: EU Regulatory

| Source | URL | Format | License | Relevance | Coverage |
|--------|-----|--------|---------|-----------|----------|
| **ECHA C&L Inventory** | https://data.europa.eu/data/datasets/echa-classification-and-labelling-inventory | XML/CSV bulk | Public | 5/5 | Harmonised + notified GHS classifications |
| **ECHA CHEM / REACH** | https://echa.europa.eu/ | Bulk download | Public | 4/5 | Use descriptors, PROC/ERC categories |
| **GHS H/P Statements** | https://github.com/mhchem/hpstatements | JSON | Open | 4/5 | H/P codes in 20+ languages |

### Primary Sources: US Regulatory

| Source | URL | Format | License | Relevance | Coverage |
|--------|-----|--------|---------|-----------|----------|
| **EPA CompTox / CPDat** | https://www.epa.gov/comptox-tools/ | SQL/CSV | Public | 4/5 | Chemical-to-product mappings |
| **EPA List of Lists** | https://www.epa.gov/epcra/consolidated-list-lists | Excel/CSV | Public | 5/5 | RCRA codes, CERCLA RQs, CAA 112(r) |
| **California DTSC Cortese** | https://calepa.ca.gov/sitecleanup/corteselist/ | Excel | Public | 3/5 | California-specific hazardous waste |

### Primary Sources: Dangerous Goods Transport

| Source | URL | Format | License | Relevance | Coverage |
|--------|-----|--------|---------|-----------|----------|
| **BAM GEFAHRGUT** | https://tes.bam.de/TES/Navigation/EN/DGG-Database/Products/ | Excel/CSV | **FREE (July 2025)** | 5/5 | UN numbers, ADR/RID/IMDG, 4,500 BAM entries |
| **MachShip DG Lookup** | https://developers.live.machship.com/api/faqs/dg-lookup-by-un | JSON API | Free | 3/5 | Quick UN number lookup |

### Primary Sources: Waste Codes

| Source | URL | Format | License | Relevance | Coverage |
|--------|-----|--------|---------|-----------|----------|
| **EEA EWC/LoW List** | https://www.eea.europa.eu/ | CSV/XLS | Public | 5/5 | European Waste Catalogue codes |
| **UK WM3 Guidance** | SEPA/Environment Agency | PDF/Excel | Public | 4/5 | Mirror entry threshold calculations |

### Secondary/Verification Sources

| Source | URL | Purpose |
|--------|-----|---------|
| UPS Dangerous Goods API | https://developer.ups.com/api/reference | Shipping acceptance validation (commercial) |
| FedEx Ship API | https://developer.fedex.com/api/ | Dangerous goods validation (commercial) |
| ETH Zürich Sonderabfall | https://ethz.ch/staffnet/ | Lab chemical disposal mappings |
| LMU Munich Entsorgung | (PDF lists) | Chemical → disposal canister mappings |
| UMN Chemical Waste Registry | https://hsrm.umn.edu/ | Chemical → hazard class (01-18) + EPA codes |
| SDSParser | https://github.com/astepe/sds_parser | Python: SDS Section 13 extraction |

### Key Integration Notes

**PubChem:**
- Primary identity resolution engine: trade names → canonical names → CID
- Use for GHS classification aggregation (H-codes, P-codes, signal words)
- Rate limit: 5 req/s - implement token bucket throttling
- Endpoint pattern: `https://pubchem.ncbi.nlm.nih.gov/rest/pug/<input>/<operation>/<output>`
- Key endpoints:
  - `/compound/name/{name}/synonyms/JSON` - synonym resolution
  - `/compound/cid/{cid}/property/FlashPoint,BoilingPoint/JSON` - physical properties
  - `/compound/cid/{cid}/classification/JSON` - GHS hazard data
- **Limitation:** Aggregator, not regulatory authority - cross-reference with ECHA/EPA for compliance

**GESTIS:**
- Gold standard for German occupational safety (TRGS compliance)
- **No scraping allowed** - use deep linking only
- Store links: `https://gestis-database.dguv.de/data?name={ZVG_NUMBER}`
- Key data: Storage Class (Lagerklasse) - critical for waste segregation
- ZVG Number is the unique identifier

**ECHA C&L Inventory:**
- No real-time API - batch ingest from EU Open Data Portal
- **Harmonised (Annex VI)** = legally binding, use as ground truth
- **Notified** = industry-submitted, implement confidence scoring (>90% agreement = definitive)
- Mirror entries: same substance may be hazardous OR non-hazardous based on concentration

**BAM GEFAHRGUT (Major Win!):**
- Transitioned from paid → FREE in July 2025
- Replaces need for ADR PDF parsing or proprietary logistics software
- Two systems:
  1. **UN Number System** - Class, Packing Group, Tunnel Code, LQ thresholds
  2. **BAM Number System** - 4,500 substances not in UN list (for n.o.s. entries)
- Download: Excel/CSV (tab-separated)

**EPA RCRA Lists:**
- **P-List** = Acutely hazardous (pure chemicals) - CAS → P-code (1:1 mapping)
- **U-List** = Toxic (pure chemicals) - CAS → U-code (1:1 mapping)
- **F-List/K-List** = Process-based waste - requires modeling `Process → generates → Waste(F006)`
- Example: Acrolein (CAS 107-02-8) → P003

**EWC Mirror Entries:**
- Challenge: Same waste has hazardous (*) vs non-hazardous code
- Example: `17 04 09*` (contaminated metal) vs `17 04 11` (clean metal)
- Logic: Concentration thresholds (e.g., >0.1% carcinogens)
- Graph must support **Conditional Edges** with threshold properties

**University Gold Standards:**
- ETH Zürich, LMU Munich, UMN publish curated chemical → waste code mappings
- Use as training data for inference engine
- Strategy: Normalize to `Synonym → EWC Code` association table

**SDS Parsing (Section 13):**
- Use SDSParser library for PDF extraction
- Target: Explicit waste codes (regex: `[0-9]{2} [0-9]{2} [0-9]{2}\*?`)
- Target: Disposal methods (keywords: "Incineration", "Neutralization", "Landfill")
- Implement **Reliability Score**: Regulatory list > SDS > Inferred

### Graph Schema Recommendations

```
Chemical (InChIKey) --[has_identifier]--> CAS_RN
Chemical --[regulated_by]--> Regulation_Entry
Regulation_Entry --[assigns_code]--> Waste_Code
Chemical --[transported_as]--> UN_Number
UN_Number --[requires_packing]--> Packing_Instruction
```

- **Primary Key:** InChIKey (structure-based, open standard)
- **Secondary Keys:** CAS RN, EC Number, UN Number (searchable properties)

### API Quick Reference

```bash
# PubChem - Resolve synonym to canonical name
curl "https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/name/Muriatic%20Acid/synonyms/JSON"

# PubChem - Get GHS classification
curl "https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/cid/313/classification/JSON"

# PubChem - Get physical properties
curl "https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/cid/180/property/MolecularFormula,FlashPoint,BoilingPoint/JSON"

# CAS Common Chemistry - Validate CAS number
curl "https://commonchemistry.cas.org/api/detail?cas_rn=67-64-1"

# MachShip - UN number lookup
curl "https://developers.live.machship.com/api/faqs/dg-lookup-by-un?unNumber=1090"
```

---

## Report 04: German Municipal Rules

### Key Finding: The "SaaS Oligopoly"

German municipal waste data is fragmented across 400+ districts, but **consolidated under 3 dominant SaaS platforms**. By integrating with these, Fessi can cover most university towns without custom connectors.

### Primary Sources: Collection Schedule APIs

| Source | URL | Format | License | Relevance | Coverage |
|--------|-----|--------|---------|-----------|----------|
| **Abfall+ / Abfall.io** | https://api.abfallplus.de | JSON API | Undocumented (public) | 5/5 | Market leader, southern/western Germany |
| **MyMüll.de / JumoMind** | https://mymuell.jumomind.com/mmapp/api.php | REST JSON | Undocumented (public) | 5/5 | NRW, Hesse, western Germany |
| **Müllmax** | (municipal web portals) | HTML → ICS | Scraping required | 3/5 | Legacy system, large cities |

### Primary Sources: Open Data Portals

| Source | URL | Format | License | Relevance | Coverage |
|--------|-----|--------|---------|-----------|----------|
| **Berlin BSR Open Data** | https://daten.berlin.de/datensaetze/bsr-recyclingh-fe-und-abfall-abc | JSON | CC BY | 5/5 | 2,000+ items, Vermeidungstipps, Verwertungstipps |
| **Munich AWM Wertstoffhöfe** | https://opendata.muenchen.de/dataset/awm_wertstoffhoefe_opendata | WFS/GeoJSON | Public | 5/5 | GPS, hours, accepted fractions |
| **Aachen Schadstoffmobil** | https://offenedaten.aachen.de/dataset/schadstoffmobil-stadt-aachen | GeoJSON | Public | 4/5 | Mobile hazardous collection schedule |

### Primary Sources: Regulatory/Classification

| Source | URL | Format | License | Relevance | Coverage |
|--------|-----|--------|---------|-----------|----------|
| **AVV (Abfallverzeichnis-Verordnung)** | https://www.gesetze-im-internet.de/avv/AVV.pdf | PDF | Public | 5/5 | German EWC implementation, 6-digit codes |
| **Destatis / Statistical Offices** | https://www.statistik.sachsen.de/ | Excel/CSV | Public | 4/5 | AVV as structured data |
| **WikiWaste UK** | https://wikiwaste.org.uk/index.php/European_Waste_Catalogue | HTML | Public | 3/5 | EWC codes in English (for bootstrap) |
| **CSG EWC Tool** | https://www.csg.co.uk/resources/ewc-codes | HTML/API | Free | 3/5 | EWC code lookup |

### Primary Sources: Community Configuration

| Source | URL | Format | License | Relevance | Coverage |
|--------|-----|--------|---------|-----------|----------|
| **hacs_waste_collection_schedule** | https://github.com/mampfes/hacs_waste_collection_schedule | Python | MIT | 5/5 | Pre-configured (key, kommune_id) for 100s of cities |
| **OpenStreetMap** | https://www.openstreetmap.org/ | Overpass API | ODbL | 4/5 | amenity=recycling, recycling_type=centre |

### Key Integration Notes

**Abfall+ / Abfall.io:**
- Multi-tenant architecture using hash keys (tenant IDs)
- Discovery: Inspect network traffic on municipal "Abfuhrtermine" page
- Hierarchical parameter flow: `f_id_kommune` → `f_id_strasse` → `f_id_abfalltyp`
- **Critical:** `f_id_abfalltyp` values are non-standard per district (51=Restmüll in one, 51=Altpapier in another)
- Must call metadata endpoint first to get name-to-ID mapping per city
- Example Waldshut key: `5b0384147b5bc055c30fee1fb6db6f76`

**MyMüll.de / JumoMind:**
- More modern REST interface with dynamic discovery
- Query `r=cities` to get complete, up-to-date list of supported municipalities
- Workflow: `GET r=cities` → `GET r=streets&city_id=X` → `GET r=trash&city_id=X&area_id=Y`
- Response includes bin color hex codes (e.g., `#FF0000` for Hazardous) - ingest for UI cues
- University cities: Flensburg, Bad Homburg, Gießen, Marburg districts

**Müllmax (Legacy):**
- No clean JSON API - server-side rendered HTML forms
- Strategy: Find ICS export URL once, then poll periodically (stable for year)
- Scraping complexity: Session state, multi-step selection, hidden input fields
- Use hacs_waste_collection_schedule docs for reverse-engineering guidance

**Berlin BSR ("Gold Standard"):**
- Use as **Base Ontology** for item vocabulary
- Item list is universal (pizza box exists in Munich too) - local rules are "overrides"
- Contains synonyms, avoidance tips, recycling tips
- Strategy: Import BSR items → overlay city-specific disposal rules

**AVV (Regulatory Backbone):**
- Hierarchical: Chapter (01-20) → Sub-Chapter → 6-digit Code
- **Chapter 20** most critical for university chatbot (municipal wastes)
- Key codes:
  - `20 01 01` Paper/cardboard
  - `20 01 02` Glass
  - `20 01 08` Biodegradable kitchen waste
  - `20 01 39` Plastics
  - `20 03 01` Mixed municipal (Restmüll)
- **Asterisk (*)** = Hazardous (e.g., `20 01 21*` Fluorescent tubes) → trigger hazard alert
- **Mirror Entries:** Same item, two codes depending on contamination
  - `20 01 37*` Wood with hazardous substances
  - `20 01 38` Wood without hazardous substances
  - Requires clarifying questions in chatbot logic

**hacs_waste_collection_schedule:**
- Bootstrap source for API keys and city configurations
- Parse `abfall_io.py` and similar files for pre-configured (key, kommune_id, provider_name) tuples
- Covers Landshut, Potsdam-Mittelmark, Baden-Württemberg districts, many more
- Reduces "time to first byte" significantly

**OpenStreetMap (Long Tail):**
- For cities without Open Data portals (Heidelberg, Göttingen, etc.)
- Query tags: `amenity=recycling`, `recycling_type=centre`
- Attributes: `opening_hours`, `recycling:wood`, `recycling:batteries`, `recycling:electronics`

### Regional Variation Challenges

**The Pizza Box Paradox:**
- Some districts: greasy → Restmüll (ruins paper pulp)
- Others: empty/dry → Altpapier
- Source: Sortierhilfen PDFs, municipal FAQs (not in APIs)
- Graph requirement: Condition node - "Is it greasy?" → Yes → Restmüll; No → Altpapier

**Bioplastics Consensus:**
- Most German processors **ban** bioplastics from Biotonne (composting too slow)
- Default rule: Reject bioplastics from organic bin unless positive city confirmation
- "Fail-safe" approach prevents compost contamination

**Schadstoffmobil (Hazardous Mobile):**
- Schedules often locked in PDF calendars
- Some APIs treat stops as "events" in calendar feed
- Acceptance limits: max 20kg, max 20L containers, no mixed chemicals

### Graph Schema Recommendations

```
Municipality --[uses_provider]--> SaaS_Provider
Municipality --[has_tenant_key]--> API_Key_Config
UserLocation (city_id, street_id) --[in_municipality]--> Municipality
WasteObject (item) --[has_avv_code]--> AVVCode
WasteObject --[disposed_via]--> DisposalRule
DisposalRule --[valid_in]--> Municipality
DisposalRule --[routes_to]--> WasteStream (bin type)
CollectionEvent (date) --[for_stream]--> WasteStream
CollectionEvent --[at_location]--> UserLocation
```

### API Quick Reference

```bash
# Abfall+ - POST to get schedule (requires discovered key + IDs)
curl -X POST "https://api.abfallplus.de/index.php" \
  -d "key=5b0384147b5bc055c30fee1fb6db6f76" \
  -d "f_id_kommune=270" \
  -d "f_id_strasse=15720" \
  -d "f_id_abfalltyp_0=51" \
  -d "modul=termine"

# JumoMind - List all supported cities
curl "https://mymuell.jumomind.com/mmapp/api.php?r=cities"

# JumoMind - Get streets for city
curl "https://mymuell.jumomind.com/mmapp/api.php?r=streets&city_id=X"

# JumoMind - Get collection schedule
curl "https://mymuell.jumomind.com/mmapp/api.php?r=trash&city_id=X&area_id=Y"

# Abfall+ Widget Scraping - Abfall-ABC HTML
curl "https://api.abfall.io/customers/363/widgets/abfall-abc.html"

# OpenStreetMap Overpass - Recycling centers near coordinates
curl -d "data=[out:json];node[amenity=recycling][recycling_type=centre](around:5000,48.137,11.576);out;" \
  "https://overpass-api.de/api/interpreter"
```

---

## Report 05: AVV/EWC Codes

### Key Finding: Mirror Entry Logic

The **AVV (Abfallverzeichnis-Verordnung)** is Germany's implementation of the EU's European Waste Catalogue (EWC). The most complex aspect is **mirror entries (Spiegeleinträge)** - pairs of codes for the same waste, distinguished by hazardous substance concentration. This requires conditional logic in the knowledge graph, not simple lookup.

### Primary Sources: Authoritative Legal Texts

| Source | URL | Format | License | Relevance | Coverage |
|--------|-----|--------|---------|-----------|----------|
| **Gesetze-im-Internet (AVV)** | https://www.gesetze-im-internet.de/avv/ | HTML, XML, PDF | Public | 5/5 | Source of truth for German AVV codes |
| **EU Open Data Portal (EWC)** | https://data.europa.eu/ | RDF, XML, CSV | Public | 5/5 | Eurostat EWC in SKOS format |
| **EU Vocabularies (SKOS)** | https://op.europa.eu/en/web/eu-vocabularies/controlled-vocabularies | RDF/SKOS | Public | 5/5 | Native KG-compatible waste codes |
| **Legislation.gov.uk EWC** | http://www.legislation.gov.uk/id/eudn/2000/532/annex | XML | Public | 4/5 | Commission Decision 2000/532/EC |

### Primary Sources: UK/SEPA Structured Data

| Source | URL | Format | License | Relevance | Coverage |
|--------|-----|--------|---------|-----------|----------|
| **SEPA Waste Thesaurus** | https://www.sepa.org.uk/ | Excel/CSV | Public | 5/5 | Common terms → EWC codes (NLU gold) |
| **UK Consolidated Lists** | https://data.gov.scot/sepa/waste/ | CSV/Excel | Public | 5/5 | Absolute vs Mirror entry flags |
| **GOV.UK WM3 Guidance** | https://assets.publishing.service.gov.uk/ | PDF/Excel | Public | 5/5 | Mirror entry threshold calculations |
| **NetRegs EWC Guide** | https://www.netregs.org.uk/ | HTML | Public | 4/5 | Northern Ireland/Scotland guidance |
| **CSG EWC Codes Tool** | https://www.csg.co.uk/resources/ewc-codes | HTML | Free | 3/5 | Quick EWC lookup |

### Primary Sources: Technical Guidelines (LAGA/TRGS)

| Source | URL | Format | License | Relevance | Coverage |
|--------|-----|--------|---------|-----------|----------|
| **LAGA Technische Hinweise** | https://www.laga-online.de/ | PDF | Public | 5/5 | HP criteria concentration limits |
| **LAGA M18** | https://www.laga-online.de/ | PDF | Public | 5/5 | Healthcare waste (Chapter 18) |
| **LAGA M31** | https://www.umweltministerkonferenz.de/ | PDF | Public | 4/5 | WEEE waste code logic |
| **TRGS 201** | https://vorschriften.bgn-branchenwissen.de/ | HTML/PDF | Public | 5/5 | Simplified lab waste classification |
| **EU POPs Regulation** | EUR-Lex 2019/1021 | PDF | Public | 4/5 | HBCD, PCB thresholds (1000 mg/kg) |
| **CLP Regulation** | EUR-Lex 1272/2008 | PDF | Public | 4/5 | HP codes (HP 1-15) definitions |

### Primary Sources: German Information Portals

| Source | URL | Format | License | Relevance | Coverage |
|--------|-----|--------|---------|-----------|----------|
| **IPA (Abfallbewertung)** | https://www.abfallbewertung.org/ | Web | Public | 5/5 | Interactive AVV code lookup, characteristics |
| **Umweltbundesamt** | https://www.umweltbundesamt.de/ | PDF | Public | 4/5 | AVV commentary, EWC updates |
| **BMUV AVV Erläuterungen** | https://www.bmuv.de/ | PDF | Public | 4/5 | Official AVV application hints |

### Primary Sources: Electronic Waste Records

| Source | URL | Format | License | Relevance | Coverage |
|--------|-----|--------|---------|-----------|----------|
| **ZEDAL eANV** | https://docs.live.zedal.de/ | API/Catalog | Commercial | 4/5 | Validated AVV codes, facility numbers |

### Secondary Sources: University Abfall-ABCs

| Source | URL | Format | License | Relevance | Coverage |
|--------|-----|--------|---------|-----------|----------|
| **LMU Munich** | (internal PDF lists) | PDF | Fair use | 5/5 | Lab chemical → AVV code mappings |
| **Uni Heidelberg** | (internal PDF lists) | PDF | Fair use | 5/5 | Biological cultures, lab waste |
| **ETH Zürich** | https://ethz.ch/ | PDF | Public | 4/5 | Chemical compatibility, separation classes |
| **Uni Gießen** | https://www.uni-giessen.de/ | PDF | Public | 4/5 | Waste statistics, AVV mapping |

### Key Integration Notes

**AVV Structure (Hierarchical):**
```
Chapter (01-20) → Group (4-digit) → Waste Code (6-digit)
```
- **Chapters 01-12:** Process-specific (e.g., 07 = organic chemical processes)
- **Chapters 13-15:** Material-specific (oils, solvents, packaging) - use only if no 01-12 match
- **Chapter 16:** "Catch-all" for unspecified waste (lab chemicals go here)
- **Chapters 17-20:** Construction, healthcare, waste treatment, municipal
- **Classification precedence:** 01-12 or 17-20 first → then 13-15 → 16 as last resort

**Mirror Entry Logic (Critical!):**
Mirror entries are pairs where hazardousness depends on substance concentration:
```
17 05 03* → Soil/stones containing hazardous substances
17 05 04  → Soil/stones without hazardous substances (mirror)

16 05 06* → Lab chemicals containing hazardous substances
16 05 09  → Discarded chemicals, non-hazardous (mirror)
```
- Graph must link pairs via `mirrorOf` relationship
- Chatbot must ask clarifying questions: "Does it contain hazardous substances?"
- Use HP criteria thresholds to automate where possible

**HP Criteria (Hazardous Properties):**
| HP Code | Description | Chatbot Logic |
|---------|-------------|---------------|
| HP 3 | Flammable | "Flash point below 60°C?" |
| HP 4 | Irritant | "Causes skin/eye irritation?" |
| HP 6 | Acute Toxicity | "Skull & crossbones symbol?" |
| HP 8 | Corrosive | "pH ≤ 2 or ≥ 11.5?" |
| HP 14 | Ecotoxic | Summation: H400/H410 substances ≥ 0.25% |

**SEPA Waste Thesaurus (NLU Gold):**
- Maps thousands of common terms to EWC codes
- Examples: "acetone" → 14 06 03*, "pallets" → 15 01 03, "fluorescent tubes" → 20 01 21*
- Users search for "old paint" not "waste from organic chemical processes"
- Import as `skos:altLabel` synonyms on WasteCode entities

**TRGS 201 (Simplified Lab Classification):**
- Allows grouping by hazard characteristics without detailed chemical analysis
- Example: "Is it a halogenated solvent?" → 14 06 02* (yes) or 14 06 03* (no)
- Reduces chatbot question complexity for lab waste

**University-Specific Domains:**

| Domain | AVV Chapters | Key Codes | Notes |
|--------|--------------|-----------|-------|
| **Laboratory** | 06, 07, 14, 16 | 16 05 06*, 14 06 03*, 06 01 01* | Solvents, acids, lab chemicals |
| **Healthcare** | 18 | 18 01 03*, 18 01 04, 18 01 08* | Infectious vs non-infectious, cytostatics |
| **Campus/IT** | 17, 20 | 20 01 35*, 20 01 36, 20 01 33* | WEEE, batteries, construction |

**Common Lab Chemical Mappings:**

| Chemical | CAS Number | AVV Codes | HP Class |
|----------|------------|-----------|----------|
| Acetone (spent) | 67-64-1 | 14 06 03* / 07 01 04* | HP 3 (Flammable) |
| Methanol | 67-56-1 | 14 06 03* / 16 05 06* | HP 3, HP 6 (Toxic) |
| Sulfuric Acid | 7664-93-9 | 06 01 01* / 16 05 06* | HP 8 (Corrosive) |
| Ethidium Bromide Gel | 1239-45-8 | 16 05 06* / 18 01 03* | HP 7, HP 11 (CMR) |
| Heavy Metal Salts | Various | 16 05 07* | HP 6, HP 14 |
| Non-hazardous Salts | Various | 16 05 09 | Non-hazardous |

**Healthcare Waste (LAGA M18):**
- **18 01 03*** = Infectious (cultures, blood, highly contagious)
- **18 01 04** = Non-infectious "B-Müll" (dressings, gowns, swabs) - municipal incineration OK
- **18 01 01** = Sharps (separate collection, treat as hazardous)
- **18 01 08*** = Cytostatic drugs (Cisplatin, Doxorubicin) - CMR substances, never mix with 18 01 09

**POPs (Persistent Organic Pollutants):**
- HBCD (insulation foam), PCBs above 1000 mg/kg threshold → automatic hazardous classification
- Require destruction (incineration), not recycling
- Add `containsPOPs` flag for construction waste, plastics, WEEE

### Graph Schema Recommendations

```
AVVCode {
  code: "16 05 06*",
  name_de: "Laborchemikalien...",
  name_en: "Laboratory chemicals...",
  isHazardous: true,
  isMirrorEntry: true
}

(AVVCode)-[:MIRROR_OF]->(AVVCode)
(AVVCode)-[:IN_GROUP]->(AVVGroup)-[:IN_CHAPTER]->(AVVChapter)
(AVVCode)-[:TRIGGERS_HP]->(HazardProperty)
(WasteItem)-[:HAS_AVV_CODE]->(AVVCode)
(WasteItem)-[:HAS_RULE]->(DisposalRule)-[:IF_HP {threshold: 0.1%}]->(AVVCode)

HazardProperty {
  code: "HP 3",
  name: "Flammable",
  chatbot_question: "Is the liquid flammable? Flash point below 60°C?"
}
```

### API / Data Access Reference

```bash
# AVV from Gesetze-im-Internet (HTML parsing or PDF)
curl "https://www.gesetze-im-internet.de/avv/AVV.pdf"

# EU Vocabularies - EWC SKOS (RDF)
# Navigate: https://op.europa.eu/en/web/eu-vocabularies/controlled-vocabularies
# Download: Waste classification (EWC) in RDF/XML or CSV

# IPA - Interactive AVV lookup (web only, no API)
# https://www.abfallbewertung.org/ipa/

# SEPA Waste Data (Scotland)
curl "https://data.gov.scot/sepa/waste/wfas-qr.html"

# GOV.UK WM3 Guidance PDF
curl "https://assets.publishing.service.gov.uk/media/6152d0b78fa8f5610b9c222b/Waste_classification_technical_guidance_WM3.pdf"

# ETH Zürich Chemical Wastewater Guidelines
curl "https://ethz.ch/content/dam/ethz/associates/services/Service/sicherheit-gesundheit-umwelt/files/chemieabwasser/de/Merkblatt_Chemieabwasser_DE.pdf"
```

### Data Ingestion Strategy

1. **Skeleton:** Parse AVV from Gesetze-im-Internet XML → 20 Chapters, Groups, ~840 Codes
2. **Enrich:** Join with SEPA Consolidated List CSV → add "Absolute/Mirror" flags + English descriptions
3. **Synonyms:** NER on university Abfall-ABC PDFs → extract "Synonym → Code" mappings → `skos:altLabel`
4. **Chemical Logic:** Ingest priority lab reagents + CLP H-phrases → SHACL rules for HP inference
5. **Mirror Resolution:** Link mirror pairs, attach clarifying questions for chatbot dialogue

---

## Report 06: Construction Materials (Bauschutt)

### Key Finding: Three Data Clusters

Construction waste (Bauschutt) is Germany's largest waste stream by mass. The report identifies **three essential data clusters** that must be federated:

1. **Regulatory Cluster:** AVV codes, LAGA M20 limits, Ersatzbaustoffverordnung (EBV)
2. **Material Science Cluster:** LCA data, density tables, crystallography
3. **Operational Cluster:** GISBAU hazard codes, municipal APIs, material passports

**Critical Insight:** The graph must distinguish **Solid Density (Rohdichte)** vs **Bulk Density (Schüttdichte)**. A concrete wall has ~2,400 kg/m³ solid density, but crushed rubble has ~1,500 kg/m³ bulk density. Using the wrong value causes massive logistics errors.

### Primary Sources: AVV/Regulatory

| Source | URL | Format | License | Relevance | Coverage |
|--------|-----|--------|---------|-----------|----------|
| **Destatis (Abfallbilanz)** | https://www.destatis.de/ | Excel/CSV | Public | 5/5 | AVV classifications, waste generation volumes |
| **AVV Chapter 17** | https://www.gesetze-im-internet.de/avv/ | PDF/HTML | Public | 5/5 | Construction & demolition codes (17 01-17 09) |
| **LAGA M20** | https://www.laga-online.de/ | PDF | Public | 5/5 | Pollutant limits Z0/Z1.1/Z1.2/Z2 classes |
| **Ersatzbaustoffverordnung (EBV)** | https://www.gesetze-im-internet.de/ | PDF | Public | 5/5 | RC-1/RC-2/RC-3 recycled aggregate classes |
| **ewccodes.com** | https://ewccodes.com/ | REST API | Free | 4/5 | EWC list with hazardous flag + approximate density |
| **Baustoffrecycling Bayern EBV Tables** | https://www.baustoffrecycling-bayern.de/ | PDF | Public | 5/5 | RC class limit value tables in structured format |

### Primary Sources: Density Data

| Source | URL | Format | License | Relevance | Coverage |
|--------|-----|--------|---------|-----------|----------|
| **Beton Eisack (South Tyrol)** | https://www.beton-eisack.it/ | PDF | Public | 5/5 | Real-world bulk densities from weighbridge data |
| **City of Vienna MA 22** | https://www.wien.gv.at/pdf/ma22/umrechnungsfaktoren.pdf | PDF | Public | 5/5 | Disposal-phase conversion factors |
| **Canton Uri (Switzerland)** | https://www.ur.ch/ | PDF | Public | 4/5 | Road construction mineral densities |
| **Bayerisches Landesamt für Statistik** | https://www.statistik.bayern.de/umrechnungsfaktoren | Excel | Public | 5/5 | AVV codes with volume-to-mass factors |
| **Materials Project API** | https://api.materialsproject.org/ | REST JSON | API Key | 4/5 | Crystallographic density of pure compounds |

### Primary Sources: Material Passports / Composition

| Source | URL | Format | License | Relevance | Coverage |
|--------|-----|--------|---------|-----------|----------|
| **Madaster API** | https://api.madaster.com/v4.0/ | REST JSON | Commercial | 5/5 | BIM material inventories, Bill of Materials |
| **2050 Materials** | https://docs.2050-materials.com/ | REST API | Free tier | 4/5 | Sustainable products, Uniclass/Masterformat taxonomy |

### Primary Sources: LCA / Environmental

| Source | URL | Format | License | Relevance | Coverage |
|--------|-----|--------|---------|-----------|----------|
| **ÖKOBAUDAT** | https://oekobaudat.de/OEKOBAUDAT/resource | ILCD XML/JSON | Public | 5/5 | German LCA data (EN 15804), EoL scenarios |
| **OpenEPD / EC3 API** | https://docs.buildingtransparency.org/ec3/api-and-integrations | JSON | Token req. | 5/5 | Global EPDs, embodied carbon |

### Primary Sources: Contamination / Hazard

| Source | URL | Format | License | Relevance | Coverage |
|--------|-----|--------|---------|-----------|----------|
| **GISBAU / WINGIS** | https://www.wingisonline.de/ | Web | Public | 5/5 | GISCODES for construction chemicals → AVV mapping |
| **EPA List of Lists** | https://www.epa.gov/epcra/consolidated-list-lists | Excel | Public | 4/5 | CAS → waste code cross-walk |
| **EPA HazWaste Portal** | https://www.hazwasteportal.org/ | Web | Public | 4/5 | Chemical → hazardous waste code search |
| **SGS INSTITUT FRESENIUS (LAGA)** | https://sgs-institut-fresenius.de/ | Web | Info | 4/5 | LAGA testing methodology reference |

### Primary Sources: Municipal / Logistics

| Source | URL | Format | License | Relevance | Coverage |
|--------|-----|--------|---------|-----------|----------|
| **RegioIT Abfallnavi API** | https://{region}-abfallapp.regioit.de/ | REST JSON | Public | 5/5 | Disposal facility locations, opening hours |
| **BSR Berlin Open Data** | https://daten.berlin.de/datensaetze/bsr-recyclingh-fe-und-abfall-abc | JSON | CC BY | 5/5 | Recyclinghöfe, Abfall-ABC |
| **abfallnavi.api.bund.dev** | https://abfallnavi.api.bund.dev/ | OpenAPI | Public | 4/5 | Federal documentation for municipal APIs |

### Secondary Sources: Open Source / GitHub

| Source | URL | Format | License | Relevance | Coverage |
|--------|-----|--------|---------|-----------|----------|
| **TACO Dataset** | https://github.com/pedropro/TACO | CSV/Images | MIT | 3/5 | Waste taxonomy for CV training |
| **waste-datasets-review** | https://github.com/AgaMiko/waste-datasets-review | CSV | Various | 3/5 | Aggregated waste image datasets |
| **abfall GitHub Topic** | https://github.com/topics/abfall | Various | Various | 4/5 | EBV limit tables in JSON/JS (ZOW-Tabelle) |
| **hacs_waste_collection_schedule** | https://github.com/mampfes/hacs_waste_collection_schedule | Python | MIT | 5/5 | Pre-configured API keys for RegioIT |

### Key Integration Notes

**AVV Chapter 17 Structure:**

| Code | German Name | Classification | Graph Properties |
|------|-------------|----------------|------------------|
| 17 01 01 | Beton | Inert, Recyclable | `hasDensity: ~2.4t/m³`, `isRecyclable: true` |
| 17 01 02 | Ziegel | Inert, Recyclable | `hasDensity: ~1.8t/m³`, `color: Red/Orange` |
| 17 01 07 | Gemische aus Beton, Ziegeln... | Mixed Mineral | `requiresSorting: false` |
| 17 09 04 | Gemischte Bau- und Abbruchabfälle | Mixed, Non-Hazardous | `hasDensity: ~0.2-0.8t/m³`, `requiresSorting: true` |
| 17 01 06* | Gemische... mit gefährlichen Stoffen | Hazardous | `requiresManifest: true`, `disposalPath: SpecialTreatment` |
| 17 06 05* | Asbesthaltige Baustoffe | Hazardous | `hazardType: Carcinogenic`, `handling: TRGS519` |

**Density Data Comparison (Critical!):**

| Source | Context | Density Type | Concrete Value | Format |
|--------|---------|--------------|----------------|--------|
| Beton Eisack | Recycling (Crushed) | Bulk (Schütt) | ~1.50 t/m³ | PDF Table |
| City of Vienna | Disposal (Mixed) | Bulk (Loose) | 0.20 t/m³ (Mixed!) | PDF |
| Canton Uri | Road Construction | Bulk (Compacted) | 1.7-1.9 t/m³ | PDF |
| Materials Project | Material Science | Solid (Crystal) | ~2.40 t/m³ | REST API |
| Madaster | BIM/Inventory | Product Specific | Variable | REST API |

**Key Insight:** A container of "mixed construction waste" (17 09 04) weighs **~85% less** than a container of "concrete rubble" (17 01 01). Graph must use AVV code to auto-select correct density factor.

**Granular Bulk Density Values (Beton Eisack):**
- RC-Sand (0-10 mm): 1.45 t/m³ (higher packing density)
- RC-Kies (30-80 mm): 1.38 t/m³ (more air voids)
- Schottergemisch (0-31.5 mm): 1.64 t/m³ (highest, used for road bases)
- Asphaltgranulat: 1.6-1.8 t/m³

**LAGA M20 / EBV Classification Logic:**

| Class | Description | Example Threshold |
|-------|-------------|-------------------|
| Z0 | Unrestricted reuse | All limits met |
| Z1.1 / Z1.2 | Restricted use (technical measures) | Some limits exceeded |
| Z2 | Special safety measures required | Multiple limits exceeded |
| RC-1 | EBV Class 1 (highest quality) | `IF Sulphate ≤ 50 mg/l` |
| RC-2 | EBV Class 2 | Higher limits |
| RC-3 | EBV Class 3 (lowest recyclable) | Even higher limits |

**Key Parameters:** Conductivity, pH, TOC, Arsenic, Lead, Cadmium, Chromium, Copper, Nickel, Mercury, Zinc, EOX, PAK

**Contamination Indicators (Building Era → Pollutant):**

| Substance | Common Locations | Era | Graph Logic |
|-----------|------------------|-----|-------------|
| **Asbestos** | Fiber cement, floor adhesives, fireproofing | Pre-1993 | `IF building.yearBuilt < 1993 → flag: asbestos_risk` |
| **PCB** | Joint sealants, paints, capacitors | Pre-1980s | `IF building.yearBuilt < 1980 → flag: pcb_risk` |
| **PAK** | Tar adhesives, roofing felt, asphalt flooring | Pre-1970s | `IF material.type = "tar_adhesive" → flag: pak_risk` |
| **Heavy Metals** | Lead pipes, anti-corrosion paints | Various | Requires chemical analysis |

**GISBAU/WINGIS Integration:**
- GISCODES simplify hazard communication (e.g., "RE1" for epoxy resins, "ZP1" for cement products)
- Mapping: GISCODE S1 (solvent-containing) → flag for AVV 17 09 03* if not separated
- Graph: `Product --[has_giscode]--> GISCODE --[triggers_avv_if_mixed]--> AVVCode`

**Madaster API (Material Passports):**
- Endpoint: `https://api.madaster.com/v4.0/`
- Auth: X-API-Key header
- Core Entities: Building, Material, Passport
- Strategy: When building flagged "To Be Demolished," query Madaster for `BillOfMaterials` → pre-classified waste stream before demolition begins
- Material Families: "I - Wood" / "I1 - Timber" hierarchy

**ÖKOBAUDAT (LCA Data):**
- Endpoint: `https://oekobaudat.de/OEKOBAUDAT/resource`
- Format: ILCD (International Life Cycle Data System)
- Modules: A1-A3 (Production), C3 (Recycling), C4 (Landfill), D (Benefits/Loads)
- Strategy: Query datasets for "Concrete C30/37" → get GWP values for "Scenario: Recycling" vs "Scenario: Landfill"
- **Caveat:** Data often "clumps" around generic values (e.g., 230 kg CO2/m³). Prefer manufacturer-specific EPDs.

**RegioIT Abfallnavi API:**
- Base URL: `https://{region}-abfallapp.regioit.de/abfall-app-{region}/rest`
- Endpoints:
  - `/fraktionen` - Waste types (Bio, Paper, Restmüll, Bauschutt)
  - `/standorte` - Disposal facility geolocation + hours
  - `/termine` - Collection dates
- Coverage: Aachen, Nürnberg, Gütersloh, many more
- Strategy: Map `avv:170101` → local `fraktionId`; use `/standorte` for distance calculations

**Environmental Impact Formula:**
```
Total Impact = (Mass × EoL_Factor_ÖKOBAUDAT)
             + (Mass × Distance × Transport_Factor)
             - (Mass × Recycling_Credit_Module_D)
```

### Graph Schema Recommendations

```
MaterialBatch {
  uid: "batch_001",
  hasMass: 500,          // derived from volume × density
  hasVolume: 200,        // from Madaster BIM
  hasContaminantLevel: {lead: 45, cadmium: 2},
  buildingEra: 1975      // triggers PCB/asbestos risk flags
}

WasteClassification {
  avvCode: "17 01 06*",
  isHazardous: true,
  requiresManifest: true,
  lagaClass: "Z2"
}

ProcessingOption {
  facilityUid: "facility_001",
  location: {lat: 48.137, lon: 11.576},
  acceptsAVV: ["17 01 01", "17 01 02", "17 01 07"],
  hasGWPImpact: 12.5   // kg CO2/ton
}

BaseMaterial (e.g., mat:Concrete) --[instantiates]--> AVVCode
  // Same material base → different codes based on contamination
  mat:Concrete → avv:170101 (clean) OR avv:170106_star (contaminated)

MaterialBatch --[classified_as]--> WasteClassification
MaterialBatch --[processed_at]--> ProcessingOption
MaterialBatch --[derived_from]--> BIMModel (Madaster)
AVVCode --[has_density {type: "bulk", region: "SouthTyrol"}]--> DensityValue
AVVCode --[reuse_requires]--> LAGAClass
LAGAClass --[threshold {param: "Sulphate", max: 50}]--> ChemicalLimit
```

### API Quick Reference

```bash
# === Density Tables ===

# Beton Eisack - Bulk density table (PDF)
curl "https://www.beton-eisack.it/_default_upload_bucket/Gewichtstabelle%202021_1.pdf"

# City of Vienna MA 22 - Conversion factors
curl "https://www.wien.gv.at/pdf/ma22/umrechnungsfaktoren.pdf"

# Canton Uri - Mineral construction waste factors
curl "https://www.ur.ch/_docn/297137/220527_318_22_ANL_Umrechnungsfaktoren_fur_mineralische_Bauabfalle.pdf"

# === Material Passports ===

# Madaster API - Get building materials (requires API key)
curl -H "X-API-Key: YOUR_KEY" "https://api.madaster.com/v4.0/buildings/{id}/materials"

# === LCA Data ===

# ÖKOBAUDAT - Search for concrete LCA data
curl "https://oekobaudat.de/OEKOBAUDAT/resource/processes?name=Beton"

# EC3/OpenEPD - Get EPD (requires token)
curl -H "Authorization: Bearer TOKEN" "https://api.buildingtransparency.org/api/epds?search=concrete"

# === Regulatory Tables ===

# EBV RC-1 Limit Values (Baustoffrecycling Bayern)
curl "https://www.baustoffrecycling-bayern.de/system/files/oeffentlich/Dokumente/EBV_Anlage%202%20Tabelle%201%20RC-1.pdf"

# EBV RC-3 Limit Values
curl "https://www.baustoffrecycling-bayern.de/system/files/oeffentlich/Dokumente/EBV_Anlage%202%20Tabelle%203%20RC-3.pdf"

# === Municipal APIs ===

# RegioIT - Get waste fractions for region
curl "https://aachen-abfallapp.regioit.de/abfall-app-aachen/rest/fraktionen"

# RegioIT - Get disposal facility locations
curl "https://aachen-abfallapp.regioit.de/abfall-app-aachen/rest/standorte"

# BSR Berlin - Recyclinghöfe JSON
curl "https://daten.berlin.de/api/3/action/package_show?id=bsr-recyclingh-fe-und-abfall-abc"

# === Hazard Lookup ===

# GISBAU/WINGIS - Search construction chemicals (web only)
# https://www.wingisonline.de/

# EPA HazWaste - Search by chemical name
# https://www.hazwasteportal.org/search.php

# EWC Codes API - Get code with hazard flag
curl "https://ewccodes.com/api/ewc?code=170101"
```

### Data Ingestion Strategy

1. **AVV Skeleton:** Parse Chapter 17 codes from Gesetze-im-Internet → 9 sub-chapters, ~25 codes
2. **Density Overlay:** Ingest Beton Eisack + Vienna tables → store `minDensity`, `maxDensity`, `avgDensity` per code
3. **LAGA/EBV Limits:** Parse RC-1/RC-2/RC-3 tables → create SHACL validation shapes (IF Lead > X → invalidate RC-1)
4. **Contamination Rules:** Create `BuildingEra → PollutantRisk` inference rules (pre-1993 → asbestos, pre-1980 → PCB)
5. **LCA Integration:** Batch-fetch ÖKOBAUDAT datasets for construction materials → link GWP to AVV codes
6. **Facility Network:** Query RegioIT `/standorte` for all regions → calculate distance-based transport emissions

---

## Report 07: Packaging & Recycling Codes

### Key Finding: Three Semantic Languages

Packaging disposal requires bridging **three distinct semantic layers**:

1. **Legal Language (EU):** Decision 97/129/EC material codes, VerpackG, PPWR
2. **Technical Language (Industry):** GS1 GDSN codes, DIN 6120 marking syntax, polymer science
3. **Pragmatic Language (Consumer):** Abfall-ABC, bin colors, regional disposal rules

**Critical Insight:** A simple "Code → Bin" mapping is **insufficient**. The graph must handle:
- **Conditional Disposal:** Driven by form (bottle vs tray), content (food vs non-food), region (Wertstofftonne vs Gelber Sack)
- **Recyclability Logic:** Driven by physical properties (density, NIR detectability) not just material identity
- **Composite Decomposition:** Requiring mass percentage analysis of "C/" codes

### Primary Sources: Regulatory/Legal

| Source | URL | Format | License | Relevance | Coverage |
|--------|-----|--------|---------|-----------|----------|
| **EU Decision 97/129/EC** | https://eur-lex.europa.eu/eli/dec/1997/129/oj/eng | PDF/HTML | Public | 5/5 | Root authority for material codes 01-99 |
| **VerpackG (Verpackungsgesetz)** | https://verpackungsgesetz-info.de/en/ | HTML | Public | 5/5 | German national transposition, ZSVR authority |
| **ZSVR Mindeststandard** | https://www.verpackungsregister.org/fileadmin/files/Mindeststandard/ | PDF | Public | 5/5 | Recyclability assessment algorithm |
| **PPWR (EU 2025/40)** | https://www.dihk.de/resource/blob/128168/a5682083c196b98fbc86d2e49d57af05/klima-dihk-merkblatt-verpackungsverordnung-ppwr-data.pdf | PDF | Public | 4/5 | New EU packaging regulation (mandatory recyclability) |
| **DIN 6120** | https://www.dinmedia.de/de/norm/din-6120/300614768 | Paid Standard | Commercial | 5/5 | Marking syntax, recycling triangle geometry |

### Primary Sources: Digital Data / Taxonomies

| Source | URL | Format | License | Relevance | Coverage |
|--------|-----|--------|---------|-----------|----------|
| **Open Food Facts Taxonomies** | https://github.com/openfoodfacts/openfoodfacts-server/blob/main/taxonomies/packaging_materials.txt | Text (DAG) | ODbL | 5/5 | Hierarchical material taxonomy (en:plastic → en:pet → en:rpet) |
| **GS1 GDSN PackagingMaterialTypeCode** | https://ref.gs1.org/voc/PackagingMaterialTypeCode | JSON-LD | Public | 5/5 | B2B controlled vocabulary (POLYMER_PET, PAPER_CORRUGATED) |
| **Wikidata P5994** | https://www.wikidata.org/wiki/Q2301084 | SPARQL | Public | 4/5 | Recycling codes linked to material entities |
| **GS1 in Europe GDSN Packaging Guide** | https://gs1.eu/wp-content/uploads/2025/06/GS1inEU_GDSN_Packaging_Guideline_v1.0.pdf | PDF | Public | 4/5 | GDSN implementation for packaging attributes |

### Primary Sources: Recyclability Assessment

| Source | URL | Format | License | Relevance | Coverage |
|--------|-----|--------|---------|-----------|----------|
| **RecyClass D4R Guidelines** | https://recyclass.eu/protocols-guidelines/design-for-recycling-guidelines/ | PDF | Public | 5/5 | Material compatibility tables (Green/Yellow/Red) |
| **Cyclos-HTP CHI Standard** | https://www.cyclos-htp.de/publications/chi-recyclability-standard/ | PDF | Public | 5/5 | Recyclability certification methodology |
| **GIZ Design-for-Recycling Report** | https://www.giz.de/de/downloads/2021-06%20Design%20for%20recycling_barrierefrei.pdf | PDF | Public | 4/5 | Carbon black, NIR detectability issues |

### Primary Sources: German Consumer/Municipal

| Source | URL | Format | License | Relevance | Coverage |
|--------|-----|--------|---------|-----------|----------|
| **Alnatura Recycling Codes Guide** | https://www.alnatura.de/de-de/magazin/oekologisch-sinnvolle-verpackung/faq-verpackung/verpackungswissen_recycling-codes/ | HTML | Public | 5/5 | Consumer-friendly code explanations (PAP 21, etc.) |
| **Verbraucherzentrale NRW** | https://www.verbraucherzentrale.nrw/wissen/umwelt-haushalt/abfall/beispiele-fuer-verpackungsmuell-was-gehoert-in-welche-tonne-32306 | HTML | Public | 5/5 | Packaging examples with conditional logic |
| **BSR Abfalltrenntabelle** | https://www.bsr.de/assets/downloads/abfalltrenntabelle-din-a4.pdf | PDF | Public | 5/5 | Berlin disposal rules |
| **AWM Munich Trennliste** | https://www.awm-muenchen.de/fileadmin/Dateien_pdfs/Trennliste_Bilder_A4.pdf | PDF | Public | 5/5 | Munich disposal rules (Wertstoffhof emphasis) |
| **Mülltrennung wirkt!** | https://www.muelltrennung-wirkt.de/de/ueber-uns/ueber-die-dualen-systeme/ | HTML | Public | 4/5 | Dual Systems ecosystem overview |
| **Deutsche Recycling** | https://deutsche-recycling.com/blog/recycling-symbols-decoding-their-meaning/ | HTML | Public | 4/5 | Symbol decoder, VerpackG compliance |

### Primary Sources: Industrial/AVV

| Source | URL | Format | License | Relevance | Coverage |
|--------|-----|--------|---------|-----------|----------|
| **AVV Chapter 15 (Packaging Waste)** | https://www.zentek.de/avv/schluessel/1501/ | HTML | Public | 5/5 | 15 01 01-07 packaging waste codes |
| **Berlin Recycling AVV 15 01** | https://www.berlin-recycling.de/wissen/avv/1501 | HTML | Public | 5/5 | Detailed 15 01 code explanations |
| **SBB mbH AVV Kapitel 15** | https://www.sbb-mbh.de/de/rechtsgrundlagen/avv-2016/avv-anleitung-kapitelindex/avv-kapitel-15/ | HTML | Public | 4/5 | Official AVV chapter 15 reference |

### Secondary Sources

| Source | URL | Purpose |
|--------|-----|---------|
| KIDV Factsheet (Netherlands) | https://kidv.nl/media/engelse_factsheets/factsheet-identification-of-packaging.pdf | C/ composite code explanation |
| EKO-KOM (Czech) | https://www.ekokom.cz/wp-content/uploads/2022/03/Packaging-marking-22-01.pdf | EU code overview |
| Fami Spa (Italy) | https://www.famispa.com/en/company/certifications-and-sustainability/environmental-responsibility/environmental-labelling-of-packaging/ | Environmental labelling guide |
| IS MUNI Recycling Codes | https://is.muni.cz/el/1431/podzim2013/C7935/Recycling_codes_WIKI_ENG.pdf | Academic reference |
| LA County DPW Plastics | https://dpw.lacounty.gov/epd/SBR/pdfs/TypesOfPlastic.pdf | US plastic codes reference |

### Key Integration Notes

**Material Code Families (97/129/EC):**

| Range | Material Family | Disposal Stream |
|-------|-----------------|-----------------|
| 01-19 | Plastics | Gelber Sack (mostly) |
| 20-39 | Paper/Fiberboard | Blaue Tonne |
| 40-49 | Metals | Gelber Sack / Dosencontainer |
| 50-59 | Wood | Wertstoffhof |
| 60-69 | Textiles | Altkleider |
| 70-79 | Glass | Glascontainer (by color) |
| 80-99 | Composites | Gelber Sack (usually) |

**Plastic Code Data Dictionary:**

| Code | Abbrev | Material (DE/EN) | Density (g/cm³) | Disposal | GS1 GDSN | Graph Attributes |
|------|--------|------------------|-----------------|----------|----------|------------------|
| 01 | PET | Polyethylenterephthalat | ~1.38 | Pfand (bottle) / Gelber Sack (tray) | POLYMER_PET | `floatability: false`, `nirDetectable: true` |
| 02 | PE-HD | Polyethylen (Hart) | ~0.95 | Gelber Sack | POLYMER_HDPE | `floatability: true` |
| 03 | PVC | Polyvinylchlorid | ~1.30-1.45 | Gelber Sack (Residual) | POLYMER_PVC | `floatability: false`, `contaminant: true` |
| 04 | PE-LD | Polyethylen (Weich) | ~0.92 | Gelber Sack (Foil) | POLYMER_LDPE | `floatability: true`, `fraction: Foil` |
| 05 | PP | Polypropylen | ~0.90 | Gelber Sack | POLYMER_PP | `floatability: true` |
| 06 | PS | Polystyrol | ~1.05 | Gelber Sack / Wertstoffhof (EPS large) | POLYMER_PS | `hasExpandedForm: EPS` |
| 07 | O | Andere Kunststoffe | Variable | Gelber Sack (Thermal) | POLYMER_OTHER | `recyclable: false`, `outcome: thermal_recovery` |

**Critical Graph Logic:**

1. **PET Bifurcation (Code 01):**
   ```
   IF Form = "Beverage Bottle" AND Volume < 3.0L
     → Destination = Pfandautomat (bottle-to-bottle recycling)
   ELSE IF Form = "Tray/Cup" OR Content = "Oil/Non-Food"
     → Destination = Gelber Sack (mixed PET fraction)
   ```

2. **Carbon Black Exception (Any plastic):**
   ```
   IF Color = "Black" AND Pigment != "NIR-Reflective"
     → NIR_Detectable = False
     → Recyclable = False (per ZSVR)
     → Disposal_Outcome = "Thermal Recovery"
   ```

3. **Floatability Property:**
   - PE-HD, PE-LD, PP: `density < 1.0` → floats in water separation
   - PET, PVC, PS: `density > 1.0` → sinks
   - PVC contaminating PET is major sorting plant issue

4. **Composite "5% Rule" (ZSVR):**
   ```
   Paper packaging with plastic coating:
   IF Plastic_Content_Percentage > 5%
     → Code = 81 (C/PAP), Destination = Yellow Bin
   ELSE
     → Code = 21 (PAP), Destination = Blue Bin
   ```

**Composite Material Codes (80-98):**

| Code | Label | Composition | Typical Use | Disposal | Processing Notes |
|------|-------|-------------|-------------|----------|------------------|
| 80 | C/PAP | Paper/Metals | Biscuit canisters | Yellow Bin | Hard to recycle, often thermal |
| 81 | C/PAP | Paper/Plastic | Bakery bags, frozen food boxes | Yellow Bin | Exception: if separable, separate |
| 82 | C/PAP | Paper/Aluminum | Butter wrappers, cigarette packs | Yellow Bin | Alu recovered, paper low quality |
| **84** | **C/PAP** | Paper/Plastic/Aluminum | **Tetra Pak, Elopak (FKN)** | Yellow Bin | **Critical:** Dedicated FKN fraction, ReCarton process |
| 90 | C/LDPE | Plastic/Aluminum | Crisp bags, coffee packs | Yellow Bin | Chemical recycling or thermal |
| 91 | C/PET | Plastic/Tinplate | Blister packs | Yellow Bin | - |
| 95 | C/GL | Glass/Plastic | Glass bottles with sleeves | Glass Container | Warning: thick sleeves confuse sorting |

**Paper/Cardboard Codes (20-22):**

| Code | Material | Use | Disposal | Exception |
|------|----------|-----|----------|-----------|
| 20 PAP | Corrugated Fiberboard | Shipping boxes | Blaue Tonne | **Instruction:** "Flach zusammenfalten" |
| 21 PAP | Non-corrugated Fiberboard | Cereal boxes | Blaue Tonne | - |
| 22 PAP | Paper | Bags, newspapers | Blaue Tonne | - |

**Dirty Paper Exception:** Pizza box (20 PAP) heavily soiled with grease → Restmüll (contaminates pulping)

**Metal Codes (40-41):**

| Code | Material | Sorting Method | Disposal | Regional Override |
|------|----------|----------------|----------|-------------------|
| 40 FE | Steel | Overband magnets | Gelber Sack / Dosencontainer | Munich: Dosencontainer |
| 41 ALU | Aluminum | Eddy Current Separator | Gelber Sack / Dosencontainer | Munich: Dosencontainer |

**Consumer-Facing Container Nodes:**

| Container | German Name | Accepts | Exclusions |
|-----------|-------------|---------|------------|
| Yellow Sack/Bin | Gelber Sack/Tonne | 01-07 (non-deposit), 40-41, 80-98 | Non-packaging plastics (in strict jurisdictions) |
| Wertstofftonne | Wertstofftonne | Yellow Bin + "Stoffgleiche Nichtverpackungen" | Berlin: accepts plastic toys |
| Blue Bin | Blaue Tonne | 20, 21, 22 | Code 81 (>5% plastic), soiled paper, thermal receipt paper |
| Glass Container | Glascontainer | 70 (White), 71 (Green), 72 (Brown) | Blue/Red glass → Green container |
| Residual | Restmüll | Dirty packaging, PVC, hygiene items, ceramics | - |

**AVV Chapter 15 Mapping (Consumer → Industrial):**

| Consumer Codes | Material | AVV Code | AVV Description |
|----------------|----------|----------|-----------------|
| 20, 21, 22 | Paper/Cardboard | 15 01 01 | Verpackungen aus Papier und Pappe |
| 01-07 | Plastics | 15 01 02 | Verpackungen aus Kunststoff |
| 50, 51 | Wood | 15 01 03 | Verpackungen aus Holz |
| 40, 41 | Metals | 15 01 04 | Verpackungen aus Metall |
| 80-98 | Composites | 15 01 05 | Verbundverpackungen |
| Mixed | Unsorted | 15 01 06 | Gemischte Verpackungen |
| 70-72 | Glass | 15 01 07 | Verpackungen aus Glas |

**Dual Systems (Licensing Entities):**
- Der Grüne Punkt (Green Dot)
- BellandVision
- Zentek
- Interseroh

Graph relation: `Packaging_Object --[licensed_via]--> DualSystem`

**Regional Variation Logic:**

```
Region Node: Berlin
  → Municipality_Has_Wertstofftonne = True
  → Non-packaging plastics accepted in curbside

Region Node: Munich
  → Municipality_Has_Wertstofftonne = False
  → Non-packaging plastics → Wertstoffhof
  → Metals → Dosencontainer (separate collection)
```

**ZSVR Three-Step Recyclability Verification:**

1. **Infrastructure Exists?**
   - True: PET, PE, PP, Paper, Steel, Aluminum, Glass
   - False/Limited: PLA (bioplastics), PVC, EPS in Yellow Bin

2. **Sortable (NIR Detection)?**
   - Carbon black = invisible to NIR → residue
   - `IF Color = Black AND Pigment != NIR-Reflective → Recyclable = False`

3. **Compatible (Density/Wash)?**
   - PET bottle + PVC sleeve: both sink → PVC contaminates PET flakes
   - `IF Body = PET AND Component = PVC → Recyclability_Grade = Downgraded`

### Graph Schema Recommendations

```
PackagingMaterial {
  code: "01",
  abbreviation: "PET",
  name_de: "Polyethylenterephthalat",
  name_en: "Polyethylene terephthalate",
  density: 1.38,
  floatability: false,
  nirDetectable: true,
  gs1Code: "POLYMER_PET"
}

CompositePackaging {
  code: "84",
  label: "C/PAP",
  predominantMaterial: "PAP",
  secondaryMaterials: ["Plastic", "Aluminum"],
  typicalApplication: "Tetra Pak",
  disposalFraction: "FKN"
}

DisposalRule {
  condition: "Form = Beverage Bottle AND Volume < 3.0L",
  destination: "Pfandautomat",
  outcomeType: "bottle-to-bottle"
}

Region {
  name: "Berlin",
  hasWertstofftonne: true,
  dualSystem: "BSR"
}

(PackagingMaterial)-[:DISPOSED_IN {region: "default"}]->(WasteStream)
(PackagingMaterial)-[:DISPOSED_IN {region: "Munich", override: true}]->(WasteStream)
(PackagingMaterial)-[:HAS_RULE]->(DisposalRule)-[:ROUTES_TO]->(WasteStream)
(PackagingMaterial)-[:MAPS_TO_AVV]->(AVVCode)
(CompositePackaging)-[:HAS_PREDOMINANT]->(PackagingMaterial)
(CompositePackaging)-[:HAS_SECONDARY]->(PackagingMaterial)
(PackagingObject)-[:LICENSED_VIA]->(DualSystem)
```

### API Quick Reference

```bash
# === Regulatory ===

# EU Decision 97/129/EC (legal basis for codes)
curl "https://eur-lex.europa.eu/eli/dec/1997/129/oj/eng"

# ZSVR Mindeststandard 2025 (recyclability assessment)
curl "https://www.verpackungsregister.org/fileadmin/files/Mindeststandard/Mindeststandard_VerpackG_2025.pdf"

# === Taxonomies ===

# Open Food Facts - Packaging materials taxonomy (hierarchical DAG)
curl "https://raw.githubusercontent.com/openfoodfacts/openfoodfacts-server/main/taxonomies/packaging_materials.txt"

# GS1 - PackagingMaterialTypeCode vocabulary
curl "https://ref.gs1.org/voc/PackagingMaterialTypeCode"

# Wikidata - Get recycling code for PET (Q144256)
curl "https://www.wikidata.org/wiki/Special:EntityData/Q144256.json"

# === Recyclability Assessment ===

# RecyClass - Design for Recycling Guidelines
# Navigate: https://recyclass.eu/protocols-guidelines/design-for-recycling-guidelines/

# Cyclos-HTP - CHI Recyclability Standard
# Navigate: https://www.cyclos-htp.de/publications/chi-recyclability-standard/

# === Consumer Guides ===

# BSR Berlin - Abfalltrenntabelle
curl "https://www.bsr.de/assets/downloads/abfalltrenntabelle-din-a4.pdf"

# AWM Munich - Trennliste (English)
curl "https://www.awm-muenchen.de/fileadmin/Dateien_pdfs/Trennliste_englisch.pdf"

# AWM Munich - Trennliste (German with images)
curl "https://www.awm-muenchen.de/fileadmin/Dateien_pdfs/Trennliste_Bilder_A4.pdf"

# BSR - Abfälle richtig trennen brochure
curl "https://www.bsr.de/assets/downloads/broschuere-abfaelle-richtig-trennen.pdf"

# === Industrial AVV ===

# Zentek - AVV 15 01 packaging codes
# Navigate: https://www.zentek.de/avv/schluessel/1501/

# Berlin Recycling - AVV 15 01 detailed
# Navigate: https://www.berlin-recycling.de/wissen/avv/1501
```

### Data Ingestion Strategy

1. **Material Skeleton:** Parse 97/129/EC to create 99 PackagingMaterial nodes (01-99)
2. **Taxonomy Overlay:** Ingest Open Food Facts taxonomy → build `skos:broader/narrower` hierarchy (en:plastic → en:pet → en:rpet)
3. **GS1 Mapping:** Link codes to GS1 GDSN vocabulary → enable B2B product data integration
4. **Properties:** Add density, floatability, NIR detectability from polymer science sources
5. **Disposal Rules:** Import BSR/AWM rules as conditional DisposalRule nodes with region qualifiers
6. **Composite Logic:** Parse C/ codes → link to constituent materials with mass percentage attributes
7. **AVV Crosswalk:** Map consumer codes (01-99) → industrial AVV codes (15 01 01-07)

---

## Report 08: Batteries & Accumulators

### Key Finding: Safety-Critical Classification

Battery disposal is a **safety-critical** vertical where misclassification can lead to **thermal runaway fires**. Unlike paper or packaging, batteries require granular chemical identification to assess fire/explosion risk. The graph must distinguish items sharing similar form factors (AA alkaline vs AA lithium) based on IEC codes and chemistry.

**Three-Layer Data Challenge:**
1. **Legal Entity Resolution:** Who made it? → Stiftung EAR (Brand → Producer → BattG Category)
2. **Physical Identification:** What is it made of? → IEC 60086 codes (CR/LR/ER prefix → Chemistry → Hazard)
3. **Conditional Disposal Logic:** What state is it in? → VdS 3103 + University rules (Intact vs Damaged → Green/Yellow/Red bin)

### Primary Sources: Regulatory & Legal Framework

| Source | URL | Format | License | Relevance | Coverage |
|--------|-----|--------|---------|-----------|----------|
| **Stiftung EAR (BattG Registry)** | https://www.ear-system.de/ear-verzeichnis/battghersteller | CSV/XLSX/XML | Public | 5/5 | All legal battery producers/brands in Germany |
| **EU Battery Regulation 2023/1542** | https://eur-lex.europa.eu/legal-content/EN/TXT/PDF/?uri=CELEX:32023R1542 | PDF/HTML/XML | Public | 5/5 | New 5-category schema, Battery Passport mandate |
| **BattG (Batteriegesetz)** | https://www.gesetze-im-internet.de/battg_2009/ | HTML/PDF | Public | 5/5 | German implementation, producer responsibility |

### Primary Sources: Dangerous Goods & Safety Logic

| Source | URL | Format | License | Relevance | Coverage |
|--------|-----|--------|---------|-----------|----------|
| **BAM Dangerous Goods Database (DGG)** | https://www.dgg.bam.de/quickinfo/de/ | Web (HTML) | Public | 5/5 | UN Numbers, ADR class, Special Provisions (SV 376) |
| **VdS 3103 (Lithium-Batterien)** | https://vds.de/fileadmin/Website_Content_Images/VdS_Publikationen/vds_3103_web.pdf | PDF | Free | 5/5 | Storage thresholds (<100Wh, >100Wh, >12kg) |
| **ADR 2025 Updates** | https://boxlab-services.com/en/adr-2025-new-un-numbers-for-batteries-and-their-safety-regulations/ | HTML | Public | 4/5 | New UN 3551/3552 for Sodium-Ion batteries |
| **BG Verkehr Lithium Guide** | https://www.bg-verkehr.de/arbeitssicherheit-gesundheit/themen/gefahrgut/unfallgefahren/lithiumbatterien | HTML/PDF | Public | 4/5 | Transport safety, fire response procedures |

### Primary Sources: Technical Identification (IEC Standards)

| Source | URL | Format | License | Relevance | Coverage |
|--------|-----|--------|---------|-----------|----------|
| **IEC 60086 Primary Batteries** | https://webstore.iec.ch/en/publication/23002 | PDF (Paid) | Commercial | 5/5 | Official nomenclature standard |
| **Wikipedia List of Battery Sizes** | https://en.wikipedia.org/wiki/List_of_battery_sizes | HTML | CC BY-SA | 5/5 | IEC code tables (free derivative) |
| **Panasonic Lithium Handbook** | https://energy.panasonic.com/dam/master/pdf/eu/material/Panasonic_Lithium_Handbook_2025.pdf | PDF | Free | 5/5 | Chemistry codes, voltage, dimensions |

### Primary Sources: Manufacturer Safety Data Sheets (SDS)

| Source | URL | Format | License | Relevance | Coverage |
|--------|-----|--------|---------|-----------|----------|
| **Energizer Technical Library** | https://data.energizer.com/ | PDF | Free | 5/5 | Chemical composition, extinguishing media |
| **Duracell SDS Library** | https://duracell.com/techlibrary/safety-data-sheets | PDF | Free | 5/5 | Material percentages, AVV codes |
| **Panasonic Technical Data** | https://energy.panasonic.com/ | PDF | Free | 4/5 | Industrial battery specifications |

### Primary Sources: Collection Infrastructure

| Source | URL | Format | License | Relevance | Coverage |
|--------|-----|--------|---------|-----------|----------|
| **Stiftung GRS Batterien** | https://www.stiftung-grs.de/downloads/ | PDF | Public | 5/5 | Collection statistics, compliance reports |
| **GRS Sicherheitsstandard** | https://www.grs-batterien.de/fileadmin/Downloads/Sicherheit/GRS_Checkliste_Kennzeichnungspflicht_Sep2023_ws23200.pdf | PDF | Free | 5/5 | Green/Yellow/Red bin system, taping rules |
| **RWTH Aachen Entsorgungshandbuch** | https://www.rwth-aachen.de/global/show_document.asp?id=aaaaaaaaaazyhtp | PDF | Public | 5/5 | SCA central storage, H2/E1 categories |
| **Uni Heidelberg BT-26** | https://www.uni-heidelberg.de/md/zentral/universitaet/beschaeftigte/service/sicherheit/bt-26.pdf | PDF | Public | 5/5 | Lab battery disposal protocols |

### Secondary/Verification Sources

| Source | URL | Purpose |
|--------|-----|---------|
| IPA (Abfallbewertung) | https://www.abfallbewertung.org/ipa/repgen.php?report=ipa&char_id=1606_Batt | AVV codes for battery waste (16 06 XX) |
| Kaggle Garbage Classification | https://www.kaggle.com/datasets/mostafaabla/garbage-classification | Visual ID (generic "battery" class) |
| TÜV SÜD Battery Regulation Guide | https://www.tuvsud.com/en-us/-/media/regions/us/pdf-files/whitepaper-report-e-books/tuvsud_overview-of-eu-battery-regulations.pdf | EU regulation summary |
| TÜV Rheinland Battery Passport | https://www.tuv.com/landingpage/en/eu-new-battery-regulation-eu-2023-1542/ | Battery Passport requirements |
| Euromot FAQ | https://www.euromot.eu/wp-content/uploads/2025/05/2025-05-02-EUROMOT-FAQ-Batteries-Regulation.pdf | Industry FAQ on EU regulation |
| Wikidata (legacy brands) | https://www.wikidata.org/ | Defunct brand resolution fallback |

### Key Integration Notes

**Stiftung EAR (Brand → Legal Category):**
- **Essential** for entity resolution: Maps consumer brand (e.g., "Varta") → Producer → BattG Category
- Key fields: `Hersteller`, `Marke`, `Batt-Reg.-Nr. DE`, `Batteriekategorie`
- Category values: `Gerätebatterien` (Portable), `Industriebatterien` (Industrial), `Fahrzeugbatterien` (Automotive)
- **Logic:** If brand only in Industriebatterien → "This is an industrial battery. Do not use standard hallway bins."
- **Gap:** Does NOT contain chemistry data (Li-ion vs NiMH) - covers "Who" and "Legal What", not "Physical What"

**EU Battery Regulation 2023/1542 (New Categories):**
- **Critical update:** Introduces **LMT Battery** (Light Means of Transport) category for e-scooters, e-bikes
- Old schema: Portable / Automotive / Industrial
- New schema: **Portable / SLI / LMT / EV / Industrial**
- **Battery Passport:** Mandatory for batteries >2kWh and LMT batteries (provision `hasBatteryPassport: Boolean`)
- LMT Definition: "Sealed, < 25kg, designed for traction of wheeled vehicles"

**IEC 60086 Nomenclature (Code → Chemistry → Hazard):**
This is the "Rosetta Stone" for battery identification. The prefix determines chemistry:

| Prefix | Chemistry | Hazard Flag | Voltage | Graph Property |
|--------|-----------|-------------|---------|----------------|
| **R** | Zinc-Carbon | Low | 1.5V | `chemistry: "ZnC"` |
| **LR** | Alkaline | Low | 1.5V | `chemistry: "Alkaline"` |
| **CR** | Lithium Manganese Dioxide | **Fire Risk** | 3.0V | `chemistry: "Li-MnO2", tapePoles: true` |
| **SR** | Silver Oxide | Low (Resource) | 1.55V | `chemistry: "AgO"` |
| **PR** | Zinc Air | Low | 1.4V | `chemistry: "ZnAir"` |
| **ER** | Lithium Thionyl Chloride | **Toxic/Fire** | 3.6V | `chemistry: "Li-SOCl2", hazardous: true` |
| **FR** | Lithium Iron Disulfide | **Fire Risk** | 1.5V | `chemistry: "Li-FeS2", tapePoles: true` |

**Dimension encoding:** In "CR2032", `20` = 20mm diameter, `32` = 3.2mm height
- Chatbot logic: "Measure the diameter of your coin cell to help identify it"

**BAM Dangerous Goods (UN Numbers):**
- Essential for `HazardClass` and `DisposalRule` nodes
- Key UN numbers for batteries:

| UN Number | Proper Shipping Name | Graph Logic |
|-----------|---------------------|-------------|
| **3480** | Lithium Ion Batteries | Standard rechargeable Li-ion rule |
| **3481** | Lithium Ion Batteries in Equipment | "Can I throw away the whole laptop?" |
| **3090** | Lithium Metal Batteries | Standard primary lithium rule |
| **3091** | Lithium Metal Batteries in Equipment | Small electronics logic |
| **2794** | Batteries, wet, filled with acid | Car/lab lead-acid (Corrosive hazard) |
| **3551** | Sodium Ion Batteries (ADR 2025) | **NEW** - emerging tech path |
| **3552** | Sodium Ion Batteries in Equipment | **NEW** - ADR 2025 |

- **Special Provision SV 376:** Applies to "Damaged/Defective Batteries" → mandatory specific packaging
- **Accessibility challenge:** No bulk API - requires query-by-query or scraping

**VdS 3103 (Energy Threshold → Disposal Rule):**
Insurance guidelines define operational storage/disposal logic based on energy capacity:

| Category | Threshold | Rule | Examples |
|----------|-----------|------|----------|
| **Low Power** | < 100 Wh | Standard collection, Green bin | Laptops, phones, power banks |
| **Medium Power** | > 100 Wh, < 12 kg | Separate storage, 2.5m fire distance | E-bike batteries, large power tools |
| **High Power** | > 12 kg | Strict separation, consultation required | Industrial UPS, EV modules |

- **Schema enhancement:** `WasteItem.energyCapacity (Wh)` property required
- Logic: `IF energyCapacity > 100 THEN route to specialized facility`

**GRS Color System (Container Logic):**
The GRS (Gemeinsames Rücknahmesystem) defines the tricolor bin system:

| Color | Contents | Condition | Action |
|-------|----------|-----------|--------|
| **Green** | Standard portable batteries | Intact | Normal collection |
| **Yellow** | High-energy lithium | Intact, tape poles | Pole isolation required |
| **Red** | Damaged/defective lithium | Swollen, hot, leaking | Sand/pyrobubbles, contact safety officer |

- **Critical rule:** IF Condition = "Damaged" → "Do not use building bin. Contact Safety Officer immediately."

**University-Specific Rules:**
- **RWTH Aachen:** SCA (Service-Center Abfallwirtschaft) as central intermediate storage, H2/E1 classification
- **Uni Heidelberg:** BT-26 directive for lab battery disposal
- These exceed municipal requirements - map Building → CollectionPoint → specific Instructions

### Critical Graph Logic

```
1. **Brand Resolution (Stiftung EAR):**
   User says "Bosch" →
   Query EAR: Marke = "Bosch" →
   IF Category = "Industriebatterien" ONLY
     → "This appears to be an industrial/power tool battery. Do not use standard bins."
   ELSE IF Category includes "Gerätebatterien"
     → Standard portable battery path

2. **IEC Code Parsing:**
   User shows "CR2032" →
   Parse prefix "CR" →
   Chemistry = "Lithium Manganese Dioxide" →
   Hazard = "Fire Risk" →
   Instruction = "Tape both poles before disposal"

3. **Energy Threshold (VdS 3103):**
   User has "e-bike battery" →
   Typical capacity > 400 Wh →
   energyCapacity > 100 Wh threshold →
   Route = "Yellow bin with pole taping" OR "Dedicated e-bike station"

4. **Damage State Detection (Diagnostic Dialog):**
   System cannot detect physical state - must ask:
   - "Is the battery swollen or deformed?"
   - "Is it hot to touch?"
   - "Is there any leakage or smell?"
   IF any = "Yes" →
     Route = "RED - Do not dispose. Contact [Safety Officer Phone]"
     Instruction = "Place in sand or fire-safe container"

5. **Sodium-Ion (ADR 2025 Future-Proofing):**
   IF Chemistry = "Sodium-Ion" →
     UN_Number = 3551 →
     Temporary rule: "Treat as Lithium until specific protocols available"
```

### AVV Codes for Batteries (Chapter 16 06)

| AVV Code | Description | Graph Use |
|----------|-------------|-----------|
| 16 06 01* | Lead batteries | Automotive/UPS (Hazardous) |
| 16 06 02* | Ni-Cd batteries | Rechargeable (Hazardous - cadmium) |
| 16 06 03* | Mercury-containing batteries | Legacy button cells (Hazardous) |
| 16 06 04 | Alkaline batteries | Modern consumer (Non-hazardous) |
| 16 06 05 | Other batteries | Catch-all for mixed chemistry |
| 16 06 06* | Separately collected battery electrolyte | Lab context (Hazardous) |

**Note:** Asterisk (*) = Hazardous waste → triggers special handling rules

### Graph Schema Recommendations

```
Battery {
  uid: String,
  iecCode: String,            // "CR2032", "LR6"
  chemistry: String,          // "Li-MnO2", "Alkaline", "NiMH"
  hazardFlag: String,         // "Low", "Fire Risk", "Toxic/Fire"
  nominalVoltage: Float,      // 3.0, 1.5
  energyCapacity: Float?,     // Wh (for VdS threshold logic)
  hasBatteryPassport: Boolean // EU 2023/1542 requirement
}

BatteryBrand {
  name: String,               // "Varta", "Duracell"
  earRegistrationNumber: String,
  battgCategory: [String]     // ["Gerätebatterien", "Industriebatterien"]
}

(Battery)-[:MANUFACTURED_BY]->(BatteryBrand)
(Battery)-[:TRANSPORTED_AS]->(UN_Number)
(Battery)-[:HAS_AVV_CODE]->(AVVCode)
(Battery)-[:DISPOSED_IN {condition: "intact"}]->(Container {color: "Green"})
(Battery)-[:DISPOSED_IN {condition: "damaged"}]->(Container {color: "Red"})
(Battery)-[:HAS_RULE]->(DisposalRule)-[:IF_CONDITION {energyCapacity: ">100Wh"}]->(Instruction)
```

### Gaps Identified

1. **Real-Time State Detection:** No database can tell if user's battery is damaged - requires diagnostic dialog
2. **Product-Integrated Batteries:** No open database linking consumer products ("Singing birthday card") to battery type
3. **Sodium-Ion Infrastructure:** ADR 2025 creates UN numbers, but disposal infrastructure lags - temporary "treat as lithium" rule needed
4. **Legacy/Defunct Brands:** EAR tracks active producers only - Wikidata fallback for old brands

### Data Ingestion Strategy

1. **Phase 1 - Regulatory Skeleton:** Ingest Stiftung EAR CSV → Brand/Producer lookup with BattG categories
2. **Phase 2 - Physical Decoder:** Build IEC nomenclature graph (CR→Chemistry→Hazard), parse manufacturer SDS for material percentages
3. **Phase 3 - Safety Logic:** Encode VdS 3103 thresholds as DisposalRule conditions, map UN Numbers from BAM
4. **Phase 4 - University Infrastructure:** Import RWTH/Heidelberg rules, map to Building → CollectionPoint

### API Quick Reference (Report 08)

```bash
# Stiftung EAR - Download battery producer registry (CSV)
# Navigate to: https://www.ear-system.de/ear-verzeichnis/battghersteller
# Use filters → Export CSV/XLSX

# BAM Dangerous Goods - Query by UN number (web interface)
# https://www.dgg.bam.de/quickinfo/de/
# Example: Search "3480" for Lithium Ion Batteries

# VdS 3103 - Download PDF directly
curl -O "https://vds.de/fileadmin/Website_Content_Images/VdS_Publikationen/vds_3103_web.pdf"

# Energizer SDS - Alkaline battery data sheet
curl -O "https://data.energizer.com/wp-content/uploads/2023/01/Alk-Min-AIS-SDS-01-23-Energizer.pdf"

# GRS Safety Checklist
curl -O "https://www.grs-batterien.de/fileadmin/Downloads/Sicherheit/GRS_Checkliste_Kennzeichnungspflicht_Sep2023_ws23200.pdf"

# IPA - AVV battery codes lookup
# https://www.abfallbewertung.org/ipa/repgen.php?report=ipa&char_id=1606_Batt&lang_id=de

# Wikipedia - IEC battery codes (structured tables)
curl "https://en.wikipedia.org/wiki/List_of_battery_sizes" | grep -E "CR|LR|SR|PR|ER"
```

---

## Report 09: Wood & Furniture

### Key Finding: Historical Forensic Classification

Waste wood disposal requires **historical forensic analysis** to determine contamination status. Unlike batteries (where IEC codes provide identification), wood contamination is **invisible** and must be inferred from:
1. **Temporal Context:** When was it made/installed?
2. **Geographic Context:** Where was it made? (DDR vs BRD)
3. **Usage Context:** Indoor vs Outdoor application?

**The Fundamental Risk:** Misclassifying hazardous A IV wood (treated with DDT, PCP, Lindane) as recyclable A I/II causes **dioxin release** during processing. Burning contaminated wood in domestic fireplaces releases some of the most toxic compounds known.

**Four-Layer Data Challenge:**
1. **Regulatory Classification:** AltholzV Categories A I → A IV + PCB-wood
2. **Historical Forensics:** DDR (Hylotox/DDT) vs BRD (Xylamon/PCP) preservative regimes
3. **Visual/Sensory Identification:** Color, smell, texture → treatment inference
4. **Material Composition:** Solid wood vs Composite vs WPC hybrids

### Primary Sources: Regulatory Framework

| Source | URL | Format | License | Relevance | Coverage |
|--------|-----|--------|---------|-----------|----------|
| **Altholzverordnung (AltholzV)** | https://www.gesetze-im-internet.de/altholzv/ | HTML/PDF | Public | 5/5 | Categories A I-IV definitions, disposal mandates |
| **AltholzV Anhang III** | https://www.gesetze-im-internet.de/altholzv/anhang_iii.html | HTML | Public | 5/5 | Default classification for common wood types |
| **AVV (Wood Codes)** | https://www.gesetze-im-internet.de/avv/ | HTML/PDF | Public | 5/5 | Waste codes 03 01, 15 01, 17 02, 20 01/03 |
| **PCP-Verbotsverordnung (1989)** | https://www.gesetze-im-internet.de/chemverbotsv/ | HTML | Public | 4/5 | Hard cutoff date for PCP contamination |
| **17th BImSchV** | https://www.gesetze-im-internet.de/bimschv_17_2013/ | HTML | Public | 4/5 | Emission limits for A IV incineration |

### Primary Sources: Material & Environmental Data

| Source | URL | Format | License | Relevance | Coverage |
|--------|-----|--------|---------|-----------|----------|
| **Ökobaudat API** | https://www.oekobaudat.de/api/datasets | ILCD XML/JSON | Public | 5/5 | LCA data, material composition, EPDs (EN 15804) |
| **IPA Abfallbewertung** | https://www.abfallbewertung.org/ | HTML | Public | 5/5 | Altholz characterization, AVV code lookup |
| **LfU Bayern Schadstoffratgeber** | https://www.lfu.bayern.de/abfall/schadstoffratgeber_gebaeuderueckbau/ | PDF | Public | 5/5 | PCP, DDT, Lindane identification guides |
| **Umweltbundesamt AltholzV Evaluation** | https://www.umweltbundesamt.de/publikationen/ | PDF | Public | 4/5 | Technical guidance, modernization proposals |

### Secondary Sources: Historical Preservatives Database

| Source | URL | Format | License | Relevance | Coverage |
|--------|-----|--------|---------|-----------|----------|
| **Holzfragen.de HSM Historie** | https://www.holzfragen.de/seiten/hsm_historie.html | HTML | Public | 5/5 | DDR/BRD preservative trade names, timelines |
| **LaGUS DDT/Lindane Guide** | https://www.lagus.mv-regierung.de/ | PDF | Public | 4/5 | Health implications, East German legacy |
| **WRAP Wood Identification Tools** | https://www.wrap.ngo/resources/tool/tools-wood-reprocessors-identify-contaminated-wood | PDF | Public | 4/5 | PAN stain test, visual identification |
| **BAV Regelvermutung Update** | https://altholzverband.de/ | PDF | Public | 4/5 | Industry default classifications |

### Secondary Sources: University & Operational

| Source | URL | Format | License | Relevance | Coverage |
|--------|-----|--------|---------|-----------|----------|
| **Uni Freiburg Abfall-ABC** | https://uni-freiburg.de/sicherheit-umweltschutz/abfall-abc/ | HTML | Public | 5/5 | Local disposal rules, contact workflows |
| **Uni Giessen Abfall-ABC** | https://www.uni-giessen.de/de/org/admin/dez/e/3/logistik-aussenanlagen/Abfallwirtschaft/ | HTML | Public | 5/5 | Wood → Container mapping, contacts |
| **Michigan CSS Furniture LCA** | https://css.umich.edu/publications/research-publications/life-cycle-assessment-office-furniture-products | PDF | Academic | 3/5 | Desk/chair material composition |

### Tertiary Sources: Semantic & Taxonomic

| Source | URL | Format | License | Relevance | Coverage |
|--------|-----|--------|---------|-----------|----------|
| **Wikidata (Chemicals)** | https://www.wikidata.org/ | SPARQL | CC0 | 4/5 | Q-IDs for DDT, PCP, Lindane (semantic linking) |
| **Wikidata Q7302557** | https://www.wikidata.org/wiki/Q7302557 | JSON-LD | CC0 | 3/5 | "Reclaimed lumber" entity |

---

### AltholzV Category System (Decision Tree Root)

| Category | Definition | Typical Items | Disposal | AVV Codes |
|----------|-----------|---------------|----------|-----------|
| **A I** | Natural/mechanically processed, NO chemical treatment | Euro-pallets (EUR/EPAL stamp), fruit crates, cable drums (>1989) | Material recycling (particleboard) | 15 01 03, 17 02 01, 03 01 05 |
| **A II** | Glued, painted, coated (NON-halogenated, NO biocides) | Interior doors, laminate flooring, particleboard furniture, shelving | Material/energy recovery | 17 02 01, 20 01 38 |
| **A III** | Halogenated coatings (PVC, Chlorine compounds) | PVC-foil furniture, older kitchen cabinets, acoustic panels | Energy recovery (special plants) | 20 01 38 |
| **A IV** | Biocidal preservatives, HAZARDOUS | Railway sleepers, window frames, fences, outdoor decking, telegraph poles | Hazardous waste incineration | **17 02 04***, **03 01 04*** |
| **PCB** | PCB content >50 mg/kg | Acoustic panels, insulation (1960s-70s) | Special hazardous disposal | **17 06 03*** |

**Critical Graph Logic:** Asterisk (*) in AVV code = HAZARDOUS. System must flag these for special handling.

---

### Historical Preservatives Mapping (DDR vs BRD)

The **1989/1990 reunification** is the key temporal boundary. Wood from buildings constructed before this date requires geographic provenance analysis.

#### DDR (East Germany) Legacy Preservatives

| Trade Name | Active Ingredients | Typical Application | Usage Period | Hazard |
|------------|-------------------|---------------------|--------------|--------|
| **Hylotox 59** | DDT + Lindane | Roof trusses, furniture, **indoor** | Until ~1989 | A IV |
| **Hylotox IP** | Pentachlorophenol (PCP) | Exterior structural wood | 1960s-1980s | A IV |
| **Dohnalit** | Fluorine, Chromium salts | Roofs, structural wood | 1960s-1990 | A IV |
| **Kombinal TO** | TBTO (Organotin), Thiram | Construction, windows | 1970s-1980s | A IV |
| **Ull-Salts (U-Salze)** | Fluorine, Chromium | Structural timber | 1960s-1980s | A IV |

**Visual Cue (DDR):** Pink or green hue from salt treatments. Oily/dark appearance from Hylotox.

#### BRD (West Germany) Legacy Preservatives

| Trade Name | Active Ingredients | Typical Application | Usage Period | Hazard |
|------------|-------------------|---------------------|--------------|--------|
| **Xylamon** | PCP, Lindane | Structural timber, furniture | Until ~1978/84 | A IV |
| **Xyladecor** | PCP, Lindane | Exterior cladding, fences | Until ~1978/84 | A IV |
| **Donarolit** | Dinitrophenol | Construction timber | Mid-20th century | A IV |
| **Basilit** | Fluorine, Chromium, **Arsenic** | Heavy construction | 1920s-1980s | A IV |
| **Carbolineum** | Tar oils (PAHs) | Sleepers, poles, sheds | Until ~1991 | A IV |

**Visual/Sensory Cues (BRD):**
- **Hoarfrost crystals ("Raureif")** on old dark wood → PCP/Lindane blooming → **DANGER**
- **Tar smell** ("railway track" odor) → Carbolineum → A IV
- **Oily/sticky surface** (warm weather) → Tar oil → A IV

---

### AVV Code Mapping for Wood Waste

| Physical Object | Condition/Context | AVV Code | AltholzV | Hazardous? |
|-----------------|-------------------|----------|----------|------------|
| Sawdust/Shavings | Pure, untreated | 03 01 05 | A I | No |
| Sawdust/Shavings | Contaminated | **03 01 04*** | A IV | **YES** |
| Pallet (EUR/EPAL) | Solid wood, HT stamp | 15 01 03 | A I | No |
| Cable Drum | Manufacturing >1989 | 15 01 03 | A I | No |
| Cable Drum | Manufacturing <1989 | **17 02 04*** | A IV | **YES** |
| Construction Wood | Untreated/Painted (Interior) | 17 02 01 | A I/A II | No |
| Construction Wood | Impregnated/Exterior | **17 02 04*** | A IV | **YES** |
| Window Frame | All types (standard presumption) | **17 02 04*** | A IV | **YES** |
| Railway Sleeper | Tar oil (Creosote) | **17 02 04*** | A IV | **YES** |
| Insulation Board | PCB-containing | **17 06 03*** | PCB | **YES** |
| Furniture (General) | Mixed bulky waste | 20 03 07 | Sperrmüll | No |
| Furniture (Wood) | Separately collected | 20 01 38 | A I-A III | No |

---

### Visual Identification Matrix (Chatbot Decision Support)

| Visual Feature | Probable Treatment | Likely Category | Recommended Action |
|----------------|-------------------|-----------------|-------------------|
| Natural, bright wood | None | A I | Material recycling |
| Painted (white/color) | Lacquer/Acrylic | A II | Energy recovery |
| **Green/olive tint** | Copper salts (CCA, ACQ) | **A IV** | Hazardous disposal |
| **Black/dark brown, oily** | Tar oil (Creosote) | **A IV** | Hazardous disposal |
| Plastic film (wood grain) | PVC/Melamine | A III / A II | Energy recovery |
| **Hoarfrost crystals** | PCP/Lindane (legacy) | **A IV** | **DANGER - Avoid contact** |
| **Incision marks (rows)** | Pressure impregnation | **A IV** | Hazardous disposal |
| EUR/EPAL stamp | Quality-controlled pallet | A I | Material recycling |
| HT stamp | Heat treated (no chemicals) | A I | Material recycling |
| MB stamp | Methyl bromide fumigation | A IV | Hazardous disposal |

---

### Sensory Identification Questions (Diagnostic Dialog)

Fessi must ask these questions for uncertain wood items:

**Smell Test:**
```
"Does the wood have a strong tar-like or mothball smell?"
→ YES: Carbolineum/Tar Oil → A IV (definitive)
```

**Touch Test:**
```
"Is the surface sticky or oily, especially in warm weather?"
→ YES: Tar Oil → A IV

"Is the surface rough/fibrous (sawn) or smooth (planed)?"
→ Rough: Construction timber (context-dependent)
→ Smooth: Furniture/interior (likely A II)
```

**Sight Test:**
```
"Do you see a greenish or olive tint?"
→ YES: Copper salts → A IV

"Do you see white frost-like crystals on old dark wood?"
→ YES: PCP/Lindane blooming → A IV (HIGH RISK - avoid contact)

"Are there rows of small, regular cuts/holes on the surface?"
→ YES: Pressure impregnation → A IV
```

---

### Temporal Logic Rules (SWRL-style)

```
# The "Outdoor" Rule
WoodObject(?w) ^ usedInContext(?w, "Outdoor") → legallyClassifiedAs(?w, Category_AIV)

# The "DDR Roof Timber" Rule (Historical Inference)
WoodObject(?w) ^ originRegion(?w, "DDR") ^ manufactureYear(?w, ?y) ^
swrlb:lessThan(?y, 1990) ^ isPartOf(?w, "RoofStructure") →
hasProbabilityOfContamination(?w, "High_DDT_Hylotox")

# The "BRD Pre-1989" Rule
WoodObject(?w) ^ originRegion(?w, "BRD") ^ manufactureYear(?w, ?y) ^
swrlb:lessThan(?y, 1989) ^ hasApplication(?w, "Structural") →
hasProbabilityOfContamination(?w, "High_PCP_Xylamon")

# The "PVC Coating" Rule
Furniture(?f) ^ hasCoating(?f, "PVC") → legallyClassifiedAs(?f, Category_AIII)

# The "Window Frame" Default Rule
WoodObject(?w) ^ isType(?w, "WindowFrame") → legallyClassifiedAs(?w, Category_AIV)
```

---

### Graph Schema Recommendations

```
WasteWood {
  uid: String,
  name: String,
  altholzCategory: Enum["A_I", "A_II", "A_III", "A_IV", "PCB"],
  avvCode: String,                    // "17 02 04*"
  isHazardous: Boolean,               // derived from asterisk
  usageContext: Enum["Indoor", "Outdoor", "Unknown"],
  manufactureYear: Int?,              // for temporal logic
  originRegion: Enum["DDR", "BRD", "Post-1990", "Unknown"],
  probableTreatment: String?,         // "Hylotox 59", "Xylamon"
  visualCues: [String]                // ["green_tint", "tar_smell"]
}

Preservative {
  uid: String,
  tradeName: String,                  // "Hylotox 59"
  activeIngredients: [String],        // ["DDT", "Lindane"]
  region: Enum["DDR", "BRD", "Both"],
  usagePeriod: String,                // "Until ~1989"
  hazardClass: String,                // "A IV"
  wikidataQID: String?                // "Q163648" (DDT)
}

VisualCue {
  uid: String,
  description: String,                // "Green/olive tint"
  indicatesTreatment: String,         // "Copper salts (CCA, ACQ)"
  impliesCategory: String,            // "A_IV"
  riskLevel: Enum["Low", "Medium", "High", "DANGER"]
}

(WasteWood)-[:PROBABLY_TREATED_WITH]->(Preservative)
(WasteWood)-[:HAS_VISUAL_CUE]->(VisualCue)
(WasteWood)-[:HAS_AVV_CODE]->(AVVCode)
(WasteWood)-[:LEGALLY_CLASSIFIED_AS]->(AltholzCategory)
(Preservative)-[:CONTAINS_CHEMICAL]->(Chemical)
(Chemical)-[:WIKIDATA_LINK]->(WikidataEntity)
```

---

### Sperrmüll (Bulky Waste) Composition Data

For furniture/wood volume estimation:

| Metric | Value | Source |
|--------|-------|--------|
| Wood fraction in bulky waste | 17-30% by weight | Sperrmüll studies |
| Furniture share of wood fraction | >50% by volume | BAV analysis |
| Recyclable if properly sorted | Up to 82% | Sperrmüll research |
| Solid wood (A I) trend | Decreasing | "Fast furniture" effect |

**Furniture Deconstruction Guidance:**
- Office desk: Unscrew metal legs → Metal bin; wooden top → A II container
- Wardrobe: Check for PVC coating (glossy plastic) → A III if present
- Kitchen cabinet: High probability of PVC foil → check carefully

---

### Identified Gaps

| Gap | Impact | Potential Solution |
|-----|--------|-------------------|
| **No visual AI for wood** | Cannot automate green tint/crystal detection | Train CV model on pressure-treated wood images |
| **Building age database** | Cannot auto-infer DDR/BRD provenance | Integrate with building registry APIs |
| **WPC identification** | Wood-Plastic Composites rejected by recyclers | Add WPC detection questions to dialog |
| **Formaldehyde levels** | Legacy particleboard pre-E1 standard | Query furniture age, default to A II |

---

### Data Ingestion Strategy

**Phase 1: Regulatory Foundation**
1. Parse AltholzV categories into `AltholzCategory` nodes
2. Load AVV wood codes (03 01, 15 01, 17 02, 20 01/03) with hazardous flags
3. Create default classification rules from Anhang III

**Phase 2: Historical Preservatives**
1. Build `Preservative` nodes from Holzfragen.de data
2. Link to Wikidata Q-IDs for chemical substances
3. Create temporal/geographic inference rules

**Phase 3: Visual Identification**
1. Create `VisualCue` nodes with treatment implications
2. Build diagnostic dialog decision tree
3. Integrate sensory questions (smell, touch, sight)

**Phase 4: Operational Integration**
1. Scrape university Abfall-ABCs for local rules
2. Link to facility contacts and container locations
3. Add Ökobaudat EPD data for LCA queries

---

## Report 10: Facility Infrastructure

### Key Finding: Federated Data Architecture

German waste infrastructure data exists in a **highly fragmented ecosystem** driven by the federalized nature of German waste administration (Kreislaufwirtschaftsgesetz). No single federal API provides a unified view—instead, data exists in **three distinct strata**:

1. **Hyper-Local Authority Layer**: Municipal open data portals (Berlin, Hamburg, Munich, Cologne)
2. **Crowdsourced Global Layer**: OpenStreetMap with standardized tagging schema
3. **Community-Aggregated Layer**: HACS framework + reverse-engineered mobile app backends

**The "SaaS Oligopoly"**: A hidden standardization layer exists through market consolidation. Providers like **Abfall+**, **MyMüll**, **JumoMind**, and **Athos** serve a significant percentage of German municipalities. Identifying the software provider used by a target city is often the key to unlocking its structured data.

---

### Primary Sources: Geospatial Basemap (OSM)

| Source | Priority | Format | Key Attributes |
|--------|----------|--------|----------------|
| **OpenStreetMap (Overpass API)** | 5/5 | JSON/XML | Universal basemap, standardized tags, 100% Germany coverage |
| **OSM Wiki (Tagging Schema)** | 4/5 | HTML | Ontology documentation for waste tags |

**OSM Tagging Ontology for Waste Infrastructure:**

| Tag | Values | Description |
|-----|--------|-------------|
| `amenity` | `recycling` | Root classifier for all recycling infrastructure |
| `recycling_type` | `container` / `centre` | Distinguishes unattended bins from staffed Wertstoffhöfe |
| `recycling:glass` | `yes` | Glass acceptance |
| `recycling:paper` | `yes` | Paper/cardboard acceptance |
| `recycling:clothes` | `yes` | Textile collection |
| `recycling:hazardous_waste` | `yes` | Schadstoff acceptance |
| `recycling:batteries` | `yes` | Battery collection |
| `recycling:electrical_appliances` | `yes` | WEEE acceptance |
| `recycling:green_waste` | `yes` | Garden waste acceptance |
| `recycling:waste_bulky` | `yes` | Sperrmüll acceptance |
| `opening_hours` | OSM syntax | e.g., `Mo-Fr 08:00-16:00` |
| `operator` | String | e.g., "BSR", "AWM", "DRK" (charity bins) |
| `location` | `underground` | Underground containers (Unterflurcontainer) |
| `fee` | `yes/no` | Fee indicator (rarely contains complex fee schedules) |

**Overpass API Query Example:**
```
[out:json][timeout:25];
(
  node["amenity"="recycling"](bbox);
  way["amenity"="recycling"](bbox);
  relation["amenity"="recycling"](bbox);
);
out body;
>;
out skel qt;
```

---

### Primary Sources: Municipal Open Data Portals

| Source | City | Priority | Format | Key Attributes |
|--------|------|----------|--------|----------------|
| **Berlin Open Data (BSR)** | Berlin | 5/5 | JSON | Recyclinghöfe, Abfall-ABC ontology, synonyms |
| **Hamburg WFS Service** | Hamburg | 5/5 | WFS/GeoJSON/GML | Layer-segmented (paper, glass, elektro, recyhof) |
| **Munich AWM Open Data** | Munich | 5/5 | GeoJSON/WFS/CKAN | Wertstoffhöfe vs Wertstoffhöfe Plus tiers |
| **Cologne AWB Open Data** | Cologne | 5/5 | XML/JSON | Structured Schadstoffmobil schedules! |

**Berlin BSR (daten.berlin.de):**
- **Abfall-ABC**: Rich ontology mapping waste items (e.g., "Couch" → "Sperrmüll") to disposal pathways
- **Vermeidungstipps**: Prevention tips for sustainability
- **Integration Value**: Critical for chatbot NLU training (synonym dictionary)

**Hamburg WFS (geodienste.hamburg.de):**
- **Layer Segmentation**:
  - `abfall_recycling_1_papier` - Paper containers
  - `abfall_recycling_2_glas` - Glass containers
  - `abfall_recycling_5_elektro` - E-waste drop-off points
  - `abfall_recycling_6_recyhof` - Recycling centers
- **Nutzungszeiten**: Typically "Werktags 07:00-20:00"
- **Access Control**: Distinguishes "household usage" (free, requires ID) vs "commercial usage" (fees)

**Munich AWM (opendata.muenchen.de):**
- **Tiered Infrastructure**: Standard Wertstoffhöfe vs **Wertstoffhöfe Plus** (larger quantities, construction waste)
- **properties.problemstoffe**: Explicit hazardous waste acceptance flag
- **Constraints Text**: "haushaltsüblicher Art und Menge" (household type and quantity)
- **CKAN API**: Enables automated freshness checking ("Last Modified")

**Cologne AWB (offenedaten-koeln.de):**
- **Schadstoffmobil Dataset**: Structured data (Location, Date, Time Window) - rare and high-value!
- **Datasets Available**:
  - Wertstoff-Center in Köln
  - Schadstoff-Mobil in Köln
  - Altkleider-Container in Köln
- **Temporal Query Support**: "Is the hazardous waste truck in Nippes today?"

---

### Primary Sources: Community Aggregators

| Source | Priority | Format | Coverage |
|--------|----------|--------|----------|
| **HACS waste_collection_schedule** | 5/5 | Python/MIT | Hundreds of German districts |
| **Abfall+ / Abfall.io API** | 5/5 | JSON API | Market leader (Göppingen, Karlsruhe, Landshut, etc.) |
| **MyMüll.de** | 4/5 | REST JSON | Aschaffenburg, Salzgitter, Hesse |
| **RegioIT / AbfallNavi** | 4/5 | REST JSON | North Rhine-Westphalia (Aachen, Gütersloh) |
| **JumoMind** | 4/5 | JSON API | Major provider, supported by HACS |

**HACS Framework (github.com/mampfes/hacs_waste_collection_schedule):**
- **Architecture**: Library of Python "Source" scripts for each municipal backend
- **Value Proposition**: Acts as "Rosetta Stone" for 100s of disparate municipal APIs
- **Schadstoffmobil Support**: Explicitly models mobile hazardous waste as distinct collection type
- **Infrastructure Discovery**: Underlying APIs often expose facility locations (glass banks, centers)
- **License**: MIT

**HACS Microservice Architecture:**
```
1. Input: User's PLZ (postal code) or City Name
2. Lookup: Map location to corresponding HACS source script
3. Execution: Execute script to fetch current collection dates
4. Output: JSON object with upcoming dates to chatbot
```

---

### Secondary Sources: Producer Responsibility Systems

| Source | Domain | Priority | Format | Coverage |
|--------|--------|----------|--------|----------|
| **Stiftung EAR Register** | E-Waste (WEEE) | 4/5 | CSV/XLSX/XML | National official |
| **Stiftung GRS Batterien** | Batteries | 4/5 | HTML Search | National, "Green Boxes" |

**Stiftung EAR (stiftung-ear.de):**
- **Content**: Comprehensive list of municipal collection centers and authorized retailers
- **Legal Status**: "Ground Truth" for ElektroG compliance
- **Use Case**: Validate and flag locations as "Officially Authorized for E-Waste"

**Stiftung GRS (grs-batterien.de):**
- **Infrastructure**: "Green Boxes" found in almost every retail outlet
- **Rücknahmestellen Search**: HTTP-queryable search function
- **Alternative**: Rule-based inference ("All Supermarkets accept Batteries") often sufficient

**Extended Producer Responsibility (EPR) Networks:**
- **Pfand System**: ~135,000 Reverse Vending Machines in private retail (no public API)
- **WEEE (ElektroG)**: Retailers >400m² must accept small e-waste regardless of purchase origin
- **BattG**: All battery retailers must accept returns

---

### Secondary Sources: Retailer Infrastructure

| Source | Retailer | Priority | Format | Key Features |
|--------|----------|----------|--------|--------------|
| **Lidl Store Finder API** | Schwarz Group | 3/5 | JSON | Pfandautomaten flags |
| **Kaufland Store Finder** | Schwarz Group | 3/5 | JSON | Store features array |
| **dm Store Finder** | dm-drogerie | 3/5 | JSON (Mapbox) | serviceTypes: RECYCLING_STATION |
| **Rossmann Store Locator** | Rossmann | 3/5 | JSON | Store services |

**Integration Strategy for Retailers:**
- **"Harvest and Cache"**: Low-frequency (monthly) fetch to populate KG, not real-time
- **OSM Inference Alternative**: Rule-based logic:
  - IF `shop=supermarket` AND Country=DE THEN `accepts → Batteries`
  - IF `shop=chemist` (e.g., dm) THEN `accepts → Batteries AND CosmeticsPackaging`

**GitHub Resources:**
- `github.com/Andre0512/lidl-plus` - Lidl Plus API authentication
- `github.com/t0ffifee/discount-scraper` - Supermarket API reverse engineering
- `softcreatr.github.io/dm-store-finder/` - dm store finder

---

### Knowledge Graph Schema for Infrastructure

```
Class: Facility
├── type: Enum (RecyclingCenter, Container, Store, MobileStop)
├── operator: String (e.g., "BSR", "AWM")
├── access: Enum (Public, Restricted, Commercial)
├── fees: Boolean or String URI
└── opening_hours: OSM-format string

Relation: accepts -> Class: WasteType
├── WasteType nodes should be hierarchical (GreenGlass subclass of Glass)
└── Synonyms from Berlin Abfall-ABC attached as properties

Class: CollectionEvent (For Schadstoffmobil)
├── atLocation: Relation to Facility or GeoPoint
├── startTime: ISO Timestamp
└── endTime: ISO Timestamp
```

---

### Four-Layer Ingestion Pipeline

| Layer | Source | Purpose | Update Frequency |
|-------|--------|---------|------------------|
| **1. Geospatial Skeleton** | OSM (Overpass) | 100% geographic coverage | Weekly batch |
| **2. Authority Overlay** | Municipal APIs | Authoritative details (hours, fees) | On dataset change |
| **3. Temporal Service** | HACS Microservice | Live schedule queries | Real-time |
| **4. Inference Engine** | Retailer rules | Battery/cosmetics acceptance | Static rules |

**Conflation Logic (Layer 2):**
- Match Official nodes to OSM nodes using **50-meter spatial buffer** + name similarity
- **Priority Rule**: Official data (Hours, Fees, specific waste types) overwrites OSM data

---

### Identified Gaps

| Gap | Impact | Mitigation |
|-----|--------|------------|
| **Fee Vacuum** | OSM lacks fee info; municipal data often unstructured text ("Free up to 1m³, then €10/m³") | Default advisory: "Fees may apply. Check operator website." |
| **Rural Coverage** | Some areas rely on PDF calendars | OCR-based "Upload your Calendar" feature |
| **Real-time Container Status** | "Is the glass bank full?" not yet standard Open Data | Hamburg experimenting with IoT sensors |
| **Opening Hours Freshness** | OSM may lag behind official municipal changes | Authority Overlay priority rule |

---

### Data Ingestion Strategy

**Phase 1: OSM Foundation**
1. Weekly batch job using Overpass API for all `amenity=recycling` nodes in Germany
2. Transform OSM tags to Facility nodes with `DERIVED_FROM` provenance

**Phase 2: Municipal Enrichment**
1. Ingest Berlin BSR Abfall-ABC for synonym dictionary
2. Fetch Hamburg WFS layers for precise container typing
3. Import Munich AWM GeoJSON for tiered facility classification
4. Parse Cologne AWB Schadstoffmobil schedules

**Phase 3: HACS Integration**
1. Deploy Python microservice wrapping waste_collection_schedule
2. Map PLZ → HACS source script
3. Query live for temporal data (collection dates, Schadstoffmobil stops)

**Phase 4: Inference Rules**
1. IF `shop=supermarket` THEN `accepts → Batteries`
2. IF `recycling_type=centre` THEN `type = RecyclingCenter`
3. IF `recycling:hazardous_waste=yes` THEN `acceptsSchadstoff = true`

---

## Report 11: Material Properties

### Key Finding: The Data Dichotomy

No single "super-database" exists for material properties. The waste management domain exhibits a **data dichotomy**:
- **Scientific databases** (PubChem, Materials Project): High-precision molecular data (crystal density, flash point)
- **Logistical datasets** (EPA SMM, WRAP): Low-precision bulk data (dumpster weight, compaction factors)

The "Fessi" system must function as a **data fusion engine**, synthesizing authoritative chemical safety data with pragmatic engineering data.

**Critical Distinction - The Density Paradox:**
| Density Type | Symbol | Description | Use Case |
|-------------|--------|-------------|----------|
| **True Density** | ρ_true | Solid material density excluding voids | Material identification, float/sink separation |
| **Bulk Density** | ρ_bulk | Mass / total occupied volume (with air gaps) | Container weight estimation, logistics |
| **Compacted** | - | After truck compression | Transport cost modeling |

⚠️ **Using ρ_true for bin calculations → 400-500% cost overestimation!**

---

### Primary Chemical & Safety Sources

#### 1. PubChem (NIH) ⭐ CRITICAL

| Field | Value |
|-------|-------|
| **Provider** | NCBI, National Library of Medicine, NIH |
| **URL** | https://pubchem.ncbi.nlm.nih.gov/ |
| **API** | PUG REST (no API key, 5 req/sec rate limit) |
| **Coverage** | 111M+ chemical structures, 300M substance records |
| **License** | **Public Domain** (US Gov work) |
| **Score** | 5/5 - Critical ("spine" of chemical knowledge layer) |

**Key Endpoints:**
```
GET /rest/pug/compound/name/{ChemicalName}/cids/JSON
GET /rest/pug/compound/cid/{CID}/property/{PropertyTags}/JSON
```

**Integration Strategy:** Use the **LCSS (Laboratory Chemical Safety Summary)** view exclusively - filters to health/safety-critical info only.

| Fessi Attribute | PubChem Field | Logic Application |
|-----------------|---------------|-------------------|
| `HazardClassification` | GHS Classification | "Red Flag" warnings in chatbot |
| `FlashPoint` | Physical Properties → Flash Point | AVV 07 01 04* (flammable liquids) |
| `Toxicity_LD50` | Toxicity → Acute Toxic Effects | Transport danger level |
| `MolecularFormula` | Molecular Formula | Isomer disambiguation |
| `Synonyms` | Synonyms | NLP training data, entity recognition |

**Unique Value:** Unmatched synonym handling - "Bleach" = "Sodium Hypochlorite" = "NaOCl"

---

#### 2. ECHA CHEM / REACH Database ⭐ COMPLIANCE ESSENTIAL

| Field | Value |
|-------|-------|
| **Provider** | European Chemicals Agency (EU) |
| **URL** | https://chem.echa.europa.eu/ |
| **Access** | Bulk download (IUCLID XML, CSV extracts) - **No real-time API** |
| **Coverage** | 25,000+ REACH substances, 150,000+ C&L Inventory entries |
| **License** | Free with **mandatory attribution** |
| **Score** | 5/5 - Compliance Essential (the **law** for German universities) |

**Key Downloads:**
- **C&L Inventory** - Classification and Labelling (updated regularly)
- **Candidate List of SVHCs** - Substances of Very High Concern

| Fessi Attribute | ECHA Data Point | Logic Application |
|-----------------|-----------------|-------------------|
| `REACH_Registration` | Registration Dossier | Legal registration verification |
| `SVHC_Status` | Candidate List | **IF True → strict segregation mandatory** |
| `CLP_Labeling` | C&L Inventory | Official H-phrases (H411, etc.) |
| `EC_Number` | EC Number | Primary EU regulatory identifier |

**Integration Pattern:** Batch ingestion - download C&L Inventory and SVHC list as local lookup tables.

⚠️ **Gap:** ECHA is document-heavy. Use ECHA for **classification**, PubChem for **properties**.

---

#### 3. EPA EPI Suite™ (Gap Filling)

| Field | Value |
|-------|-------|
| **Provider** | U.S. EPA & SRC, Inc. |
| **URL** | https://www.epa.gov/tsca-screening-tools/epi-suitetm-estimation-program-interface |
| **Access** | Downloadable Windows software (QSAR models) |
| **Web Beta** | https://episuite.dev/EpiWebSuite/ |
| **License** | **Public Domain** (US Gov) |
| **Score** | 3/5 - Gap Filling (biodegradability estimates) |

**QSAR Modules:**
| Fessi Attribute | EPI Suite Module | Logic Application |
|-----------------|------------------|-------------------|
| `Biodegradability_Predicted` | BIOWIN | Organic vs Chemical waste routing |
| `Bioaccumulation_Factor` | BCFBAF | Aquatic environment risk |
| `Kow` | KOWWIN | Groundwater mobility prediction |

**Strategy:** Pre-calculate properties for top 500-1,000 lab chemicals offline.

---

### Physical & Engineering Sources

#### 4. The Materials Project (LBNL)

| Field | Value |
|-------|-------|
| **Provider** | Lawrence Berkeley National Laboratory |
| **URL** | https://next-gen.materialsproject.org/ |
| **API** | `mp-api` Python client (free API key required) |
| **Coverage** | 150,000+ inorganic compounds |
| **License** | CC BY 4.0 (requires citation) |
| **Score** | 4/5 - Specific Utility (E-Waste, batteries, ceramics) |

| Fessi Attribute | Materials Project Field | Logic Application |
|-----------------|------------------------|-------------------|
| `TrueDensity` | `density` | Material identification |
| `Composition` | `elements` | Heavy metal detection (Cd, Pb) |
| `Conductivity_BandGap` | `band_gap` | Metal vs insulator distinction |
| `Stability` | `formation_energy_per_atom` | Chemical stability prediction |

⚠️ **Gap:** Data is for "theoretical perfect crystals" - values are ρ_true, NOT for bin calculations.

---

#### 5. MatWeb (Polymer Gap Filler)

| Field | Value |
|-------|-------|
| **Provider** | MatWeb, LLC |
| **URL** | https://www.matweb.com/ |
| **Access** | Web interface only - **No public API, scraping prohibited** |
| **Coverage** | 185,000+ material data sheets |
| **License** | **Proprietary/Commercial** |
| **Score** | 3/5 - High Value / High Friction |

**Unique Value:** Best source for polymer differentiation (HDPE vs LDPE vs PET).

| Fessi Attribute | MatWeb Field | Logic Application |
|-----------------|--------------|-------------------|
| `Material_Density` | Density | Engineering density ranges |
| `MeltingPoint` | Melting Point | Autoclave suitability |
| `TensileStrength` | Tensile Strength | "Shreddability" proxy |

**Integration Strategy:** Manually curate "Top 50" waste polymers (Nylon, ABS, Polystyrene, etc.)

---

### Waste Logistics Sources (Bulk Density)

#### 6. EPA Volume-to-Weight Conversion Factors ⭐ LOGISTICS ESSENTIAL

| Field | Value |
|-------|-------|
| **Provider** | U.S. EPA (Office of Resource Conservation and Recovery) |
| **URL** | https://www.epa.gov/smm/volume-weight-conversion-factors-solid-waste |
| **Format** | PDF (April 2016 update) |
| **License** | **US Public Domain** |
| **Score** | 5/5 - Logistics Essential ("Rosetta Stone" for infrastructure layer) |

**Material States Covered:**
- **Loose** - thrown in bin
- **Compacted** - in truck
- **Baled** - ready for recycling

| Fessi Attribute | EPA Data Point | Example Value |
|-----------------|----------------|---------------|
| `BulkDensity_Loose` | Volume-to-weight (Loose) | 200 lbs/yd³ (Cardboard) |
| `BulkDensity_Compacted` | Volume-to-weight (Compacted) | 500 lbs/yd³ (Cardboard) |
| `Material_State` | Material Form | "Whole", "Flattened", "Crushed" |

⚠️ **Gap:** US Imperial units (lbs/yd³) → requires strict conversion to kg/m³ for German context.

---

#### 7. UK WRAP Material Bulk Densities

| Field | Value |
|-------|-------|
| **Provider** | WRAP (UK Charity) |
| **URL** | https://www.wrap.ngo/resources/report/material-bulk-densities |
| **Relevance** | **Metric counterpart** to EPA data, closer to German logistics model |

---

### German-Specific Sources

#### 8. AVV (Abfallverzeichnis-Verordnung) ⭐ TAXONOMY CORE

| Field | Value |
|-------|-------|
| **Provider** | German Federal Ministry of Justice / EU Commission |
| **URL** | https://www.gesetze-im-internet.de/avv/ |
| **Format** | XML, PDF, HTML |
| **Structure** | 6-digit hierarchical codes (800+ waste codes) |
| **Score** | 5/5 - Taxonomy Core (NOT a property database - it's the **Ontology**) |

**Integration Strategy:** Ingest AVV XML as "skeleton" of Knowledge Graph. Attach physical properties from PubChem/EPA to AVV nodes.

**Hazard Flag:** Any code with asterisk (*) = legally hazardous
- Example: `20 01 35*` = discarded electrical equipment with hazardous components

---

### Emerging Domains

#### 9. NREL Battery Failure Databank (Safety Critical)

| Field | Value |
|-------|-------|
| **Provider** | NREL & NASA |
| **URL** | https://www.nrel.gov/transportation/battery-failure.html |
| **Format** | Excel/CSV downloads |
| **Score** | 4/5 - Safety Critical |

**Data:** Heat Output, Mass Ejection, Thermal Runaway profiles

**Application:** "Can I throw this swollen laptop battery in the bin?" → triggers "STOP - EXPLOSION RISK" warning based on Li-ion high-risk profile.

---

#### 10. DIN CERTCO Database (Bioplastics) ⭐ LOGIC CRITICAL

| Field | Value |
|-------|-------|
| **Provider** | TÜV Rheinland / DIN |
| **Standard** | DIN EN 13432 (Compostability) |
| **Score** | 5/5 - Logic Critical |

**Critical Distinction:**
- **Bio-based** = made from plants (NOT necessarily degradable)
- **Biodegradable** = compostable (DIN EN 13432 certified)

⚠️ **Gap:** No API - requires scraping or manual certification lookup (e.g., "7P0023")

---

#### 11. NIST WebBook (Thermochemistry)

| Field | Value |
|-------|-------|
| **Provider** | National Institute of Standards and Technology |
| **URL** | https://webbook.nist.gov/chemistry/ |
| **Access** | Web / Scraping |
| **License** | Public Domain |
| **Key Field** | Enthalpy of Combustion |

---

### Federated Integration Architecture

**Four-Layer Data Ingestion Pipeline:**

| Layer | Source | Purpose | Output |
|-------|--------|---------|--------|
| **1. Regulatory Skeleton** | AVV XML | Legal waste code structure | 800+ AVVCode nodes |
| **2. Chemical Flesh** | PubChem + ECHA | Safety properties, hazard flags | HazardClassification, H-phrases |
| **3. Physical Body** | EPA SMM + WRAP | Bulk density tables | BulkDensity_Loose/Compacted |
| **4. Engineering Patch** | MatWeb + Materials Project | Polymer/inorganic properties | Material-specific attributes |

**Identified Gaps:**
| Gap | Impact | Mitigation |
|-----|--------|------------|
| **Composite Objects** | No database for "Nike Air Max" or "iPhone 13" components | Decomposition rules: `Sneaker → [rubber, textile, foam]` |
| **Electrical Conductivity** | Mixed e-waste conductivity unknown | Heuristic based on metal content |
| **German Synonyms** | Scientific APIs are Anglocentric | SKOS synonym layer: "Styropor" ↔ "Expanded Polystyrene" |

---

### Source Profile Matrix

| Source | Domain | Access | License | Key Attribute |
|--------|--------|--------|---------|---------------|
| **PubChem** | Chemical Safety | API (REST), JSON | Public Domain | FlashPoint, Toxicity |
| **ECHA CHEM** | EU Regulation | Bulk Download (IUCLID) | Attribution Req. | SVHC_Status, CLP_Labeling |
| **EPA SMM** | Waste Logistics | PDF / Static Tables | Public Domain | BulkDensity |
| **Materials Project** | Inorganic Science | API (Python Client) | CC BY 4.0 | TrueDensity, Composition |
| **MatWeb** | Polymers/Engineering | Web / Proprietary | Commercial | TensileStrength, PolymerDensity |
| **NREL Battery DB** | Li-ion Safety | CSV Download | Public Domain | ThermalRunawayProfile |
| **NIST WebBook** | Thermochemistry | Web / Scraping | Public Domain | EnthalpyOfCombustion |
| **Abfall+ / MyMuell** | Local Logistics | JSON / ICS API | Var. (Open Data) | CollectionDate, BinType |
| **EPI Suite** | Biodegradability | Software (QSAR) | Public Domain | BiodegradationHalfLife |

---

## Report 12: Universal Taxonomies

### Key Finding: The Semantic Gap Problem

The fundamental challenge in waste management is the **semantic gap** between:
- **User vocabulary**: Product-centric ("toner cartridge", "coffee cup", "lab centrifuge")
- **Regulatory framework**: Material-centric + hazard-centric ("waste printing toner", "composite packaging")

The knowledge graph must function as a **translation layer** ingesting diverse product taxonomies and mapping them to AVV waste codes.

**Core Insight:** No single taxonomy is sufficient:
| Taxonomy | Domain | Role in Fessi |
|----------|--------|---------------|
| **UNSPSC** | Procurement | Back-office ERP linkage ("Buy" → "Dispose") |
| **GPC** | Consumer/Retail | Barcode resolution (GTIN → Brick → Attribute) |
| **ECLASS** | Industrial/Engineering | Material declarations, REACH compliance |
| **HS/CN** | Customs/Trade | Material proxy via tariff classification |
| **CPC** | Statistics | Bridge between NACE (activity) and HS (product) |
| **NACE/WZ** | Economic Activity | Source industry filtering for AVV chapters |
| **AVV** | Waste Regulation | **Target ontology** - all taxonomies resolve here |

---

### 1. UNSPSC (UN Standard Products and Services Code) ⭐ PROCUREMENT

**Architecture:** 4-level hierarchy encoded in 8 digits:
| Level | Digits | Example | AVV Mapping Potential |
|-------|--------|---------|----------------------|
| Segment | 1-2 | 44 = Office Equipment | Low (too broad) |
| Family | 3-4 | 4412 = Office Supplies | Medium |
| Class | 5-6 | 441217 = Writing Instruments | High (material inference) |
| Commodity | 7-8 | 44121706 = Wooden Pencils | **Exact** (specific AVV code) |

#### Data Sources

**Data.gov (Oklahoma) CSV** ⭐ PRIMARY
| Field | Value |
|-------|-------|
| **URL** | https://catalog.data.gov/dataset/unspsc-codes-93778 |
| **Format** | CSV |
| **Updated** | November 2024 |
| **License** | Public Domain (US Gov) |
| **Use Case** | "Skeleton" of product ontology |

**Cornell University Cheat Sheets**
| Field | Value |
|-------|-------|
| **URL** | https://finance.cornell.edu/procurement/buyers/unspsc |
| **Format** | Excel (.xlsx) |
| **Use Case** | University relevance filter (prioritize "Lab Beaker" over "Industrial Vat") |

**GitHub ML Repositories**
| Repository | Files | Use Case |
|------------|-------|----------|
| `KathiravanNatarajan/UNSPSCPrediction` | UNSPSCdataset.csv | NLU training data |
| `kasivisu4/cordiance-experential-project` | final_result.json | Natural language → UNSPSC mapping |

**O*NET Resource Center**
| Field | Value |
|-------|-------|
| **URL** | https://www.onetcenter.org/dictionary/21.0/excel/unspsc_reference.html |
| **Format** | Excel (.xlsx) |
| **Structure** | Separate columns per hierarchy level |

---

### 2. GPC (Global Product Classification) ⭐ CONSUMER BARCODES

GPC underpins the GS1 barcode system (GTINs). **Critical differentiator**: The "Attribute" layer provides material data that UNSPSC lacks.

**Architecture:** 4-tier + Attributes
| Level | Description | Example |
|-------|-------------|---------|
| Segment | Highest level | Food, Beverage and Tobacco |
| Family | Broad grouping | Milk/Butter/Cream... |
| Class | Narrower grouping | Milk/Milk Substitutes |
| **Brick** | Primary classification unit | 10000025 = "Milk (Perishable)" |
| **Attribute** | Variant descriptors | Packaging Type, Fat Content |

**Game-Changer for Waste:** Brick tells content (Milk), Attribute tells container (Carton vs. Glass).

**Data Flow Example:**
```
User scans GTIN → Resolve to GPC Brick (Yogurt) → Get Attribute (Packaging: Plastic)
→ Graph query: MATCH (m:Material {name: 'Plastic'})-->(b:Bin {type: 'Yellow Sack'})
→ Result: "Dispose in Yellow Bin (Gelber Sack)"
```

#### Data Sources

**GS1 GPC Browser** ⭐ PRIMARY
| Field | Value |
|-------|-------|
| **URL** | https://gpc-browser.gs1.org/ |
| **Formats** | XML, Excel (XLSX), JSON |
| **Updates** | Regular releases (e.g., "GPC as of November 2025") |
| **License** | GS1 IP policy - structure OK for internal systems |

**Delta Documents** - Track changes between versions for graph maintenance.

---

### 3. ECLASS ⭐ INDUSTRIAL/GERMAN STANDARD

Dominant standard for European industrial/engineering procurement. **Unique capability**: Built-in "Material Declarations" for REACH/SCIP compliance.

**Architecture:** 4-level hierarchy (8 digits, similar to UNSPSC)
- Example: `27-01-01-01` = Electrical engineering → Power supply → Battery → Lithium battery

**Release 16.0 Feature:** Block `0173-1#01-AKA421#001` (Declaration of substances) allows chemical compounds to be associated with product classes.

**Hazard Detection:** ECLASS classification linked to Mercury compounds → automatic "Sondermüll" flag.

#### Data Sources

**Amazon Business Punchout Catalogs** ⭐ GREY SOURCE
| Field | Value |
|-------|-------|
| **URL** | Amazon Business DE "System Integrations" help pages |
| **Format** | CSV/Excel |
| **Content** | ECLASS codes mapping Amazon catalog |
| **Strategic Value** | Curated subset of commercially active codes |

**ECLASS Asset Administration Shell**
| Field | Value |
|-------|-------|
| **Format** | XML (Asset.xml) |
| **Access** | Free download |
| **Use Case** | Schema design for ECLASS-compliant ontology |

**University Libraries** - Check institutional access to full ECLASS database.

---

### 4. HS/CN (Harmonized System / Combined Nomenclature) ⭐ MATERIAL PROXY

Customs codes are determined by **material composition** - excellent proxy for material identification.

**Structure:**
| System | Digits | Scope |
|--------|--------|-------|
| HS | 6 | Global standard (WCO) |
| CN | 8 | EU extension (Eurostat) |
| TARIC | 10 | EU + legal measures |
| EZT | 11 | German national |

**Key Sections for Waste:**
| Section | Chapters | Content | AVV Mapping |
|---------|----------|---------|-------------|
| VII | 39, 40 | Plastics, Rubber | Yellow Sack |
| XV | 72-83 | Base Metals | Recyclables |
| XVI | 84-85 | Machinery, Electronics | **WEEE → AVV 16/20 01 36** |

#### Data Sources

**EU Vocabularies (CN)** ⭐ PRIMARY - GOLD STANDARD FOR KNOWLEDGE GRAPHS
| Field | Value |
|-------|-------|
| **URL** | https://data.europa.eu/data/datasets/combined-nomenclature-2021 |
| **Formats** | RDF, XML, SKOS (Linked Open Data) |
| **Coverage** | CN 2021-2025 |
| **License** | Open |

**TARIC Database**
| Field | Value |
|-------|-------|
| **URL** | https://data.europa.eu/data/datasets/eu-customs-tariff-taric |
| **Size** | ~5 GiB XML |
| **Content** | CN codes + EU legal measures (bans, restrictions) |
| **Waste Relevance** | Ozone Depleting Substances flagged with "Prohibitions" |

**EZT-online (German TARIC)**
| Field | Value |
|-------|-------|
| **URL** | https://www.zoll.de/EN/Businesses/Movement-of-goods/Import/Duties-and-taxes/Normal-customs-clearance/Customs-Tariff/ |
| **Precision** | 11-digit national codes |

---

### 5. CPC (Central Product Classification) - BRIDGE TAXONOMY

CPC is the "Rosetta Stone" between production (NACE/WZ) and trade (HS).

**Unique Value:**
- Unlike HS (goods only), CPC covers **services**
- Example: "Septic Tank Cleaning Services" (CPC 94) → wastewater codes

#### Data Sources

| Source | Format | Use Case |
|--------|--------|----------|
| UN Stats / EU Vocabularies | Correspondence tables (Excel/CSV) | CPC-to-HS, CPC-to-NACE mappings |
| Eurostat RAMON (archives) | CSV | Historical versions |

**Graph Traversal:** `Service (NACE) → Product Category (CPC) → Trade Code (HS)`

---

### 6. NACE / WZ 2008 - SOURCE INDUSTRY FILTER

Waste must often be categorized by **source** (waste generator). WZ 2008 is the German implementation of EU NACE Rev. 2.

**University Department Mapping:**
| Department | WZ Code | AVV Chapter Filter |
|------------|---------|-------------------|
| Chemistry | 72.11 (Biotechnology R&D) | Chapter 06, 07 |
| Canteen | 56.29 (Food services) | Chapter 02, 20 |
| Art | 90.03 (Artistic creation) | Chapter 08, 20 |

**Benefit:** Filter AVV to relevant chapters only → reduced search space, lower error rate.

#### Data Sources

**Destatis (Federal Statistical Office)** ⭐ PRIMARY
| Field | Value |
|-------|-------|
| **URL** | https://www.destatis.de/DE/Methoden/Klassifikationen/Gueter-Wirtschaftsklassifikationen/Downloads/klassifikationen-wz-2008.html |
| **Format** | Excel, XML |
| **Coverage** | Complete WZ 2008 hierarchy |

---

### 7. AVV (Abfallverzeichnis-Verordnung) - TARGET ONTOLOGY

**All product taxonomies must ultimately resolve to AVV.** This is the legal destination.

**Structure:**
| Level | Digits | Basis | Example |
|-------|--------|-------|---------|
| Chapter | 2 | Source-based | 03 Wood processing, 20 Municipal waste |
| Sub-chapter | 4 | Process-based | 20 01 = Separately collected fractions |
| Code | 6 | Unique identifier | 20 01 01 = Paper and Cardboard |

**Hazard Marker:** Codes with asterisk (*) = Sondermüll (hazardous waste)

#### Data Sources

**Gesetze-im-Internet (Official XML)** ⭐ AUTHORITATIVE
| Field | Value |
|-------|-------|
| **URL** | https://www.gesetze-im-internet.de/avv/ |
| **Format** | XML, HTML |
| **Use Case** | Populate `label` and `description` properties |

**Umweltbundesamt (UBA)**
| Field | Value |
|-------|-------|
| **URL** | https://www.umweltbundesamt.de/en/topics/waste-resources/waste-management/waste-regulations |
| **Format** | PDF/CSV |
| **Content** | Waste statistics, code lists |

**GitHub Repositories:**
| Repository | Format | Content |
|------------|--------|---------|
| `bennokr/6d5f1e55...` | NDJSON | European Waste Catalogue (EWC) |
| `MarvinJWendt/...` | Text | German waste terminology (Abfallverzeichnis) |
| `spherity/product-pass` | Schema | Product → waste code linkage (Digital Product Passports) |

---

### 8. Semantic Integration: Schema.org + Wikidata

**Schema.org Modeling:**
| Type | Use Case | Key Property |
|------|----------|--------------|
| `schema:Product` | Store UNSPSC/ECLASS/GPC | `gtin`, `category`, `material` |
| `schema:usageInfo` | Link product to disposal | Disposal instructions URL |

**Wikidata Alignment:**
| Property | Use Case | Example |
|----------|----------|---------|
| P279 (subclass of) | Hierarchy | Waste paper → subclass of → Waste |
| P31 (instance of) | Classification | Banana peel → instance of → Biodegradable waste |
| EWC code property | Pre-mapped relationships | Items with EWC codes queryable via SPARQL |

**SPARQL Bootstrap Query:**
```sparql
SELECT ?item ?ewc WHERE { ?item wdt:P... ?ewc }
```
Returns thousands of pre-mapped product-to-waste relationships.

---

### Correspondence Tables - CRITICAL FOR CROSS-TAXONOMY LINKING

| Table | Source | Purpose |
|-------|--------|---------|
| CN to CPA to NACE | Eurostat | Infer industry from product code |
| CPC to HS | UN Stats | Product category → trade code |
| CPC to NACE | UN Stats | Service → activity |
| UNSPSC to AVV (custom) | Manual | Direct procurement → waste mapping |

**URL:** https://ec.europa.eu/eurostat/web/cpa/correspondence-tables

---

### Graph Schema Design (from Report 12)

**Proposed Nodes:**
- `Product`, `Material`, `WasteStream`, `Regulation`, `TaxonomyTerm`

**Proposed Edges:**
- `IS_A` (hierarchy)
- `COMPOSED_OF` (material composition)
- `DISPOSE_IN` (bin routing)
- `REGULATED_BY` (legal linkage)

**"Golden Record" Example - Toner Cartridge:**
```
Properties:
  UNSPSC_ID: 44103103
  ECLASS_ID: 19-14-06-01
  CN_ID: 8443 99 90
  Material: Plastic, Toner
Edges:
  (:Concept)-->(:UNSPSC_Node {id: 44103103})
  (:Concept)-->(:AVV_Node {code: '08 03 18'})
```

---

### Source Profile Matrix (Report 12)

| Source | Domain | Format | Access | License | Fessi Attributes |
|--------|--------|--------|--------|---------|------------------|
| **Data.gov UNSPSC** | Procurement | CSV | Bulk Download | Public Domain | ProductCode, Category |
| **GS1 GPC Browser** | Consumer | XML/JSON | Registration | GS1 Terms | BrickCode, Attributes, GTIN |
| **Amazon Business ECLASS** | Industrial | CSV | Help Pages | Commercial Use OK | MaterialDeclaration, REACH |
| **EU Vocabularies CN** | Trade/Customs | RDF/SKOS | Open | Open | MaterialSection, TariffCode |
| **Destatis WZ 2008** | Activity | Excel/XML | Open | Open | IndustryCode, AVVChapter |
| **Gesetze-im-Internet AVV** | Waste Law | XML | Open | Public Domain | WasteCode, HazardFlag |
| **Wikidata** | Semantic Hub | SPARQL | Open | CC0 | QID, EWC_Property |
| **Eurostat Correspondence** | Cross-reference | CSV | Open | Open | TaxonomyMappings |

---

## Report 13: Multilingual Synonyms

### Key Finding: The Federated Semantic Architecture

No single "master database" for waste terminology exists. Fessi must be constructed as a **federated knowledge system** that layers:
1. **Global semantic data** (Wikidata, BabelNet) - Universal concepts, synonyms
2. **Regulatory thesauri** (GEMET, EuroVoc) - Legal definitions
3. **Product-specific attributes** (Open Food Facts, GS1) - Material composition
4. **Hyper-local municipal rules** (Abfallnavi, Abfall+) - Ground-truth disposal logic

**The Dual Gap Problem:**
| Gap | Description | Example |
|-----|-------------|---------|
| **Product-Material Gap** | Users think in products, rules written for materials | "Yogurt pot" → "Polystyrene" |
| **Language Gap** | Official German vs migrant vernacular | "Gelber Sack" → "Ambalaj atıkları" (TR) / "العبوات الخفيفة" (AR) |

**Layered Ontology Authority Model:**
| Layer | Source | Function | Authority |
|-------|--------|----------|-----------|
| 1. Semantic Base | Wikidata, BabelNet | Universal concepts, Synonyms | Low (Default) |
| 2. Regulatory | GEMET, EuroVoc, LoW | Legal definitions, Hazards | Medium |
| 3. Product | Open Food Facts, GS1 | Item composition, Packaging | High (Item Specific) |
| 4. Local Rules | Abfall APIs, ABCs | Disposal logic, Exceptions | **Highest** (The Law) |

**Conflict Resolution:** "Specific Overrides General" - Munich pizza box rule (Restmüll if greasy) overrides Wikidata (cardboard = recyclable).

---

### 1. Linguistic Linked Open Data (LLOD)

#### Wikidata - The Graph of Everything
| Attribute | Value |
|-----------|-------|
| **URL** | https://query.wikidata.org/sparql |
| **License** | CC0 (Public Domain) |
| **Languages** | 100+ (excellent DE, AR, TR coverage) |
| **ETL Role** | Foundation for multilingual synonyms |

**Critical Properties for Waste Identification:**
| Property | Name | Function |
|----------|------|----------|
| **P5994** | Recycling Code | Material → Resin code (01=PET, 02=HDPE) - unlocks Gelber Sack logic |
| **P186** | Material Used | Object → Material composition (chair → polypropylene) |
| **P279** | Subclass Of | Is-A hierarchy (Wine Bottle → Bottle → Container → Packaging) |
| **P5997** | Object Stated in Reference As | Cross-reference regulatory terminology |

**Multilingual Extraction Strategy:**
```sparql
# Extract waste-related entities with multilingual labels
SELECT ?item ?label_de ?label_ar ?label_tr ?recycling_code
WHERE {
  ?item wdt:P279*/wd:Q45701 .  # Subclass of Waste
  ?item rdfs:label ?label_de . FILTER(LANG(?label_de) = "de")
  OPTIONAL { ?item rdfs:label ?label_ar . FILTER(LANG(?label_ar) = "ar") }
  OPTIONAL { ?item rdfs:label ?label_tr . FILTER(LANG(?label_tr) = "tr") }
  OPTIONAL { ?item wdt:P5994 ?recycling_code }
}
```

**German Coverage:** High-quality compound nouns specific to DACH region:
- "Mülltrennung" (waste separation)
- "Altglascontainer" (waste glass container)
- "Wertstoffhof" (recycling center)

#### BabelNet - Encyclopedic Semantic Network
| Attribute | Value |
|-----------|-------|
| **URL** | https://babelnet.org/downloads |
| **License** | Mixed/Paid (academic free tier) |
| **Languages** | 500 languages |
| **Size** | ~22M synsets (local index available) |
| **APIs** | Java API (v5.3), Python API (v1.1.0) |

**Key Capability: Synset Disambiguation**
BabelNet organizes knowledge into "synsets" (synonym sets), critical for polysemy:
- **"Glass"** → Material (recyclable) vs Drinking vessel (often not) vs Window pane (construction waste)
- **"Battery"** → Electrical accumulator (hazardous) vs Artillery unit (irrelevant)

**Gap Bridging:** Infers DE↔AR/TR connections via inter-language links even when direct translations missing. Example: "Wertstoffhof" → Arabic equivalent via shared WordNet parents.

#### ConceptNet - Folk Knowledge Graph
| Attribute | Value |
|-----------|-------|
| **URL** | https://conceptnet.io/ |
| **License** | CC-BY-SA 4.0 |
| **Version** | 5.8 (improved language separation) |
| **API** | REST API |

**Relational Edges for Waste Logic:**
| Relation | Example | Waste Application |
|----------|---------|-------------------|
| **MadeOf** | /c/en/bottle MadeOf /c/en/plastic | Apply material rules to object queries |
| **UsedFor** | /c/en/brush UsedFor /c/en/cleaning_toilets | Infer hygiene → Restmüll |
| **AtLocation** | AtLocation "kitchen" vs "garage" | Context-aware stream inference |
| **/r/Synonym** | Multilingual synonym edges | Synonym expansion in real-time |

#### Open Multilingual Wordnet (OMW)
| Attribute | Value |
|-----------|-------|
| **URL** | https://omwn.org/ |
| **License** | Various (per wordnet) |
| **Languages** | 150+ |
| **Version** | OMW 2.0 with CILI |

**Collaborative Interlingual Index (CILI):** Decouples concepts from English-centric Princeton WordNet. Enables direct DE↔TR mapping without semantic drift through English pivot.

---

### 2. Regulatory Environmental Thesauri

#### GEMET - General Multilingual Environmental Thesaurus
| Attribute | Value |
|-----------|-------|
| **URL** | https://www.eionet.europa.eu/gemet/ |
| **Maintainer** | European Environment Agency (EEA) |
| **License** | Open (EEA) |
| **Languages** | 36 (including Arabic and Turkish) |
| **Format** | RDF/SKOS |

**Semantic Precision:**
- **Concept 9041 ("Waste"):** Rigorous definition with scope notes (inclusions/exclusions)
- **Hierarchy:** Themes → Wastes → Types of Waste → Municipal Waste
- **Arabic:** Dual synonyms "مخلفات" (Mukhallafat = residue) + "نفايات" (Nufayat = refuse)
- **Turkish:** "atık" as standard equivalent

**ETL Value:** GEMET IDs guarantee legal equivalence: German "Abfall" = Arabic "نفايات" within regulatory context.

#### EuroVoc and List of Wastes (LoW)
| Attribute | Value |
|-----------|-------|
| **URL** | https://op.europa.eu/en/web/eu-vocabularies/controlled-vocabularies |
| **License** | Open |
| **Languages** | 24 EU official languages |
| **Waste Types** | 842 (6-digit codes) |

**List of Wastes (LoW) Integration:**
- **Chapter 20:** Municipal Wastes (most relevant for consumer-facing)
- **Hazardous Flag:** Asterisk (*) entries = legally hazardous → trigger warning
- **Mapping:** User term → LoW code (e.g., "Baby Wipes" → 20 03 01 "Mixed Municipal Waste")

**Domain Filtering:** Distinguishes "Cooking Oil" (Chapter 20 - Municipal) from "Engine Oil" (Chapter 13 - Hazardous) to prevent dangerous advice.

#### IATE - Interactive Terminology for Europe
| Attribute | Value |
|-----------|-------|
| **URL** | https://iate.europa.eu/download-iate |
| **License** | Open |
| **Format** | TBX (TermBase eXchange) |
| **Domains** | Environment, Waste Management subsets |

**High-Fidelity Features:**
- **Reliability Ratings:** 1-4 stars (ingest only 3-4 star terms)
- **Technical Phrases:** "Separate Collection" (Getrennte Sammlung), "Bring System" (Bringsystem)
- Captures terminology from municipal guides that standard dictionaries miss

---

### 3. Product-Material Gap Bridging

#### Open Food Facts - Packaging Taxonomy
| Attribute | Value |
|-----------|-------|
| **URL** | https://world.openfoodfacts.org/data |
| **License** | ODbL |
| **Format** | JSONL (~4.8GB compressed daily dump) |
| **Wiki** | https://wiki.openfoodfacts.org/Global_packaging_taxonomy |

**Global Packaging Taxonomy Structure:**
| Component | Field | Example Values |
|-----------|-------|----------------|
| **Shape** | packaging_shape | Box, Tray, Bottle, Pouch |
| **Material** | packaging_material | Cardboard, Plastic, Glass |
| **Recycling** | packaging_recycling | Recycle, Throw-away, Compost |

**Nested Multi-Unit Packaging:**
```json
{
  "product": "Cookies Box",
  "packaging_units": [
    {"shape": "Box", "material": "Cardboard", "recycling": "Recycle"},
    {"shape": "Tray", "material": "Plastic", "recycling": "Throw-away"}
  ]
}
```
→ Enables granular advice: "Separate plastic tray from cardboard box"

**Multilingual Tags:** Prefixed system (en:bottle, de:Flasche, fr:Bouteille) allows translation inference.

#### GS1 Global Product Classification (GPC)
| Attribute | Value |
|-----------|-------|
| **URL** | https://gpc-browser.gs1.org/ |
| **Format** | XML/JSON |
| **Access** | Browser free, bulk requires registration |

**Brick Categorization:** Products → Bricks (e.g., "Aluminum Foil" → material inference)

**"Verified by GS1":** Recent packaging attributes for sustainability support.

#### LUCID Packaging Register (ZSVR)
| Attribute | Value |
|-----------|-------|
| **URL** | https://www.verpackungsregister.org/ |
| **Maintainer** | Zentrale Stelle Verpackungsregister |
| **Access** | Public search |

**Trust Signal:** Confirms brand compliance with German "Dual System" recycling.

**Der Grüne Punkt Distinction:**
- **Sales Packaging** → Gelber Sack
- **Transport Packaging** (e.g., Amazon boxes) → Paper/Cardboard bin

---

### 4. Hyper-Local Municipal Data

#### Abfallnavi API
| Attribute | Value |
|-----------|-------|
| **URL** | https://abfallnavi.api.bund.dev/ |
| **GitHub** | https://github.com/bundesAPI/abfallnavi-api |
| **Coverage** | Aachen, Nürnberg, many others |
| **Access** | Relatively open |

**Endpoints:**
| Endpoint | Function |
|----------|----------|
| `GET /orte` | Resolve street addresses |
| `GET /fraktionen` | Bin types available at address |
| `GET /termine` | Collection calendar |

**Data Value:** Knowing available fractions enables intelligent fallback: "Your building has no bio bin → put compost in Restmüll"

#### Abfall+ / MyMuell
| Attribute | Value |
|-----------|-------|
| **Coverage** | Other German regions |
| **Reliability** | Variable (some instability, reverse-engineering) |
| **Strategy** | Treat as secondary, cache aggressively |

#### Municipal Abfall-ABC Lists
| City | Notable Rules |
|------|---------------|
| **Berlin (BSR)** | Kassenzettel (receipts) handling |
| **Munich (AWM)** | Glass colors (Blue → Green bin) |
| **Friesland** | Blue glass bottle → grüner Altglascontainer |

**Semantic Conflicts Requiring Special Logic:**

**1. Receipt (Kassenzettel) Conflict:**
- Old: Thermal paper (Bisphenol-A) → Restmüll
- New: "Blue4est" eco-paper (blue/grey) → Altpapier
- **Fessi Logic:** Prompt "Is receipt white or blue-grey?"

**2. Baking Paper (Backpapier) Trap:**
- Looks like paper → User assumes Paper/Bio bin
- Reality: Silicone-coated → Restmüll
- **Fessi Logic:** Proactive misconception correction

**3. Pizza Box Paradox:**
- Cardboard = Paper, but grease ruins paper recycling
- German Standard: Greasy → Restmüll
- **Fessi Logic:** Ask "Is box stained with oil/cheese?"

---

### 5. Multilingual Integration Resources

#### Official Municipal Translations ("Rosetta Stones")
Municipalities publish flyers in Arabic and Turkish - high-value official synonym sources:

| German Term | Turkish | Arabic |
|-------------|---------|--------|
| Gelber Sack | Ambalaj atıkları | العبوات الخفيفة |
| Altpapier | Kâğıt atıkları | الورق القديم |
| Restmüll | Evsel atık | النفايات المنزلية |
| Biotonne | Organik atık | النفايات العضوية |

**Sources:**
- Landkreis Karlsruhe: Arabic waste sorting guide
- Bremen Stadtreinigung: Multilingual flyer collection
- AHA Region Hannover: Arabic separation overview

#### Handbook Germany
| Attribute | Value |
|-----------|-------|
| **URL** | https://handbookgermany.de/ar/waste-separation |
| **Languages** | German, English, Arabic, Turkish, and more |
| **Content** | Explains Mülltrennung concept for newcomers |

#### False Friends and Cultural Gaps

**The "Pringles Dose" Problem:**
- Looks like paper tube
- Actually: Paper + Foil + Metal composite
- User search: "Cips kutusu" (TR) / "علبة بطاطس" (AR)
- Disposal: Gelber Sack (composite), NOT Paper bin

---

### 6. Hazardous Waste Safety Layer

#### ECHA C&L Inventory + GHS
| Attribute | Value |
|-----------|-------|
| **URL** | https://data.europa.eu/data/datasets/echa-term |
| **License** | Public |
| **Format** | Multilingual terminology database |

**H-Statements:** Language-agnostic codes
- H300 → "Lebensgefahr bei Verschlucken" (DE)
- H410 → "Toxic to aquatic life"

**Visual Hazard Recognition:** Wikidata/Wikimedia Commons host official GHS pictogram SVGs. Fessi can present icons for user confirmation without requiring chemical name reading.

#### GHS Pictograms (UNECE)
| Attribute | Value |
|-----------|-------|
| **URL** | https://unece.org/transport/dangerous-goods/ghs-pictograms |
| **Format** | SVG/PNG |
| **Integration** | Show user "Do you see this symbol?" prompts |

---

### 7. Geospatial and Visual Context

#### OpenStreetMap - Recycling Infrastructure
| Attribute | Value |
|-----------|-------|
| **URL** | https://wiki.openstreetmap.org/wiki/Tag:amenity%3Drecycling |
| **License** | ODbL |
| **API** | Overpass API |

**Relevant Tags:**
| Tag | Example | Purpose |
|-----|---------|---------|
| `amenity=recycling` | - | Recycling point |
| `recycling_type=container` | - | Glass/paper banks |
| `recycling:glass=yes` | - | Accepts glass |
| `recycling:glass:colour=mixed` | - | Mixed color glass (handles blue glass rule) |

#### Computer Vision Datasets
| Dataset | Content | Use Case |
|---------|---------|----------|
| **TrashNet** | Labeled waste images | Material classification training |
| **ZeroWaste** | Industrial waste images | Fine-grained sorting |
| **Gemini API integration** | Real-time photo analysis | Visual identification of Pringles can, pizza box |

---

### Source Profile Matrix

| Source | Type | Format | License | Languages | ETL Role |
|--------|------|--------|---------|-----------|----------|
| **Wikidata** | LLOD Graph | SPARQL | CC0 | 100+ | Semantic backbone, P5994/P186 |
| **BabelNet** | Semantic Net | API | Mixed/Paid | 500 | Synset disambiguation |
| **ConceptNet** | Knowledge Graph | REST API | CC-BY-SA | 300+ | Folk knowledge relations |
| **OMW** | Wordnet | Download | Various | 150+ | CILI interlingual index |
| **GEMET** | Thesaurus | RDF/SKOS | Open (EEA) | 36 | Legal environmental terms |
| **EuroVoc/LoW** | Thesaurus | CSV/XML | Open | 24 | 842 waste type codes |
| **IATE** | Terminology | TBX | Open | 24 | Technical phrase translations |
| **Open Food Facts** | Product DB | JSONL | ODbL | Many | Packaging taxonomy |
| **Abfallnavi** | Municipal API | REST JSON | Open | DE | Local fractions, calendars |
| **ECHA-term** | Chemical | XML/CSV | Public | EU langs | GHS H-statement translations |
| **OpenStreetMap** | Geospatial | Overpass | ODbL | Tags | Recycling location finder |

---

## Report 14: Textiles & Clothing

### Key Finding: The Domain Bifurcation Problem

The textile waste domain is **bifurcated** between:
1. **Legacy static structures**: PDF regulations, standard lists (current reality)
2. **Emerging dynamic standards**: EU Digital Product Passport (DPP) initiative (2027-2030)

Fessi must bridge both worlds - ingesting current "ground truth" while building schema compliance for future traceability standards.

**The "Monstrous Hybrid" Problem:**
Fiber blends are the core technical challenge. The T-REX Project highlights:
- **Polyester/Cotton**: Most common blend, chemical separation is energy-intensive
- **Elastane (Spandex)**: The "contaminant" - **even 5% elastane** can clog mechanical shredders and disrupt chemical recycling

| Condition | Classification | Destination |
|-----------|---------------|-------------|
| Wearable, Clean | Alttextilien (reuse) | Altkleidercontainer |
| Torn, Clean | Textilabfall (recycling) | Textile recycling |
| Wet/Moldy | Contaminated | **Restmüll** (LAGA M 40) |
| Paint/Oil-stained | Hazardous | Schadstoffmobil / Restmüll |
| >5% Elastane blend | Disruptor | Downcycle / Incineration |

### Data Source Categories

#### 1. Material Composition & Science Layer

**NIST NIR-SORT Database (2025)** - Essential (5/5)
- **Provider**: National Institute of Standards and Technology (USA)
- **URL**: https://www.nist.gov/news-events/news/2025/01/nist-database-can-help-increase-recycling-textiles-and-clothing
- **Format**: JCAMP-DX (spectroscopy), CSV (metadata)
- **Access**: Free (Public Domain)
- **Coverage**: 64 distinct fabric types, including pure fibers and complex blends
- **Value**: Provides NIR spectral "fingerprints" for automated sorting - moves from inaccurate consumer labels to scientific verification
- **Entity Mapping**: `Material` nodes with `polymer_type`, `blend_ratio`, `condition_state`
- **Graph Logic**: `IF ItemCategory = "Sportswear" AND User_Input_Material = NULL -> Inferred_Material = "Polyester/Elastane" -> Recyclability = "Low"`

**ISO 3758 & ISO 2076 (via GINETEX)** - High (4/5)
- **Provider**: ISO / GINETEX
- **URL**: https://www.ginetex.net/gb/labelling/care-symbols.asp
- **Format**: PDF Standards, Web Definitions
- **Access**: Restricted (standards are paid, symbol images are trademarked)
- **Coverage**: Global fiber naming (ISO 2076: Modal, Lyocell, Viscose) + care symbols (ISO 3758)
- **Entity Mapping**: `CareInstruction` nodes with `symbol_id`, `temperature_limit`, `process_type`
- **Constraint**: Cannot display trademarked GINETEX symbol images without license - use text descriptions

**MatWeb Material Property Database** - Supporting (3/5)
- **Provider**: MatWeb, LLC
- **URL**: https://www.matweb.com/
- **Format**: Web (HTML), Premium Export (Excel/XML)
- **Access**: Freemium
- **Coverage**: Technical properties of polymers (tensile strength, melting point, density)
- **Value**: For technical textiles (lab coats, protective gear) - determines thermal recycling compatibility
- **Entity Mapping**: `Polymer` nodes with `melting_point_celsius`, `density_g_cm3`, `biodegradability_status`

#### 2. Logic, Rules & Recyclability Layer

**LAGA Mitteilung 40** - Critical (5/5)
- **Provider**: Bund/Länder-Arbeitsgemeinschaft Abfall (LAGA)
- **URL**: https://www.laga-online.de/documents/laga-m40-vollzugshilfe-zur-vermeidung-sowie-zur-erfassung,-sortierung-und-verwertung-von-alttextilien_1685020405.pdf
- **Format**: PDF (unstructured text)
- **Access**: Free
- **Coverage**: Germany (Federal)
- **Key Concepts**:
  - **End-of-Waste criteria**: Textiles only "prepared for reuse" if sorted, checked, and market exists
  - **Sorting Criteria**: Teilsortierung (Partial) vs Vollsortierung (Full)
  - **Hazard Markers**: Moldy/wet textiles → Restmüll, NOT textile banks
- **Entity Mapping**: `DisposalRule` and `Condition` nodes
- **Decision Tree Logic**:
  - `Condition:Wet` → `VIOLATES_RULE` → `Rule:LAGA_M40_Dry_Storage`
  - `Result: Disposal_Action = "Dispose in Restmüll immediately"`

**T-REX Project Technical Guidance (2025)** - High (5/5)
- **Provider**: T-REX Project (EU Horizon 2020 Consortium)
- **URL**: https://circulartextiles.aalto.fi/trexproject/wp-content/uploads/2025/06/T-REX-Technical-Guidance-For-Recyclable-Garments.pdf
- **Format**: PDF
- **Access**: Free
- **Coverage**: EU - focused on Polyester, Polyamide 6, Cotton
- **"Traffic Light" System**:
  | Color | Status | Description | Example |
  |-------|--------|-------------|---------|
  | **Red** | Disrupter | Makes whole garment unrecyclable | >5% Elastane in Polyester |
  | **Yellow** | Compatible | Accepted within thresholds | Minor trims, buttons |
  | **Green** | Preferred | Mono-material | 100% Cotton, 100% Polyester |
- **Entity Mapping**: `RecyclabilityConstraint` with `threshold_percentage`, `disruptor_material`, `base_material`
- **Graph Query**: `MATCH (m:Material {name: 'Elastane'})-->(i:WasteItem) WHERE r.percentage > 5 RETURN 'Disruptor'`

**Textile Exchange Preferred Fiber & Materials Matrix (PFMM)** - Contextual (3/5)
- **Provider**: Textile Exchange
- **URL**: https://textileexchange.org/pfmm/
- **Format**: Interactive Web Tool / Reports
- **Access**: Free / Registration
- **Coverage**: Global sustainability metrics (Water, Energy, Chemical use)
- **Value**: "Preferred" vs "Conventional" fiber categorization for educational feedback
- **Entity Mapping**: `SustainabilityMetric` linked to `Material` with `water_impact_score`, `carbon_footprint_relative`

#### 3. Infrastructure & Supply Chain Layer

**OpenStreetMap (OSM) - recycling:clothes** - Essential (5/5)
- **Provider**: OSM Foundation
- **URL**: https://overpass-turbo.eu/
- **Format**: XML / JSON / GeoJSON (via Overpass API)
- **Access**: Free (ODbL)
- **Tags of Interest**:
  - `amenity=recycling`
  - `recycling:clothes=yes`
  - `recycling:shoes=yes`
  - `operator=*` (e.g., "DRK", "Malteser", "Private Commercial")
- **Entity Mapping**: `Container`, `Location` nodes
- **Overpass Query**:
```
[out:json];
area["name"="Heidelberg"]->.searchArea;
(
  node["amenity"="recycling"]["recycling:clothes"="yes"](area.searchArea);
  node["social_facility"="clothing_bank"](area.searchArea);
);
out body;
```

**Local Municipal Abfallkalender APIs** - Essential (5/5)
- **Providers**: Local Waste Management Utilities (AWB, BSR, etc.)
- **URLs**:
  - Würzburg: https://opendata.wuerzburg.de/explore/dataset/abfallkalender-wuerzburg/api/
  - Mannheim: https://mannheim.opendatasoft.com/explore/dataset/150824-altkleidercontainer-mannheim/api/
- **Format**: REST API (JSON), ICS (iCal), CSV
- **Access**: Free (Open Data Germany)
- **Value**: Official data for Schadstoffmobil schedules, Wertstoffhof closures - data OSM might miss
- **Entity Mapping**: `Facility` with `Schedule`
- **Logic**: `IF Item = "Contaminated Textile (Paint/Oil)" -> Find next Schadstoffmobil date -> Return Date & Location`

**FairWertung & BVSE (Trusted Collectors)** - High trust, low tech (3/5)
- **Providers**: Dachverband FairWertung e.V. / BVSE
- **URLs**: https://fairwertung.de/, https://www.bvse.de/
- **Format**: HTML map interfaces (web scraping required)
- **Value**: FairWertung = charitable/ethical collectors, BVSE = certified commercial recyclers
- **Entity Property**: `TrustScore` or `OrganizationType`
- **Logic**: Prioritize FairWertung locations over generic/unknown containers in search results

**Open Supply Hub (OAR)** - Supply Chain Context (4/5)
- **Provider**: Non-profit (formerly Open Apparel Registry)
- **URL**: https://opensupplyhub.org/api/docs/
- **Format**: REST API
- **Access**: Free (registration required)
- **Value**: Maps Brand → Supplier relationships for supply chain transparency
- **Use Case**: If Fessi has brand transparency feature ("How sustainable is this brand?"), OAR provides the supply chain map

**Good On You (Brand Ratings)** - High value, high barrier (3/5)
- **Provider**: Good On You (Social Enterprise)
- **URL**: https://partnerships.goodonyou.eco/solutions/publishers
- **Format**: JSON API
- **Access**: **Paid / Commercial Partnership required**
- **Coverage**: Thousands of brands rated on People, Planet, Animals
- **Use Case**: Annotate `Brand` entity - when discarding "Shein" item, optionally display low durability rating

#### 4. Future-Proofing Layer: Digital Product Passport (DPP)

**CIRPASS (Digital Product Passport)** - Architecture Definition (5/5)
- **Provider**: EU Consortium
- **URL**: https://cirpassproject.eu/
- **Value**: Defines the schema for future DPP - not a data source itself, but the structure definition
- **Key Fields**: `unique_product_identifier`, `recyclability_information`, `substances_of_concern`
- **Integration**: Fessi ontology should use CIRPASS class names (e.g., `dpp:RecycledContent`) now to ensure DPP readiness when mandate arrives (2027-2030)

**GS1 Digital Link & Barcode Lookup** - Identification (4/5)
- **Provider**: GS1 / Commercial Providers (Barcode Lookup)
- **URLs**:
  - https://www.gs1.org/standards/gs1-digital-link
  - https://www.barcodelookup.com/api
- **Value**: Mechanism for DPP - barcode scan resolves to URL `https://id.brand.com/01/{GTIN}`
- **Integration**: Implement "Scan Barcode" feature, backend queries Barcode Lookup to identify item, then query internal Graph for disposal logic

**Fashion Revolution Transparency Index** - Static (2/5)
- **Provider**: Fashion Revolution (NGO)
- **URL**: https://www.fashionrevolution.org/
- **Format**: PDF Report / CSV (sometimes released)
- **Coverage**: Ranks top 250 brands on disclosure
- **Use Case**: Flag brands that should have take-back programs → suggest "Check Brand Website for Take-Back"

**Wikidata (Textile Properties)** - NLP Support (4/5)
- **URL**: https://www.wikidata.org/
- **Format**: SPARQL Endpoint
- **Access**: Free
- **Value**: Multilingual synonyms essential for NLP (English "Cotton" = German "Baumwolle")
- **Query**: Get all instances of "synthetic fiber" with "recycling code"

### Source Profile Matrix

| Source | Layer | Format | Access | Entity Types | Score |
|--------|-------|--------|--------|--------------|-------|
| NIST NIR-SORT | Material | JCAMP-DX/CSV | Free | Material | 5/5 |
| ISO 3758/2076 | Material | PDF/Web | Paid | CareInstruction | 4/5 |
| MatWeb | Material | HTML/Excel | Freemium | Polymer | 3/5 |
| LAGA M 40 | Logic | PDF | Free | DisposalRule, Condition | 5/5 |
| T-REX | Logic | PDF | Free | RecyclabilityConstraint | 5/5 |
| Textile Exchange | Logic | Web/Reports | Free | SustainabilityMetric | 3/5 |
| OSM | Infra | JSON/GeoJSON | Free | Container, Location | 5/5 |
| Municipal APIs | Infra | REST/ICS | Free | Facility, Schedule | 5/5 |
| FairWertung | Infra | HTML | Free | TrustScore | 3/5 |
| Open Supply Hub | Supply | REST API | Free | Brand, Supplier | 4/5 |
| Good On You | Supply | JSON API | **Paid** | Brand Rating | 3/5 |
| CIRPASS | DPP | Schema | Free | DPP Schema | 5/5 |
| GS1 Digital Link | DPP | Resolver | Mixed | GTIN | 4/5 |
| Fashion Revolution | DPP | PDF/CSV | Free | Brand Ranking | 2/5 |
| Wikidata | NLP | SPARQL | Free | Synonyms | 4/5 |

### Critical Domain Concepts

**Alttextilien vs Textilabfall Distinction (KrWG)**
```
Alttextilien (used textiles) → Reuse pathway → Altkleidercontainer
Textilabfall (textile waste) → Recycling pathway → Material recovery
```

**The "Paint Can Problem" for Textiles**
- Paint-stained t-shirt: contamination (paint) **overrides** material potential (cotton)
- Classification shifts: AVV 20 01 10 (Clothes) → potentially hazardous or Restmüll

**Elastane Threshold Rule (T-REX)**
```
IF Material.elastane_percentage > 5% THEN
  Recyclability = "Disruptor"
  Instruction = "Check label for 'Elastane'. If present, item acts as contaminant in PET recycling."
END
```

### Gaps Identified

1. **Real-Time Brand Take-Back Inventory**: No public API to query "Does H&M at Heidelberg accept torn shoes today?"
   - *Mitigation*: Create manual "Brand Program" nodes for major retailers, update quarterly

2. **Container Capacity (Fill Levels)**: OSM provides location, not status. Smart bin IoT data is proprietary
   - *Mitigation*: Rely on user feedback ("Bin Full") to temporarily flag nodes

3. **Chemical Additive Transparency**: NIST identifies fiber, but dyes/finishes (potentially hazardous) require lab testing
   - *Mitigation*: Conservative logic - if "treated" or "industrial," default to Restmüll or Hazardous stream

---

## Report 15: Disassembly Instructions

### Key Finding: The Information Asymmetry Problem

The core challenge in e-waste management is **information asymmetry**:
- **Users** hold broken devices but lack knowledge about hazardous components
- **Regulatory frameworks** (ElektroG, WEEE Directive) define complex compliance rules
- **"How-to" knowledge** lives in community platforms (iFixit), while **"compliance" data** is locked in manufacturer PDFs and regulatory databases (ECHA SCIP)

**Fessi's role**: A semantic translation layer bridging these domains through a knowledge graph.

**The Four Pillars of E-Waste Ontology:**
1. **The Entity**: Physical device (Brand, Model, GTIN, Category)
2. **The Component Structure**: Mereology - `(Device)-[:HAS_PART]->(Battery)`
3. **The Procedure**: Disassembly sequence - ordered `Step` nodes with tools and instructions
4. **The Classification**: Legal waste code mapping to AVV/EWC

**WEEE Directive Article 15 (The "North Star"):**
Manufacturers must provide "Information for Recyclers" identifying **Annex VII components** requiring selective treatment:
- Lithium-ion batteries
- Printed circuit boards > 10 cm²
- Mercury-containing components (CFL backlights)
- Capacitors containing PCBs or electrolytes
- Plastic containing brominated flame retardants

| Device State | Component Present | AVV Code | Destination |
|--------------|-------------------|----------|-------------|
| Intact, functional | - | Repair/Reuse | iFixit guide |
| Broken, no hazards | - | 20 01 36 | E-Schrott (Wertstoffhof) |
| Broken, battery intact | Lithium-ion | 20 01 35* | Battery removal first |
| Broken, battery swollen | Lithium-ion (damaged) | 16 06 05 | **Fire hazard** - special handling |
| Lab equipment | Unknown SVHCs | Query SCIP | Depends on dossier |

### Data Source Categories

#### 1. Community & Crowd-Sourced Repair Knowledge

**iFixit API v2.0** - Essential (5/5)
- **Provider**: iFixit (USA)
- **URL**: https://www.ifixit.com/api/2.0
- **Format**: REST API (JSON)
- **Access**: Free (no auth for read-only), rate-limited
- **License**: CC-BY-NC-SA 3.0 (non-commercial, attribution required)
- **Coverage**: 100,000+ repair guides, excellent for consumer IT (laptops, phones), poor for lab equipment
- **Key Endpoints**:
  - `/categories` - Device taxonomy tree (Electronics → Computer → Laptop → MacBook)
  - `/devices/{id}` - Device specs, repairability score
  - `/guides/{id}` - Disassembly steps with `text_raw`, tools, images
  - `/suggest` - NLP entity resolution ("MacBook" → Device ID)
- **Entity Mapping**:
  - `Category` → `ProductCategory` nodes with `subClassOf` hierarchy
  - `Device` → `Product` nodes with `brand`, `model`, `repairability_score`
  - `Guide` → `DisassemblyProcess` with ordered `Step` nodes
  - `Tool` → standardized `Tool` entities
  - `Part` → `Component` nodes (flag `IsHazardous` via keyword heuristics)
- **Graph Logic**: `IF Device.category = "Laptop" -> INFER HasComponent(Lithium-ion Battery) -> REQUIRES Battery Removal Guide`

**Open Repair Data Standard (ORDS)** - High (4/5)
- **Provider**: Open Repair Alliance
- **URL**: https://standard.openrepair.org/
- **GitHub**: https://github.com/openrepair/data
- **Format**: CSV/JSON batch downloads
- **Access**: Free (aggregated from repair cafés)
- **Schema Fields**:
  - `product_category` - Controlled vocabulary (maps to iFixit hierarchy)
  - `fault_type` - "Battery failure", "Screen broken", "PCB failure"
  - `repair_status` - "End of Life" vs "Repairable"
  - `repair_barrier` - "Spare parts unavailable", "Cost prohibitive"
- **Value**: Enables **predictive triage** - if 60% of toasters fail due to heating elements (non-hazardous), Fessi can adjust questions
- **Entity Mapping**: Statistical properties on `ProductCategory` nodes (`hasRepairSuccessRate`, `commonFaultType`)

**python-ifixit / ifixit2zim** - Supporting (3/5)
- **Provider**: OpenZIM (Kiwix)
- **GitHub**: https://github.com/openzim/ifixit
- **Format**: Python library
- **Access**: MIT License
- **Value**: Handles pagination, can dump entire categories for batch ingestion

#### 2. Manufacturer Compliance Data (Authoritative Source)

**Apple Recycler Guides** - Essential (5/5)
- **Provider**: Apple Inc.
- **URL**: https://www.apple.com/recycling/recycler-guides/
- **Format**: PDF (static, no API)
- **Access**: Free download
- **Coverage**: Most Apple products (iPhone, MacBook, iPad, Apple Watch)
- **Key Section**: **Annex VII Components Table** - explicitly maps WEEE Directive requirements to device components
  - Example (MacBook Air): "Main logic board" (PCB > 10 cm²), "Lithium-ion polymer batteries"
- **Value**: Authoritative hazard identification for high-frequency devices
- **Ingestion Strategy**: PDF scraper targeting "Annex VII" table (consistent structure across documents)
- **Entity Mapping**: `Product.model_number` (e.g., "A2179") → `Component` nodes with `IsHazardous=true`
- **Hybrid Verification**: Cross-reference Apple's hazard data with iFixit guides to flag priority disassembly steps

**Dell WEEE Recycler Information** - High (4/5)
- **Provider**: Dell Technologies
- **URL**: https://www.dell.com/support/kbdoc/en-us/000144813/recycling-dell-monitors-how-to-get-disassembly-instructions-for-dell-monitors
- **Format**: PDF
- **Access**: Free (on request for some models)
- **Content**: Declarative statements ("Product does not contain Mercury", "Uses Lithium Primary Coin Cell Battery")
- **Automation**: Dell APIs for inventory export (`HardwareInventory` JSON) help identify device configurations
- **Entity Mapping**: Negative assertions create `DoesNotContain` relationships; positive assertions create `Contains` relationships

**HP Disassembly Instructions** - High (4/5)
- **Provider**: HP Inc. / HPE
- **URL**: https://www.hpe.com/global/livingprogress/environmentalprogress/productdata/
- **Format**: PDF with exploded view diagrams
- **Access**: Free
- **Value**: Visual identification of capacitors containing PCBs or electrolytes (Annex VII hazard)
- **Entity Mapping**: `Component.location` (visual reference), `Component.hazard_type`

**I4R Platform (Information for Recyclers)** - High (4/5)
- **Provider**: WEEE Forum
- **URL**: https://i4r-platform.eu/
- **Format**: Harmonized "fiches" (category-level data)
- **Access**: Designed for registered recyclers (seek academic access)
- **Coverage**: ~47 product fiches across 6 categories
- **Value**: **Fallback logic** - if specific device guide is missing, use category-level hazard profile (e.g., "Small Industrial Appliance" → check capacitors, heavy metals)
- **Entity Mapping**: `ProductCategory` → default `HazardProfile` for category inference

#### 3. Hazardous Material Intelligence

**ECHA SCIP Database** - Essential (5/5)
- **Provider**: European Chemicals Agency
- **URL**: https://echa.europa.eu/scip
- **Format**: IUCLID (XML/JSON) via System-to-System (S2S) API
- **Access**: Public query, registration for bulk access
- **Data Model**:
  - `Complex Object` → Finished product (Laptop)
  - `Article` → Component (O-ring, Chip)
  - `Substance` → Chemical (Lead, Cadmium)
- **Key Data**:
  - **SVHCs**: Candidate List substances > 0.1% concentration
  - **Safe Use Instructions**: Handling guidance text
- **Value**: For lab equipment with unknown composition, query SCIP by GTIN/EAN for authoritative chemical data
- **Entity Mapping**: `Product.gtin` → SCIP Dossier → `Substance` nodes with `IsSVHC=true`
- **Graph Logic**: `IF Product.hasSCIP_Dossier AND SVHC.concentration > 0.1% -> WasteCode = "Hazardous" (20 01 35*)`

**ProSUM / Urban Mine Platform** - High (4/5)
- **Provider**: WEEE Forum / EU Horizon 2020
- **URL**: https://weee-forum.org/urban-mine-platform/
- **Format**: Web interface, downloadable reports
- **Access**: Free
- **Content**: Average chemical composition of product groups (e.g., "Smartphone" → 0.03g Gold, 0.3g Silver)
- **Value**: Powers "Impact" messaging - "Recycling this phone recovers 0.03g of Gold" (behavioral nudge)
- **Entity Mapping**: `ProductCategory.avgMaterialComposition` property

#### 4. Waste Classification & Municipal Integration

**ewc-onto (European Waste Catalogue Ontology)** - Essential (5/5)
- **Provider**: DBFZ (German Biomass Research Centre)
- **GitHub**: https://github.com/kschmidt-at-dbfz/ewc-onto
- **URL**: https://datalab.dbfz.de/ontologies/
- **Format**: OWL (Web Ontology Language)
- **Access**: Free
- **Coverage**: 20 Chapters, 842 Waste Types from EWC/AVV
- **Key Feature**: `isHazardous` property (captures asterisk codes like 20 01 35*)
- **Value**: Fessi's "Output Layer" - all products must map to a waste code
- **Graph Logic (SWRL)**: `Product(?p) ^ hasComponent(?c) ^ IsHazardous(?c, true) -> hasWasteCode(?p, "20 01 35*")`
- **Entity Mapping**: Import entire ontology as `WasteCode` nodes with hierarchy

**MyMüll API (JumoMind)** - High (4/5)
- **Provider**: JumoMind GmbH
- **URL**: https://mymuell.jumomind.com/mmapp/api.php
- **Format**: REST API (JSON)
- **Parameters**: `r=dates`, `city_id`, `area_id`
- **Coverage**: Many German municipalities (NRW, Hesse focus)
- **Value**: Answers "When" and "Where" - collection dates for Sperrmüll, Elektroschrott
- **Entity Mapping**: `DisposalSchedule` nodes linked to `Location`

**AbfallPlus API** - High (4/5)
- **Provider**: AbfallPlus GmbH
- **GitHub**: https://github.com/Bouni/abfallplus
- **URL**: https://api.abfallplus.de/index.php
- **Format**: REST API (JSON)
- **Access**: Requires API key (often found in municipal website network traffic)
- **Value**: Street-level collection schedules
- **Entity Mapping**: Address-specific `CollectionPoint` nodes

### Source Profile Matrix

| Source | Format | Access | Entities | Priority |
|--------|--------|--------|----------|----------|
| **iFixit API v2.0** | REST JSON | Free (CC-BY-NC-SA) | Product, Component, DisassemblyStep, Tool | Essential |
| **Apple Recycler Guides** | PDF | Free | Product, Component (Annex VII) | Essential |
| **ECHA SCIP** | IUCLID S2S | Free/Register | Product, Article, Substance (SVHC) | Essential |
| **ewc-onto** | OWL | Free | WasteCode (842 types) | Essential |
| **ORDS** | CSV/JSON | Free | ProductCategory (statistics) | High |
| **Dell WEEE Info** | PDF | Free | Product, Component | High |
| **HP Disassembly** | PDF | Free | Product, Component | High |
| **I4R Platform** | Fiches | Register | ProductCategory (fallback) | High |
| **ProSUM** | Web/Reports | Free | ProductCategory (materials) | High |
| **MyMüll API** | REST JSON | Free | DisposalSchedule, Location | High |
| **AbfallPlus API** | REST JSON | API Key | CollectionPoint | High |
| **python-ifixit** | Python | MIT | (ingestion tool) | Supporting |

### Critical Domain Concepts

**ElektroG (Elektro- und Elektronikgerätegesetz)**
German transposition of EU WEEE Directive 2012/19/EU. Defines 10 categories of EEE and producer responsibilities.

**WEEE Directive Article 15**
Mandates manufacturers provide "Information for Recyclers" free of charge - the legal basis for Apple/Dell/HP recycler guides.

**Annex VII Components**
Components requiring selective treatment before shredding:
- PCBs > 10 cm²
- Mercury-containing components
- Batteries
- Plastic containing brominated flame retardants
- Capacitors containing PCBs
- External electric cables

**WEEE AVV Codes**
| Code | Description | Hazardous |
|------|-------------|-----------|
| 20 01 35* | Discarded electrical equipment containing hazardous components | Yes |
| 20 01 36 | Discarded electrical equipment (non-hazardous) | No |
| 16 02 14 | Discarded equipment other than 16 02 09-13 | No |
| 16 06 05 | Other batteries and accumulators (Lithium) | No (but fire risk) |

### Three Data Ingestion Pipelines

**Pipeline A: Structured Harvester (iFixit & APIs)**
1. Poll `GET /categories` → Build `ProductCategory` taxonomy
2. Poll `GET /guides` → Fetch guides for relevant categories
3. Entity Mapping: iFixit "Tools" → `Tool` nodes; iFixit "Parts" → `Component` nodes
4. Hazard Flagging: Keyword heuristics ("Battery", "Mercury") → `IsHazardous = true`

**Pipeline B: Document Scraper (Manufacturer PDFs)**
1. Acquisition: Fetch PDFs from known URLs (apple.com/recycling/recycler-guides/)
2. Extraction: Use `pdfparser` to locate "Annex VII" table
3. Triangulation: Match model number (e.g., "A2179") to iFixit device
4. Enrichment: Inject authoritative hazard data, overwriting heuristics

**Pipeline C: Compliance Layer (SCIP & AVV)**
1. SCIP Query: For high-volume assets (monitors, PCs), query S2S API by GTIN
2. AVV Mapping: Use SWRL rules to infer waste codes from component hazards

### Gaps Identified

1. **"Long Tail" Lab Equipment**: iFixit covers consumer electronics well, but university labs contain specialized equipment (centrifuges, spectrometers) without public repair guides
   - *Mitigation*: Use I4R category-level fallback; allow technicians to submit PDF manuals via Pipeline B

2. **German Localization**: iFixit German guides are less comprehensive than English
   - *Mitigation*: Translation layer (DeepL API) for German-facing chatbot

3. **Digital Product Passport (DPP) Future**: EU moving toward standardized DPP (2027+)
   - *Mitigation*: Design Fessi ontology to be DPP-compatible (Product → Component → Material aligns with GS1/EPCIS)

---

## Report 16: Computer Vision Datasets

**Focus**: Training data for visual waste classification, contamination detection, volume estimation, and German-specific localization (Pfand, Gelber Sack)

**Key Insight**: The "Domain Gap Problem" - models trained on clean studio datasets fail in real-world deployment. Fessi requires a multi-model architecture: Gatekeeper (fast detection) → Specialist heads (Pfand-Net, CODD-Net, Lab-Net) → Knowledge Graph semantic mapper.

### The Visual Intelligence Challenge

A photo-based query system must handle:
1. **Studio → Wild transition**: From idealized white-background images to cafeteria tables, outdoor bins, hands holding objects
2. **German regulatory compliance**: Pfand (deposit) system, Gelber Sack (lightweight packaging), bin color codes
3. **Safety-critical detection**: GHS symbols, lab glassware, batteries, sharps - must override all other classification
4. **Volume estimation**: Fill level monitoring, Sperrmüll bulk waste sizing

### Dataset Categories by Domain

| Domain | Key Datasets | Purpose | Priority |
|--------|--------------|---------|----------|
| **Domestic/General** | TrashNet, Garbage Classification V2, TACO | Baseline classification, material textures | Critical |
| **Industrial/Sorting** | ZeroWaste, RealWaste | Conveyor belt clutter, soft plastics, contamination | Critical |
| **Volume/Depth** | MJU-Waste | RGB-D pairs for volume estimation | High |
| **Construction (C&D)** | CODD, RWTH RCM | Bauschutt (rubble), renovation waste | High |
| **Hazardous/Lab** | LabPics, Hazmat Placards, GHS Symbols | Safety layer, lab glassware, chemical phases | Critical |
| **German-Specific** | RVM Dataset, Pfand datasets, Bin color datasets | Pfand verification, Gelber Sack logic | High |
| **Biomedical** | Biomedical Waste Classification, TrashBox | Sharps detection, clinical waste | Medium |

### Source Profiles

#### D1: TACO (Trash Annotations in Context)
- **URL**: https://github.com/pedropro/TACO / http://tacodataset.org/
- **Format**: MS COCO JSON + images
- **License**: MIT
- **Coverage**: 1,500+ images, 60 categories, 28 super-categories
- **Annotation**: **Polygon segmentation masks** (not just bounding boxes)
- **Key Strength**: Hierarchical ontology maps to Knowledge Graph (Drink carton → Composite_Packaging)
- **Wild contexts**: Forests, roads, beaches, urban outdoor
- **Fessi Use**: Contamination detection via shape analysis, litter classification

#### D2: ZeroWaste-f (MRF Conveyor Belt)
- **URL**: http://ai.bu.edu/zerowaste/ / https://github.com/dbash/zerowaste
- **Format**: MS COCO format
- **License**: CC BY 4.0
- **Coverage**: 4,503 images from US Material Recovery Facility
- **Key Classes**: **Soft Plastic** (films, wrappers), rigid containers
- **Key Strength**: Extreme clutter, deformable objects, occlusion handling
- **Paper**: CVPR 2022 - "ZeroWaste Dataset: Towards Deformable Object Segmentation in Cluttered Scenes"
- **Fessi Use**: Critical for Gelber Sack soft plastic identification, contamination in paper streams

#### D3: MJU-Waste (RGB-D Volume)
- **URL**: https://github.com/realwecan/mju-waste
- **Format**: PNG (RGB) + PNG (Depth) + JSON annotations
- **License**: MIT
- **Coverage**: 2,475 RGB-D image pairs
- **Annotation**: Instance segmentation masks
- **Key Strength**: Co-registered depth data enables volume inference from monocular images
- **Fessi Use**: Bin fill level monitoring ("70% Full"), Sperrmüll pile sizing

#### D4: CODD (Construction & Demolition Waste)
- **URL**: https://data.mendeley.com/datasets/wds85kt64j/3
- **Format**: PASCAL VOC XML (bounding boxes + polygons)
- **License**: CC BY 4.0
- **Coverage**: 3,129 images, 16,000+ annotated samples
- **Classes**: Bricks, Concrete, Tiles, Wood, Pipes, Plastics, General Waste, Foaming Insulation, Stones, Plaster Boards
- **Key Strength**: 10-class taxonomy maps to German C&D regulations
- **Fessi Use**: Gips (plasterboard) vs Bauschutt separation, hazardous insulation detection (HBCD, asbestos)

#### D5: LabPics (Laboratory Context)
- **URL**: https://www.kaggle.com/datasets/sagieppel/labpics-chemistry-labpics-medical
- **Format**: Images + annotations
- **License**: CC BY 4.0
- **Coverage**: ~1,000 images of lab glassware and medical settings
- **Annotation**: Material phase segmentation (liquid, solid, foam)
- **Key Strength**: Detects "full" vs "empty" vessels for safe disposal
- **Fessi Use**: Lab glass (borosilicate) vs beverage glass (soda-lime) separation, solvent detection

#### D6: Garbage Classification V2
- **URL**: https://www.kaggle.com/datasets/mostafaabla/garbage-classification
- **Format**: JPG images in class folders
- **License**: Open (CC0/similar on Kaggle)
- **Coverage**: 15,150 images, 12 classes
- **Key Classes**: Battery, Biological, Paper, Cardboard, Metal, Glass, Plastic, Shoes, Clothes, Trash
- **Key Strength**: **Battery class** for fire hazard detection, **Bio class** for Biotonne routing
- **Fessi Use**: Baseline large-scale training, critical battery detection

#### D7: RVM Dataset (Reverse Vending Machine)
- **URL**: https://github.com/taeyoungYoo/rvm-dataset
- **Format**: Images + YOLO annotations
- **License**: MIT
- **Coverage**: Beverage bottles and cans from multiple angles (top-down, front-facing)
- **Key Strength**: Mimics machine verification perspective, fraud detection context
- **Fessi Use**: Foundation for "Pfand Check" logic - DPG logo detection, Einweg vs Mehrweg

#### D8: Hazmat Placards / GHS Symbols
- **URLs**:
  - https://www.kaggle.com/datasets/anmatngu/hazmat-placard-image-dataset
  - https://universe.roboflow.com/fireljj/ghs-hze4d
  - https://unece.org/transport/dangerous-goods/ghs-pictograms (Official SVG)
- **Format**: JPG/PNG images
- **License**: Open
- **Coverage**: Explosive, Flammable, Corrosive, Biohazard, Radioactive, CMR, Environmental
- **Key Strength**: **Safety override layer** - must run on every uploaded image
- **Fessi Use**: Abort standard classification if GHS detected, trigger safety protocol

#### D9: TrashNet (Stanford Benchmark)
- **URL**: https://github.com/garythung/trashnet
- **Format**: JPG images in class folders
- **License**: MIT
- **Coverage**: 2,527 images, 6 classes (glass, paper, cardboard, plastic, metal, trash)
- **Key Strength**: Controlled studio environment, standard benchmark
- **Limitation**: No German localization, limited taxonomy
- **Fessi Use**: Pre-training feature extractors, transfer learning baseline

#### D10: RWTH Aachen RCM (Recycled Concrete)
- **URL**: https://publications.rwth-aachen.de/record/1005633/files/1005633.pdf
- **Format**: Images + particle analysis
- **License**: Academic
- **Coverage**: Particle size distribution, shape analysis of recycled aggregates
- **Key Strength**: "Deep Material Analysis" - 70% concrete / 30% brick estimation
- **Fessi Use**: Advanced C&D quality assessment for circular economy auditing

### German Localization Sources

| Source | URL | Purpose |
|--------|-----|---------|
| **"thats pfand again" (p48)** | https://universe.roboflow.com/pfand48/p48 | Bottle vending machine training |
| **"bank bottle"** | https://universe.roboflow.com/zz-77ljl/bank-bottle | Pfand bottle detection |
| **WaDaBa** | https://wadaba.pcz.pl/ | Polish plastic streams (similar to Gelber Sack) |
| **RWTH NIR-MFCO** | https://www.fb5.rwth-aachen.de/... | Near-infrared plastics composition |
| **German bin colors** | Shutterstock/Stock images | Blue/Yellow/Brown/Black bin recognition |
| **Grüner Punkt** | Logo detection training | Green Dot heuristic for packaging |

### Multi-Model Architecture Recommendation

```
┌─────────────────────────────────────────────────────────────────┐
│                      IMAGE INPUT                                 │
└─────────────────────────┬───────────────────────────────────────┘
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                 LAYER 1: SAFETY GATEKEEPER                       │
│                 (YOLOv8 on Hazmat + GHS data)                    │
│                                                                  │
│  IF GHS_SYMBOL or HAZMAT detected → ABORT → Safety Protocol     │
└─────────────────────────┬───────────────────────────────────────┘
                          ▼
┌─────────────────────────────────────────────────────────────────┐
│                 LAYER 2: GENERAL DETECTOR                        │
│         (YOLOv8-seg on Garbage Classification + TACO)            │
│                                                                  │
│  Output: Class (Bottle, Box, Bag, Rubble, Glassware, Battery)   │
└────────┬────────────┬────────────┬────────────┬────────────────┘
         │            │            │            │
         ▼            ▼            ▼            ▼
    ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐
    │Pfand-Net│  │CODD-Net │  │ Lab-Net │  │Battery  │
    │ (RVM)   │  │ (C&D)   │  │(LabPics)│  │Detector │
    └────┬────┘  └────┬────┘  └────┬────┘  └────┬────┘
         │            │            │            │
         ▼            ▼            ▼            ▼
┌─────────────────────────────────────────────────────────────────┐
│                 LAYER 3: SEMANTIC MAPPER                         │
│                (Knowledge Graph Query)                           │
│                                                                  │
│  Input: Class="PET_Bottle", Logo="DPG", Condition="Crushed"     │
│  Output: "Return to supermarket for 0.25€ deposit"              │
└─────────────────────────────────────────────────────────────────┘
```

### Decision Logic: Pfand Check

```
User uploads: Photo of beverage container
│
├─ STEP 1: Is it a bottle or can?
│   └─ Detect via YOLOv8 on TACO/RealWaste
│
├─ STEP 2: Pfand verification
│   ├─ DPG logo detected? → "Return to supermarket (€0.25 deposit)"
│   ├─ Mehrweg bottle shape? → "Return to supermarket (€0.08-0.15 deposit)"
│   └─ No Pfand markers → Continue to material classification
│
├─ STEP 3: Material routing
│   ├─ Glass → Altglascontainer (sort by color)
│   ├─ PET/HDPE → Gelber Sack
│   └─ Aluminum → Gelber Sack
│
└─ STEP 4: Context check (optional)
    └─ If bin visible in photo, verify correct routing
```

### Volume Estimation Algorithm

Using MJU-Waste depth training:
```python
# Mechanism:
# 1. Segment waste pile (TACO/CODD model)
# 2. Infer average depth (MJU-Waste depth features)
# 3. Calculate volume

Volume ≈ Pixel_Area × Average_Depth × Scale_Factor

# Applications:
# - "Bin 70% Full" → Trigger cleaning request
# - "Sperrmüll pile ~2m³" → Order correct skip size
```

### Contamination Detection Logic

Using ZeroWaste and TACO segmentation:
```
User claims: "I'm recycling paper"
│
├─ Analyze segmentation masks for anomalies
│   ├─ Round rim detected (coffee cup)? → "Remove cup, it contaminates paper"
│   ├─ Metallic glint (can)? → "Remove metal before disposing"
│   └─ Plastic film over paper? → "Separate plastic wrapper"
│
└─ If contaminated:
    └─ "Contamination Alert: Remove [item] before disposal"
```

### Model Training Pipeline

| Stage | Dataset | Architecture | Purpose |
|-------|---------|--------------|---------|
| **1. Pre-training** | TrashNet, Garbage V2 | ResNet-50 backbone | Material texture features |
| **2. Detection** | TACO, ZeroWaste | YOLOv8-seg | Localization + segmentation |
| **3. Specialization** | RVM, CODD, LabPics | Fine-tuned heads | Domain-specific detection |
| **4. Depth** | MJU-Waste | Monocular depth network | Volume inference |
| **5. Safety** | Hazmat, GHS | Fast classifier | Pre-emptive hazard scan |

### Critical Implementation Notes

1. **Battery Detection Priority**: Batteries in general waste = fire hazard. Must achieve >95% recall.
2. **Lab Glass Separation**: Borosilicate (lab) vs soda-lime (beverage) - different melting points contaminate recycling.
3. **Soft Plastic Challenge**: Films/wrappers are deformable, translucent, crumpled - ZeroWaste is essential training data.
4. **German Bin Colors**: Blue (Paper), Yellow (Packaging), Brown/Green (Bio), Black/Grey (Residual) - must recognize for context checks.
5. **License Compliance**: Most datasets MIT or CC BY 4.0 - safe for commercial use. Verify before deployment.

### Pre-trained Models Available

| Source | Platform | Notes |
|--------|----------|-------|
| **waste-classification-yolov8-ken** | Hugging Face | Pre-trained YOLOv8 on waste |
| **litter-detection** | GitHub | YOLOv8 + TACO |
| **waste-datasets-review** | GitHub (AgaMiko) | Meta-repository of all datasets |

### Data Gaps & Mitigations

1. **No native German Gelber Sack dataset**
   - *Mitigation*: Use WaDaBa (Polish, similar streams) + ZeroWaste soft plastic class

2. **Limited DPG logo training data**
   - *Mitigation*: Synthetic data generation, fine-tune on p48/bank bottle Roboflow datasets

3. **Lab waste underrepresented**
   - *Mitigation*: LabPics + Vector-LabPics + synthetic augmentation

4. **No German university campus dataset**
   - *Mitigation*: Collect and annotate custom dataset using Fessi deployment feedback loop

---

## Report 17: Transport/ADR Regulations

**Focus**: ADR 2025 compliance, dangerous goods transport APIs, 1000-Points Rule (Kleinmengenregelung), UN Number resolution, and packaging requirements for university hazardous waste transport.

**Critical Finding**: **BAM Gefahrgutdatenservice became FREE on July 23, 2025** - paradigm shift for academic projects. Previously paid subscription now open access.

### Key Regulatory Concepts

| Concept | Description | Implementation |
|---------|-------------|----------------|
| **ADR 2025** | International road transport of dangerous goods | UNECE agreement, German implementation via GGVSEB |
| **1000-Points Rule** | ADR 1.1.3.6 small quantity exemption | Allows simplified transport without full ADR compliance |
| **Transport Categories** | Risk levels 0-4 with point multipliers | BK 0 = prohibited, BK 4 = unlimited |
| **AVV→UN Gap** | Waste codes vs transport codes disconnect | AVV = source-based, ADR = hazard-based |
| **Limited Quantities (LQ)** | Inner packaging size exemption | LQ-marked goods exempt from points calculation |

### 1000-Points Rule Multipliers (ADR 1.1.3.6)

```
┌─────────────────────────────────────────────────────────────────┐
│               TRANSPORT CATEGORY MULTIPLIERS                     │
├──────────────┬────────────┬───────────────────┬─────────────────┤
│ Category(BK) │ Multiplier │ Examples          │ Max Qty (1000pt)│
├──────────────┼────────────┼───────────────────┼─────────────────┤
│      0       │    ∞       │ UN 2814 (Infect A)│     0 kg/L      │
│              │ (Prohibited)│ UN 1005 (Toxic Gas)                │
├──────────────┼────────────┼───────────────────┼─────────────────┤
│      1       │    50      │ Explosives 1.1    │    20 kg/L      │
│              │            │ Toxic PG I (UN2810)│                │
├──────────────┼────────────┼───────────────────┼─────────────────┤
│      2       │    3       │ Acetone (UN 1090) │   333 kg/L      │
│              │            │ Li-Batteries(3480)│                 │
├──────────────┼────────────┼───────────────────┼─────────────────┤
│      3       │    1       │ Diesel (PG III)   │  1000 kg/L      │
│              │            │ Acids/Bases PG III│                 │
├──────────────┼────────────┼───────────────────┼─────────────────┤
│      4       │    0       │ Empty uncleaned   │   Unlimited     │
│              │            │ (UN 3509), LQ     │                 │
└──────────────┴────────────┴───────────────────┴─────────────────┘

Formula: Total Points = Σ(Quantity_i × Multiplier_i)
         If Total ≤ 1000 → Exemption applies (simplified transport)
```

### ADR Table A Column Mapping

| Column | Attribute | Chatbot Use |
|--------|-----------|-------------|
| Col 1 | **UN Number** | Primary key, all logic flows from this |
| Col 2 | Name/Description | Search vector for NLU matching |
| Col 3a | Class | Hazard icon display (Flame, Corrosive) |
| Col 3b | Classification Code | Compatibility logic (F1 = flammable liquids) |
| Col 4 | Packing Group | Risk level I/II/III → container quality |
| Col 5 | Labels | Visual compliance (diamond stickers) |
| Col 6 | Special Provisions | Exception codes (274 = add technical name) |
| Col 7a | Limited Quantities (LQ) | Max inner packaging for simplified transport |
| Col 7b | Excepted Quantities (EQ) | Micro-quantity codes E0-E5 |
| Col 8 | Packing Instructions | P001, IBC02 → container specifications |
| Col 15 | Transport Category | **1000-Points calculation input** |
| Col 15 | Tunnel Code | Routing restrictions (D/E) |

### Data Sources: Primary Tier (Government Open Data)

#### 1. BAM Gefahrgutdatenservice (FREE since July 2025!)
- **URL**: `tes.bam.de` / `dgg.bam.de`
- **Format**: CSV, Excel
- **Content**: ADR 2025 Table A (UN Numbers, Proper Shipping Names, Classes, Transport Categories)
- **Status**: **FREE for all uses** (paradigm shift from paid subscription)
- **License**: Attribution required
- **Integration**: Direct database seed, quarterly refresh recommended

#### 2. UNECE ADR 2025 Files
- **URL**: `unece.org/adr-2025-files`
- **URL**: `unece.org/transport/dangerous-goods`
- **Format**: PDF (primary), Word, Excel (secondary)
- **Content**: Full regulatory text, packaging instruction paragraphs (P001, P002)
- **Status**: Open Access
- **Integration**: RAG ingestion for qualitative questions ("What are 4G box requirements?")

#### 3. ECHA C&L Inventory API
- **URL**: `api.store/eu-institutions-api/european-chemicals-agency-api/echa-classification-and-labelling-inventory-api`
- **URL**: `chem.echa.europa.eu`
- **Format**: IUCLID XML, CSV bulk
- **Content**: CAS Number → GHS Classification mapping
- **Function**: Input CAS (67-64-1 for Acetone) → Output "Flam. Liq. 2, Eye Irrit. 2"
- **Integration**: Bridge from chemical name to ADR Class inference

#### 4. Wikidata SPARQL (Property P2326)
- **Properties**: P2326 (UN Number), P200 (CAS Number), P274 (Chemical Formula)
- **Query**: Massive translation layer for synonyms → UN Numbers
- **Example**: "Rubbing Alcohol" → "Isopropanol" → UN 1219
- **Links**: PubChem cross-validation
- **Status**: Open Data (CC0)

### Data Sources: Secondary Tier (Chemical Classification)

#### 5. UPS Dangerous Goods API
- **URL**: `developer.ups.com/api/reference?tag=Dangerous-Goods`
- **GitHub**: `github.com/UPS-API/api-documentation/blob/main/DangerousGoods.yaml`
- **Content**: ChemicalReferenceData endpoint - UN Number validation, Proper Shipping Names
- **Output**: Structured JSON (Class, Transport Category, packaging limits)
- **Status**: Free with API key
- **Use**: Spell-checker for Proper Shipping Names

#### 6. GESTIS Substance Database
- **URL**: `gestis-database.dguv.de`
- **Content**: Chemical safety data, storage classes, TRGS references
- **Access**: Link only (no scraping allowed)
- **Use**: Safety property enrichment

#### 7. ChemRadar UN Number Search
- **URL**: `chemradar.com/tools/un`
- **Content**: UN Number → Special Provisions cross-reference by region (EU/US/China)
- **Status**: Commercial (CIRS Group)
- **Use**: Import labeling understanding for research labs

### Data Sources: University-Specific

#### 8. University Abfall-ABCs (AVV→UN Heuristic Mapping)

| University | URL | Content |
|------------|-----|---------|
| **Uni Hamburg** | `epub.sub.uni-hamburg.de/epub/volltexte/2014/30664/pdf/2_06b.pdf` | Abfallhandbuch with waste code mappings |
| **RWTH Aachen** | `rwth-aachen.de/global/show_document.asp?id=aaaaaaaaaazyhtp` | Störfall-Verordnung, Abfallwirtschaft protocols |
| **TU Munich** | `portal.mytum.de/archiv/form_gebaeude/...` | Sonderabfälle guide for Garching campus |
| **Uni Duisburg-Essen** | `uni-due.de/imperia/md/images/ak_schulz/entsorgungsrichtlinie.pdf` | Entsorgungsrichtlinie PDF |

**Extraction Strategy**: Scrape tables mapping "Waste Description" → "AVV" → "UN Number"

### Critical Edge Cases

#### Clinical Waste Decision Tree (UN 2814 vs UN 3291)
```
User: "Petri dishes with bacteria cultures"
  │
  └─► AVV: 18 01 03* (infection prevention waste)
      │
      └─► Question: Category A or B?
          │
          ├─► Category A (Ebola, Anthrax) → UN 2814
          │   Transport Category 0 (PROHIBITED in Kleinmengen)
          │
          └─► Category B (routine cultures) → UN 3291
              Transport Category 2 (3 × quantity)
```

#### Laboratory Chemicals (AVV 16 05 06*)
```
One AVV code → HUNDREDS of UN numbers depending on hazard:
  • Mixed inorganic acids → UN 3264
  • Mixed organic acids → UN 3265
  • Flammable solvent → UN 1993
  • Toxic synthesis byproduct → UN 2810
```

#### Lithium Battery Transport (UN 3480/3481)
- UN 3480: Li-ion batteries standalone
- UN 3481: Li-ion batteries contained in/packed with equipment
- Transport Category 2 (Multiplier × 3)
- New for ADR 2025: **Sodium-ion battery provisions**

### API Integration Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                   HYBRID KNOWLEDGE LAYER                         │
├─────────────────────────────────────────────────────────────────┤
│  RELATIONAL (PostgreSQL)         VECTOR (Pinecone/Milvus)       │
│  ┌─────────────────────┐        ┌─────────────────────────┐     │
│  │ adr_master (BAM CSV)│        │ ADR 2025 Full Text PDFs │     │
│  │ • UN Number (PK)    │        │ University Abfall-ABCs  │     │
│  │ • PSN, Class, PG    │        │ → RAG for qualitative   │     │
│  │ • TransportCategory │        │   questions             │     │
│  │ • TunnelCode        │        └─────────────────────────┘     │
│  └─────────────────────┘                                         │
│  ┌─────────────────────┐                                         │
│  │ ghs_chemicals (ECHA)│        SEARCH (MeiliSearch)            │
│  │ • CAS → UN Mapping  │        ┌─────────────────────────┐     │
│  └─────────────────────┘        │ Fuzzy PSN matching      │     │
│  ┌─────────────────────┐        │ "Alkohol" → UN 1170     │     │
│  │ university_inventory│        │ Wikidata synonyms       │     │
│  │ • Container → P-Code│        └─────────────────────────┘     │
│  └─────────────────────┘                                         │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
         ┌────────────────────────────────────────┐
         │       1000-POINTS MICROSERVICE         │
         │                                        │
         │ Input: [{un_number, quantity_kg_l}]    │
         │ Process:                               │
         │   1. Fetch transport_category          │
         │   2. Map to multiplier (0,1,3,50,∞)    │
         │   3. Compute sum = qty × multiplier    │
         │ Output:                                │
         │   {total_points: 450,                  │
         │    is_exempt: true,                    │
         │    mandatory_equipment:                │
         │      ["2kg Fire Extinguisher"]}        │
         └────────────────────────────────────────┘
```

### Packaging P-Code Resolution

| P-Code | Type | Physical Requirement |
|--------|------|----------------------|
| P001 | Liquids | Combination: Glass ≤10L inner, 4G fibreboard ≤400kg outer |
| P002 | Solids | Drum (1A2) or Box (4G) |
| IBC02 | Bulk | Intermediate Bulk Container |

**University Mapping**: P001 → "Blue 10L Safety Canister (Stock #12345)"

### Transport Document Format (Beförderungspapier)
```
Format: UN [Number], [Proper Shipping Name], [Class], [PG], ([Tunnel])
Example: UN 1993, FLAMMABLE LIQUID, N.O.S. (Acetone, Ethanol), 3, II, (D/E)

Note: Special Provision 274 requires technical names in brackets
```

### Resources and Tools

| Resource | URL | Purpose |
|----------|-----|---------|
| ProSafeCon 1000-Points | `prosafecon.de/die-1000-punkte-regel/` | 1000-Points calculation guide |
| ProSafeCon Beförderungspapier | `prosafecon.de/gefahrgut-befoerderungspapier/` | Transport document templates |
| ADR-Tool.com | `adr-tool.com/` | ADR 2025 dangerous goods lookup |
| IHK Niederbayern ADR 2025 | `ihk-niederbayern.de/pdfs/adr_2025_neuerungen_folien_v2.pdf` | ADR 2025 changes presentation |
| Transport Community | `transport-community.org/wp-content/uploads/2024/12/9-RID-ADR-2025e-What-is-new.pdf` | RID/ADR 2025 what's new |
| Umweltbundesamt | `umweltbundesamt.de/en/topics/waste-resources/waste-management/waste-types/waste-classification` | AVV classification guidance |
| HSE Exemptions (UK) | `hse.gov.uk/cdg/manual/exemptions.htm` | CDG exemptions reference |

### Summary Table

| Source | Content | Access | Priority |
|--------|---------|--------|----------|
| **BAM Gefahrgutdatenservice** | ADR 2025 Table A (UN, Class, BK) | CSV/Excel (FREE!) | **Critical** |
| **UNECE ADR 2025** | Full regulatory text | PDF (Open) | **Critical** |
| **ECHA C&L API** | CAS → GHS mapping | API/Bulk | **Critical** |
| **Wikidata P2326** | Synonym → UN resolution | SPARQL (CC0) | **Critical** |
| **UPS DG API** | PSN validation | REST (Free key) | High |
| **University Abfall-ABCs** | AVV → UN heuristics | PDF scraping | High |
| **GESTIS** | Chemical safety data | Link only | Medium |
| **ChemRadar** | Regional provisions | Commercial | Low |

---

## Report 18: Medical & Lab Waste

**Focus**: LAGA M18 enforcement, AVV Chapter 18 (healthcare waste codes), TRBA biological risk groups, PubChem GHS integration, cytostatic/CMR drug classification, radioactive decay clearance (Freigabe), and UN transport packaging requirements.

### Critical Insight: The "Paint Can Problem" for Medical Waste

Medical waste exemplifies the conditional classification problem at its most complex:

```
User: "How do I dispose of this blood bag?"

Decision Tree:
├── Is it filled with blood?
│   └── YES → AVV 18 01 02 (Anatomical/Body Parts)
│       └── EthicalRequirement: Respectful incineration
│
└── Is it empty (trace contamination only)?
    └── What pathogen was it used with?
        ├── Standard flora (Staph aureus) → AVV 18 01 04 (B-Müll, cheap)
        │   └── Waste-to-energy plant (€50/t)
        │
        └── Risk Group 3+ pathogen (Salmonella typhi)
            └── AVV 18 01 03* (Hazardous infectious)
                └── Special incineration (€500/t)
                └── Transport: UN 3291
```

**Economic Impact**: Correct classification between 18 01 03* and 18 01 04 can mean 10× cost difference. The Knowledge Graph prevents "over-classification" of safe waste into the expensive hazardous stream.

### Regulatory Framework Hierarchy

```
┌─────────────────────────────────────────────────────────────────┐
│                     EU DIRECTIVE LAYER                          │
│  European Waste Catalogue (EWC) - Chapter 18                    │
└────────────────────────┬────────────────────────────────────────┘
                         │
┌────────────────────────▼────────────────────────────────────────┐
│                    GERMAN FEDERAL LAYER                         │
│  Abfallverzeichnis-Verordnung (AVV) - Chapter 18               │
│  Biostoffverordnung (BioStoffV) - Risk Group assignment        │
│  Gefahrstoffverordnung (GefStoffV) - Chemical hazards          │
│  Strahlenschutzverordnung (StrlSchV) - Radioactive clearance   │
└────────────────────────┬────────────────────────────────────────┘
                         │
┌────────────────────────▼────────────────────────────────────────┐
│                   TECHNICAL RULES LAYER                         │
│  LAGA M18 - Healthcare waste enforcement aid (PRIMARY!)        │
│  TRBA 250 - Biological agents in healthcare                    │
│  TRBA 460 - Fungi risk groups                                  │
│  TRBA 462 - Viruses/TSE risk groups                            │
│  TRBA 466 - Bacteria/Archaea risk groups                       │
│  TRGS 525 - Cytostatic drugs handling                          │
└────────────────────────┬────────────────────────────────────────┘
                         │
┌────────────────────────▼────────────────────────────────────────┐
│                  INSTITUTIONAL LAYER                            │
│  University Abfall-ABCs, Hospital Hygiene Plans                │
│  Lab inventory integration (LIMS)                              │
└─────────────────────────────────────────────────────────────────┘
```

### Data Source 1: LAGA M18 (Vollzugshilfe)

**The Central Regulatory Document for Healthcare Waste**

| Attribute | Value |
|-----------|-------|
| **Full Title** | Vollzugshilfe zur Entsorgung von Abfällen aus Einrichtungen des Gesundheitsdienstes |
| **Publisher** | LAGA (Länderarbeitsgemeinschaft Abfall) |
| **URL** | https://www.laga-online.de/documents/laga-m-18_stand_2021-06-23_1626849905.pdf |
| **Format** | PDF (119 pages) |
| **Access** | Open |
| **ETL Priority** | **CRITICAL** - Primary interpretation of AVV Chapter 18 |

**Key Concepts for Knowledge Graph**:

| Concept | German | Graph Modeling |
|---------|--------|----------------|
| Protection Targets | Schutzziele | `protectionTarget: [InfectionPrevention, OccupationalSafety]` |
| Infectious Waste | Infektiöse Abfälle | `(WasteItem)-[:HAS_HAZARD]->(Hazard {type: "Infectious"})` |
| Sharps | Spitze/scharfe Gegenstände | `physicalForm: "Sharp"` triggers container requirement |
| Adhesion Rule | Anhaftung | Trace contamination → lower classification |
| Specific Requirements | Besondere Anforderungen | Boolean: requires special handling |

### Data Source 2: AVV Chapter 18 (Healthcare Waste Codes)

**Machine-Readable AVV Catalog**

| Source | URL | Format | Notes |
|--------|-----|--------|-------|
| **Destatis** | https://www.destatis.de | XML/CSV | Federal Statistical Office |
| **SH Open Data** | https://opendata.schleswig-holstein.de | Excel/CSV | State-level AVV volumes |
| **Gesetze-im-Internet** | https://www.gesetze-im-internet.de/avv/ | HTML | Legal text source |

**Critical AVV 18 Codes for Knowledge Graph**:

| Code | Name | Hazard | Graph Logic |
|------|------|--------|-------------|
| **18 01 01** | Sharps | Physical | `IF physicalForm IN (Sharp, Pointed) → 18 01 01` |
| **18 01 02** | Body parts, organs, blood bags | Ethical | `ethicalRequirement: true`, Separate collection |
| **18 01 03*** | Infectious waste (RG ≥3) | Biological | `IF pathogen.riskGroup >= 3 → 18 01 03*` |
| **18 01 04** | Non-hazardous medical (B-Müll) | None | Default for standard pathogens |
| **18 01 06*** | Chemicals with hazardous properties | Chemical | CMR drugs, lab reagents |
| **18 01 08*** | Cytotoxic/cytostatic medicines | CMR | NIOSH Table 1, TRGS 525 |
| **18 01 09** | Non-hazardous medicines | None | Medicines not in 18 01 08* |

### Data Source 3: TRBA Documents (Biological Risk Groups)

**Technical Rules for Biological Agents (BAuA)**

| Document | Scope | URL | Format |
|----------|-------|-----|--------|
| **TRBA 466** | Bacteria, Archaea | https://www.baua.de/DE/Angebote/Regelwerk/TRBA/pdf/TRBA-466.pdf | PDF (Tables) |
| **TRBA 462** | Viruses, TSE agents | https://www.baua.de/DE/Angebote/Regelwerk/TRBA/TRBA-462 | PDF |
| **TRBA 460** | Fungi | https://www.baua.de/DE/Angebote/Regelwerk/TRBA/TRBA-460 | PDF |
| **TRBA 250** | Healthcare occupational safety | https://www.baua.de/DE/Angebote/Regelwerk/TRBA/pdf/TRBA-250.pdf | PDF |

**Risk Group Classification Table (TRBA 466/462)**:

| Risk Group | Meaning | Waste Code | Example Organisms |
|------------|---------|------------|-------------------|
| **RG 1** | No disease in healthy humans | 18 01 04 | E. coli K12, Saccharomyces cerevisiae |
| **RG 2** | Can cause disease, treatment exists | 18 01 04* | Salmonella, Aspergillus fumigatus |
| **RG 3** | Serious disease, treatment may exist | **18 01 03*** | Mycobacterium tuberculosis, HIV |
| **RG 3**/3*** | RG 3 with limited airborne risk | **18 01 03*** | Influenza H1N1 (3**), Hepatitis B (3***) |
| **RG 4** | Severe disease, no treatment | **18 01 03*** | Ebola, Marburg (UN 2814!) |

**Special Notations in TRBA**:
- **Z** = Zoonosis (animal transmission)
- **T** = Toxin producer (e.g., Clostridium botulinum)
- **A** = Allergenic potential (triggers respiratory protection)
- **3**/3*** = Limited/no airborne transmission (affects transport requirements)

### Data Source 4: DSMZ (German Collection of Microorganisms)

**The Biological Reference Database**

| Attribute | Value |
|-----------|-------|
| **Name** | Leibniz Institute DSMZ |
| **Coverage** | 80,000+ strains with biosafety data |
| **URL (Catalogue)** | https://www.dsmz.de/collection/catalogue/microorganisms/catalogue |
| **URL (Restrictions)** | https://www.dsmz.de/collection/catalogue/microorganisms/permissions-/-restrictions |
| **Access** | Web search, structured queries |
| **ETL Priority** | High - Ground truth for risk group verification |

**Use Cases**:
1. **Validation**: If lab claims strain is RG 1 but DSMZ lists RG 2 → compliance flag
2. **Transport**: Shipping restrictions inform UN 3373 vs UN 2814 classification
3. **Strain Resolution**: `Organism(strain) → DSMZ_ID → RiskGroup`

### Data Source 5: ZKBS (GMO Organisms)

**Central Committee on Biological Safety**

| Attribute | Value |
|-----------|-------|
| **Name** | ZKBS Organismenliste |
| **URL** | https://zkbs-online.de/en/databases/microorganisms |
| **URL (Download)** | https://zkbs-online.de/fileadmin/user_upload/Downloads/Datenbanken/Organismenliste.pdf |
| **Format** | PDF, **Excel/CSV export available** |
| **Access** | Open |
| **ETL Priority** | High - GMO-specific classifications |

**Key Difference from TRBA**: ZKBS classifications are for genetic engineering work (Gentechnik), which may differ from TRBA occupational safety classifications. The Knowledge Graph must store BOTH values.

**Gene Transfer Logic**:
```
Donor (RG 3) + Recipient (RG 1) → GMO Waste Classification?
└── ZKBS database provides combination rules
└── Graph: (DonorOrganism)-[:GENE_TRANSFER]->(RecipientOrganism)-[:RESULTS_IN]->(GMOWaste)
```

### Data Source 6: PubChem PUG REST/View API (GHS Classification)

**The Chemical Oracle for Lab Waste**

| Endpoint | Purpose | Example |
|----------|---------|---------|
| **PUG REST** | Resolve name/CAS to CID | `/compound/name/Methanol/cids/JSON` |
| **PUG View** | Get GHS classification | `/pug_view/data/compound/887/JSON?heading=GHS%20Classification` |

**GHS H-Code to Waste Stream Mapping**:

| H-Code | Meaning | University Waste Stream | Container |
|--------|---------|-------------------------|-----------|
| **H224, H225** | Extremely/Highly Flammable | HalogenFreeSolvent | 10L PE Conductive Canister |
| **H226** | Flammable Liquid | HalogenFreeSolvent | 10L PE Conductive Canister |
| **H340, H350, H360** | CMR Category 1A/1B | **CytotoxicWaste (18 01 08*)** | Blue Cytobox |
| **H290** | Corrosive to metals | InorganicAcid/Base | Glass/PE (check pH) |
| **H400, H410** | Aquatic toxicity | HeavyMetalWaste | Separate collection |

**The Halogen Rule** (Critical for German Labs):
```
IF MolecularFormula.contains(Cl OR Br OR F OR I) AND isOrganic:
    → WasteStream = HalogenatedSolvent
    → Requires >1100°C incineration (dioxin prevention)
    → Cost: ~€2000/t

ELSE:
    → WasteStream = HalogenFreeSolvent
    → Energy recovery at lower temp
    → Cost: ~€500/t
```

### Data Source 7: NIOSH & BGW Hazardous Drug Lists

**Cytostatic/CMR Drug Classification**

| Source | Publisher | URL | Coverage |
|--------|-----------|-----|----------|
| **NIOSH List 2024** | CDC | https://www.cdc.gov/niosh/docs/2025-103/pdfs/2025-103.pdf | US standard, 3 groups |
| **BGW List** | Berufsgenossenschaft | https://www.bgw-online.de | German occupational health |
| **TRGS 525** | BAuA | https://www.baua.de | Technical rules for cytostatics |

**NIOSH Group Classification**:

| Group | Drugs | Graph Rule | Example |
|-------|-------|------------|---------|
| **Group 1** | Antineoplastics | **Always 18 01 08*** | Cisplatin, Doxorubicin, Methotrexate |
| **Group 2** | Non-antineoplastic hazardous | Context-dependent | Carbamazepine, Ganciclovir |
| **Group 3** | Reproductive hazards | Context-dependent | Warfarin, Finasteride |

**The "Anhaftung" (Adhesion) Rule**:
```
Trace-contaminated items (gloves, swabs, empty vials):
├── Is drug residue visible/liquid? → 18 01 08* (hazardous)
└── No visible residue? → 18 01 04 (non-hazardous, cheap)

This rule can save universities €450/t in disposal costs!
```

### Data Source 8: StrlSchV Anlage 4 (Radioactive Clearance)

**Clearance Levels for Radioactive Waste**

| Attribute | Value |
|-----------|-------|
| **Regulation** | Strahlenschutzverordnung (StrlSchV) |
| **Table** | Anlage 4 (Freigabewerte) |
| **URL** | https://www.gesetze-im-internet.de/strlschv_2018/anlage_4.html |
| **Format** | HTML (table), requires parsing |
| **ETL Priority** | Critical for nuclear medicine/physics labs |

**Selected Isotope Clearance Levels**:

| Isotope | Half-Life (T½) | Clearance Solid (Bq/g) | Clearance Liquid (Bq/g) |
|---------|----------------|------------------------|-------------------------|
| **H-3** | 12.3 years | 1,000 | 1,000 |
| **C-14** | 5,730 years | 1,000 | 100 |
| **P-32** | 14.3 days | 1,000 | 100 |
| **I-125** | 59.4 days | 100 | 10 |
| **Tc-99m** | 6.0 hours | 100 | 10 |

**Decay Calculator Logic**:
```python
# Time to reach clearance level from initial activity
t = (ln(A0 / L) * T_half) / ln(2)

# Example: P-32 waste at 1 MBq/g, limit 1 kBq/g
# t = ln(1000) * 14.3 / 0.693 ≈ 142 days
# → Schedule notification: "Container P32-001 ready for clearance measurement"
```

**Graph Action**: `(RadioactiveWaste)-[:SCHEDULED_FOR_CLEARANCE {date: 142_days_from_now}]->(Notification)`

### Data Source 9: ISO 23907 (Sharps Containers)

**Container Certification Standard**

| Attribute | Value |
|-----------|-------|
| **Standard** | ISO 23907:2019 (Parts 1 & 2) |
| **URL (BSI)** | https://knowledge.bsigroup.com/products/sharps-injury-protection-requirements-and-test-methods-single-use-sharps-containers |
| **Access** | Paid standard (but specs known) |
| **ETL Priority** | Medium - Container validation |

**Technical Requirements**:
| Requirement | Specification |
|-------------|---------------|
| **Penetration Resistance** | >15N force |
| **Closure** | Temporary (day-use) + Permanent (final) |
| **Color** | Yellow body, red lid (hazard signal) |
| **Labeling** | Biohazard symbol |

**Graph Validation**: `IF wasteType = "Sharps" → validateContainer(SKU, ISO_23907_certified = true)`

### Data Source 10: UN Transport Codes (ADR Class 6.2)

**Clinical Waste Transport Requirements**

| UN Number | Name | Packaging | AVV Mapping |
|-----------|------|-----------|-------------|
| **UN 3291** | Clinical Waste, unspecified, n.o.s. | Type 4H2/1H2, P621 | 18 01 03* |
| **UN 2814** | Infectious Substance, Category A | Triple packaging, P620 | 18 01 03* (RG 4) |
| **UN 2900** | Infectious Substance (animal only) | P620 | Veterinary |
| **UN 3373** | Biological Substance, Category B | P650 | Diagnostic specimens |

**Category A vs B Decision**:
```
Pathogen capable of causing permanent disability or
life-threatening disease in healthy humans?
├── YES → Category A → UN 2814 (Triple packaging!)
│   Examples: Ebola, Anthrax, Rabies virus
│
└── NO → Category B → UN 3373 (Simpler packaging)
    Examples: Diagnostic samples, routine cultures
```

### Ontologies for Medical Waste

**Recommended Ontology Stack**:

| Ontology | Purpose | URL |
|----------|---------|-----|
| **EnvO** | Environmental classes | http://environmentontology.org/ |
| **EWC-Onto** | European Waste Catalogue structure | https://datalab.dbfz.de/ontologies/ |
| **ChEBI** | Chemical entities | https://www.ebi.ac.uk/chebi/ |

**Custom University Waste Ontology (UWO) Classes**:
```turtle
@prefix uwo: <http://university-waste.de/ontology#>.

uwo:WasteStream
    uwo:hasAVVCode       -> Link to EWC-Onto
    uwo:requiresContainer -> Link to uwo:Container
    uwo:hasRiskLevel     -> "High" | "Medium" | "Low"

uwo:Container
    uwo:certification    -> "UN 3291" | "ISO 23907"
    uwo:material         -> "PP" | "HDPE"

uwo:RegulatoryRule
    uwo:source           -> "LAGA M18" | "TRGS 525"
    uwo:logic            -> SHACL constraints
```

### ETL Pipeline Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    STATIC DATA ("Cold" Layer)                   │
│  Source: AVV XML, TRBA PDFs, StrlSchV tables                   │
│  Tools: PyPDF2, Tabula (table extraction)                      │
│  Normalization: Species names → NCBI Taxonomy IDs              │
│  Update: Quarterly (regulation changes)                         │
└────────────────────────┬────────────────────────────────────────┘
                         │
┌────────────────────────▼────────────────────────────────────────┐
│                   DYNAMIC DATA ("Hot" Layer)                    │
│  Source: PubChem API, NIOSH Lists, DSMZ queries                │
│  Trigger: Chemical added to lab inventory                       │
│  Cache: Store in graph after first lookup                       │
│  Update: Real-time or daily refresh                             │
└────────────────────────┬────────────────────────────────────────┘
                         │
┌────────────────────────▼────────────────────────────────────────┐
│                    INFERENCE ENGINE                             │
│  1. User: "Culture of Salmonella typhi"                        │
│  2. Lookup: TRBA 466 → Risk Group 3**                          │
│  3. Rule: RG ≥ 3 → AVV 18 01 03*                              │
│  4. Rule: 3** (limited airborne) → UN 3291 (not UN 2814)       │
│  5. Output: Yellow bin, autoclave before release               │
└─────────────────────────────────────────────────────────────────┘
```

### Summary Table

| Source | Content | Access | Priority |
|--------|---------|--------|----------|
| **LAGA M18** | Healthcare waste enforcement aid | PDF (Open) | **Critical** |
| **AVV Chapter 18** | Waste codes 18 01 01 - 18 01 10 | XML/CSV | **Critical** |
| **TRBA 466** | Bacteria/Archaea risk groups | PDF (BAuA) | **Critical** |
| **TRBA 462** | Virus/TSE risk groups | PDF (BAuA) | **Critical** |
| **DSMZ Catalogue** | 80k+ strains, biosafety data | Web search | High |
| **ZKBS Organismenliste** | GMO classifications | Excel/CSV | High |
| **PubChem GHS** | Chemical hazard classification | REST API | High |
| **NIOSH Drug List** | Cytostatic identification | PDF (CDC) | High |
| **BGW Lists** | German cytostatic classification | PDF | High |
| **StrlSchV Anlage 4** | Radioactive clearance levels | HTML | High |
| **ISO 23907** | Sharps container specs | Paid standard | Medium |
| **EnvO/EWC-Onto** | Ontology frameworks | OWL/RDF | Medium |

---

## Summary by Priority

### Tier 1: Must Have (Start Here)
| Source | Report | Access | Notes |
|--------|--------|--------|-------|
| Open Food Facts | 01 | API/Dump | Primary product database |
| BSR Abfall-ABC | 01, 04 | JSON/CSV | Waste category ontology, **Base Ontology** for items |
| ZSVR Mindeststandard | 01, 07 | PDF | Legal recyclability rules, **ZSVR Three-Step Verification** |
| **BAM GEFAHRGUT** | 03 | Excel/CSV | UN numbers, ADR/RID/IMDG (**FREE since 2025!**) |
| PubChem PUG REST | 03 | JSON API | Chemical identity, GHS hazards, synonyms |
| **Abfall+ / Abfall.io** | 04 | JSON API | Collection schedules, market leader for municipal apps |
| **MyMüll.de / JumoMind** | 04 | REST JSON | Collection schedules, NRW/Hesse coverage |
| **hacs_waste_collection_schedule** | 04 | Python/MIT | Pre-configured API keys for 100s of cities |
| **Gesetze-im-Internet (AVV)** | 05 | HTML/XML/PDF | Source of truth for German waste codes |
| **SEPA Waste Thesaurus** | 05 | Excel/CSV | Common terms → EWC codes (**NLU gold standard**) |
| **EU Vocabularies (EWC SKOS)** | 05 | RDF/CSV | Native KG-compatible waste classification |
| **ÖKOBAUDAT** | 06 | ILCD XML/JSON | German LCA data (EN 15804), **End-of-Life scenarios** |
| **LAGA M20** | 06 | PDF | Pollutant limits Z0/Z1/Z2 for mineral waste reuse |
| **Madaster API** | 06 | REST JSON | Material passports from BIM, **Bill of Materials** |
| **EU Decision 97/129/EC** | 07 | PDF/HTML | Root authority for material codes 01-99, **foundational** |
| **Open Food Facts Taxonomies** | 07 | Text (DAG) | Hierarchical material taxonomy (`en:plastic → en:pet`) |
| **GS1 GDSN PackagingMaterialTypeCode** | 07 | JSON-LD | B2B controlled vocabulary (POLYMER_PET, etc.) |
| **RecyClass D4R Guidelines** | 07 | PDF | Material compatibility tables (Green/Yellow/Red) |
| **Stiftung EAR (BattG Registry)** | 08 | CSV/XLSX/XML | Brand → Producer → BattG category (**entity resolution**) |
| **EU Battery Regulation 2023/1542** | 08 | PDF/HTML | New 5-category schema (LMT batteries), Battery Passport |
| **IEC 60086 / Wikipedia Battery Sizes** | 08 | HTML | Battery nomenclature "Rosetta Stone" (CR/LR/ER → Chemistry) |
| **VdS 3103 (Lithium-Batterien)** | 08 | PDF | Storage thresholds (<100Wh, >100Wh, >12kg), **safety-critical** |
| **Altholzverordnung (AltholzV)** | 09 | HTML/PDF | Categories A I-IV, **core regulatory logic** for wood waste |
| **Ökobaudat API** | 09 | ILCD XML/JSON | EPDs, material composition, LCA data (EN 15804) |
| **Holzfragen.de HSM Historie** | 09 | HTML | DDR/BRD preservative history (**historical forensics**) |
| **OpenStreetMap (Overpass API)** | 10 | JSON/XML | **Universal basemap**, 100% Germany coverage, standardized waste tags |
| **Hamburg WFS Service** | 10 | WFS/GeoJSON | Layer-segmented (paper, glass, elektro, recyhof), OGC-compliant |
| **Berlin BSR Open Data** | 10 | JSON | Recyclinghöfe + **Abfall-ABC synonym ontology** for NLU |
| **Cologne AWB Schadstoffmobil** | 10 | XML/JSON | **Structured mobile hazardous waste schedules** (rare!) |
| **PubChem PUG REST (LCSS)** | 11 | JSON API | Chemical "spine" - FlashPoint, Toxicity, **synonym handling** |
| **ECHA C&L Inventory** | 11 | IUCLID XML/CSV | EU compliance - SVHC status, H-phrases (**legal authority**) |
| **EPA Volume-to-Weight** | 11 | PDF | Bulk density by material state (loose/compacted/baled) |
| **DIN CERTCO (EN 13432)** | 11 | Web/Manual | Bioplastics compostability certification |
| **Data.gov UNSPSC CSV** | 12 | CSV | Product taxonomy "skeleton" (**Public Domain**) |
| **GS1 GPC Browser** | 12 | XML/JSON | Barcode → Brick → Attribute (**material inference**) |
| **EU Vocabularies (CN)** | 12 | RDF/SKOS | Material proxy via customs codes (**KG gold standard**) |
| **Wikidata (P5994/P186)** | 13 | SPARQL | Multilingual synonyms, **Recycling Code + Material Used** (CC0) |
| **GEMET Thesaurus** | 13 | RDF/SKOS | 36-language environmental terms, **AR/TR support** (EEA) |
| **Open Food Facts Packaging** | 13 | JSONL | **Global Packaging Taxonomy** (Shape/Material/Recycling) |
| **NIST NIR-SORT Database** | 14 | JCAMP-DX/CSV | 64 fiber spectral fingerprints, **textile material "ground truth"** |
| **LAGA Mitteilung 40** | 14 | PDF | **End-of-Waste criteria** for textiles, Alttextilien vs Textilabfall |
| **T-REX Technical Guidance** | 14 | PDF | **Traffic Light recyclability** (Red/Yellow/Green), Elastane threshold |
| **CIRPASS (DPP Schema)** | 14 | JSON-LD | Digital Product Passport architecture, **2027-2030 future-proofing** |
| **iFixit API v2.0** | 15 | REST JSON | Core disassembly instructions, device taxonomy, **100k+ guides** (CC-BY-NC-SA) |
| **Apple Recycler Guides** | 15 | PDF | Official WEEE Article 15 compliance, **Annex VII tables** |
| **ewc-onto (EWC Ontology)** | 15 | OWL | 842 waste types, `isHazardous` property, **Fessi output layer** |
| **TACO Dataset** | 16 | MS COCO + Images | **60 classes**, polygon segmentation, wild context, **contamination detection** (MIT) |
| **ZeroWaste-f** | 16 | MS COCO | MRF conveyor belt, **Soft Plastic class**, extreme clutter (CC BY 4.0) |
| **MJU-Waste (RGB-D)** | 16 | PNG + Depth | **Volume estimation** via RGB-D pairs, bin fill level (MIT) |
| **Garbage Classification V2** | 16 | JPG Images | 15k images, 12 classes, **Battery + Bio classes** (Open) |
| **UNECE ADR 2025** | 17 | PDF/Word/Excel | Full regulatory text, **packaging instructions** (P-codes), open access |
| **Wikidata P2326 (UN Numbers)** | 17 | SPARQL | **Synonym → UN Number** resolution, CAS/formula cross-links (CC0) |
| **LAGA M18** | 18 | PDF (119 pages) | **Healthcare waste enforcement aid**, AVV Chapter 18 interpretation |
| **TRBA 466 (Bacteria)** | 18 | PDF (BAuA) | Prokaryote → Risk Group 1-4 classification, **strain specificity** |
| **TRBA 462 (Viruses)** | 18 | PDF (BAuA) | Virus → Risk Group, **3**/3*** airborne notation |
| **StrlSchV Anlage 4** | 18 | HTML | Radioactive **clearance levels** (Freigabewerte) by isotope |

### Tier 2: High Value
| Source | Report | Access | Notes |
|--------|--------|--------|-------|
| Stiftung EAR | 02 | CSV | ElektroG taxonomy, producer registry |
| Icecat Open Catalog | 02 | XML/JSON API | Product dimensions, battery chemistry |
| EPREL | 02 | JSON API (QES) | Screen area, lighting tech (regulatory) |
| ECHA SCIP | 02 | IUCLID XML | Hazardous substances in products |
| iFixit API | 02 | JSON API | Teardowns, battery removability |
| LAGA Mitteilung 31 | 02 | PDF → Rules | AVV waste code logic |
| ECHA C&L Inventory | 03 | XML/CSV bulk | Harmonised GHS classifications (EU) |
| EPA List of Lists | 03 | Excel/CSV | RCRA codes, CERCLA RQs |
| GESTIS | 03 | Link only | Storage classes, TRGS (no scraping) |
| EEA EWC/LoW List | 03 | CSV/XLS | European waste codes |
| **AVV (Abfallverzeichnis-Verordnung)** | 04 | PDF/Excel | Regulatory backbone, 6-digit waste codes |
| **Munich AWM Open Data** | 04 | WFS/GeoJSON | Wertstoffhöfe GPS, hours, accepted fractions |
| **OpenStreetMap** | 04 | Overpass API | Recycling centers for "long tail" cities |
| **UK Consolidated Lists (SEPA)** | 05 | CSV/Excel | Absolute vs Mirror entry flags |
| **GOV.UK WM3 Guidance** | 05 | PDF/Excel | Mirror entry threshold calculations |
| **TRGS 201** | 05 | HTML/PDF | Simplified lab waste classification |
| **LAGA M18** | 05 | PDF | Healthcare waste (Chapter 18) implementation |
| **IPA (Abfallbewertung)** | 05 | Web | Interactive AVV lookup, characteristics |
| **Ersatzbaustoffverordnung (EBV)** | 06 | PDF | RC-1/RC-2/RC-3 recycled aggregate classes |
| **Beton Eisack Density Tables** | 06 | PDF | Real-world bulk densities from weighbridge |
| **OpenEPD / EC3 API** | 06 | JSON | Global EPDs, embodied carbon calculations |
| **GISBAU / WINGIS** | 06 | Web | GISCODES → AVV mapping for construction |
| **RegioIT Abfallnavi** | 06 | REST JSON | Disposal facility locations + hours |
| **ewccodes.com** | 06 | REST API | EWC list with hazardous flag + density |
| **Destatis Abfallbilanz** | 06 | Excel/CSV | AVV classifications + generation volumes |
| **Cyclos-HTP CHI Standard** | 07 | PDF | Recyclability certification methodology |
| **VerpackG** | 07 | HTML | German packaging act, Systembeteiligungspflicht boolean |
| **Wikidata P5994** | 07 | SPARQL | Recycling codes linked to material entities |
| **AVV Chapter 15** | 07 | HTML | 15 01 01-07 packaging waste codes (industrial) |
| **Alnatura Recycling Codes** | 07 | HTML | Consumer-friendly PAP/PET code guide |
| **Verbraucherzentrale NRW** | 07 | HTML | Packaging disposal conditional logic examples |
| **GRS Sicherheitsstandard** | 08 | PDF | Green/Yellow/Red bin system, pole taping rules |
| **Energizer/Duracell SDS Libraries** | 08 | PDF | Chemical composition, extinguishing media, AVV codes |
| **Panasonic Lithium Handbook** | 08 | PDF | Chemistry codes, voltage, dimensions |
| **RWTH Aachen Entsorgungshandbuch** | 08 | PDF | University SCA storage, H2/E1 classification |
| **Stiftung GRS Batterien** | 08 | PDF | Collection statistics, compliance reports |
| **BG Verkehr Lithium Guide** | 08 | HTML/PDF | Transport safety, fire response procedures |
| **IPA Abfallbewertung (Altholz)** | 09 | HTML | Waste wood characterization, AVV code lookup |
| **LfU Bayern Schadstoffratgeber** | 09 | PDF | PCP/DDT/Lindane identification guides |
| **WRAP Wood Identification Tools** | 09 | PDF | PAN stain test, contamination detection |
| **Uni Freiburg/Giessen Abfall-ABC** | 09 | HTML | University wood disposal rules, contacts |
| **BAV Regelvermutung Update** | 09 | PDF | Industry default wood classifications |
| **Umweltbundesamt AltholzV Evaluation** | 09 | PDF | Technical guidance, modernization proposals |
| **Munich AWM GeoJSON** | 10 | GeoJSON/CKAN | Wertstoffhöfe vs **Plus** tiers, CKAN metadata API |
| **Stiftung EAR (WEEE)** | 10 | CSV/XML | Official WEEE collection point registry |
| **Stiftung GRS Rücknahmestellen** | 10 | HTML Search | Battery "Green Boxes" location search |
| **RegioIT / AbfallNavi** | 10 | REST JSON | NRW regional backend (Aachen, Gütersloh) |
| **OSM Wiki Tagging Schema** | 10 | HTML | `amenity=recycling`, `recycling_type` ontology docs |
| **The Materials Project** | 11 | Python API | Inorganic density, composition (E-Waste, batteries) |
| **EPA EPI Suite** | 11 | Software (QSAR) | Biodegradability prediction (BIOWIN, BCFBAF) |
| **UK WRAP Bulk Densities** | 11 | PDF | Metric counterpart to EPA (European logistics model) |
| **NREL Battery Failure DB** | 11 | CSV/Excel | Li-ion thermal runaway profiles (**safety critical**) |
| **NIST WebBook** | 11 | Web | Thermochemistry, enthalpy of combustion |
| **ECLASS (Amazon Business)** | 12 | CSV/Excel | Industrial taxonomy, **Material Declarations** for REACH/SCIP |
| **Destatis WZ 2008** | 12 | Excel/XML | Economic activity classification, AVV chapter filtering |
| **TARIC Database** | 12 | XML (5 GiB) | CN codes + EU legal measures, **Ozone Depleting Substances** |
| **Eurostat Correspondence Tables** | 12 | CSV | CN↔CPA↔NACE mappings for cross-taxonomy linking |
| **Wikidata (EWC Properties)** | 12 | SPARQL | Pre-mapped product→waste relationships, semantic hub |
| **BabelNet** | 13 | API | 500-language synset disambiguation, **polysemy handling** (Mixed/Paid) |
| **ConceptNet** | 13 | REST API | Folk knowledge relations (MadeOf, UsedFor), **CC-BY-SA** |
| **EuroVoc / List of Wastes** | 13 | CSV/XML | 842 waste types, 24 EU languages, **hazardous flag (*)** |
| **IATE Terminology** | 13 | TBX | High-reliability EU terms, **Environment/Waste domains** |
| **ECHA-term (GHS)** | 13 | XML/CSV | H-statement multilingual translations, **hazard symbols** |
| **Abfallnavi API** | 13 | REST JSON | Municipal fractions, calendars, **address-level bin availability** |
| **ISO 3758/2076 (GINETEX)** | 14 | PDF/Web | Care symbols, fiber generic names (Modal, Lyocell), **label parsing** |
| **OSM recycling:clothes** | 14 | Overpass API | Altkleidercontainer locations, `operator` tags (DRK, Malteser) |
| **Municipal Altkleider APIs** | 14 | REST/ICS | Mannheim/Würzburg container datasets, Schadstoffmobil schedules |
| **Open Supply Hub (OAR)** | 14 | REST API | Brand→Supplier mapping, **supply chain transparency** |
| **GS1 Digital Link** | 14 | Resolver | Barcode→URL resolution, **DPP mechanism** |
| **Wikidata (Textile)** | 14 | SPARQL | Cotton=Baumwolle synonyms, **NLP essential** |
| **Open Repair Data Standard (ORDS)** | 15 | CSV/JSON | Repair outcome statistics, **predictive triage** (fault_type, repair_barrier) |
| **Dell WEEE Recycler Information** | 15 | PDF | Manufacturer compliance docs, `HardwareInventory` JSON export |
| **HP Disassembly Instructions** | 15 | PDF | Exploded view diagrams, capacitor/battery identification |
| **I4R Platform** | 15 | Fiches | WEEE Forum centralized hub, **category-level fallback** (~47 fiches) |
| **ProSUM / Urban Mine Platform** | 15 | Web/Reports | Average material composition per product group (Gold, Silver recovery) |
| **CODD (C&D Waste)** | 16 | PASCAL VOC | 3,129 images, 10 classes (Brick/Concrete/Plaster), **Bauschutt regulations** (CC BY 4.0) |
| **LabPics** | 16 | Images | Lab glassware, liquid/solid phases, **borosilicate vs soda-lime** (CC BY 4.0) |
| **RVM Dataset** | 16 | YOLO Annotations | Reverse vending machine views, **Pfand verification** foundation (MIT) |
| **Hazmat Placards / GHS** | 16 | JPG/PNG/SVG | GHS pictograms, **safety override layer** - scan every image (Open) |
| **TrashNet** | 16 | JPG Images | Stanford benchmark, 2,527 images, **pre-training baseline** (MIT) |
| **RealWaste** | 16 | High-Res Images | UCI landfill reception, **Textile + Vegetation classes** |
| **UPS Dangerous Goods API** | 17 | REST JSON | **Proper Shipping Name validation**, ChemicalReferenceData endpoint (free key) |
| **University Abfall-ABCs** | 17 | PDF | AVV→UN heuristic mappings (Hamburg, RWTH, TUM, UDE) |
| **ECHA C&L Inventory (API)** | 17 | XML/CSV/API | CAS → GHS classification, **Transport Class inference** |
| **DSMZ Catalogue** | 18 | Web search | **80,000+ strains** with biosafety data, RG validation |
| **ZKBS Organismenliste** | 18 | Excel/CSV | GMO classifications, **donor/recipient risk** combinations |
| **TRBA 460 (Fungi)** | 18 | PDF (BAuA) | Fungi → Risk Group, **allergenic potential (A)** notation |
| **TRBA 250** | 18 | PDF (BAuA) | Healthcare occupational safety, **autoclave requirements** |
| **NIOSH Hazardous Drug List** | 18 | PDF (CDC) | Cytostatic Groups 1-3, **antineoplastic identification** |
| **BGW Drug Lists** | 18 | PDF | German CMR drug classification, **TRGS 525 compliance** |
| **ISO 23907** | 18 | Paid standard | Sharps container specs, **penetration resistance >15N** |
| **UBA UN 3291 Guidelines** | 18 | PDF | Clinical waste transport, **packaging P621/P620** |

### Tier 3: Enrichment
| Source | Report | Access | Notes |
|--------|--------|--------|-------|
| Blue Angel | 01 | CSV/JSON | Eco-certification flags |
| CAS Common Chemistry | 03 | JSON API | CAS number validation |
| SDSParser | 03 | Python lib | SDS Section 13 extraction |
| UMN Chemical Waste Registry | 03 | Web DB | Chemical → hazard class training data |
| Müllmax | 04 | HTML → ICS | Legacy scraping, find ICS URL once |
| Aachen Schadstoffmobil | 04 | GeoJSON | Mobile hazardous collection (rare Open Data) |
| WikiWaste UK / CSG EWC | 04 | HTML | EWC codes English bootstrap |
| **LMU/Heidelberg Abfall-ABC** | 05 | PDF | University lab chemical → AVV mappings (training data) |
| **ETH Zürich Guidelines** | 05 | PDF | Chemical compatibility, separation classes |
| **ZEDAL eANV** | 05 | API/Catalog | Validated codes, facility numbers (commercial) |
| **Legislation.gov.uk EWC** | 05 | XML | Commission Decision 2000/532/EC in XML |
| **LAGA Technische Hinweise** | 05 | PDF | HP criteria concentration limits |
| **Materials Project API** | 06 | REST JSON | Crystallographic density of pure compounds |
| **2050 Materials** | 06 | REST API | Sustainable products, Uniclass taxonomy |
| **City of Vienna MA 22** | 06 | PDF | Disposal conversion factors (0.20 t/m³ mixed!) |
| **Canton Uri Guidelines** | 06 | PDF | Road construction mineral densities |
| **Baustoffrecycling Bayern** | 06 | PDF | RC-1/RC-2/RC-3 limit value tables |
| **TACO / waste-datasets (GitHub)** | 06 | CSV/Images | Waste taxonomy for CV model training |
| **abfallnavi.api.bund.dev** | 06 | OpenAPI | Federal documentation for municipal APIs |
| **DIN 6120** | 07 | Paid Standard | Marking syntax, recycling triangle geometry |
| **PPWR (EU 2025/40)** | 07 | PDF | New EU packaging regulation (future reference) |
| **GS1 Europe GDSN Guide** | 07 | PDF | GDSN implementation for packaging attributes |
| **GIZ D4R Report** | 07 | PDF | Carbon black / NIR detectability issues |
| **KIDV Factsheet** | 07 | PDF | Netherlands composite code explanation |
| **AWM Munich Trennliste** | 07 | PDF | Regional disposal rules (Wertstoffhof system) |
| **Mülltrennung wirkt!** | 07 | HTML | Dual Systems ecosystem (Der Grüne Punkt, etc.) |
| **Kaggle Garbage Classification** | 08 | JPG Images | Generic "battery" class for CV detection |
| **TÜV SÜD/Rheinland Guides** | 08 | PDF | EU Battery Regulation summaries, Battery Passport |
| **IPA Battery Codes (16 06)** | 08 | Web | AVV codes for battery waste (16 06 01-06) |
| **Uni Heidelberg BT-26** | 08 | PDF | Lab battery disposal protocols |
| **ADR 2025 Updates** | 08 | HTML | New UN 3551/3552 for Sodium-Ion batteries |
| **Wikidata (Legacy Brands)** | 08 | SPARQL | Defunct brand resolution fallback |
| **Wikidata (Chemicals Q-IDs)** | 09 | SPARQL | DDT (Q163648), PCP (Q423985), Lindane (Q207050) |
| **Michigan CSS Furniture LCA** | 09 | PDF | Office furniture material composition |
| **LaGUS DDT/Lindane Guide** | 09 | PDF | East German health implications |
| **AltholzV Anhang III** | 09 | HTML | Default classification lookup table |
| **Lidl/Kaufland Store Finder API** | 10 | JSON | Pfandautomaten flags, store features (reverse-engineered) |
| **dm Store Finder (Mapbox)** | 10 | JSON | serviceTypes: RECYCLING_STATION for cosmetics recycling |
| **Rossmann Store Locator** | 10 | JSON | Store services array |
| **lidl-plus (GitHub)** | 10 | Python | Lidl Plus API authentication library |
| **discount-scraper (GitHub)** | 10 | Python | Supermarket API reverse engineering |
| **MatWeb (Manual Curation)** | 11 | Web | Top 50 polymer properties (HDPE/LDPE/PET) - **no API** |
| **ChemSpider** | 11 | API (Restricted) | PubChem alternative (paid for high-volume) |
| **ChEMBL** | 11 | API | Bioactivity data (less useful for waste properties) |
| **EPI Suite Web Beta** | 11 | Web | QSAR estimates without local software install |
| **O*NET UNSPSC Reference** | 12 | Excel | Structured UNSPSC hierarchy (separate columns per level) |
| **Cornell UNSPSC Cheat Sheets** | 12 | Excel | University procurement relevance filter |
| **GitHub UNSPSCPrediction** | 12 | CSV/JSON | NLU training data (colloquial → formal mappings) |
| **GitHub AVV Repos (bennokr)** | 12 | NDJSON | Pre-parsed European Waste Catalogue |
| **spherity/product-pass** | 12 | Schema | Digital Product Passport → EWC code linkage |
| **CPC Correspondence Tables** | 12 | CSV | Bridge taxonomy (NACE↔CPC↔HS) |
| **Schema.org (Product, usageInfo)** | 12 | JSON-LD | Semantic web integration for disposal info |
| **EZT-online (German TARIC)** | 12 | Web | 11-digit German customs codes |
| **Open Multilingual Wordnet (OMW)** | 13 | Download | 150+ languages, **CILI interlingual index** (v2.0) |
| **LUCID Packaging Register** | 13 | Web Search | German Dual System compliance verification |
| **Municipal Multilingual Flyers** | 13 | PDF | Karlsruhe/Bremen/AHA official **AR/TR translations** |
| **Handbook Germany** | 13 | Web | Mülltrennung educational content (AR/TR/EN/DE) |
| **TrashNet / ZeroWaste** | 13 | Images | Labeled waste photos for **CV model training** |
| **GHS Pictograms (UNECE)** | 13 | SVG/PNG | Official hazard symbols for **visual confirmation** |
| **OSM recycling:glass:colour** | 13 | Overpass | Glass bank color tags for **blue glass rule** |
| **MatWeb (Polymers)** | 14 | HTML/Excel | Technical textile properties (melting point, density) |
| **Textile Exchange PFMM** | 14 | Web/Reports | Sustainability metrics, "Preferred" vs "Conventional" fibers |
| **FairWertung** | 14 | HTML (scrape) | Ethical textile collector registry, **TrustScore** |
| **BVSE Mitgliedersuche** | 14 | HTML (scrape) | Certified commercial recycler registry |
| **Good On You** | 14 | JSON API (Paid) | Brand sustainability ratings (People/Planet/Animals) |
| **Fashion Revolution Index** | 14 | PDF/CSV | Top 250 brand transparency rankings |
| **Barcode Lookup API** | 14 | REST API | GTIN→product identification, **commercial** |
| **python-ifixit / ifixit2zim** | 15 | Python | iFixit API wrapper, handles pagination, batch category dumps (MIT) |
| **apple-documentation-archive-scraper** | 15 | Python | GitHub tool for scraping Apple docs (goranmoomin) |
| **smalot/pdfparser** | 15 | PHP | PDF table extraction for Annex VII tables |
| **SpectralWaste** | 16 | HSI + RGB | Hyperspectral + RGB, **PP vs PET material signatures** (Research) |
| **RWTH Aachen RCM** | 16 | Images | Particle size distribution, **C&D material science** (Academic) |
| **Biomedical Waste Classification** | 16 | JPG Images | Kaggle, **sharps detection** for clinical safety |
| **TrashBox** | 16 | Images | GitHub, medical waste subset, multi-domain coverage |
| **WaDaBa** | 16 | Images | Polish plastic streams, **Gelber Sack proxy** |
| **RWTH NIR-MFCO** | 16 | NIR Images | Near-infrared plastics composition analysis |
| **p48 / bank bottle (Roboflow)** | 16 | YOLO Annotations | **Pfand bottle detection** training data |
| **waste-datasets-review (AgaMiko)** | 16 | GitHub Meta | Master index of all CV waste datasets |
| **waste-classification-yolov8-ken** | 16 | Hugging Face | Pre-trained YOLOv8 model on waste |
| **litter-detection (GitHub)** | 16 | Python/YOLO | YOLOv8 + TACO integration |
| **ProSafeCon 1000-Points** | 17 | Web/PDF | ADR points calculation guide, **Beförderungspapier templates** |
| **ADR-Tool.com** | 17 | Web | ADR 2025 dangerous goods lookup by UN Number |
| **ChemRadar UN Search** | 17 | Web (Commercial) | UN Number → Special Provisions by region (EU/US/China) |
| **IHK Niederbayern ADR Guide** | 17 | PDF | ADR 2025 changes presentation (German) |
| **Transport Community RID/ADR** | 17 | PDF | What's new in RID/ADR 2025 summary |
| **GESTIS (DGUV)** | 17 | Web (Link only) | Chemical safety data, storage classes, **TRGS references** |
| **HSE CDG Exemptions (UK)** | 17 | HTML | Dangerous goods exemptions reference |
| **EnvO (Environment Ontology)** | 18 | OWL | Waste classes, **medical waste** semantic concepts |
| **EWC-Onto (DBFZ)** | 18 | RDF/OWL | European Waste Catalogue structure for KG |
| **ChEBI** | 18 | OWL | Chemical entity ontology, **PubChem cross-links** |
| **Abfallmanager Medizin** | 18 | Web (Commercial) | AVV 18 01 08* guidance, **Cytobox specs** |
| **Sharpsmart Container Guide** | 18 | PDF | Reusable sharps container compliance standards |
| **Daniels Health Sharps Guide** | 18 | PDF | Container testing specs, drop/puncture requirements |
| **PHMSA Infectious Transport** | 18 | PDF | US DOT guidance on Category A/B transport |

---

## API Quick Reference

```bash
# === Report 01: Food & Grocery ===

# Open Food Facts - Get product by barcode
curl "https://world.openfoodfacts.org/api/v2/product/40111410.json?fields=code,product_name,packaging_tags,ecoscore_data"

# Open Food Facts - Bulk download (MongoDB)
wget https://static.openfoodfacts.org/data/openfoodfacts-mongodbdump.tar.gz

# === Report 02: Consumer Electronics ===

# Icecat - Get product by GTIN or Brand+Code
curl "https://live.icecat.biz/api?lang=de&ProductCode=XYZ&Brand=Samsung"

# iFixit - Get teardown guide
curl "https://www.ifixit.com/api/2.0/guides/[id]"

# === Report 03: Chemicals & Hazardous ===

# PubChem - Resolve synonym to canonical name
curl "https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/name/Acetone/synonyms/JSON"

# PubChem - Get GHS hazard classification
curl "https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/cid/180/classification/JSON"

# PubChem - Get physical properties (FlashPoint, BoilingPoint)
curl "https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/cid/180/property/MolecularFormula,FlashPoint,BoilingPoint/JSON"

# CAS Common Chemistry - Validate CAS number
curl "https://commonchemistry.cas.org/api/detail?cas_rn=67-64-1"

# MachShip - UN number lookup
curl "https://developers.live.machship.com/api/faqs/dg-lookup-by-un?unNumber=1090"

# === Report 04: German Municipal Rules ===

# Abfall+ - POST to get collection schedule (requires discovered key + IDs)
curl -X POST "https://api.abfallplus.de/index.php" \
  -d "key=5b0384147b5bc055c30fee1fb6db6f76" \
  -d "f_id_kommune=270" \
  -d "f_id_strasse=15720" \
  -d "f_id_abfalltyp_0=51" \
  -d "modul=termine"

# JumoMind - List all supported cities (dynamic discovery!)
curl "https://mymuell.jumomind.com/mmapp/api.php?r=cities"

# JumoMind - Get streets for city
curl "https://mymuell.jumomind.com/mmapp/api.php?r=streets&city_id=X"

# JumoMind - Get collection schedule (includes bin color hex codes)
curl "https://mymuell.jumomind.com/mmapp/api.php?r=trash&city_id=X&area_id=Y"

# Abfall+ Widget - Scrape Abfall-ABC HTML (item → disposal mapping)
curl "https://api.abfall.io/customers/363/widgets/abfall-abc.html"

# OpenStreetMap Overpass - Recycling centers near Munich (48.137, 11.576)
curl -d "data=[out:json];node[amenity=recycling][recycling_type=centre](around:5000,48.137,11.576);out;" \
  "https://overpass-api.de/api/interpreter"

# === Report 05: AVV/EWC Codes ===

# AVV from Gesetze-im-Internet (official German source)
curl "https://www.gesetze-im-internet.de/avv/AVV.pdf"

# GOV.UK WM3 Guidance (mirror entry thresholds)
curl "https://assets.publishing.service.gov.uk/media/6152d0b78fa8f5610b9c222b/Waste_classification_technical_guidance_WM3.pdf"

# SEPA Waste Data (Scotland)
# Navigate: https://data.gov.scot/sepa/waste/

# EU Vocabularies - EWC SKOS (RDF)
# Navigate: https://op.europa.eu/en/web/eu-vocabularies/controlled-vocabularies
# Download: "Waste" → RDF/XML or CSV format

# IPA (Abfallbewertung) - Interactive AVV lookup (web only)
# https://www.abfallbewertung.org/ipa/

# ETH Zürich Chemical Wastewater Guidelines
curl "https://ethz.ch/content/dam/ethz/associates/services/Service/sicherheit-gesundheit-umwelt/files/chemieabwasser/de/Merkblatt_Chemieabwasser_DE.pdf"

# ETH Zürich Hazardous Waste Disposal Guidelines
curl "https://ethz.ch/content/dam/ethz/associates/services/Service/sicherheit-gesundheit-umwelt/files/sonderabfall_neu/de/MB_%20Entsorgung%20von%20Sonderabf%C3%A4llen_Grundlagen_DE_.pdf"

# === Report 06: Construction Materials (Bauschutt) ===

# Beton Eisack - Bulk density table (real weighbridge data)
curl "https://www.beton-eisack.it/_default_upload_bucket/Gewichtstabelle%202021_1.pdf"

# City of Vienna MA 22 - Volume-to-mass conversion factors
curl "https://www.wien.gv.at/pdf/ma22/umrechnungsfaktoren.pdf"

# Canton Uri - Mineral construction waste density factors
curl "https://www.ur.ch/_docn/297137/220527_318_22_ANL_Umrechnungsfaktoren_fur_mineralische_Bauabfalle.pdf"

# ÖKOBAUDAT - Search for LCA data (ILCD format)
curl "https://oekobaudat.de/OEKOBAUDAT/resource/processes?name=Beton"

# Madaster API - Get building Bill of Materials (requires API key)
curl -H "X-API-Key: YOUR_KEY" "https://api.madaster.com/v4.0/buildings/{id}/materials"

# EC3/OpenEPD - Get Environmental Product Declarations
curl -H "Authorization: Bearer TOKEN" "https://api.buildingtransparency.org/api/epds?search=concrete"

# EBV RC-1 Limit Values (Baustoffrecycling Bayern)
curl "https://www.baustoffrecycling-bayern.de/system/files/oeffentlich/Dokumente/EBV_Anlage%202%20Tabelle%201%20RC-1.pdf"

# RegioIT Abfallnavi - Waste fractions (substitute region name)
curl "https://aachen-abfallapp.regioit.de/abfall-app-aachen/rest/fraktionen"

# RegioIT Abfallnavi - Disposal facility locations
curl "https://aachen-abfallapp.regioit.de/abfall-app-aachen/rest/standorte"

# ewccodes.com - EWC code lookup with metadata
curl "https://ewccodes.com/api/ewc?code=170101"

# Materials Project - Crystallographic density (requires API key)
# Register: https://next-gen.materialsproject.org/api

# === Report 07: Packaging & Recycling Codes ===

# EU Decision 97/129/EC - Foundational material codes
curl "https://eur-lex.europa.eu/eli/dec/1997/129/oj/eng"

# ZSVR Mindeststandard 2025 - Recyclability assessment
curl "https://www.verpackungsregister.org/fileadmin/files/Mindeststandard/Mindeststandard_VerpackG_2025.pdf"

# Open Food Facts - Packaging materials taxonomy (hierarchical DAG)
curl "https://raw.githubusercontent.com/openfoodfacts/openfoodfacts-server/main/taxonomies/packaging_materials.txt"

# GS1 - PackagingMaterialTypeCode vocabulary (JSON-LD)
curl "https://ref.gs1.org/voc/PackagingMaterialTypeCode"

# Wikidata - Get recycling code for PET (Q144256)
curl "https://www.wikidata.org/wiki/Special:EntityData/Q144256.json"

# BSR Berlin - Abfalltrenntabelle (consumer disposal guide)
curl "https://www.bsr.de/assets/downloads/abfalltrenntabelle-din-a4.pdf"

# AWM Munich - Trennliste (English version)
curl "https://www.awm-muenchen.de/fileadmin/Dateien_pdfs/Trennliste_englisch.pdf"

# BSR - Abfälle richtig trennen brochure
curl "https://www.bsr.de/assets/downloads/broschuere-abfaelle-richtig-trennen.pdf"

# === Report 08: Batteries & Accumulators ===

# Stiftung EAR - Battery producer registry (CSV export via web)
# Navigate to: https://www.ear-system.de/ear-verzeichnis/battghersteller
# Use filters: Batteriekategorie = "Gerätebatterien" → Export CSV/XLSX

# EU Battery Regulation 2023/1542 (official text)
curl "https://eur-lex.europa.eu/legal-content/EN/TXT/PDF/?uri=CELEX:32023R1542"

# BattG (Batteriegesetz) - German Battery Act
curl "https://www.gesetze-im-internet.de/battg_2009/BJNR158200009.html"

# VdS 3103 - Lithium battery storage guidelines (insurance standard)
curl -O "https://vds.de/fileadmin/Website_Content_Images/VdS_Publikationen/vds_3103_web.pdf"

# BAM Dangerous Goods - Query UN number (web interface only)
# https://www.dgg.bam.de/quickinfo/de/
# Example: Search "3480" for Lithium Ion Batteries

# Energizer SDS - Alkaline battery safety data
curl -O "https://data.energizer.com/wp-content/uploads/2023/01/Alk-Min-AIS-SDS-01-23-Energizer.pdf"

# Duracell SDS Library (navigate for specific products)
# https://duracell.com/techlibrary/safety-data-sheets

# Panasonic Lithium Handbook 2025
curl -O "https://energy.panasonic.com/dam/master/pdf/eu/material/Panasonic_Lithium_Handbook_2025.pdf"

# GRS Sicherheitsstandard - Green/Yellow/Red bin checklist
curl -O "https://www.grs-batterien.de/fileadmin/Downloads/Sicherheit/GRS_Checkliste_Kennzeichnungspflicht_Sep2023_ws23200.pdf"

# Stiftung GRS - Collection reports and downloads
# https://www.stiftung-grs.de/downloads/

# RWTH Aachen - Entsorgungshandbuch (disposal handbook)
curl -O "https://www.rwth-aachen.de/global/show_document.asp?id=aaaaaaaaaazyhtp"

# Uni Heidelberg - BT-26 Lab battery disposal
curl -O "https://www.uni-heidelberg.de/md/zentral/universitaet/beschaeftigte/service/sicherheit/bt-26.pdf"

# IPA - AVV codes for battery waste (16 06 XX)
# https://www.abfallbewertung.org/ipa/repgen.php?report=ipa&char_id=1606_Batt&lang_id=de

# Wikipedia - IEC battery size codes (structured tables)
curl "https://en.wikipedia.org/wiki/List_of_battery_sizes"

# BG Verkehr - Lithium battery transport safety
# https://www.bg-verkehr.de/arbeitssicherheit-gesundheit/themen/gefahrgut/unfallgefahren/lithiumbatterien

# TÜV SÜD - EU Battery Regulation overview
curl -O "https://www.tuvsud.com/en-us/-/media/regions/us/pdf-files/whitepaper-report-e-books/tuvsud_overview-of-eu-battery-regulations.pdf"

# === Report 09: Wood & Furniture ===

# Altholzverordnung (AltholzV) - Full text
curl "https://www.gesetze-im-internet.de/altholzv/BJNR030200002.html"

# AltholzV Anhang III - Default classification table
curl "https://www.gesetze-im-internet.de/altholzv/anhang_iii.html"

# Ökobaudat API - Environmental Product Declarations
# Query by UUID or search term (e.g., "Spanplatte", "Fensterrahmen")
curl "https://www.oekobaudat.de/api/datasets?search=Spanplatte"

# Ökobaudat - Python example (github.com/ocni-dtu/okobau_example)
# pip install requests lxml
# import requests
# response = requests.get("https://www.oekobaudat.de/api/datasets")

# IPA Abfallbewertung - Altholz characterization
# https://www.abfallbewertung.org/ipa/repgen.php?char_id=Altholz&report=ipa

# LfU Bayern - Holzschutzmittel und Pestizide guide (PCP, DDT, Lindane)
curl -O "https://www.lfu.bayern.de/abfall/schadstoffratgeber_gebaeuderueckbau/suchregi ster/doc/507.pdf"

# Holzfragen.de - Historical wood preservatives (DDR/BRD)
curl "https://www.holzfragen.de/seiten/hsm_historie.html"

# WRAP - Tools for identifying contaminated wood
curl -O "https://www.wrap.ngo/sites/default/files/2023-10/WRAP-tools-wood-reprocessors.pdf"

# BAV Altholzverband - Regelvermutung update proposal
curl -O "https://altholzverband.de/wp-content/uploads/2018/07/2018-07-20-BAV-Vorschlag-Regelvermutung.pdf"

# Umweltbundesamt - AltholzV evaluation study
curl -O "https://www.umweltbundesamt.de/sites/default/files/medien/479/publikationen/texte_95-2020_evaluierung_der_altholzverordnung.pdf"

# Uni Freiburg - Abfall-ABC (Wood disposal rules)
curl "https://uni-freiburg.de/sicherheit-umweltschutz/abfall-abc/"

# Uni Giessen - Abfallwirtschaft (Wood → Container mapping)
curl "https://www.uni-giessen.de/de/org/admin/dez/e/3/logistik-aussenanlagen/Abfallwirtschaft/Abfalltrennung"

# Wikidata SPARQL - Query chemical hazards
# DDT: Q163648, Lindane: Q207050, PCP: Q423985
curl -G "https://query.wikidata.org/sparql" \
  --data-urlencode "query=SELECT ?hazard WHERE { wd:Q423985 wdt:P279 ?hazard }" \
  -H "Accept: application/json"

# LaGUS - DDT and Lindane in interior spaces (East German legacy)
curl -O "https://www.lagus.mv-regierung.de/serviceassistent/download?id=1671281"

# Michigan CSS - Office Furniture LCA Study
curl -O "https://css.umich.edu/sites/default/files/publications/css00-02.pdf"

# === Report 10: Facility Infrastructure ===

# OpenStreetMap Overpass API - Query recycling infrastructure
# Replace bbox with bounding box coordinates: south,west,north,east
curl -X POST "https://overpass-api.de/api/interpreter" \
  -d '[out:json][timeout:25];
(
  node["amenity"="recycling"](52.3,13.0,52.7,13.8);
  way["amenity"="recycling"](52.3,13.0,52.7,13.8);
);
out body;
>;
out skel qt;'

# OSM Wiki - Tagging ontology documentation
# https://wiki.openstreetmap.org/wiki/Key:recycling_type
# https://wiki.openstreetmap.org/wiki/Tag:amenity%3Drecycling

# Berlin Open Data (BSR) - Recyclinghöfe + Abfall-ABC
curl "https://daten.berlin.de/datensaetze/bsr-recyclingh-fe-und-abfall-abc"
# Documentation: https://www.bsr.de/opendata/standortdaten_rc_eboxen/Doku.txt

# Hamburg WFS Service - Segmented waste infrastructure layers
# Base URL: https://geodienste.hamburg.de/HH_WFS_abfall_recycling
curl "https://geodienste.hamburg.de/HH_WFS_abfall_recycling?SERVICE=WFS&REQUEST=GetCapabilities"

# Hamburg WFS - Get recycling centers (recyhof layer)
curl "https://geodienste.hamburg.de/HH_WFS_abfall_recycling?SERVICE=WFS&REQUEST=GetFeature&TYPENAME=abfall_recycling_6_recyhof&OUTPUTFORMAT=application/json"

# Hamburg WFS - Get glass containers
curl "https://geodienste.hamburg.de/HH_WFS_abfall_recycling?SERVICE=WFS&REQUEST=GetFeature&TYPENAME=abfall_recycling_2_glas&OUTPUTFORMAT=application/json"

# Munich AWM Open Data (CKAN)
curl "https://opendata.muenchen.de/api/3/action/package_show?id=awm_wertstoffhoefe_opendata"

# Munich AWM - GeoJSON Wertstoffhöfe
curl "https://geoportal.muenchen.de/geoserver/opendata/ows?service=WFS&request=GetFeature&typeName=opendata:vablock_wertstoffhoefe&outputFormat=application/json"

# Cologne AWB Open Data - Schadstoffmobil (XML)
# https://offenedaten-koeln.de/dataset/schadstoff-mobil-k
curl "https://www.offenedaten-koeln.de/sites/default/files/schadstoffmobil.xml"

# Cologne AWB - Wertstoff-Center (JSON)
# https://offenedaten-koeln.de/dataset/wertstoff-center-k

# HACS waste_collection_schedule - Community framework
# GitHub: https://github.com/mampfes/hacs_waste_collection_schedule
# Abfall+ source: https://github.com/mampfes/hacs_waste_collection_schedule/blob/master/doc/source/abfall_io.md

# Stiftung EAR - WEEE collection point registry
# https://www.ear-system.de/ear-verzeichnis/sammel-und-ruecknahmestellen
# Download: CSV/XLSX/XML available in downloads section

# Stiftung GRS - Battery collection point search
curl "https://www.grs-batterien.de/ruecknahmestellen/kommunale-ruecknahmestellen/"

# Lidl Plus API (reverse-engineered) - GitHub reference
# https://github.com/Andre0512/lidl-plus
# pip install lidl-plus

# dm Store Finder (Mapbox-based)
# https://softcreatr.github.io/dm-store-finder/
# API returns serviceTypes array including RECYCLING_STATION

# Discount Scraper - Supermarket API reverse engineering
# https://github.com/t0ffifee/discount-scraper

# === Report 11: Material Properties ===

# PubChem PUG REST - Get compound by name (resolve to CID)
curl "https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/name/Acetone/cids/JSON"

# PubChem - Get properties by CID (FlashPoint, MolecularWeight, etc.)
curl "https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/cid/180/property/MolecularFormula,MolecularWeight,IUPACName/JSON"

# PubChem - Get Laboratory Chemical Safety Summary (LCSS view)
curl "https://pubchem.ncbi.nlm.nih.gov/rest/pug_view/data/compound/180/JSON?heading=GHS+Classification"

# PubChem - Get synonyms for entity recognition
curl "https://pubchem.ncbi.nlm.nih.gov/rest/pug/compound/name/Sodium%20Hypochlorite/synonyms/JSON"

# ECHA CHEM - C&L Inventory bulk download (no API)
# Download page: https://echa.europa.eu/information-on-chemicals/cl-inventory-database
# Direct link changes - check downloads section for latest CSV/XML

# ECHA - Candidate List of SVHCs
# https://echa.europa.eu/candidate-list-table

# Materials Project - Python client installation
# pip install mp-api

# Materials Project - Python API usage example:
# from mp_api.client import MPRester
# with MPRester("YOUR_API_KEY") as mpr:
#     docs = mpr.materials.summary.search(elements=["Li", "Co", "O"])
#     for doc in docs:
#         print(doc.material_id, doc.density)

# EPA Volume-to-Weight Conversion Factors (PDF)
curl -O "https://www.epa.gov/sites/default/files/2016-04/documents/volume_to_weight_conversion_factors_memorandum_04192016_508fnl.pdf"

# UK WRAP Bulk Densities Report
# https://www.wrap.ngo/resources/report/material-bulk-densities

# NREL Battery Failure Databank
# https://www.nrel.gov/transportation/battery-failure.html
# Data downloads: https://data.nrel.gov/submissions/153

# NIST Chemistry WebBook - Compound search
curl "https://webbook.nist.gov/cgi/cbook.cgi?Name=acetone&Units=SI"

# EPI Suite Web Beta (QSAR estimates)
# https://episuite.dev/EpiWebSuite/

# DIN CERTCO - Compostability certification database
# https://www.dincertco.de/din-certco/en/main-navigation/products-and-services/certification-of-products/packaging/compostable-products-seedling/

# MatWeb (manual lookup only - no API)
# https://www.matweb.com/search/MaterialGroupSearch.aspx

# === Report 12: Universal Taxonomies ===

# UNSPSC - Data.gov CSV download
curl -O "https://catalog.data.gov/dataset/unspsc-codes-93778/resource/113e3005-7def-4b17-a2d6-56b7904ba133"

# UNSPSC - O*NET Resource Center Excel
curl -O "https://www.onetcenter.org/dictionary/21.0/excel/unspsc_reference.xlsx"

# GS1 GPC Browser (registration required for bulk download)
# https://gpc-browser.gs1.org/

# EU Vocabularies - Combined Nomenclature RDF/SKOS
curl -O "https://data.europa.eu/data/datasets/combined-nomenclature-2021"
# Bulk downloads: https://ec.europa.eu/eurostat/data/bulkdownload

# TARIC Database (massive XML)
# https://data.europa.eu/data/datasets/eu-customs-tariff-taric

# German EZT-online (web interface)
# https://www.zoll.de/DE/Fachthemen/Zoelle/TARIC/taric_node.html

# Destatis WZ 2008 - Classification download
# https://www.destatis.de/DE/Methoden/Klassifikationen/Gueter-Wirtschaftsklassifikationen/Downloads/klassifikationen-wz-2008.html

# Eurostat Correspondence Tables (CPA, CN, NACE)
# https://ec.europa.eu/eurostat/web/cpa/correspondence-tables

# AVV - Gesetze-im-Internet (official XML)
curl "https://www.gesetze-im-internet.de/avv/BJNR337910001.html"

# GitHub - EWC/AVV JSON (bennokr)
curl "https://gist.githubusercontent.com/bennokr/6d5f1e556a78148ff3087c4fd4618781/raw/ewc.ndjson"

# GitHub - UNSPSC ML datasets
# https://github.com/KathiravanNatarajan/UNSPSCPrediction
# https://github.com/kasivisu4/cordiance-experential-project

# Amazon Business ECLASS taxonomy (DE marketplace)
# https://business.amazon.com/en/blog/release-notes/taxonomy-for-punchout-integration

# ECLASS Asset Administration Shell (free XML download)
# https://eclass.eu/en/eclass-standard/releases

# Wikidata SPARQL - Items with EWC codes
# PREFIX wdt: <http://www.wikidata.org/prop/direct/>
# SELECT ?item ?ewc WHERE { ?item wdt:PXXXX ?ewc } # Replace PXXXX with actual property
# Endpoint: https://query.wikidata.org/sparql

# Schema.org - Product type reference
# https://schema.org/Product
# https://schema.org/usageInfo

# === Report 13: Multilingual Synonyms ===

# Wikidata SPARQL - Multilingual waste terms with recycling codes
# Endpoint: https://query.wikidata.org/sparql
# Example query for waste items with DE/AR/TR labels:
# SELECT ?item ?label_de ?label_ar ?label_tr ?recycling_code
# WHERE {
#   ?item wdt:P279*/wd:Q45701 .  # Subclass of Waste
#   ?item rdfs:label ?label_de . FILTER(LANG(?label_de) = "de")
#   OPTIONAL { ?item rdfs:label ?label_ar . FILTER(LANG(?label_ar) = "ar") }
#   OPTIONAL { ?item rdfs:label ?label_tr . FILTER(LANG(?label_tr) = "tr") }
#   OPTIONAL { ?item wdt:P5994 ?recycling_code }
# }

# GEMET - General Multilingual Environmental Thesaurus (EEA)
# RDF/SKOS format, 36 languages including AR/TR
# https://www.eionet.europa.eu/gemet/
# Example: Concept 9041 = "Waste"
curl "https://www.eionet.europa.eu/gemet/concept/9041"

# BabelNet - 500-language semantic network
# Requires API key registration
# https://babelnet.org/downloads
# Python API: pip install babelnet

# ConceptNet - Common sense knowledge graph (REST API)
curl "https://api.conceptnet.io/c/de/abfall?limit=10"
curl "https://api.conceptnet.io/query?node=/c/en/recycling&rel=/r/MadeOf"

# Open Multilingual Wordnet
# https://omwn.org/
# Downloads available per language

# EuroVoc / List of Wastes
# https://op.europa.eu/en/web/eu-vocabularies/controlled-vocabularies
# Chapter 20 = Municipal Wastes

# IATE - Interactive Terminology for Europe (TBX download)
# https://iate.europa.eu/download-iate
# Filter by domain: "Environment", "Waste Management"

# ECHA-term - GHS multilingual terminology
# https://data.europa.eu/data/datasets/echa-term

# Open Food Facts - Packaging taxonomy (daily JSONL dump ~4.8GB)
wget "https://static.openfoodfacts.org/data/openfoodfacts-products.jsonl.gz"
# Packaging fields: packaging_shape, packaging_material, packaging_recycling
# Multilingual tags: en:bottle, de:Flasche, fr:Bouteille

# Abfallnavi API - Municipal fractions and calendars
# Documentation: https://github.com/bundesAPI/abfallnavi-api
# OpenAPI: https://abfallnavi.api.bund.dev/
# Endpoints:
#   GET /orte - Resolve addresses
#   GET /fraktionen - Available bin types at address
#   GET /termine - Collection calendar

# LUCID Packaging Register (German compliance)
# https://www.verpackungsregister.org/

# Municipal multilingual flyers (PDF scraping)
# Landkreis Karlsruhe (Arabic): https://www.awb-landkreis-karlsruhe.de/
# Bremen Stadtreinigung: https://www.die-bremer-stadtreinigung.de/
# AHA Region Hannover (Arabic): https://www.aha-region.de/

# Handbook Germany - Waste separation explainer
curl "https://handbookgermany.de/ar/waste-separation"
curl "https://handbookgermany.de/tr/waste-separation"

# GHS Pictograms (UNECE official SVG/PNG)
# https://unece.org/transport/dangerous-goods/ghs-pictograms

# OpenStreetMap Overpass - Recycling containers with glass color tags
# Example: Find glass banks accepting mixed colors in Berlin
# [out:json];
# area["name"="Berlin"]->.searchArea;
# node["amenity"="recycling"]["recycling:glass"="yes"](area.searchArea);
# out body;
# Endpoint: https://overpass-api.de/api/interpreter

# TrashNet dataset (waste classification images)
# https://github.com/garythung/trashnet
# Categories: cardboard, glass, metal, paper, plastic, trash

# === Report 14: Textiles & Clothing ===

# NIST NIR-SORT Database (2025) - Textile spectral fingerprints
# 64 distinct fabric types, JCAMP-DX format for spectroscopy
# https://www.nist.gov/news-events/news/2025/01/nist-database-can-help-increase-recycling-textiles-and-clothing
# Download from NIST Public Data Repository (Public Domain)

# LAGA Mitteilung 40 - German textile waste enforcement aid (PDF)
# End-of-Waste criteria, Alttextilien vs Textilabfall distinction
# https://www.laga-online.de/documents/laga-m40-vollzugshilfe-zur-vermeidung-sowie-zur-erfassung,-sortierung-und-verwertung-von-alttextilien_1685020405.pdf

# T-REX Technical Guidance (2025) - Recyclability "Traffic Light" system
# EU Horizon 2020 - Elastane threshold rules
# https://circulartextiles.aalto.fi/trexproject/wp-content/uploads/2025/06/T-REX-Technical-Guidance-For-Recyclable-Garments.pdf

# GINETEX Care Symbols - ISO 3758 label definitions
# https://www.ginetex.net/gb/labelling/care-symbols.asp
# Note: Symbol images are trademarked - use text descriptions only

# OpenStreetMap Overpass - Textile recycling containers (Altkleidercontainer)
# [out:json];
# area["name"="Heidelberg"]->.searchArea;
# (
#   node["amenity"="recycling"]["recycling:clothes"="yes"](area.searchArea);
#   node["social_facility"="clothing_bank"](area.searchArea);
# );
# out body;

# Mannheim Altkleidercontainer Open Data
curl "https://mannheim.opendatasoft.com/api/explore/v2.1/catalog/datasets/150824-altkleidercontainer-mannheim/records"

# Würzburg Abfallkalender API
# https://opendata.wuerzburg.de/explore/dataset/abfallkalender-wuerzburg/api/

# FairWertung - Ethical textile collector search (scraping required)
# https://fairwertung.de/standortsuche/

# BVSE - Certified commercial recycler search (scraping required)
# https://www.bvse.de/mitgliedersuche/alle.html

# Open Supply Hub API - Brand→Supplier mapping
# Registration required for API key
# https://opensupplyhub.org/api/docs/
curl -H "Authorization: Token YOUR_API_KEY" "https://opensupplyhub.org/api/facilities/?q=H%26M"

# GS1 Digital Link - Barcode→URL resolution
# https://www.gs1.org/standards/gs1-digital-link
# Example: https://id.gs1.org/01/{GTIN}

# Barcode Lookup API - Product identification (commercial)
# https://www.barcodelookup.com/api
curl "https://api.barcodelookup.com/v3/products?barcode=012345678905&formatted=y&key=YOUR_API_KEY"

# CIRPASS - Digital Product Passport schema definitions
# https://cirpassproject.eu/
# JSON-LD schema for 2027-2030 DPP mandate

# Textile Exchange PFMM - Sustainability fiber matrix
# https://textileexchange.org/pfmm/
# Registration required for full access

# Good On You API - Brand sustainability ratings (Paid partnership required)
# https://partnerships.goodonyou.eco/solutions/publishers
# Ratings: People, Planet, Animals

# Fashion Revolution Transparency Index
# https://www.fashionrevolution.org/
# Annual PDF report, sometimes CSV available

# MatWeb - Polymer thermal properties (freemium, no bulk API)
# https://www.matweb.com/
# Manual lookup: melting point, density, biodegradability

# === Report 15: Disassembly Instructions ===

# iFixit API v2.0 - Core disassembly instructions
# Base URL: https://www.ifixit.com/api/2.0
# No auth required for read-only, CC-BY-NC-SA 3.0 license

# Get device category hierarchy
curl "https://www.ifixit.com/api/2.0/categories?requireGuides=true"

# Get specific guide with steps and tools
curl "https://www.ifixit.com/api/2.0/guides/12345"

# NLP entity resolution (autocomplete)
curl "https://www.ifixit.com/api/2.0/suggest/MacBook"

# Device metadata
curl "https://www.ifixit.com/api/2.0/devices/MacBook%20Pro%2015%22%202019"

# python-ifixit library (handles pagination)
# pip install ifixit
# https://github.com/openzim/ifixit

# Apple Recycler Guides - PDF downloads
# https://www.apple.com/recycling/recycler-guides/
# Target: "Annex VII Components" table for hazard identification
# Example: iPhone 17, MacBook Air, iPad Pro

# Apple documentation scraper (Python)
# https://github.com/goranmoomin/apple-documentation-archive-scraper

# smalot/pdfparser - PHP PDF extraction (for Annex VII tables)
# https://github.com/smalot/pdfparser

# Dell WEEE Recycler Information
# https://www.dell.com/support/kbdoc/en-us/000144813/recycling-dell-monitors-how-to-get-disassembly-instructions-for-dell-monitors
# Dell hardware inventory export (JSON)
# https://www.dell.com/support/manuals/en-al/idrac9-lifecycle-controller-v5.x-series/idrac9_5.00.00.00_lcug/about-view-and-export-current-inventory

# HP Disassembly Instructions
# https://www.hpe.com/global/livingprogress/environmentalprogress/productdata/
# PDF format with exploded view diagrams

# I4R Platform (Information for Recyclers) - WEEE Forum
# https://i4r-platform.eu/
# Seek academic access for bulk data
# ~47 product fiches across 6 categories

# ECHA SCIP Database - Substances of Very High Concern
# https://echa.europa.eu/scip
# S2S API for System-to-System queries
# IUCLID format (XML/JSON)
# Query by GTIN/EAN for chemical composition

# ProSUM / Urban Mine Platform - Material composition statistics
# https://weee-forum.org/urban-mine-platform/
# Average Gold/Silver/Copper content per device category

# Open Repair Data Standard (ORDS) - Repair outcome statistics
# https://standard.openrepair.org/
# GitHub: https://github.com/openrepair/data
# CSV/JSON format with fault_type, repair_status, repair_barrier

# ewc-onto - European Waste Catalogue Ontology (OWL)
# https://github.com/kschmidt-at-dbfz/ewc-onto
# https://datalab.dbfz.de/ontologies/
# 842 waste types with isHazardous property

# MyMüll API - Collection schedules (many German municipalities)
curl "https://mymuell.jumomind.com/mmapp/api.php?r=dates&city_id=CITY_ID&area_id=AREA_ID"

# AbfallPlus API - Street-level collection data
# https://api.abfallplus.de/index.php
# https://github.com/Bouni/abfallplus
# API key often in municipal website network traffic

# === Report 16: Computer Vision Datasets ===

# TACO Dataset - Trash Annotations in Context
# https://github.com/pedropro/TACO
# http://tacodataset.org/
# MS COCO format, MIT license, 60 categories with segmentation masks

# ZeroWaste Dataset - MRF Conveyor Belt
# http://ai.bu.edu/zerowaste/
# https://github.com/dbash/zerowaste
# CC BY 4.0 license, extreme clutter, soft plastic class

# MJU-Waste Dataset - RGB-D Volume Estimation
# https://github.com/realwecan/mju-waste
# https://opendatalab.com/OpenDataLab/MJU-Waste/download
# MIT license, 2,475 RGB-D pairs for depth-based volume inference

# Garbage Classification V2 (Kaggle)
# https://www.kaggle.com/datasets/mostafaabla/garbage-classification
# 15k images, 12 classes including Battery and Bio

# TrashNet (Stanford) - Baseline Benchmark
# https://github.com/garythung/trashnet
# MIT license, 2,527 images, 6 classes

# CODD - Construction & Demolition Waste
# https://data.mendeley.com/datasets/wds85kt64j/3
# CC BY 4.0, 3,129 images, PASCAL VOC format

# LabPics - Laboratory Glassware
# https://www.kaggle.com/datasets/sagieppel/labpics-chemistry-labpics-medical
# CC BY 4.0, material phase segmentation (liquid/solid/foam)

# RVM Dataset - Reverse Vending Machine (Pfand)
# https://github.com/taeyoungYoo/rvm-dataset
# MIT license, YOLO annotations, bottle/can verification views

# Hazmat Placard Dataset
# https://www.kaggle.com/datasets/anmatngu/hazmat-placard-image-dataset
# GHS pictograms for safety override layer

# GHS Symbol Detection (Roboflow)
# https://universe.roboflow.com/fireljj/ghs-hze4d
# Object detection format

# Official GHS Pictograms (UNECE)
# https://unece.org/transport/dangerous-goods/ghs-pictograms
# SVG/PNG official symbols

# Biomedical Waste Classification
# https://www.kaggle.com/datasets/mario78/biomedical-waste-classification-images-dataset
# Sharps, clinical waste detection

# RealWaste - UCI Machine Learning Repository
# https://archive.ics.uci.edu/dataset/908/realwaste
# Landfill reception images, textile class

# WaDaBa - Polish Plastic Streams (Gelber Sack proxy)
# https://wadaba.pcz.pl/
# Similar waste composition to German yellow bag

# Pfand Detection - Roboflow Datasets
# https://universe.roboflow.com/pfand48/p48  (thats pfand again)
# https://universe.roboflow.com/zz-77ljl/bank-bottle (bank bottle)
# Bottle vending machine training data

# waste-datasets-review (AgaMiko) - Meta Repository
# https://github.com/AgaMiko/waste-datasets-review
# Comprehensive index of all CV waste datasets

# Pre-trained YOLOv8 Waste Models (Hugging Face)
# https://huggingface.co/kendrickfff/waste-classification-yolov8-ken

# litter-detection - YOLOv8 + TACO
# https://github.com/jeremy-rico/litter-detection

# TrashBox - Multi-domain Waste Dataset
# https://github.com/nikhilvenkatkumsetty/TrashBox
# Includes medical waste subset

# SpectralWaste - Hyperspectral + RGB
# https://arxiv.org/html/2403.18033v1
# Material spectral signatures (PP vs PET)

# RWTH Aachen - RCM (Recycled Concrete Materials)
# https://publications.rwth-aachen.de/record/1005633/files/1005633.pdf
# Particle size distribution analysis

# RWTH NIR-MFCO - Near-Infrared Plastic Analysis
# https://www.fb5.rwth-aachen.de/cms/georessourcen/forschung/publikationen/publikationen/~bbvzxu/details/?file=954195&lidx=1

# === Report 17: Transport/ADR Regulations ===

# BAM Gefahrgutdatenservice - **FREE since July 2025!**
# https://tes.bam.de/TES/Navigation/EN/DGG-Database/Products/Dangerous-Goods-Dataservice/dangerous-goods-dataservice.html
# https://dgg.bam.de
# CSV/Excel download, ADR 2025 Table A: UN Numbers, Classes, Transport Categories
# Paradigm shift: previously paid subscription, now free for all uses

# UNECE ADR 2025 - Full Regulatory Text
# https://unece.org/adr-2025-files
# https://unece.org/transport/dangerous-goods
# PDF/Word/Excel, packaging instructions (P001, P002), tunnel codes

# ECHA C&L Inventory API - Chemical to GHS Mapping
# https://api.store/eu-institutions-api/european-chemicals-agency-api/echa-classification-and-labelling-inventory-api
# https://chem.echa.europa.eu
# Input: CAS Number (67-64-1) → Output: "Flam. Liq. 2, Eye Irrit. 2"

# Wikidata SPARQL - UN Number Resolution (Property P2326)
# SELECT ?item ?itemLabel ?un WHERE {
#   ?item wdt:P2326 ?un .
#   SERVICE wikibase:label { bd:serviceParam wikibase:language "en,de". }
# }
# P2326=UN Number, P200=CAS Number, P274=Chemical Formula

# UPS Dangerous Goods API - Proper Shipping Name Validation
# https://developer.ups.com/api/reference?tag=Dangerous-Goods
# https://github.com/UPS-API/api-documentation/blob/main/DangerousGoods.yaml
# ChemicalReferenceData endpoint, free with API key

# ProSafeCon - 1000-Points Rule Calculator
# https://prosafecon.de/die-1000-punkte-regel/
# https://prosafecon.de/gefahrgut-befoerderungspapier/
# Transport Category multipliers, Beförderungspapier templates

# ADR-Tool.com - ADR 2025 Lookup
# https://adr-tool.com/
# UN Number search, dangerous goods lookup

# ChemRadar - UN Number Cross-Reference
# https://www.chemradar.com/tools/un
# Special Provisions by region (EU/US/China), commercial

# GESTIS Substance Database (DGUV)
# https://gestis-database.dguv.de
# Chemical safety data, storage classes (link only, no scraping)

# University Abfall-ABCs (AVV→UN Mapping Training Data)
# Uni Hamburg: https://epub.sub.uni-hamburg.de/epub/volltexte/2014/30664/pdf/2_06b.pdf
# RWTH Aachen: https://www.rwth-aachen.de/global/show_document.asp?id=aaaaaaaaaazyhtp
# TU Munich: https://portal.mytum.de/archiv/form_gebaeude/form_gebaeude_20030303_113231/20030303_113311/1
# Uni Duisburg-Essen: https://www.uni-due.de/imperia/md/images/ak_schulz/entsorgungsrichtlinie.pdf

# IHK Niederbayern ADR 2025 Changes
# https://www.ihk-niederbayern.de/pdfs/adr_2025_neuerungen_folien_v2.pdf
# German industry guidance on ADR 2025 changes

# Transport Community RID/ADR 2025
# https://www.transport-community.org/wp-content/uploads/2024/12/9-RID-ADR-2025e-What-is-new.pdf
# What's new summary document

# Umweltbundesamt - Waste Classification
# https://www.umweltbundesamt.de/en/topics/waste-resources/waste-management/waste-types/waste-classification
# AVV classification guidance

# HSE CDG Exemptions (UK Reference)
# https://www.hse.gov.uk/cdg/manual/exemptions.htm
# Dangerous goods exemptions documentation

# === Report 18: Medical & Lab Waste ===

# LAGA M18 - Primary Healthcare Waste Enforcement Aid
# https://www.laga-online.de/documents/laga-m-18_stand_2021-06-23_1626849905.pdf
# 119 pages, AVV Chapter 18 interpretation, Schutzziele logic

# AVV Chapter 18 (Destatis / Gesetze-im-Internet)
# https://www.gesetze-im-internet.de/avv/
# HTML source, also available as XML from Destatis

# TRBA 466 - Bacteria/Archaea Risk Groups (BAuA)
# https://www.baua.de/DE/Angebote/Regelwerk/TRBA/pdf/TRBA-466.pdf
# PDF tables: Genus, Species, Risk Group, Notations (Z, T)
# Parse columns: "Spezies", "Risikogruppe", "Bemerkungen"

# TRBA 462 - Viruses/TSE Risk Groups (BAuA)
# https://www.baua.de/DE/Angebote/Regelwerk/TRBA/TRBA-462
# 3** = limited airborne, 3*** = no airborne transmission

# TRBA 460 - Fungi Risk Groups (BAuA)
# https://www.baua.de/DE/Angebote/Regelwerk/TRBA/TRBA-460
# Allergenic potential notation (A), disposal implications

# TRBA 250 - Healthcare Occupational Safety (BAuA)
# https://www.baua.de/DE/Angebote/Regelwerk/TRBA/pdf/TRBA-250.pdf
# Autoclave requirements, container closure rules

# DSMZ Microorganism Catalogue - 80,000+ strains
# https://www.dsmz.de/collection/catalogue/microorganisms/catalogue
# https://www.dsmz.de/collection/catalogue/microorganisms/permissions-/-restrictions
# Safety information per strain, ground truth for risk group validation

# ZKBS Organismenliste - GMO Classifications
# https://zkbs-online.de/en/databases/microorganisms
# https://zkbs-online.de/fileadmin/user_upload/Downloads/Datenbanken/Organismenliste.pdf
# Excel/CSV export available, donor/recipient combination rules

# PubChem PUG View - GHS Classification for Lab Chemicals
# https://pubchem.ncbi.nlm.nih.gov/rest/pug_view/data/compound/{CID}/JSON?heading=GHS%20Classification
# Example: CID 887 (Methanol) → H225, H301, H311, H331, H370
# H340/H350/H360 → CMR → 18 01 08*

# The Halogen Rule (Molecular Formula Check)
# GET /rest/pug/compound/cid/{CID}/property/MolecularFormula/JSON
# Check for Cl, Br, F, I → HalogenatedSolvent → €2000/t vs €500/t

# NIOSH Hazardous Drug List 2024 (CDC)
# https://www.cdc.gov/niosh/docs/2025-103/pdfs/2025-103.pdf
# Group 1: Antineoplastics (always 18 01 08*)
# Group 2/3: Context-dependent hazards

# BGW Drug Classification (German Berufsgenossenschaft)
# https://www.bgw-online.de/bgw-online-de/service/medien-arbeitshilfen/mediencenter/agg-gefahrstoffe/gefahrstoffrechtliche-kennzeichnung-von-arzneistoffen-in-der-18278
# TRGS 525 compliance, German-specific CMR lists

# StrlSchV Anlage 4 - Radioactive Clearance Levels
# https://www.gesetze-im-internet.de/strlschv_2018/anlage_4.html
# https://www.buzer.de/Anlage_4_StrlSchV.htm
# Parse HTML tables: Nuclide, T½, Clearance Solid, Clearance Liquid

# Decay Calculator Formula (Python)
# t = (math.log(A0 / L) * T_half) / math.log(2)
# Schedule notification for clearance measurement

# UBA UN 3291 Transport Guidelines
# https://www.umweltbundesamt.de/sites/default/files/medien/3662/dokumente/hcw-transport-un3291_eng.pdf
# https://www.washinhcf.org/wp-content/uploads/2021/07/Guidelines-Transport-of-infectious-waste-UN3291.pdf
# Packaging types 4H2/1H2, P621 instruction

# ISO 23907 Sharps Containers (BSI Knowledge)
# https://knowledge.bsigroup.com/products/sharps-injury-protection-requirements-and-test-methods-single-use-sharps-containers
# Penetration resistance >15N, closure requirements

# Sharps Container Guides (Manufacturers)
# Sharpsmart: https://www.sharpsmart.co.uk/compliance-standards-reusable-sharps-containers
# Daniels Health: https://www.danielshealth.com/sites/danielshealth.com/files/PDFs/Other/Sharps%20Container%20Guide_0.pdf
# Drop test (1.2m), leakproofness requirements

# Abfallmanager Medizin - AVV 18 01 08* Guidance
# https://www.abfallmanager-medizin.de/abfall-abc/abfallschluesselnummer-180108/
# Commercial but useful for Cytobox specifications

# EnvO (Environment Ontology)
# http://environmentontology.org/
# Classes: waste disposal process, medical waste, hazardous waste

# EWC-Onto (DBFZ Ontologies)
# https://datalab.dbfz.de/ontologies/
# European Waste Catalogue hierarchy: Chapter → Subchapter → Code

# ChEBI (Chemical Entities of Biological Interest)
# https://www.ebi.ac.uk/chebi/
# Cross-links to PubChem, chemical entity classification

# University Lab Waste Guides (Training Data)
# Uni Heidelberg: https://www.uni-heidelberg.de/universitaet/beschaeftigte/service/sicherheit/gefahrstoffe/entsorgung.html
# Uni Freiburg: https://uni-freiburg.de/sicherheit-umweltschutz/startseite/abteilung-umweltschutz-und-nachhaltigkeit-sun-2/abfall-abc/
# TUM Halogen Merkblatt: https://portal.mytum.de/archiv/komp_gts/komp_gts_20030306_151203/20030707_183256/2
```

---

## Report 19: Office Supplies, Stationery & University Products

**Focus**: Structured product data for office/stationery items (pens, paper, toner, folders, adhesives), BMEcat catalog ecosystem, eco-label databases, material composition sources, and university procurement data availability.

### Critical Insight: No "Open Office Facts" Exists

Unlike food (Open Food Facts) or electronics (Icecat), there is no single open database for everyday office products. The structured data landscape is fragmented across commercial B2B platforms, eco-label registries, and classification taxonomies. The most comprehensive source (PBSeasy) is paid B2B-only.

### Primary Sources

| Source | URL | Format | License | Relevance | Coverage |
|--------|-----|--------|---------|-----------|----------|
| **PBSeasy (PBS Network)** | https://pbsnetwork.eu | REST API, JSON, Excel | Commercial B2B (paid) | 5/5 | 1M+ office/stationery products, 150+ EU manufacturers |
| **Blauer Engel Fo.KUS DB** | (request via blauerengel@fo-kus.info) | XML, file download | Free (request) | 5/5 | 8,850+ EAN-coded eco-certified products, daily updates |
| **EU Ecolabel ECAT** | https://data.europa.eu | CSV, API | Open data (free) | 4/5 | 109,096 products with EAN13 barcodes |
| **ECHA SCIP Database** | https://echa.europa.eu/scip-database | Web portal (IUCLID) | Free | 4/5 | 15M+ factsheets — only source linking products to hazardous substances |
| **Icecat Open Catalog** | https://icecat.biz | JSON/XML/CSV API | Free tier + paid | 4/5 | 26M+ datasheets, toner/cartridge compatibility data |

### Secondary/Classification Sources

| Source | URL | Purpose |
|--------|-----|---------|
| UNSPSC Segment 44 | https://catalog.data.gov/dataset/unspsc-codes-93778 | Free Excel taxonomy: 157k items, includes office supplies (44-12-17 Writing Instruments, 44-10-31 Printer Supplies) |
| GS1 GPC Segment 10005000 | https://gpc-browser.gs1.org | Free stationery/office classification with brick-level attributes. Verified by GS1: 30 free GTIN lookups/day |
| eCl@ss Segment 24 | https://eclass.eu | Dominant German industry standard (~50k classes). Free search tool; full download licensed. Release 16.0 adds material declarations |
| Wikidata | https://query.wikidata.org | CC0 SPARQL. P186 ("made from material") for product types. Q15714692 = office supplies hierarchy |
| GESTIS | https://gestis.dguv.de | 9,400+ substances with GHS classification — authoritative German source for toner/ink chemistry |
| ECHA CHEM | https://echa.europa.eu/information-on-chemicals | 100,000+ REACH dossiers for substance hazard data (carbon black, styrene-acrylate, iron oxide) |

### Toner/Ink Cartridge Sources (Multi-Layer Pipeline)

No single database maps cartridge → composition → AVV code. Requires chaining five layers:

| Layer | Source | What It Provides | Access |
|-------|--------|------------------|--------|
| 1. Product ID | Icecat | EAN/GTIN, brand, model, compatible printers, page yield | Free tier |
| 2. Cross-reference | TRI Resources T3 API | OEM ↔ aftermarket cartridge mapping, 1000+ photos | Paid (1-month free trial) |
| 3. Safety Data Sheets | Xerox, HP, Canon, Brother portals | Composition (Section 3), disposal instructions (Section 13) | Free (scraping) |
| 4. Substance hazard | GESTIS + ECHA CHEM | GHS classification, CAS numbers, toxicological data | Free |
| 5. AVV assignment | Threshold calculation | 08 03 17* (hazardous) vs. 08 03 18 (non-hazardous) | Constructed |

**Key finding:** Most modern commercial toner cartridges are NOT classified as hazardous under CLP/GHS — toner powder is typically styrene-acrylate polymer with encapsulated carbon black, silica, and wax.

### BMEcat Ecosystem

BMEcat catalogs are closed B2B documents — no real-world office supply catalogs are openly downloadable. Available resources:

| Resource | URL | Notes |
|----------|-----|-------|
| BME e.V. specs + examples | https://bme.de | XSD schemas, DTDs, XML examples ZIP (free, no registration) |
| BMECat.net GitHub | https://github.com/stephanstapel/BMECat.net | 14 real-world Weidmuller XML files + demo "Office Material" catalog |
| olivere/bmecat Go library | GitHub | Test files with office supply sample data |
| SEPIA BMEcatHub | https://sepia.de/en/bmecathub | Import/edit/export BMEcat catalogs (free basic version) |

**Suppliers using BMEcat:** Unite/Mercateo (requires BMEcat 1.2), Schafer Shop (eCl@ss 4.0-7.1, B2B only), Lyreco (BMEcat + JSON + OCI), Simple System (BMEcat-exclusive, widely used by German universities), Bechtle (BMEcat + JSON + CIF).

### Key Integration Notes

**PBSeasy:**
- Industry-standard data exchange for European PBS sector (Papier, Buro, Schreibwaren)
- 1M+ products from 150+ manufacturers (LEITZ ACCO Brands, Schneider Novus, etc.)
- eCl@ss Segment 24 classification, 200+ quality checks, GDSN via b-synced
- **Access barrier**: Commercial B2B, requires dealer/retailer registration. Negotiate academic access.
- Material composition is NOT a primary field — eCl@ss properties may include some material attributes

**Blauer Engel Fo.KUS:**
- Email blauerengel@fo-kus.info for free XML interface access
- Covers: printers/MFDs (2,825 devices), paper (267 products), office supplies (230+), writing utensils, remanufactured toner (DE-UZ 200)
- Key product group standards: DE-UZ 14a/14b (recycled paper), DE-UZ 171 (printers), DE-UZ 177 (writing utensils), DE-UZ 200 (remanufactured toner)
- EAN/GTIN codes enable cross-referencing with barcode lookups

**EU Ecolabel ECAT:**
- Zero-effort acquisition: CSV download from data.europa.eu, no registration
- Fields: product name, licence number, group code/name, EAN13 (EXTID_TYPE), company name/country, VAT, expiration date
- Office-relevant groups: graphic paper, cardboard, converted paper products, printed matter, furniture
- API and Power BI dashboard also available

**ECHA SCIP:**
- Only covers articles with SVHCs above 0.1% w/w — standard paper, basic pens, folders will NOT appear
- Office products likely present: toner cartridges, solvent-based markers, correction fluids, adhesives, PVC binders (phthalate plasticizers)
- No bulk download API — web portal only. Submission API (S2S/IUCLID) is write-only
- "Severe delays in SCIP dissemination" reported as of January 2026

**OEM Safety Data Sheets (toner/ink):**
- Xerox: open directory at xerox.com/download/ehs/msds/ (product-code organized, DE PDFs, highly scrapeable)
- HP: systematic document IDs (e.g., c07705107)
- Canon: files.canon-europe.com/files/webcontent/safety-data-sheets/
- Sharp, Kyocera, OKI, Brother, Konica Minolta all publish per-product SDS
- SDS Section 3 → composition with CAS numbers; Section 13 → disposal instructions
- SDS Manager API (paid): 16M+ SDS, JSON/XML output, auto-extracts all 16 sections

### Take-Back Programs (Unstructured)

| Program | Provider | Notes |
|---------|----------|-------|
| HP Planet Partners | HP | "Non-returnable supplies list" PDF (Rev. 8, Oct 2025) — closest thing to a recyclability database |
| Der Sammeldrache | Interzero | School/business cartridge collection (1.4M/year). No structured data |
| Canon Return Program | Canon | Take-back, no public database |
| DIN 33870 / DIN 33871 | DIN | Standards for remanufactured toner (B&W) / inkjet. Referenced by ETIRA |

### University Procurement: Entirely Closed

- **No German university publishes procurement catalogs as open data**
- Framework contracts publicly tendered (award notices published), but actual product catalogs confidential
- Kaufhaus des Bundes (kdb.bund.de): federal agencies only, no public product data
- E-procurement portals (cosinex, Vergabe24, DTVP): tender notices with CPV codes (30192700 = Buromaterial) but no product-level data
- **Recommendation**: Partnership with university procurement department essential (likely uses Simple System or JAGGAER with BMEcat catalogs)

### Material Composition: Critical Unsolved Gap

No database systematically provides "this pen is 40% polypropylene, 30% polystyrene, 20% brass, 10% ink." Available fragments:

| Partial Source | What It Covers | Limitation |
|----------------|----------------|------------|
| ECHA SCIP | Hazardous substances >0.1% w/w | No bulk download; non-hazardous products absent |
| Safety Data Sheets | Chemical products (toner, markers, adhesives, correction fluid) | Not available for paper, pens, folders |
| eCl@ss properties | Material declaration aspects (Release 16.0) | Schema only — actual data in commercial platforms |
| Wikidata P186 | Generic material links for product types | Not specific commercial SKUs |
| Manufacturer declarations | Varies | Rarely public; no aggregated source |

For non-chemical products (standard paper, plastic pens, binders), **no automated source exists** — manual research from manufacturer environmental reports is the only option.

### ETL Priority Phases

**Phase 1 — Immediate (free, structured):**
1. EU Ecolabel ECAT CSV (data.europa.eu)
2. Blauer Engel Fo.KUS XML (email request)
3. UNSPSC Segment 44 Excel (free download)
4. Wikidata SPARQL extraction (P186 material, P3962 GTIN)
5. GS1 GPC Excel download

**Phase 2 — Medium effort (free, requires scraping/parsing):**
6. OEM Safety Data Sheet scraping (Xerox open directory, HP, Canon)
7. ECHA SCIP portal queries for office product categories
8. Icecat Open Catalog API (toner/ink cartridge datasheets)
9. BMEcat sample files (BME.de examples + GitHub repos for parser development)

**Phase 3 — Negotiated access (highest data richness):**
10. PBSeasy (PBS Network) — academic/research access
11. Unite/Mercateo Content API — partner program
12. eCl@ss license — Segment 24 with material declarations
13. SDS Manager API — structured SDS extraction (16M+ SDS)

**Phase 4 — Specialized enrichment:**
14. FSC API (api.fsc.org) — paper supply chain verification
15. EPEAT CSV export — printer/copier environmental ratings
16. TRI Resources T3 API — cartridge compatibility cross-referencing
17. GESTIS substance linking — toner/ink CAS → hazard profiles
