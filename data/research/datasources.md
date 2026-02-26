# Data Sources Research for Fessi Knowledge Graph

This document catalogs potential data sources for the Fessi waste disposal knowledge graph. The goal is to identify data that can populate the three schema layers: **Core Entities**, **Logic & Rules**, and **Infrastructure**.

**Last Updated:** February 2026

---

## Table of Contents

1. [Waste Item Categories](#part-1-waste-item-categories)
2. [Types of Information Needed](#part-2-types-of-information-needed)
3. [Legal Classifications (AVV Codes)](#part-3-legal-classifications-avv-codes)
4. [Infrastructure Data](#part-4-infrastructure-data-where-to-dispose)
5. [Universal Classification Systems](#part-5-universal-classification-systems)
6. [Computer Vision Datasets](#part-6-computer-vision-datasets)
7. [Priority Matrix](#part-7-priority-matrix)
8. [Data Integration Architecture](#part-8-data-integration-architecture)
9. [Data Gaps & Challenges](#part-9-data-gaps--challenges)
10. [Research Agent Topics](#part-10-research-agent-topics)
11. [Sources Referenced](#sources-referenced)

---

## Part 1: Waste Item Categories

These are the categories of waste items we need to cover in the knowledge graph. For each category, we list what information is needed, potential data sources, and access/licensing details.

### 1.1 Food & Organic Waste

**What we need:**
- Product names and categories (chocolate, salad, dairy, etc.)
- Packaging materials (plastic, glass, cardboard, composite)
- Organic vs. packaging separation rules
- Shelf life / decomposition properties

**Data Sources:**

| Source | URL | Format | License | Size | Notes |
|--------|-----|--------|---------|------|-------|
| Open Food Facts | https://world.openfoodfacts.org/data | API/JSON/CSV | ODbL | 4M+ products | **Best source.** Has `packaging_tags` for material composition. |
| Open Pet Food Facts | https://world.openpetfoodfacts.org | API/JSON | ODbL | 13k+ products | Same schema as OFF, for pet food. |
| Open Beauty Facts | https://world.openbeautyfacts.org | API/JSON | ODbL | 62k+ products | Cosmetics and beauty products. |

**Key OFF Fields for Disposal:**
```json
{
  "packaging_tags": ["en:plastic", "en:pet-polyethylene-terephthalate"],
  "packaging_materials_tags": ["en:plastic"],
  "packaging_recycling_tags": ["en:recycle"],
  "ecoscore_grade": "b",
  "nutriscore_grade": "c"
}
```

**Example API Query:**
```bash
# Get packaging info for a product by barcode
curl "https://world.openfoodfacts.org/api/v0/product/737628064502.json" \
  | jq '.product.packaging_tags'
```

---

### 1.2 Consumer Electronics & Appliances

**What we need:**
- Product categories and specifications
- Material composition (plastic types, metals, glass)
- Battery types and chemistry (Li-ion, NiMH, Lead-acid, NiCd)
- WEEE categories (6 EU categories)
- Hazardous components (mercury, lead, cadmium)

**Data Sources:**

| Source | URL | Format | License | Size | Notes |
|--------|-----|--------|---------|------|-------|
| Icecat (Open) | https://icecat.com | API/XML/JSON | Free tier + Paid | 25M+ datasheets | Electronics, appliances, DIY. Material specs, battery types. |
| Open Products Facts | https://world.openproductsfacts.org | API/JSON | ODbL | Growing | Non-food products. Smaller but community-driven. |
| iFixit | https://www.ifixit.com | Web/API | CC BY-NC-SA | 100k+ guides | Repair guides with disassembly steps. Shows internal components. |

**Icecat Data Points Useful for Disposal:**
- `EnergyEfficiencyClass` - regulatory compliance
- `BatteryTechnology` - Li-ion, NiMH, etc.
- `Weight` - for disposal cost estimation
- `HazardousMaterialsContained` - REACH/RoHS compliance
- `PackagingData` - materials in packaging

**WEEE Categories (EU Directive 2012/19/EU):**
```
1. Temperature exchange equipment (fridges, AC)
2. Screens and monitors
3. Lamps
4. Large equipment (washing machines, PV panels)
5. Small equipment (vacuums, toasters, electronics)
6. Small IT and telecom (phones, routers, keyboards)
```

---

### 1.3 Chemicals & Hazardous Materials

**What we need:**
- Chemical names and synonyms in multiple languages
- Hazard classifications (GHS pictograms, H/P statements)
- Handling and disposal instructions (SDS Section 13)
- Incompatible materials (for storage/transport safety)
- UN transport numbers (ADR classification)

**Data Sources:**

| Source | URL | Format | License | Size | Notes |
|--------|-----|--------|---------|------|-------|
| PubChem | https://pubchem.ncbi.nlm.nih.gov | REST API/JSON | Public Domain | 293M+ substances | **Best free source.** GHS data, LCSS, synonyms. |
| ECHA | https://echa.europa.eu | API | Public | 23k+ substances | EU REACH/CLP database. Authoritative for EU. |
| ChemSpider | https://www.chemspider.com | API | Free registration | 32M+ structures | Royal Society of Chemistry. Good for structure search. |
| GESTIS | https://gestis.dguv.de | Web | Free | 8,600+ substances | German hazardous substance database (IFA/DGUV). |

**Key PubChem Fields:**
```json
{
  "GHSClassification": {
    "Pictogram": ["GHS02", "GHS07"],
    "SignalWord": "Danger",
    "HazardStatement": ["H225", "H319"],
    "PrecautionaryStatement": ["P210", "P305+P351+P338"]
  },
  "PhysicalProperties": {
    "Density": "0.79 g/cm³",
    "FlashPoint": "13°C",
    "BoilingPoint": "78.4°C"
  },
  "Synonyms": ["Ethanol", "Ethyl alcohol", "Alkohol", "Äthanol"]
}
```

**Example PubChem Query:**
```bash
# Get GHS classification for ethanol (CID 702)
curl "https://pubchem.ncbi.nlm.nih.gov/rest/pug_view/data/compound/702/JSON?heading=GHS+Classification"
```

---

### 1.4 Construction & Demolition Waste (Bauschutt)

**What we need:**
- Material types (concrete, brick, wood, insulation, glass, metals)
- Density data (for weight estimation from visual volume)
- Contamination indicators (paint, asbestos, treated wood, tar)
- Recycling potential (pure mineral vs. mixed construction waste)
- LCA end-of-life scenarios

**Data Sources:**

| Source | URL | Format | License | Size | Notes |
|--------|-----|--------|---------|------|-------|
| ÖKOBAUDAT | https://www.oekobaudat.de | API/XML (ILCD+EPD) | Free | 1,400+ datasets | **Best for Germany.** Official database. Disposal processes included. |
| OpenEPD / EC3 | https://www.buildingtransparency.org | API/JSON | Free | Global | Environmental Product Declarations. Material composition. |
| Materials Project | https://materialsproject.org | API | Free (academic) | 150k+ materials | Scientific properties including density. |
| MakeItFrom.com | https://www.makeitfrom.com | Web | Free | Curated | Engineering material properties. Easy comparison. |

**ÖKOBAUDAT Key Data:**
- Life cycle assessment for 1,400+ building products
- End-of-life scenarios (recycling %, incineration %, landfill %)
- CO2 equivalents (GWP in kg CO2-eq)
- Compliant with DIN EN 15804 standard

**Key Distinction for Rubble Disposal:**
```
Bauschutt (pure mineral)     → Cheap disposal, recyclable
  - Concrete, brick, tiles, stone

Baumischabfall (mixed)       → Expensive, limited recycling
  - Rubble + wood + plastics + wallpaper + metals
```

---

### 1.5 Wood & Furniture (Altholz)

**What we need:**
- Wood type classification (solid wood, MDF, chipboard)
- Treatment status (untreated, painted, preserved)
- Contamination level (German Altholz categories A1-A4)
- Composite materials (wood + metal, wood + fabric)
- Recycling vs. energy recovery routes

**German Altholz Categories (Altholzverordnung):**

| Category | Description | Examples | Recycling Allowed | Energy Recovery |
|----------|-------------|----------|-------------------|-----------------|
| **A I** | Untreated, mechanically processed only | Pallets, crates, solid wood furniture | Yes (unrestricted) | All facilities |
| **A II** | Glued, painted, coated (no halogen, no preservatives) | Painted furniture, laminated panels, MDF | Yes (after coating removal) | Facilities >1MW |
| **A III** | Halogen-organic coatings, no wood preservatives | PVC-coated chipboard, some laminates | No | Facilities >1MW with exhaust cleaning |
| **A IV** | Wood preservative treated | Railway sleepers, utility poles, fence posts | **No** | Facilities >1MW with special exhaust cleaning |
| **PCB** | PCB-contaminated | Old insulation panels | **No** | Special hazardous waste disposal |

**Data Sources:**

| Source | URL | Format | Notes |
|--------|-----|--------|-------|
| Altholzverordnung | gesetze-im-internet.de | Legal text | Official German regulation |
| ÖKOBAUDAT | oekobaudat.de | API/XML | End-of-life data for wood products |
| Bundesverband Altholz | altholzverband.de | Reference | Industry association, technical guidance |

---

### 1.6 Textiles & Clothing

**What we need:**
- Fiber composition (cotton, polyester, wool, blends)
- Care labels and material codes
- Recyclability assessment (pure vs. blended fibers)
- Condition (reusable vs. waste textile)

**Data Sources:**

| Source | URL | Format | License | Notes |
|--------|-----|--------|---------|-------|
| NIST NIR-SORT | nist.gov (2025) | Database | Public | Molecular fingerprints for fiber sorting. New in 2025. |
| ISO 2076 | ISO standard | Reference | Licensed | Textile fiber generic names and definitions |
| EPA Textiles Data | epa.gov | Statistics | Public | US textile waste data, categories |

**Textile Fiber Categories:**
```
Natural Fibers (biodegradable, recyclable):
  - Cotton, Wool, Silk, Linen, Hemp

Synthetic Fibers (recyclable but challenging):
  - Polyester (PET), Nylon, Acrylic, Spandex

Blends (difficult to recycle):
  - Cotton/Polyester, Wool/Synthetic
  - Require chemical separation or downcycling
```

**Recycling Routes:**
- **Reusable condition** → Second-hand shops, donation (Altkleidercontainer)
- **Pure cotton/wool** → Mechanical recycling (shredding → insulation, rags)
- **Pure polyester** → Chemical recycling (back to PET pellets)
- **Blends** → Downcycling or incineration

---

### 1.7 Packaging Materials

**What we need:**
- Material types (by recycling code 01-07, 20-22, 40-41, 70-72, 80-84)
- Multi-layer/composite packaging identification
- Recyclability by local infrastructure (varies by municipality)
- Grüner Punkt / Dual System compatibility

**Data Sources:**

| Source | URL | Format | Notes |
|--------|-----|--------|-------|
| ASTM D7611 | astm.org | Standard | Resin Identification Code definitions (01-07) |
| Open Food Facts | openfoodfacts.org | API | `packaging_materials_tags` field from product scans |
| LUCID | verpackungsregister.org | Registry | German packaging register. B2B but authoritative. |
| How2Recycle | how2recycle.info | Reference | Good recyclability taxonomy (US-based) |

**Complete Recycling Code Reference:**
```
PLASTICS (01-07):
  01 PET   - Polyethylene terephthalate (bottles, containers) → Widely recycled
  02 HDPE  - High-density polyethylene (milk jugs, bags)      → Widely recycled
  03 PVC   - Polyvinyl chloride (pipes, blister packs)        → Rarely recycled
  04 LDPE  - Low-density polyethylene (bags, films)           → Limited recycling
  05 PP    - Polypropylene (yogurt cups, bottle caps)         → Increasingly recycled
  06 PS    - Polystyrene (styrofoam, cups)                    → Rarely recycled
  07 Other - Mixed or other plastics                          → Not recycled

PAPER (20-22):
  20 PAP   - Corrugated cardboard                             → Widely recycled
  21 PAP   - Other cardboard                                  → Widely recycled
  22 PAP   - Paper                                            → Widely recycled

METALS (40-41):
  40 FE    - Steel                                            → Widely recycled
  41 ALU   - Aluminium                                        → Widely recycled

GLASS (70-72):
  70 GL    - Clear/colorless glass                            → Widely recycled
  71 GL    - Green glass                                      → Widely recycled
  72 GL    - Brown glass                                      → Widely recycled

COMPOSITES (80-84):
  80 C/PAP - Paper + plastic (e.g., beverage cartons)         → Special recycling
  81 C/PAP - Paper + metal (e.g., coffee pods)                → Limited recycling
  82 C/PAP - Paper + metal + plastic (e.g., Tetra Pak)        → Special recycling
  84 C/PAP - Paper + plastic + aluminium                      → Special recycling
```

---

### 1.8 Medical & Biological Waste

**What we need:**
- Waste categories (infectious, sharps, pharmaceutical, cytotoxic, radioactive)
- Handling requirements (PPE, containers, labeling)
- Disposal routes (autoclaving, incineration, special collection)
- Legal classifications (AVV Chapter 18)

**Data Sources:**

| Source | URL | Format | Notes |
|--------|-----|--------|-------|
| AVV Chapter 18 | gesetze-im-internet.de | Legal text | Medical/veterinary waste codes |
| LAGA M18 | laga-online.de | PDF | German federal guidelines for healthcare waste |
| WHO Guidelines | who.int | PDF | International safe management of healthcare waste |
| RKI Guidelines | rki.de | PDF | Robert Koch Institute infection control |

**Medical Waste Categories (simplified):**
```
AS 18 01 01   - Sharps (needles, scalpels)           → Special sharps containers
AS 18 01 02   - Body parts, organs                   → Special incineration
AS 18 01 03*  - Infectious waste (cultures, etc.)    → Autoclaving or incineration
AS 18 01 04   - Non-infectious medical waste         → Restmüll (if not contaminated)
AS 18 01 06*  - Cytotoxic/cytostatic drugs           → High-temperature incineration
AS 18 01 08*  - Medicines (hazardous)                → Pharmacy take-back or Schadstoffmobil
AS 18 01 09   - Medicines (non-hazardous)            → Pharmacy take-back or Restmüll
```

---

### 1.9 Batteries & Accumulators

**What we need:**
- Battery chemistry (Li-ion, NiMH, NiCd, Lead-acid, Alkaline, Zinc)
- Hazard levels (fire risk, heavy metals, corrosive electrolyte)
- Size categories (button cell, AA/AAA, vehicle battery)
- Collection requirements (retailer take-back, Wertstoffhof)
- UN transport numbers

**Data Sources:**

| Source | URL | Format | Notes |
|--------|-----|--------|-------|
| ADR Dangerous Goods | adr-tool.com | Web tool | UN numbers for battery types |
| EU Battery Regulation | EUR-Lex 2023/1542 | Legal text | New 2024 regulation (replaces 2006/66/EC) |
| GRS Batterien | grs-batterien.de | Reference | German battery collection system |
| Stiftung EAR | stiftung-ear.de | Registry | German producer responsibility register |

**Battery Types and Disposal Routes:**

| Type | Chemistry | Hazards | Collection Point |
|------|-----------|---------|------------------|
| Primary (single-use) | Alkaline, Zinc-Carbon | Low | Supermarket collection boxes |
| Primary (button) | Lithium, Silver oxide | Mercury, fire | Supermarket, Wertstoffhof |
| Rechargeable | Li-ion, Li-Po | **Fire risk** | Electronics retailers, Wertstoffhof |
| Rechargeable | NiMH, NiCd | Cadmium (toxic) | Electronics retailers |
| Vehicle | Lead-acid | Corrosive acid, lead | Return to seller (mandatory deposit) |
| E-bike/scooter | Li-ion (large) | **High fire risk** | Manufacturer, specialized collection |

**UN Numbers for Transport:**
```
UN3090 - Lithium metal batteries (standalone)
UN3091 - Lithium metal batteries in/with equipment
UN3480 - Lithium ion batteries (standalone)
UN3481 - Lithium ion batteries in/with equipment
UN2794 - Lead-acid batteries (wet, filled)
UN2795 - Lead-acid batteries (wet, filled with acid)
UN3028 - Dry batteries containing potassium hydroxide
```

---

### 1.10 Oils, Lubricants & Fuels

**What we need:**
- Oil types (motor oil, cooking oil, hydraulic fluid)
- Contamination level (used vs. fresh)
- Container material (for separate disposal)
- Quantity limits for private disposal

**Disposal Routes:**
```
Motor oil / lubricants    → Wertstoffhof, gas stations (free take-back)
Cooking oil / fat         → Wertstoffhof, some Biomüll (check locally)
Heating oil residues      → Schadstoffmobil, Wertstoffhof
Fuel (petrol, diesel)     → Schadstoffmobil (small quantities only)
```

**Legal Note:** Retailers selling motor oil must accept used oil for free (Altölverordnung).

---

### 1.11 Office Supplies, Stationery & University Products

**What we need:**
- Product identification (names, EAN barcodes, UNSPSC/eCl@ss codes)
- Material composition (plastic types, wood, metal, ink/toner chemistry, adhesives)
- Disposal routes and take-back programs (especially toner/ink cartridges)
- Eco-label data (Blauer Engel, EU Ecolabel, FSC/PEFC for paper)
- Conditional logic (empty vs. full cartridge, dried-out vs. liquid correction fluid)

**Data Sources:**

| Source | URL | Format | License | Size | Notes |
|--------|-----|--------|---------|------|-------|
| PBSeasy (PBS Network) | pbsnetwork.eu | REST API, JSON | Commercial B2B | 1M+ products | Industry-standard data platform for EU office/stationery. eCl@ss Segment 24. |
| Blauer Engel Fo.KUS DB | (email request) | XML, file download | Free (request) | 8,850+ EANs | Eco-certified office products with GTIN codes. Daily updates. |
| EU Ecolabel ECAT | data.europa.eu | CSV, API | Open data | 109,096 products | EAN13 barcodes, product names, company data. Zero-effort download. |
| ECHA SCIP | echa.europa.eu/scip-database | Web portal | Free | 15M+ factsheets | Only source linking products to hazardous substance composition. |
| Icecat Open Catalog | icecat.biz | JSON/XML/CSV API | Free tier + paid | 26M+ datasheets | Toner/cartridge specs, compatibility data, EAN/GTIN barcodes. |

**Material Complexity Examples:**
```
Wooden pencil     → Wood + graphite + paint + rubber + metal ferrule → Restabfalltonne
Ballpoint pen     → PP housing + oil-based ink + metal tip → Restabfalltonne (or Verpackungstonne)
Toner cartridge   → ABS/PS plastic + iron oxide toner + OPC drum → Retailer take-back (NEVER regular bin)
Ring binder       → PP cover + metal rings + paper labels → Must disassemble: metal→Wertstoffhof, plastic→Verpackungstonne
Correction fluid  → Solvent + titanium dioxide → Schadstoffmobil (if liquid), Restabfalltonne (if dried)
Laminated paper   → Paper + plastic film → Restabfalltonne (non-recyclable composite)
```

**Key Gap:** No single database provides structured material composition for office products. Must combine SCIP (hazardous substances), Safety Data Sheets (chemical products only), eCl@ss properties (schema without data), and Wikidata P186 (generic product types).

**Relevant AVV Codes:**
- 20 01 01: Paper/cardboard
- 15 01 01/02: Paper/plastic packaging
- 08 03 17*: Waste printing toner (hazardous)
- 08 03 18: Waste printing toner (non-hazardous)

---

## Part 2: Types of Information Needed

Beyond identifying what an item IS, we need information about HOW to handle it.

### 2.1 Disposal Instructions

**What we need:**
- Target bin/container (Restmüll, Gelber Sack, Biomüll, Papiertonne, etc.)
- Pre-disposal preparation (empty, clean, separate components, remove labels)
- Special collection requirements (Wertstoffhof, Schadstoffmobil, retailer)
- Maximum quantities for household disposal

**Data Sources:**

| Source | Type | Access | Notes |
|--------|------|--------|-------|
| Municipal Abfall-ABC | Websites | Free | Most authoritative. BSR (Berlin), AWM (Munich), etc. |
| MyMüll.de | App/API | Free | White-label app for many municipalities. |
| AbfallPlus (Abfall.IO) | App/API | Free | Another major white-label provider. |
| HACS Waste Schedule | GitHub | MIT | Open source Home Assistant integration. |

**Abfall-ABC Scraping Strategy:**
Many German cities publish "Abfall-ABC" or "Abfall-Lexikon" pages that can be scraped:
- Berlin BSR: https://www.bsr.de/abfall-abc-20563.php
- Munich AWM: https://www.awm-muenchen.de/abfallarten
- Hamburg SRH: https://www.stadtreinigung.hamburg/abfall-abc/

---

### 2.2 Transportation Requirements (ADR)

**What we need:**
- ADR classifications for hazardous goods
- UN numbers and proper shipping names
- Packaging requirements (approved containers, labeling)
- Quantity limits (Kleinmengenregelung for private transport)
- Required documentation

**Data Sources:**

| Source | URL | Format | Notes |
|--------|-----|--------|-------|
| ADR 2025 | unece.org | PDF | Full legal text, updated biennially |
| ADR-Tool | adr-tool.com | Web | Searchable by UN number |
| DGUV | dguv.de | PDF | German employer liability association guides |

**ADR Classes Relevant for Waste:**
```
Class 2   - Gases (aerosol cans, gas cylinders)
Class 3   - Flammable liquids (solvents, fuels)
Class 4.1 - Flammable solids
Class 5.1 - Oxidizing substances
Class 6.1 - Toxic substances
Class 8   - Corrosive substances (acids, bases)
Class 9   - Miscellaneous (lithium batteries, asbestos)
```

---

### 2.3 Disassembly & Component Separation

**What we need:**
- Component separation instructions (e.g., remove battery before electronics disposal)
- Hazardous component locations (e.g., mercury in old thermostats, PCBs in capacitors)
- Recyclable component identification
- Tools required for safe disassembly

**Data Sources:**

| Source | URL | Notes |
|--------|-----|-------|
| iFixit | ifixit.com | 100k+ repair guides with teardowns. Often shows hazardous components. |
| WEEE Producer Info | per Art. 15 Directive | Producers must provide treatment info for recyclers. |
| Manufacturer Programs | varies | Apple, Samsung, Dell have recycling/disassembly guides. |

**Common Disassembly Requirements:**
```
Smartphones      → Remove battery if possible before disposal
Laptops          → Remove battery (often requires tools)
Thermometers     → Old ones may contain mercury → special disposal
Smoke detectors  → May contain radioactive Am-241 → return to manufacturer
Refrigerators    → Contains refrigerant (CFCs/HFCs) → professional removal
CRT monitors     → Lead in glass, requires special handling
Fluorescent tubes → Mercury vapor → special collection
```

---

### 2.4 Material Properties

**What we need:**
- Density (for weight estimation from visual volume - critical for disposal cost)
- Chemical compatibility (what shouldn't be mixed)
- Degradation properties (biodegradability, photodegradation)
- Thermal properties (flammability, melting point, autoignition)
- Toxicity indicators

**Data Sources:**

| Source | URL | Format | License | Notes |
|--------|-----|--------|---------|-------|
| MakeItFrom.com | makeitfrom.com | Web | Free | Curated engineering properties. Easy comparison. |
| Open Materials DB | openmaterialsdb.se | API | Open | Computational materials. Academic focus. |
| NIST WebBook | webbook.nist.gov | API | Public Domain | Thermochemical data for 40k+ compounds. |
| PubChem | pubchem.ncbi.nlm.nih.gov | API | Public Domain | Physical properties, safety data. |
| MatWeb | matweb.com | Web | Free/Paid | 100k+ material datasheets. |

**Density Reference (for volume-to-weight estimation):**
```
Material              Density (kg/m³)   Notes
──────────────────────────────────────────────────
Concrete              2,400             Heavy - expensive disposal
Brick                 1,800             Bauschutt category
Drywall (gypsum)        800             Light - cheap if pure
Wood (solid)            600             Depends on treatment (A1-A4)
MDF/Chipboard           750             Usually A II (coated)
Soil/Earth            1,500             Clean vs. contaminated matters
Glass                 2,500             Recyclable
Steel                 7,800             Valuable scrap
Aluminium             2,700             Valuable scrap
Paper/Cardboard         700             Compressed bales
Mixed plastics          950             Variable
Textiles                200             Very light, bulky
```

---

### 2.5 Conditional Disposal Logic

**What we need:**
- State-dependent rules (liquid vs. dry, full vs. empty, intact vs. broken)
- Contamination detection (paint on rubble, food residue on packaging)
- Quantity thresholds (household vs. commercial quantities)
- Time-based rules (old vs. new appliances, pre-2003 treated wood)

**This maps to our schema's `DisposalRule → Condition → IF_TRUE/IF_FALSE` pattern.**

**Example Condition Trees:**

```
Paint Can:
├── Is it empty and dry?
│   ├── YES → Gelber Sack (packaging waste)
│   └── NO → Is there liquid paint remaining?
│       ├── YES → Schadstoffmobil (hazardous waste)
│       └── NO (dried residue) → Restmüll or Wertstoffhof

Cooking Oil:
├── Is it solid/solidified fat?
│   ├── YES → Biomüll (wrapped in paper)
│   └── NO → Is it liquid oil?
│       ├── Small amount → Paper towel + Restmüll
│       └── Large amount → Wertstoffhof (oil collection)

Wood:
├── Is it treated/painted?
│   ├── NO → Wertstoffhof (A I - recyclable)
│   └── YES → Does it have wood preservative?
│       ├── NO → Wertstoffhof (A II - energy recovery)
│       └── YES → Wertstoffhof (A IV - special disposal)
```

---

## Part 3: Legal Classifications (AVV Codes)

The Abfallverzeichnis-Verordnung (AVV) is mandatory for waste classification in Germany. It implements the European Waste Catalogue (EWC).

### Structure
```
XX        Chapter (20 total, based on waste origin)
XX XX     Subgroup (process or industry-specific)
XX XX XX  Specific waste type
       *  Hazardous waste marker (special handling required)
```

### Key Chapters for University Context

| Chapter | Name (DE) | Name (EN) | Relevance |
|---------|-----------|-----------|-----------|
| 06 | Anorganisch-chemische Prozesse | Inorganic chemical processes | Chemistry labs |
| 07 | Organisch-chemische Prozesse | Organic chemical processes | Chemistry/bio labs |
| 08 | Farben, Lacke, Klebstoffe | Paints, coatings, adhesives | Art, workshops |
| 13 | Ölabfälle | Waste oils | Workshops, machinery |
| 14 | Organische Lösemittel | Organic solvents | Labs, cleaning |
| 15 | Verpackungsabfall | Packaging waste | All buildings |
| 16 | Nicht anderswo genannt | Not otherwise specified | Catch-all, batteries, electronics |
| 17 | Bau- und Abbruchabfälle | Construction/demolition | Campus renovation |
| 18 | Medizinische Abfälle | Medical/veterinary waste | Health sciences, bio labs |
| 20 | Siedlungsabfälle | Municipal waste | General campus, offices |

### Common AVV Codes for University

```
20 03 01    Gemischte Siedlungsabfälle (Restmüll)
20 01 01    Papier und Pappe
20 01 02    Glas
20 01 39    Kunststoffe
20 01 40    Metalle
20 01 08    Biologisch abbaubare Küchen- und Kantinenabfälle
15 01 01    Verpackungen aus Papier und Pappe
15 01 02    Verpackungen aus Kunststoff
15 01 04    Verpackungen aus Metall
15 01 07    Verpackungen aus Glas
16 06 01*   Bleibatterien (hazardous)
16 06 02*   Ni-Cd-Batterien (hazardous)
16 06 03*   Quecksilberhaltige Batterien (hazardous)
16 06 04    Alkalibatterien
16 06 05    Andere Batterien und Akkumulatoren
20 01 21*   Leuchtstoffröhren (hazardous - mercury)
20 01 35*   Elektro-/Elektronik-Altgeräte mit gefährlichen Bauteilen
20 01 36    Elektro-/Elektronik-Altgeräte (ohne gefährliche Bauteile)
08 01 11*   Farb- und Lackabfälle mit organischen Lösemitteln (hazardous)
08 01 12    Farb- und Lackabfälle (ohne Lösemittel)
07 01 04*   Andere organische Lösemittel (hazardous)
17 01 01    Beton
17 01 02    Ziegel
17 01 07    Gemische aus Beton, Ziegeln, Fliesen (ohne gefährliche Stoffe)
17 02 01    Holz (unbehandelt)
17 02 04*   Holz mit gefährlichen Stoffen (hazardous)
18 01 03*   Infektiöse Abfälle (hazardous)
18 01 04    Nichtinfektiöse medizinische Abfälle
```

### Data Sources for AVV

| Source | URL | Notes |
|--------|-----|-------|
| Umweltbundesamt | umweltbundesamt.de | Official German guidance and code list |
| LAGA | laga-online.de | Federal state working group guidelines |
| EUR-Lex | eur-lex.europa.eu | Commission Decision 2000/532/EC (EWC) |
| CSG EWC Tool | csg.co.uk/resources/ewc-codes | UK-based but same EU codes, searchable |

---

## Part 4: Infrastructure Data (Where to Dispose)

### 4.1 OpenStreetMap Tags

Query via Overpass API for disposal locations.

**Relevant Tags:**
```
amenity=recycling                    # Recycling points
  recycling_type=centre              # Wertstoffhof (full service)
  recycling_type=container           # Street containers

  # Specific material acceptance:
  recycling:glass=yes
  recycling:glass_bottles=yes
  recycling:paper=yes
  recycling:plastic=yes
  recycling:plastic_packaging=yes
  recycling:clothes=yes
  recycling:batteries=yes
  recycling:small_appliances=yes
  recycling:scrap_metal=yes
  recycling:wood=yes
  recycling:green_waste=yes
  recycling:oil=yes
  recycling:hazardous_waste=yes

amenity=waste_disposal               # General waste bins (large)
amenity=waste_basket                 # Street litter bins
amenity=waste_transfer_station       # Transfer stations
landuse=landfill                     # Landfill sites

# Additional useful tags:
opening_hours=*                      # Operating hours
operator=*                           # Operating company
phone=*                              # Contact number
website=*                            # More information
```

**Example Overpass Query (Wertstoffhöfe in Munich):**
```
[out:json][timeout:25];
area[name="München"][admin_level=6]->.searchArea;
(
  node["amenity"="recycling"]["recycling_type"="centre"](area.searchArea);
  way["amenity"="recycling"]["recycling_type"="centre"](area.searchArea);
  relation["amenity"="recycling"]["recycling_type"="centre"](area.searchArea);
);
out center;
```

**Example Overpass Query (Glass containers near coordinates):**
```
[out:json][timeout:10];
(
  node["amenity"="recycling"]["recycling:glass"="yes"]
    (around:1000, 48.1351, 11.5820);
);
out;
```

### 4.2 Municipal Data Sources (Germany)

| City | Waste Company | Abfall-ABC URL | API/Integration |
|------|---------------|----------------|-----------------|
| Berlin | BSR | bsr.de/abfall-abc | MyMüll integration |
| Munich | AWM | awm-muenchen.de | Abfallkalender API |
| Hamburg | SRH | stadtreinigung.hamburg | MyMüll integration |
| Frankfurt | FES | fes-frankfurt.de | AbfallPlus |
| Cologne | AWB | awbkoeln.de | AbfallPlus |
| Stuttgart | AWS | stuttgart.de | Custom system |
| Düsseldorf | AWISTA | awista.de | AbfallPlus |

### 4.3 Schadstoffmobil (Mobile Hazardous Waste Collection)

Most German cities operate mobile collection for household hazardous waste:
- Search: `[City name] Schadstoffmobil` or `[City name] Giftmobil`
- Typically accepts: paints, solvents, batteries, chemicals, pesticides, medicines
- Often published as calendar PDF or in waste app

---

## Part 5: Universal Classification Systems

### 5.1 Taxonomies for the "Translation Layer"

These systems help map user queries ("I have an old printer") to known waste categories.

| System | Coverage | Access | Best For |
|--------|----------|--------|----------|
| UNSPSC | 157,000 codes | Free PDF | Generic product categorization |
| ECLASS | 45,000+ classes | Licensed (€€) | German industry, B2B, detailed properties |
| GPC (GS1) | Global | Browse free | Barcode-linked products |
| Wikidata | Everything | Free SPARQL | Generic knowledge, multilingual synonyms |
| HS Codes | International trade | Free | Customs classification, materials |

### 5.2 Wikidata for Waste Agents

Wikidata is extremely useful for:
- **Multilingual synonyms** - Same item in German, English, Turkish, etc.
- **Material relationships** - "made from material" property
- **Type hierarchies** - "subclass of" chains
- **Recycling codes** - Standard packaging codes

**Key Wikidata Properties:**

| Property | ID | Description | Example |
|----------|-----|-------------|---------|
| instance of | P31 | What type of thing is this | Coca-Cola can → beverage can |
| subclass of | P279 | Parent category | beverage can → container |
| made from material | P186 | What is it made of | plastic bottle → PET |
| recycling code | P1149 | Standard recycling number | PET → 1 |
| part of | P361 | Component relationships | battery → laptop |
| has part | P527 | Contains components | laptop → battery |
| mass | P2067 | Weight | concrete block → 20 kg |
| density | P2054 | Density | concrete → 2400 kg/m³ |

**Wikidata Multilingual Example:**
```sparql
# Get all labels for "plastic bottle" in German, English, Turkish
SELECT ?item ?labelDE ?labelEN ?labelTR
WHERE {
  ?item wdt:P31 wd:Q131696 .  # instance of: plastic bottle
  OPTIONAL { ?item rdfs:label ?labelDE FILTER(LANG(?labelDE) = "de") }
  OPTIONAL { ?item rdfs:label ?labelEN FILTER(LANG(?labelEN) = "en") }
  OPTIONAL { ?item rdfs:label ?labelTR FILTER(LANG(?labelTR) = "tr") }
}
LIMIT 100
```

**SPARQL Query - Materials with Recycling Codes:**
```sparql
SELECT ?item ?itemLabel ?material ?materialLabel ?code
WHERE {
  ?item wdt:P186 ?material .   # made from material
  ?item wdt:P1149 ?code .      # has recycling code
  SERVICE wikibase:label { bd:serviceParam wikibase:language "de,en". }
}
LIMIT 100
```

**API Endpoint for Synonyms:**
```bash
# Get all labels and aliases for "battery" (Q267298)
curl "https://www.wikidata.org/w/api.php?action=wbgetentities&ids=Q267298&props=labels|aliases&languages=de|en|tr&format=json"
```

---

## Part 6: Computer Vision Datasets

For image-based waste identification (camera input, not barcode).

### 6.1 TACO (Trash Annotations in Context)

**Best for:** Detecting litter "in the wild" - on streets, beaches, parks.

| Attribute | Value |
|-----------|-------|
| URL | http://tacodataset.org |
| Size | 1,500 images, 4,784 annotations |
| Classes | 60 waste types |
| Format | COCO format (JSON + images) |
| License | CC BY 4.0 |

**Categories include:** Aluminium foil, batteries, beverage cans, bottles (glass, plastic), cardboard, cigarette butts, cups, food waste, plastic bags, styrofoam, and more.

**Limitation:** Small dataset, class imbalance. Good for prototyping, needs augmentation for production.

### 6.2 TrashNet

**Best for:** Simple "Hello World" classification into 6 categories.

| Attribute | Value |
|-----------|-------|
| URL | github.com/garythung/trashnet |
| Size | 2,527 images |
| Classes | 6 (glass, paper, cardboard, plastic, metal, trash) |
| License | MIT |

**Limitation:** Very small, studio-lit photos only. Not suitable for real-world deployment.

### 6.3 WasteNet (Recycleye)

**Best for:** Industrial MRF (Material Recovery Facility) sorting.

| Attribute | Value |
|-----------|-------|
| Size | Millions of images (claimed) |
| Focus | Conveyor belt waste sorting |
| Access | Commercial/partnership |

### 6.4 Roboflow Universe - CDW Datasets

**Best for:** Construction & demolition waste, rubble classification.

| Attribute | Value |
|-----------|-------|
| URL | universe.roboflow.com (search "construction waste") |
| Focus | Rubble, bricks, concrete, rebar |
| Format | YOLO, COCO, TFRecord |
| License | Varies by dataset |

**Search terms:** "construction waste", "demolition", "rubble", "CDW classification"

### 6.5 Building a Custom Dataset

For university-specific items not in public datasets:
1. Photograph items in different lighting/angles
2. Annotate with Roboflow, CVAT, or Label Studio
3. Augment with rotations, crops, color jitter
4. Consider synthetic data generation

---

## Part 7: Priority Matrix

Based on the university use case, here's a suggested priority for data integration:

### Tier 1: Core Functionality (Must Have)

| Priority | Source | Reason |
|----------|--------|--------|
| 1 | **Local Abfall-ABC** | Authoritative local disposal rules |
| 2 | **Open Food Facts** | Food packaging is most common query |
| 3 | **AVV Codes** | Legal compliance, proper classification |
| 4 | **PubChem** | Lab chemicals (university context) |
| 5 | **Wikidata** | Synonyms, translations, fallback lookup |
| 5b | **EU Ecolabel ECAT + Blauer Engel** | Office/stationery products with EAN barcodes (free, structured) |

### Tier 2: Enhanced Features (Should Have)

| Priority | Source | Reason |
|----------|--------|--------|
| 6 | **ÖKOBAUDAT** | Construction waste (campus renovation) |
| 7 | **Icecat** | Electronics and appliances |
| 8 | **OpenStreetMap** | Wertstoffhof locations |
| 9 | **ADR/UN Numbers** | Transport safety info |
| 9b | **PBSeasy / Icecat (toner)** | Office supply product data, toner/cartridge specs (negotiated/free tier) |

### Tier 3: Advanced Features (Nice to Have)

| Priority | Source | Reason |
|----------|--------|--------|
| 10 | **UNSPSC/ECLASS** | Product taxonomy enrichment |
| 11 | **Material properties** | Weight estimation, safety checks |
| 12 | **TACO/CV datasets** | Image recognition feature |
| 13 | **iFixit** | Disassembly guidance |

---

## Part 8: Data Integration Architecture

```
                              User Query
                                  │
                    ┌─────────────┴─────────────┐
                    │                           │
               [Barcode]                   [Text/Image]
                    │                           │
                    ▼                           ▼
    ┌───────────────────────────┐   ┌───────────────────────────┐
    │  API Lookup Layer         │   │  NLP/CV Layer             │
    │                           │   │                           │
    │  • Open Food Facts        │   │  • Entity extraction      │
    │  • Icecat                 │   │  • Image classification   │
    │  • PubChem                │   │  • Wikidata lookup        │
    │  • EU Ecolabel / Bl.Engel │   │  • PBSeasy (office)       │
    └───────────────────────────┘   └───────────────────────────┘
                    │                           │
                    └─────────────┬─────────────┘
                                  │
                                  ▼
              ┌───────────────────────────────────────┐
              │  Classification Layer                  │
              │                                        │
              │  • Material extraction (packaging_tags)│
              │  • Hazard check (GHS, ADR)            │
              │  • AVV code assignment                │
              │  • WasteStream mapping                │
              └───────────────────────────────────────┘
                                  │
                                  ▼
              ┌───────────────────────────────────────┐
              │  Rules Engine (Neo4j Graph)            │
              │                                        │
              │  (WasteItem)-[:HAS_RULE]->(DisposalRule)│
              │  (DisposalRule)-[:HAS_CONDITION]->(...)│
              │  (Condition)-[:IF_TRUE]->(...WasteStream)│
              │  (Condition)-[:IF_FALSE]->(...)        │
              └───────────────────────────────────────┘
                                  │
                                  ▼
              ┌───────────────────────────────────────┐
              │  Output Layer                          │
              │                                        │
              │  • Disposal instruction (text)         │
              │  • Target bin/container                │
              │  • Nearest facility (OSM query)        │
              │  • Tips and warnings                   │
              └───────────────────────────────────────┘
```

---

## Part 9: Data Gaps & Challenges

### 9.1 Known Gaps

| Gap | Description | Mitigation |
|-----|-------------|------------|
| **No unified "Open Rubble Facts"** | No single database for construction materials like OFF for food | Combine ÖKOBAUDAT + Wikidata + custom entries |
| **Local rules fragmentation** | Each municipality has different rules | Scrape Abfall-ABC from major cities, allow campus-specific override |
| **Composite materials** | Multi-material products (wood+metal, paper+plastic) hard to classify | Use Condition nodes for "primary material" logic |
| **Product variants** | Can't track every Milka chocolate variant | Map to generic categories ("chocolate bar" → organic waste + packaging) |
| **Condition-dependent logic** | Same item, different state = different disposal | Core strength of graph model - build comprehensive Condition trees |
| **Language coverage** | German students may query in Turkish, Arabic, etc. | Wikidata aliases, OpenAI/Claude for translation |
| **No "Open Office Facts"** | No single open database for office supplies/stationery like OFF for food | Combine EU Ecolabel + Blauer Engel + Wikidata + PBSeasy (negotiated) |
| **Office product material composition** | No database provides "this pen is 40% PP, 30% PS, 20% brass, 10% ink" | Chain SCIP + SDS + eCl@ss properties; manual research for non-chemical items |
| **University procurement catalogs** | All German university procurement data is closed/confidential | Partnership with university procurement department (Simple System / JAGGAER) |
| **Toner cartridge → AVV mapping** | No pre-built dataset mapping cartridges to waste codes | Build 5-layer pipeline: Icecat → SDS → GESTIS/ECHA → threshold calc → AVV code |

### 9.2 Data Quality Considerations

| Source | Quality | Freshness | Coverage |
|--------|---------|-----------|----------|
| Open Food Facts | Medium (crowdsourced) | Real-time | Excellent for packaged food |
| Icecat | High (vendor data) | Good | Electronics, appliances |
| PubChem | High (scientific) | Good | Chemicals |
| ÖKOBAUDAT | High (government) | Annual updates | German building materials |
| Wikidata | Variable | Real-time | Everything, but shallow |
| Municipal Abfall-ABC | High (authoritative) | Varies | City-specific |
| EU Ecolabel ECAT | High (official) | Updated | Eco-certified products with EAN13 |
| Blauer Engel Fo.KUS | High (government-backed) | Daily | German eco-certified office/stationery |
| PBSeasy | High (vendor data, 200+ checks) | Continuous | EU office supplies (paid B2B) |

### 9.3 Technical Challenges

1. **API rate limits** - Open Food Facts, PubChem have limits
2. **Data format heterogeneity** - JSON, XML, CSV, SPARQL all need different parsers
3. **ID reconciliation** - Matching items across sources (barcode vs. Wikidata ID vs. AVV code)
4. **Keeping data fresh** - Some sources update frequently, need ETL refresh strategy
5. **Copyright/licensing** - ECLASS is licensed, some SDS databases are paywalled

---

## Part 10: Research Agent Topics

This section defines focused research tasks for autonomous agents to find additional data sources. Each agent is assigned a specific domain to investigate.

### Tier 1: Core Data (High Impact)

These agents target the most essential data sources for the knowledge graph.

| # | Agent Topic | Research Goal |
|---|-------------|---------------|
| 1 | **Food & Grocery Products** | Find APIs/datasets with packaging materials, barcodes, organic vs. packaging separation |
| 2 | **Consumer Electronics & E-Waste** | Product specs, battery types, WEEE categories, hazardous components |
| 3 | **Chemicals & Hazardous Materials** | GHS data, SDS databases, lab chemicals (university context) |
| 4 | **German Municipal Waste Rules** | Abfall-ABC sources, MyMüll/AbfallPlus APIs, city-specific disposal rules |
| 5 | **AVV/EWC Legal Codes** | Official German/EU waste classification codes with hazard markers |
| 19 | **Office Supplies & Stationery** | PBSeasy, Blauer Engel, EU Ecolabel, BMEcat catalogs, toner/ink data, university procurement |

### Tier 2: Domain-Specific (Medium Impact)

These agents cover important but more specialized waste categories.

| # | Agent Topic | Research Goal |
|---|-------------|---------------|
| 6 | **Construction Materials (Bauschutt)** | ÖKOBAUDAT, EPDs, density data, pure vs. contaminated rubble |
| 7 | **Packaging & Recycling Codes** | Complete recycling code datasets (01-84), composite material identification |
| 8 | **Batteries & Accumulators** | Chemistry types, UN numbers, collection point requirements |
| 9 | **Wood & Furniture (Altholz)** | A1-A4 classification data, treatment indicators |
| 10 | **Facility/Infrastructure Locations** | OpenStreetMap tags, Wertstoffhof databases, Schadstoffmobil schedules |

### Tier 3: Enrichment (Nice to Have)

These agents provide supplementary data for enhanced features.

| # | Agent Topic | Research Goal |
|---|-------------|---------------|
| 11 | **Material Properties** | Density, toxicity, flammability databases for weight estimation and safety |
| 12 | **Universal Taxonomies** | UNSPSC, ECLASS, GPC for product categorization |
| 13 | **Multilingual Synonyms (Wikidata)** | German/English/Turkish labels for waste items |
| 14 | **Textiles & Clothing** | Fiber composition databases, recyclability by material |
| 15 | **Disassembly Instructions** | iFixit-style data, component separation for electronics |

### Tier 4: Advanced Features

These agents target specialized or future-looking capabilities.

| # | Agent Topic | Research Goal |
|---|-------------|---------------|
| 16 | **Computer Vision Datasets** | TACO, TrashNet, Roboflow for image-based waste recognition |
| 17 | **Transport/ADR Regulations** | UN numbers, packaging requirements for hazardous waste handling |
| 18 | **Medical & Lab Waste** | Sharps, infectious waste, cytotoxic - relevant for university labs |

### Agent Output Requirements

Each research agent should document:
1. **Data sources found** - Name, URL, description
2. **Data format** - API, CSV, JSON, XML, web scraping required
3. **Access method** - Free, registration, paid, rate limits
4. **License** - ODbL, CC, public domain, proprietary
5. **Coverage** - How many items/records, geographic scope
6. **Relevance score** - How useful for our knowledge graph (1-5)
7. **Integration notes** - Technical considerations for ETL

### Recommended Priority

**Start with Tier 1 + Tier 2 (agents 1-10)** for comprehensive coverage of:
- Branded products (food, electronics)
- Hazardous materials (chemicals, batteries)
- Construction waste (rubble)
- Legal compliance (AVV codes)
- Location data (facilities)

---

## Sources Referenced

### Open Data Catalogs
- [Open Food Facts](https://world.openfoodfacts.org/data) - ODbL license
- [Open Products Facts](https://world.openproductsfacts.org/discover) - ODbL license
- [Icecat Open Catalog](https://icecat.com) - Free tier available
- [PubChem](https://pubchem.ncbi.nlm.nih.gov) - Public domain

### German Official Sources
- [ÖKOBAUDAT](https://www.oekobaudat.de/en.html) - Free
- [Umweltbundesamt](https://www.umweltbundesamt.de) - AVV guidance
- [GESTIS](https://gestis.dguv.de) - Hazardous substance database
- [Blauer Engel Fo.KUS](https://www.blauer-engel.de) - Eco-label product database (XML, free on request)

### Office & Stationery
- [PBSeasy](https://pbsnetwork.eu) - 1M+ office products (commercial B2B)
- [EU Ecolabel ECAT](https://data.europa.eu) - 109k products with EAN13 (open data CSV)
- [ECHA SCIP](https://echa.europa.eu/scip-database) - Hazardous substances in articles
- [BMECat.net GitHub](https://github.com/stephanstapel/BMECat.net) - BMEcat parser + sample catalogs

### Classification Systems
- [UNSPSC](https://www.undp.org/unspsc) - Free PDF
- [ECLASS](https://eclass.eu) - Licensed
- [Wikidata SPARQL](https://query.wikidata.org) - CC0

### Infrastructure
- [OpenStreetMap](https://wiki.openstreetmap.org/wiki/Tag:amenity=recycling) - ODbL
- [MyMüll.de](https://www.mymuell.de) - App
- [HACS Waste Schedule](https://github.com/mampfes/hacs_waste_collection_schedule) - MIT

### Transport & Safety
- [ADR Dangerous Goods](https://unece.org/transport/dangerous-goods) - Official
- [ADR-Tool](https://adr-tool.com) - Web tool

### Computer Vision
- [TACO Dataset](http://tacodataset.org) - CC BY 4.0
- [TrashNet](https://github.com/garythung/trashnet) - MIT
- [Roboflow Universe](https://universe.roboflow.com) - Various licenses

### Material Properties
- [MakeItFrom.com](https://www.makeitfrom.com) - Free
- [NIST Chemistry WebBook](https://webbook.nist.gov) - Public domain
- [Open Materials Database](https://openmaterialsdb.se) - Open
