# BMEcat Import: Findings & Implementation Plan

## What is BMEcat?

BMEcat is an XML-based product catalog exchange format widely used in German B2B procurement. It is a **format standard, not a data source** — the actual product data lives in catalogs published by suppliers like Lyreco, Schäfer Shop, or Mercateo.

Universities typically procure office supplies through framework contracts (Rahmenverträge) with distributors who deliver catalogs in BMEcat format via e-procurement platforms like Simple System or JAGGAER.

## Data Availability

### Real catalogs: all closed

No openly downloadable, real-world BMEcat office supply catalogs exist. They are confidential B2B exchange documents between suppliers and purchasing departments.

| Supplier | BMEcat Support | Access |
|----------|---------------|--------|
| Unite/Mercateo | Requires BMEcat 1.2 as primary catalog format | Dealer/partner accounts only |
| Schäfer Shop | BMEcat + Excel catalogs, eCl@ss 4.0–7.1 | Registered B2B customers only |
| Lyreco | BMEcat, JSON, OCI punchout, HTML | B2B integration only |
| Simple System | Requires suppliers to provide BMEcat exclusively | University procurement portal (widely used by German universities) |
| Bechtle | BMEcat, JSON, CIF with eCl@ss/UNSPSC classification | B2B customers only |

### Free sample files (for parser development)

| Resource | URL | Contents |
|----------|-----|----------|
| BME e.V. specs + examples | https://bme.de (Downloads page) | XSD schemas, DTDs, `eclass_bmecat_examples.zip` for BMEcat 2005.1. Free, no registration. |
| BMECat.net GitHub | https://github.com/stephanstapel/BMECat.net | 14 real-world Weidmüller industrial catalog XML files + a demo **"Office Material" catalog**. C# parser library. |
| olivere/bmecat (Go) | https://github.com/olivere/bmecat | Test files with office supply sample data (Post-Safe Polythene Envelopes, Office Supplies AG). |
| SEPIA BMEcatHub | https://sepia.de/en/bmecathub | Import/edit/export BMEcat catalogs. Free basic version. |

These samples are useful for building and testing a parser, but contain only toy data — not real catalogs with thousands of products.

## BMEcat Data Structure

A BMEcat catalog defines rich product fields relevant to Fessi:

```xml
<ARTICLE>
  <SUPPLIER_AID>12345</SUPPLIER_AID>           <!-- Supplier article number -->
  <ARTICLE_DETAILS>
    <DESCRIPTION_SHORT>Kugelschreiber K15</DESCRIPTION_SHORT>
    <DESCRIPTION_LONG>Kugelschreiber mit Clip...</DESCRIPTION_LONG>
    <EAN>4004675064107</EAN>                    <!-- Barcode -->
    <MANUFACTURER_AID>K15-BL</MANUFACTURER_AID>
    <MANUFACTURER_NAME>Schneider</MANUFACTURER_NAME>
  </ARTICLE_DETAILS>
  <ARTICLE_FEATURES>
    <FEATURE>
      <FNAME>ECLASS-8.1</FNAME>                <!-- eCl@ss classification -->
      <FVALUE>24-26-04-90</FVALUE>              <!-- Segment 24: Office products -->
    </FEATURE>
    <FEATURE>
      <FNAME>UNSPSC-15.0</FNAME>               <!-- UNSPSC classification -->
      <FVALUE>44121704</FVALUE>                 <!-- Ballpoint pens -->
    </FEATURE>
  </ARTICLE_FEATURES>
  <MIME_INFO>
    <MIME>
      <MIME_TYPE>application/pdf</MIME_TYPE>
      <MIME_SOURCE>sds/K15-BL-safety.pdf</MIME_SOURCE>  <!-- Safety Data Sheet -->
      <MIME_PURPOSE>safety_data_sheet</MIME_PURPOSE>
    </MIME>
  </MIME_INFO>
  <ARTICLE_PRICE_DETAILS>...</ARTICLE_PRICE_DETAILS>    <!-- Not needed for Fessi -->
</ARTICLE>
```

**Fields useful for the knowledge graph:**
- `EAN` — barcode for cross-referencing with Open Food Facts, Icecat, Blauer Engel
- `DESCRIPTION_SHORT` / `DESCRIPTION_LONG` — product name and description
- `MANUFACTURER_NAME` / `MANUFACTURER_AID` — provenance
- `ARTICLE_FEATURES` with eCl@ss/UNSPSC codes — product classification and (in eCl@ss 16.0+) material declaration properties
- `MIME_INFO` with `safety_data_sheet` purpose — links to SDS PDFs for composition extraction

**Fields NOT typically present:**
- Material composition (what the product is made of)
- Disposal instructions
- Hazard classification (unless linked via SDS)

## How to Get a Real Catalog

The recommended path is to partner with the university's procurement department (Vergabestelle):

1. **Identify the e-procurement system** — likely Simple System or JAGGAER
2. **Request a BMEcat export** of the current office supplies framework contract catalog
3. The catalog will contain all products available to the university with EAN barcodes, eCl@ss codes, and descriptions
4. This is the single richest structured data source for "what office products are actually in use at this university"

This is not a technical problem — it requires an institutional conversation.

## Implementation Plan

### Phase 1: Build the parser (using free samples)

Build a BMEcat XML parser that extracts product data into our `ProductRecord` format (as defined in `docs/graph_import_agent.md`). Test against:

1. BME e.V. example files (BMEcat 2005.1 format reference)
2. BMECat.net "Office Material" demo catalog
3. olivere/bmecat test files

The parser should handle both BMEcat 1.2 (used by Mercateo/Unite) and BMEcat 2005.1 (newer standard).

**Key extraction targets:**
- `SUPPLIER_AID`, `EAN`, `MANUFACTURER_NAME`, `MANUFACTURER_AID`
- `DESCRIPTION_SHORT`, `DESCRIPTION_LONG`
- eCl@ss and UNSPSC codes from `ARTICLE_FEATURES`
- SDS links from `MIME_INFO`

### Phase 2: Map to knowledge graph

Once products are parsed, the ETL pipeline should:

1. **Create `Product` nodes** with barcode, name, manufacturer, eCl@ss code
2. **Link to existing `WasteItem` nodes** via name/category matching (eCl@ss → waste category)
3. **Link to `WasteStream` nodes** where deterministic (e.g., paper products → Altpapiertonne)
4. **Create `DisposalRule` / `Condition` nodes** for items requiring conditional logic (e.g., toner cartridges → take-back vs. Restabfall depending on state)
5. **Attach `Source` provenance** (`DERIVED_FROM` → Source node with BMEcat catalog metadata)

### Phase 3: Plug in real catalog

When a real BMEcat catalog is obtained from the university procurement department, run the same parser + ETL pipeline on it. The parser is format-agnostic — it doesn't care whether the catalog has 10 demo products or 10,000 real ones.

## BMEcat Parser Tools

Existing libraries that could accelerate parser development:

| Library | Language | Notes |
|---------|----------|-------|
| BMECat.net | C# | Full BMEcat 1.2 + 2005 parser. Could call from Python via subprocess or rewrite logic. |
| olivere/bmecat | Go | Lightweight, good test files. |
| No Python library found | — | Will need to write our own using `lxml` or `xml.etree.ElementTree`. |

Since no Python BMEcat parser exists, we would write one using `lxml`, targeting the `ARTICLE` elements and extracting the fields listed above. The BMEcat XSD schemas (free from BME e.V.) provide the structural reference.

## Alternative: EAN/Barcode Databases as Drop-in Replacement

Since BMEcat catalogs are closed and no real catalog is available without institutional access, we investigated whether public EAN/barcode databases could serve as an "Open Food Facts equivalent" for non-food products. The short answer: **no such equivalent exists.**

### Databases evaluated

| Source | Size | Free? | Material Data | Packaging Data | Verdict |
|--------|------|-------|--------------|---------------|---------|
| Open Products Facts | ~40k products | Yes (ODbL) | Schema yes, data no | Virtually empty | Ghost town — same schema as OFF but nobody populates it |
| Open Beauty Facts | ~13k products | Yes (ODbL) | Ingredients yes | Sparse | Useful for cosmetics/personal care hazmat only |
| opengtindb.org | ~50k (est.) | Yes (GNU FDL) | `contents` flags | `pack` flags | Small but German, open, has rudimentary material/packaging flags |
| UPCitemdb | 686M barcodes | 100 req/day | No | No | Barcode→name+category only, no material info |
| Icecat | 26M datasheets | Open catalog | Yes (electronics) | Yes | Strong for electronics/IT, not office supplies |
| ECHA SCIP | 15M factsheets | Web only | Hazardous only | No | Only flags products with SVHCs >0.1% |
| EAN-Search.org | 1.07B barcodes | 1€ trial | No | No | Huge coverage, German names, zero material data |
| Go-UPC | 500M products | $75/month | Claims `material` field | On request | Unverified, expensive |
| Barcodelookup | 500M products | $99/month | Unstructured text | No | Too expensive, no structured material fields |
| GS1 GEPIR/Verified | All GTINs | $500+ API | No (GPC category only) | GPC Attributes | Authoritative registry, not a product details database |
| Digit-Eyes | 368M records | $99 one-time | `ingredients` field | Limited | US-centric, limited European coverage |
| Barcode Spider | 670M barcodes | 100/day | No | No | Identification only |
| Brocade.io | ~722k items | Yes (open source) | Custom properties | No | Too small, poorly maintained |

### The fundamental gap

**No general-purpose barcode database provides structured material composition for everyday non-food products.** The large databases (UPCitemdb, EAN-Search, Barcodelookup) have hundreds of millions of barcodes but only store name, brand, and category — not what the product is made of or how to dispose of it.

Open Food Facts succeeds because food products are legally required to list ingredients, and a large volunteer community enters them. No such legal requirement or community exists for office supplies, cleaning products, or household items.

### What we can use (free, available now)

Despite the gap, a multi-source approach can partially solve the problem:

| Source | Role in pipeline | Access |
|--------|-----------------|--------|
| **opengtindb.org** | German product lookup with `contents`/`pack` flags | Free, GNU FDL, API with registration |
| **UPCitemdb** | Barcode→product name + category resolution | Free tier: 100 requests/day |
| **Open Beauty Facts** | Cosmetics ingredient data for hazmat classification | Free, ODbL, same API as OFF |
| **Icecat Open Catalog** | Electronics material, battery type, packaging data | Free tier (open catalog subset) |
| **ECHA SCIP** | Hazardous substance flags on specific GTINs | Free web portal |

### What we imported

We implemented and ran ETL pipelines for the free sources listed above. Results:

**Step 1: EU Ecolabel ECAT** (`src/etl/ecolabel.py`)

Fetches product data directly from the EU Ecolabel Data Query API (`apps.data.env.service.ec.europa.eu/dataquery/v2/ecolabel/products`). The API provides paginated JSON with EAN13, product name, product group, licence holder, and country.

```bash
python -m src.etl.ecolabel            # fetch from API + import
python -m src.etl.ecolabel --dry-run  # preview without DB changes
```

Result: **16,333 Product nodes** created with eco-label properties (21,609 total in API, 1,108 skipped due to invalid EANs). Product groups include hard covering products (4,981), tissue paper (4,023), floor coverings (1,982), cleaning products (1,374), paints/varnishes (1,145), textiles, furniture, cosmetics, and more.

**Step 2: Open Beauty Facts** (`src/etl/open_beauty_fetch.py` + `src/etl/products.py`)

Downloads the OBF CSV dump (~17 MB compressed), filters for products with barcodes and names, and outputs ProductInfo JSON that the existing `products.py` importer can load.

```bash
python -m src.etl.open_beauty_fetch --output data/obf_products.json
python -m src.etl.products --input data/obf_products.json
```

Result: **40,033 Product nodes** created (cosmetics, personal care, shampoos, soaps, etc.). No packaging component data was present in the dump — OBF's packaging fields are sparsely populated.

**Step 3 (optional): opengtindb.org enrichment** (`src/etl/opengtindb_fetch.py` + `src/barcode/opengtindb.py`)

Queries the opengtindb.org API to enrich existing Product nodes (from steps 1–2) with German product names, vendor info, and packaging/contents bitwise flags. The `--from-graph` mode finds Product nodes missing names and looks them up.

```bash
python -m src.etl.opengtindb_fetch --from-graph --output data/opengtindb_products.json
python -m src.etl.products --input data/opengtindb_products.json
```

**Not executed.** Registration at opengtindb.org requires a 35 EUR donation as proof of good faith, and even then the API is limited to 500 requests/day. At ~16K products needing enrichment, this would take over a month. The infrastructure is in place if a query ID is obtained in the future (`OPENGTINDB_QUERY_ID` in `.env`).

**Barcode resolver fallback chain** (`src/barcode/resolver.py`)

The barcode resolver was extended with a three-step lookup chain for real-time single-barcode queries (e.g., when a user scans a product):

1. Open Food Facts (~4M food products)
2. Open Beauty Facts (~13K cosmetics)
3. UPCitemdb (686M barcodes, 100 req/day free, name+brand+category only)

**Total: ~56K Product nodes** in the graph from free sources, all cross-referenceable by EAN barcode via deterministic UIDs (`SHA256(ean)[:16]`). If the same EAN appears in multiple sources, `MERGE` enriches the same node — each source adds its own properties and a `DERIVED_FROM` provenance edge.

### Recommended approach: product-type classification

Since individual barcode-level material data does not exist for non-food products, the practical alternative is to classify at the **product category level**:

1. **Barcode → product category** via UPCitemdb or opengtindb.org (→ GPC Brick code or eCl@ss class)
2. **Product category → waste stream** via rules we encode (e.g., "ballpoint pen" → Restabfall, "dishwashing detergent, empty" → Verpackungstonne)
3. **Hazardous substance check** via ECHA SCIP for specific GTINs
4. **Electronics detail** via Icecat for battery type, hazardous materials
5. **Cosmetics detail** via Open Beauty Facts for ingredient-based hazmat

The waste disposal rule lives at the **category level** (all ballpoint pens → Restabfall), not the individual barcode level. This is sufficient for Fessi's use case — the disposal route for a Schneider K15 pen is the same as any other ballpoint pen.

## Relation to Other Sources

BMEcat is one piece of the office product data puzzle. The full picture:

| Source | What it provides | Overlap with BMEcat |
|--------|-----------------|---------------------|
| EU Ecolabel ECAT | EAN13 + eco-certification status | Cross-reference by EAN to flag eco-certified products |
| Blauer Engel Fo.KUS | EAN/GTIN + DE-UZ product group standards | Cross-reference by EAN; adds eco-label metadata |
| Icecat | Toner/cartridge specs, compatibility | Supplements BMEcat with detailed technical specs |
| UNSPSC / eCl@ss | Product classification taxonomy | BMEcat catalogs already contain these codes in `ARTICLE_FEATURES` |
| ECHA SCIP | Hazardous substance composition | Cross-reference by EAN or article ID for SVHC data |
| Wikidata | Generic material-to-product-type mapping (P186) | Fallback for material inference when BMEcat lacks composition |
