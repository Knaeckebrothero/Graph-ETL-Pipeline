# Open Food Facts Data Import

## Data Sources

Open Food Facts (OFF) provides several dump formats. We evaluated two:

### 1. JSONL Dump (`static.openfoodfacts.org`)

- **URL:** `https://static.openfoodfacts.org/data/openfoodfacts-products.jsonl.gz`
- **Size:** 2.6 GB compressed
- **Products:** 719,174
- **Fields per record:** ~186 (from ~425 unique across all records)
- **Updated:** Nightly

Around Oct 2025, this dump was [restructured](https://forum.openfoodfacts.org/t/jsonl-file-massive-file-size-reduction/2071) from 62 GB → 9 GB (extracted). It now contains a **filtered subset** of higher-completeness products with some fields stripped (e.g., no image URLs).

### 2. Parquet on Hugging Face (full database)

- **URL:** `https://huggingface.co/datasets/openfoodfacts/product-database/resolve/main/food.parquet`
- **Size:** 4.42 GB (single file, columnar)
- **Products:** 4,290,376
- **Columns:** 111 (cleaned, no debug/duplicate fields)
- **Updated:** Nightly (via [openfoodfacts-exports](https://github.com/openfoodfacts/openfoodfacts-exports))
- **Source:** Generated from MongoDB, pushed to Hugging Face

This is the **complete dataset**. Readable with `pyarrow` / `pandas` without loading everything into memory (columnar access).

### Comparison

| Metric | JSONL dump | Parquet (HF) |
|--------|-----------|--------------|
| Total products | 719,174 | **4,290,376** |
| Unique barcodes | 719,174 | 4,290,313 |
| Avg completeness | **0.59** | 0.42 |
| Has product_name | 704,319 (97.9%) | 4,016,060 (93.6%) |
| Has images | 0 (0.0%) | **3,313,252 (77.2%)** |
| Has packagings (structured) | 190,673 (26.5%) | **400,831 (9.3%)** |
| Has categories | 544,178 (75.7%) | 1,790,866 (41.7%) |
| Germany (en:germany) | 41,310 | **385,542** |

**Conclusion:** The JSONL is a curated high-completeness subset (~17% of total). The Parquet contains the full database. We use the **Parquet** as our primary data source.

## Full Parquet Schema (111 columns)

Coverage measured across all 4,290,376 products. Columns grouped by relevance.

### Group A: Product Identity (import)

| Column | Type | Coverage | Example | Notes |
|--------|------|----------|---------|-------|
| `code` | string | 4,290,374 (100%) | `"3017620422003"` | EAN/UPC barcode. Maps to `Product.ean`. |
| `product_name` | list\<struct\<lang, text\>\> | 4,016,060 (93.6%) | `[{lang:"main", text:"Nutella"}, {lang:"de", text:"Nutella"}, {lang:"en", text:"Nutella"}]` | Multilingual. Extract `de` > `en` > `main` > first. |
| `generic_name` | list\<struct\<lang, text\>\> | 183,453 (4.3%) | `[{lang:"fr", text:"Crème de marron"}]` | Generic product type name. Same multilingual struct. Sparse but useful when present. |
| `brands` | string | 2,660,763 (62.0%) | `"Ferrero"`, `"Coca-Cola"` | Flat string. Maps to `Product.brand`. |
| `brands_tags` | list\<string\> | 2,660,548 (62.0%) | `["xx:ferrero"]` | Normalized brand tag. Redundant with `brands`. |
| `quantity` | string | 1,365,492 (31.8%) | `"400 g"`, `"1.5 L"`, `"330g"` | Free-text quantity. Maps to `Product.quantity`. |
| `product_quantity` | string | 1,358,430 (31.7%) | `"500.0"` | Numeric part only (parsed from `quantity`). |
| `product_quantity_unit` | string | 1,124,484 (26.2%) | `"g"`, `"ml"` | Unit part only. |

### Group B: Classification & Categories (import)

| Column | Type | Coverage | Example | Notes |
|--------|------|----------|---------|-------|
| `categories_tags` | list\<string\> | 1,790,866 (41.7%) | `["en:spreads", "en:chocolate-spreads", "en:hazelnut-spreads"]` | Normalized category hierarchy. Primary for graph mapping. |
| `categories` | string | 1,791,106 (41.7%) | `"Spreads, Chocolate spreads"` | Human-readable, redundant with tags. |
| `compared_to_category` | string | 1,716,947 (40.0%) | `"en:cocoa-and-hazelnuts-spreads"` | Leaf category used for Nutri-Score comparison. |
| `food_groups_tags` | list\<string\> | 1,394,238 (32.5%) | `["en:sugary-snacks", "en:sweets"]` | Broad food grouping. |
| `labels_tags` | list\<string\> | 1,183,317 (27.6%) | `["en:organic", "en:green-dot", "en:fsc"]` | Certifications and labels. Top: organic, no-gluten, non-gmo, green-dot, kosher, vegan. |
| `labels` | string | 1,183,323 (27.6%) | `"FSC, FSC Mix, Green Dot"` | Human-readable, redundant with tags. |

### Group C: Packaging (import — critical for waste routing)

| Column | Type | Coverage | Example | Notes |
|--------|------|----------|---------|-------|
| `packagings` | list\<struct\> | 400,831 (9.3%) | `[{material:"en:clear-glass", shape:"en:jar", recycling:"en:recycle-in-glass-bin", number_of_units:1, weight_measured:210.0, ...}]` | **Structured per-component breakdown.** Primary packaging source. |
| `packagings_complete` | bool | 438,703 (10.2%) | `true`/`false` | Whether packagings list is considered complete. |
| `packaging_tags` | list\<string\> | 372,975 (8.7%) | `["en:glass", "en:plastic"]` | Flat material tags. Derived from `packagings`. |
| `packaging_shapes_tags` | list\<string\> | 296,679 (6.9%) | `["en:food-can", "en:lid"]` | Flat shape tags. Derived from `packagings`. |
| `packaging_recycling_tags` | list\<string\> | 70,573 (1.6%) | `["en:recycle", "en:Do Not Recycle"]` | Flat recycling tags. Derived from `packagings`. |
| `packaging` | string | 372,994 (8.7%) | `"Plastique, Sachet"` | Free-text packaging description. |
| `packaging_text` | list\<struct\<lang, text\>\> | 32,841 (0.8%) | Multilingual packaging description. Very sparse. |

**Overlap note:** `packaging_tags`, `packaging_shapes_tags`, and `packaging_recycling_tags` are always derived from the structured `packagings` array — they never exist independently. All packaging info is in `packagings`.

#### Packagings sub-fields

Each element in the `packagings` array represents one physical packaging component.
Coverage measured on a 50K sample (1,996 products with packaging, 2,784 total components):

| Sub-field | Type | Coverage | Example |
|-----------|------|----------|---------|
| `material` | string | 82.5% of components | `"en:clear-glass"`, `"en:pp-5-polypropylene"`, `"en:aluminium"` |
| `shape` | string | 82.1% | `"en:jar"`, `"en:lid"`, `"en:film"`, `"en:food-can"` |
| `recycling` | string | 12.4% | `"en:recycle-in-glass-bin"`, `"en:recycle"`, `"en:Do Not Recycle"` |
| `number_of_units` | int64 | 23.4% | `1`, `2`, `6` |
| `weight_measured` | float | 1.3% | `210.0`, `20.55` (grams, very sparse) |
| `quantity_per_unit` | string | 8.7% | `"500g"` |
| `quantity_per_unit_unit` | string | — | `"g"` (parsed unit) |
| `quantity_per_unit_value` | string | — | `"500"` (parsed value) |
| `food_contact` | — | N/A | **Not in Parquet schema.** Present in JSONL only. |

### Group D: Environmental & Nutritional Scores (import)

| Column | Type | Coverage | Example | Notes |
|--------|------|----------|---------|-------|
| `ecoscore_grade` | string | 1,149,002 (26.8%) | `"a"` to `"e"` | Environmental impact score. |
| `ecoscore_score` | int32 | 162,702 (3.8%) | Numeric score (sparse). |
| `nutriscore_grade` | string | 4,244,067 (98.9%) | `"a"` to `"e"`, `"unknown"` | 98.9% but most are `"unknown"` — real scores in ~31%. |
| `nutriscore_score` | int32 | 1,331,281 (31.0%) | Numeric score. |
| `nova_group` | int32 | 1,089,891 (25.4%) | `1` (unprocessed) to `4` (ultra-processed). Distribution: 4=67%, 3=21%, 1=12%, 2=1%. |

### Group E: Ingredients & Allergens (maybe import)

| Column | Type | Coverage | Example | Notes |
|--------|------|----------|---------|-------|
| `ingredients_tags` | list\<string\> | 1,233,192 (28.7%) | `["en:water", "en:sugar", "en:salt"]` | Normalized ingredient tags. |
| `ingredients_text` | list\<struct\<lang, text\>\> | 1,236,525 (28.8%) | Full ingredient text, multilingual. |
| `ingredients` | string | 1,236,035 (28.8%) | Raw JSON with percent estimates. |
| `ingredients_analysis_tags` | list\<string\> | 1,308,677 (30.5%) | `["en:palm-oil-free", "en:vegan", "en:vegetarian"]` | Vegan/vegetarian/palm-oil status. |
| `allergens_tags` | list\<string\> | 713,203 (16.6%) | `["en:gluten", "en:milk", "en:nuts"]` | Allergen declarations. |
| `traces_tags` | list\<string\> | 222,514 (5.2%) | `["en:gluten", "en:milk", "en:nuts"]` | "May contain" traces. |
| `additives_tags` | list\<string\> | 671,053 (15.6%) | `["en:e202", "en:e322", "en:e330"]` | E-number additives. |
| `additives_n` | int32 | 1,235,906 (28.8%) | Count of additives. |

### Group F: Geography & Sales (import)

| Column | Type | Coverage | Example | Notes |
|--------|------|----------|---------|-------|
| `countries_tags` | list\<string\> | 4,267,261 (99.5%) | `["en:france", "en:germany"]` | Countries where product is sold. Useful as filter or property. |
| `stores_tags` | list\<string\> | 502,656 (11.7%) | `["walmart", "carrefour-city"]` | Retailer names. |
| `stores` | string | 502,663 (11.7%) | `"Intermarché,Carrefour City"` | Redundant with tags. |
| `origins_tags` | list\<string\> | 165,486 (3.9%) | `["en:france", "en:united-kingdom"]` | Country of origin of ingredients. |
| `manufacturing_places_tags` | list\<string\> | 214,081 (5.0%) | `["07000-privas", "france"]` | Where the product is made. |
| `purchase_places_tags` | list\<string\> | 205,654 (4.8%) | Where users bought the product. |
| `cities_tags` | list\<string\> | 96,655 (2.3%) | City-level purchase data. |
| `emb_codes` | string | 218,277 (5.1%) | `"FR 56.094.004 EC"` | EU packaging/processing facility codes. |

### Group G: Images (import)

| Column | Type | Coverage | Example | Notes |
|--------|------|----------|---------|-------|
| `images` | list\<struct\> | 3,313,252 (77.2%) | `[{key:"front_de", sizes:{100:{h:100,w:75}, 400:{h:400,w:300}, full:{h:3264,w:2448}}, uploaded_t:1494692392, uploader:"kiliweb"}]` | Image metadata. URL constructed from barcode + key. |

Image URL pattern: `https://images.openfoodfacts.org/images/products/{barcode_path}/{key}.{size}.jpg`
where `barcode_path` for EAN-13 `3017620422003` = `301/762/042/2003` (split into 3/3/3/rest).

Image keys: `front_{lang}`, `ingredients_{lang}`, `nutrition_{lang}`, or numeric (`1`, `2`, `3`).

### Group H: Metadata & Quality (skip — editorial only)

| Column | Type | Coverage | Notes |
|--------|------|----------|-------|
| `completeness` | float | 4,290,129 (100%) | Data quality score (0.0-1.1). Useful as filter/property. |
| `complete` | int32 | 4,290,376 (100%) | Boolean: 1 if completeness threshold met. |
| `lang` | string | 4,290,375 (100%) | Primary language of the product entry. |
| `languages_tags` | list\<string\> | 4,290,375 (100%) | All languages in the entry. |
| `created_t` | int64 | 4,290,375 (100%) | Unix timestamp of creation. |
| `last_modified_t` | int64 | 4,290,375 (100%) | Unix timestamp of last edit. |
| `last_updated_t` | int64 | 4,290,297 (100%) | Unix timestamp of last update. |
| `states_tags` | list\<string\> | 4,290,376 (100%) | Entry completion states (`en:nutrition-facts-completed`, etc.). |
| `data_sources_tags` | list\<string\> | 3,716,740 (86.6%) | Source app (e.g., `app-yuka`, `apps`). |
| `popularity_key` | int64 | 4,290,374 (100%) | Internal popularity rank. |
| `popularity_tags` | list\<string\> | 1,309,378 (30.5%) | Popularity buckets. |
| `scans_n` | int64 | 1,315,721 (30.7%) | Number of barcode scans. |
| `unique_scans_n` | int64 | 1,315,721 (30.7%) | Unique users who scanned. |
| `rev` | int32 | 4,290,375 (100%) | Revision number. |
| `obsolete` | bool | 4,290,376 (100%) | Whether product is marked obsolete. |
| `link` | string | 81,126 (1.9%) | External product URL. |
| `schema_version` | int32 | 4,290,376 (100%) | OFF schema version. |

### Group I: Editorial / Contributors (skip)

| Column | Type | Coverage | Notes |
|--------|------|----------|-------|
| `creator` | string | 4,290,372 (100%) | Who created the entry. |
| `editors` | list\<string\> | 58,282 (1.4%) | List of editors. |
| `last_editor` | string | 4,119,397 (96.0%) | Most recent editor. |
| `last_modified_by` | string | 4,110,811 (95.8%) | Same as last_editor. |
| `correctors_tags` | list\<string\> | 2,336,993 (54.5%) | Users who corrected data. |
| `informers_tags` | list\<string\> | 4,290,376 (100%) | Users who added info. |
| `checkers_tags` | list\<string\> | 9,865 (0.2%) | Users who verified. |
| `photographers` | list\<string\> | 3,464 (0.1%) | Image uploaders. |
| `owner` | string | 121,046 (2.8%) | Data owner org (`org-kiolbassa-smoked-meats`). |
| `owner_fields` | list\<struct\> | 119,408 (2.8%) | Fields owned by producer. |

### Group J: Nutrition Detail (skip — not waste-relevant)

| Column | Type | Coverage | Notes |
|--------|------|----------|-------|
| `nutriments` | list\<struct\> | 3,357,379 (78.3%) | Full nutrition table (fat, protein, carbs, etc. per 100g/serving). |
| `nutrient_levels_tags` | list\<string\> | 1,461,832 (34.1%) | e.g., `["en:fat-in-high-quantity", "en:salt-in-low-quantity"]`. |
| `no_nutrition_data` | bool | 1,876,929 (43.7%) | Flag for products without nutrition info. |
| `nutrition_data_per` | string | 4,228,730 (98.6%) | `"100g"` or `"serving"`. |
| `serving_size` | string | 1,341,213 (31.3%) | e.g., `"1 TEA BAG (1.5 g)"`. |
| `serving_quantity` | string | 1,319,431 (30.8%) | Numeric serving size. |
| `ciqual_food_name_tags` | list\<string\> | 1,386,261 (32.3%) | French CIQUAL food database reference. |
| `minerals_tags` | list\<string\> | 156,064 (3.6%) | Mineral content tags. |
| `vitamins_tags` | list\<string\> | 157,201 (3.7%) | Vitamin content tags. |
| `nucleotides_tags` | list\<string\> | 437 (0.0%) | Negligible. |

### Group K: Misc / Low-Value (skip)

| Column | Type | Coverage | Notes |
|--------|------|----------|-------|
| `misc_tags` | list\<string\> | 4,290,363 (100%) | Internal flags (`en:nutriscore-2021-e-2023-e`, etc.). |
| `data_quality_errors_tags` | list\<string\> | 162,814 (3.8%) | Quality issues. |
| `data_quality_info_tags` | list\<string\> | 4,245,908 (99.0%) | Quality info. |
| `data_quality_warnings_tags` | list\<string\> | 4,236,924 (98.8%) | Quality warnings. |
| `entry_dates_tags` | list\<string\> | 4,290,375 (100%) | Date tags. |
| `last_edit_dates_tags` | list\<string\> | 4,290,375 (100%) | Date tags. |
| `max_imgid` | int32 | 3,313,445 (77.2%) | Highest image ID. |
| `last_image_t` | int64 | 3,314,961 (77.3%) | Last image upload time. |
| `ingredients_from_palm_oil_n` | int32 | 927,088 (21.6%) | Count. |
| `ingredients_n` | int32 | 1,235,915 (28.8%) | Count. |
| `ingredients_original_tags` | list\<string\> | 1,233,218 (28.7%) | Redundant with `ingredients_tags`. |
| `ingredients_percent_analysis` | int32 | 1,235,965 (28.8%) | Internal. |
| `ingredients_with_specified_percent_n` | int32 | 1,235,874 (28.8%) | Internal. |
| `ingredients_with_unspecified_percent_n` | int32 | 1,235,874 (28.8%) | Internal. |
| `ingredients_without_ciqual_codes_n` | int32 | 1,235,833 (28.8%) | Internal. |
| `ingredients_without_ciqual_codes` | list\<string\> | 1,094,669 (25.5%) | Internal. |
| `known_ingredients_n` | int32 | 1,235,905 (28.8%) | Internal. |
| `unknown_ingredients_n` | int32 | 1,235,941 (28.8%) | Internal. |
| `ecoscore_data` | string | 1,149,002 (26.8%) | Raw eco-score JSON. |
| `ecoscore_tags` | list\<string\> | 4,203,185 (98.0%) | Internal. |
| `nova_groups_tags` | list\<string\> | 4,244,291 (98.9%) | Redundant with `nova_group`. |
| `nova_groups` | string | 1,089,891 (25.4%) | Redundant with `nova_group`. |
| `new_additives_n` | int32 | 502 (0.0%) | Negligible. |
| `with_sweeteners` | int32 | 6,000 (0.1%) | Negligible. |
| `with_non_nutritive_sweeteners` | int32 | 31,742 (0.7%) | Negligible. |
| `unknown_nutrients_tags` | list\<string\> | 3,012 (0.1%) | Negligible. |
| `main_countries_tags` | list\<string\> | 0 (0.0%) | Empty column. |
| `categories_properties` | struct | 4,244,347 (98.9%) | CIQUAL/Agribalyse codes — internal food DB references. |

## Top 15 Countries (Parquet)

| Country | Products |
|---------|----------|
| France | 1,204,960 |
| United States | 854,693 |
| Germany | 385,542 |
| Spain | 349,853 |
| Italy | 263,110 |
| United Kingdom | 176,846 |
| Canada | 114,453 |
| Switzerland | 101,044 |
| Belgium | 95,729 |
| Ireland | 77,570 |
| World | 73,201 |
| Australia | 68,903 |
| Netherlands | 56,937 |
| Japan | 34,939 |
| Brazil | 31,736 |

## Import Manifest

Final column selection for baseline import. All 4.29M products are imported — no filtering by country or completeness. Missing fields are left NULL for the agent pipeline to fill later.

### Group A: Product Identity

| Parquet Column | Neo4j Property | Type | Coverage | Transform |
|----------------|---------------|------|----------|-----------|
| `code` | `Product.uid` | string | 100% | `sha256(code)[:16]` |
| `code` | `Product.ean` | string | 100% | as-is |
| `product_name` | `Product.name` | string | 93.6% | Multilingual extraction: `de` > `en` > `main` > first |
| `brands` | `Product.brand` | string | 62.0% | as-is |
| `quantity` | `Product.quantity` | string | 31.8% | as-is |

**Skipped:** `generic_name` (4.3% — LLM pipeline can use later), `brands_tags` (redundant), `product_quantity` / `product_quantity_unit` (derived from `quantity`)

---

## Import Recommendation (draft — being replaced by Import Manifest above)

### Strategy: Import all products, grab every relevant field, leave gaps as NULL

No filtering by country or completeness — import all 4.29M products. Missing fields are left empty for the agent pipeline to fill later from other sources. This gives us a baseline to measure gaps.

### Columns to import (31 of 111)

**Product node properties:**
- `code` → `Product.ean` + deterministic `uid`
- `product_name` → `Product.name` / `Product.name_de` (extract `de` > `en` > `main` > first)
- `generic_name` → `Product.generic_name` (same multilingual extraction)
- `brands` → `Product.brand`
- `quantity` → `Product.quantity`
- `categories_tags` → `Product.categories_tags`
- `labels_tags` → `Product.labels_tags`
- `countries_tags` → `Product.countries_tags`
- `stores_tags` → `Product.stores_tags`
- `ecoscore_grade` → `Product.ecoscore_grade`
- `nutriscore_grade` → `Product.nutriscore_grade`
- `nova_group` → `Product.nova_group`
- `ingredients_tags` → `Product.ingredients_tags`
- `ingredients_analysis_tags` → `Product.ingredients_analysis` (vegan/vegetarian/palm-oil flags)
- `allergens_tags` → `Product.allergens_tags`
- `traces_tags` → `Product.traces_tags`
- `completeness` → `Product.completeness`
- `lang` → `Product.lang`
- `images` → `Product.image_url` (construct front image URL from barcode path + first `front_{lang}` key)
- `origins_tags` → `Product.origins_tags`
- `manufacturing_places_tags` → `Product.manufacturing_places_tags`
- `scans_n` → `Product.scans_n` (popularity signal)

**PackagingComponent nodes (from `packagings[]`):**
- `material` → `MADE_OF` edge to existing `Material` node
- `shape` → `PackagingComponent.shape`
- `recycling` → `PackagingComponent.recycling_hint`
- `number_of_units` → `PackagingComponent.number_of_units`
- `weight_measured` → `PackagingComponent.weight_g`
- `quantity_per_unit` → `PackagingComponent.quantity_per_unit`

**Flat packaging fields (as fallback when `packagings` is empty):**
- `packaging_tags` → `Product.packaging_tags` (flat material tags, useful even without structured breakdown)

### Columns to skip (80 of 111)

- **Redundant with tags:** `categories`, `labels`, `stores`, `origins`, `manufacturing_places`, `packaging`, `brands_tags`
- **Nutrition detail:** `nutriments`, `nutrient_levels_tags`, `serving_size`, `serving_quantity`, etc. (not waste-relevant)
- **Internal counts:** `additives_n`, `ingredients_n`, `known_ingredients_n`, `unknown_ingredients_n`, etc.
- **Editorial:** `creator`, `editors`, `last_editor`, `correctors_tags`, `informers_tags`, `photographers`, `owner`, `owner_fields`
- **Quality flags:** `data_quality_*`, `states_tags`, `misc_tags`
- **Timestamps:** `created_t`, `last_modified_t`, `last_updated_t`, `last_image_t`
- **Internal:** `rev`, `schema_version`, `popularity_key`, `popularity_tags`, `entry_dates_tags`, `last_edit_dates_tags`
- **Empty/negligible:** `main_countries_tags` (0%), `nucleotides_tags` (0%), `new_additives_n` (0%), `with_sweeteners` (0.1%)
- **Derived/redundant packaging:** `packaging_shapes_tags`, `packaging_recycling_tags`, `packaging_text`, `packagings_complete` (all derived from `packagings`)
- **Raw JSON/ecoscore internals:** `ecoscore_data`, `ecoscore_tags`, `ecoscore_score`, `nova_groups_tags`, `nova_groups`

### Value transformations needed

1. **`product_name` / `generic_name`** — Multilingual `list<struct<lang, text>>`. Extract priority: `de` > `en` > `main` > first available.
2. **`images` → single URL** — Find first `front_{lang}` key, construct: `https://images.openfoodfacts.org/images/products/{code[:3]}/{code[3:6]}/{code[6:9]}/{code[9:]}/{key}.400.jpg`
3. **`nutriscore_grade`** — Filter out `"unknown"` (98.9% coverage is inflated — real grades are ~31%).
4. **`packaging_tags` as fallback** — When `packagings` array is empty but `packaging_tags` exists, store the flat tags on the Product node so downstream agents can still infer materials.

### Gap analysis (what's missing, where agents can fill in)

| Data point | Available products | Missing | Agent opportunity |
|---|---|---|---|
| Product name | 4.0M (93.6%) | 274K | Low priority — mostly junk entries |
| Brand | 2.7M (62.0%) | 1.6M | Could enrich from manufacturer databases |
| Categories | 1.8M (41.7%) | 2.5M | LLM classification from product name |
| Structured packaging | 400K (9.3%) | 3.9M | **Biggest gap** — LLM inference from product type + packaging_tags |
| Ingredients | 1.2M (28.7%) | 3.1M | OCR from product images (77% have images) |
| Eco-Score | 1.1M (26.8%) | 3.2M | Compute from packaging + transport data |
| Allergens | 713K (16.6%) | 3.6M | Extract from ingredients text |
| Country | 4.3M (99.5%) | 23K | Nearly complete |

## Local Files

```
data/off_dump/
├── food.parquet                       # 4.42 GB — full dataset (4.29M products)
└── openfoodfacts-products.jsonl.gz    # 2.6 GB — filtered subset (719K products)
```

## References

- [Open Food Facts /data page](https://world.openfoodfacts.org/data)
- [Hugging Face dataset](https://huggingface.co/datasets/openfoodfacts/product-database)
- [openfoodfacts-exports repo](https://github.com/openfoodfacts/openfoodfacts-exports)
- [JSONL file size reduction (Forum)](https://forum.openfoodfacts.org/t/jsonl-file-massive-file-size-reduction/2071)
- [DuckDB + OFF blog post](https://blog.openfoodfacts.org/en/news/food-transparency-in-the-palm-of-your-hand-explore-the-largest-open-food-database-using-duckdb-%F0%9F%A6%86x%F0%9F%8D%8A)
