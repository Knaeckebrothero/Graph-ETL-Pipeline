# Barcode Scanner & Product Lookup

Alternative product identification for the Fessi waste disposal chatbot. Scan a barcode (from an image or by number) and get structured product data back — including packaging materials, which are the key signal for routing to the correct waste stream.

## Setup

```bash
# Python dependencies
pip install pyzbar Pillow requests

# System dependency for image scanning (not needed for number-only lookups)
# Debian/Ubuntu:
sudo apt install libzbar0
# macOS:
brew install zbar
# Fedora:
sudo dnf install zbar
```

## CLI Usage

```bash
# Look up by barcode number
python -m src.barcode.lookup --number 3017620422003

# Scan barcode from an image
python -m src.barcode.lookup --image path/to/barcode.jpg

# Save result to file
python -m src.barcode.lookup --number 3017620422003 --output result.json

# Verbose logging
python -m src.barcode.lookup --number 3017620422003 -v
```

Exit codes: `0` = product found, `1` = not found or error.

## Programmatic Usage

```python
from src.barcode import BarcodeResolver

resolver = BarcodeResolver()
product = resolver.lookup("3017620422003")

if product.found:
    print(product.product_name)            # "Nutella"
    print(product.brands)                  # "Ferrero,Nutella"
    print(product.packaging_materials_tags) # ["en:clear-glass", "en:pp-5-polypropylene", ...]
    print(product.quantity)                # "400 g"

resolver.close()
```

For image scanning:

```python
from src.barcode import BarcodeScanner, BarcodeResolver

scanner = BarcodeScanner()
scan = scanner.scan("photo.jpg")

if scan.has_barcodes:
    resolver = BarcodeResolver()
    product = resolver.lookup(scan.barcodes[0])
    resolver.close()
```

## Output Structure

The `ProductInfo` model returned by `resolver.lookup()`:

| Field | Type | Description |
|-------|------|-------------|
| `barcode` | `str` | EAN/UPC number |
| `found` | `bool` | Whether the API had data for this barcode |
| `source_api` | `str` | API that provided the data (`"openfoodfacts"`, `"openbeautyfacts"`, or `"upcitemdb"`) |
| `product_name` | `str` | Product name (best available language) |
| `product_name_de` | `str` | German product name, if available |
| `brands` | `str` | Brand name(s) |
| `quantity` | `str` | e.g. `"400 g"`, `"1.5 L"` |
| `categories` | `str` | Product categories (comma-separated) |
| `categories_tags` | `list[str]` | Normalized category tags |
| `packaging` | `str` | Packaging description (free text) |
| `packaging_tags` | `list[str]` | Normalized packaging tags |
| `packaging_materials_tags` | `list[str]` | Material codes — most useful for waste routing |
| `ecoscore_grade` | `str` | Eco-Score (a-e) |
| `nutriscore_grade` | `str` | Nutri-Score (a-e) |
| `labels` | `str` | Product labels (e.g. `"Vegetarian, Bio"`) |
| `image_url` | `str` | Product image URL |
| `lookup_timestamp` | `str` | ISO 8601 timestamp of the lookup |

## Packaging Materials → Waste Streams

The `packaging_materials_tags` field maps directly to Frankfurt FES waste streams:

| Material Tag | Waste Stream | Bin |
|-------------|-------------|-----|
| `en:clear-glass`, `en:green-glass`, `en:brown-glass` | Altglascontainer | Glass container |
| `en:pp-5-polypropylene`, `en:pet-1-*`, `en:hdpe-2-*` | Verpackungstonne | Yellow lid |
| `en:non-corrugated-cardboard`, `en:corrugated-cardboard` | Altpapiertonne | Blue/green lid |
| `en:steel`, `en:aluminium` | Verpackungstonne | Yellow lid |
| `xx:82-c-pap` (composites) | Verpackungstonne | Yellow lid |

## APIs

The resolver uses a **3-step lookup chain** (stops at first hit):

| Step | API | Coverage | Data Depth | Rate Limit |
|------|-----|----------|------------|------------|
| 1 | [Open Food Facts](https://openfoodfacts.github.io/openfoodfacts-server/api/) | ~4M food products | Full (name, packaging, materials, eco-score) | ~100 req/min |
| 2 | [Open Beauty Facts](https://world.openbeautyfacts.org/) | ~13K cosmetics/personal care | Full (same schema as OFF) | ~100 req/min |
| 3 | [UPCitemdb](https://www.upcitemdb.com/wp/docs/main/development/responses/) | 686M all-category barcodes | Minimal (name, brand, category only) | 100 req/day |

All three are free and require no API key.

### opengtindb.org (optional enrichment)

The `OpenGTINDBClient` provides access to a German EAN database with ~50K products, German product names, and bitwise packaging/contents flags. Requires registration at [opengtindb.org](https://opengtindb.org/userid.php) (35 EUR donation) and is limited to 500 requests/day.

```bash
# Set in .env
OPENGTINDB_QUERY_ID=your_query_id
```

```python
from src.barcode import OpenGTINDBClient

client = OpenGTINDBClient()  # reads OPENGTINDB_QUERY_ID from env
product = client.lookup("4004675064107")
```

## Module Structure

```
src/barcode/
    __init__.py    # Package exports: BarcodeResolver, BarcodeScanner, ProductInfo, ...
    models.py      # Pydantic data models
    scanner.py     # Image → barcode number (pyzbar + Pillow)
    resolver.py    # Barcode number → product data (OFF → OBF → UPCitemdb)
    opengtindb.py  # opengtindb.org API client (optional enrichment)
    lookup.py      # CLI entry point
```
