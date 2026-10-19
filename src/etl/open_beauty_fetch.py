"""
Open Beauty Facts bulk fetch script.

Downloads the OBF data dump (CSV) and converts products with barcodes
into ProductInfo JSON format compatible with src/etl/products.py.

Open Beauty Facts covers ~13K cosmetics/personal care products with
ingredient lists useful for hazmat classification.

API/dump: https://world.openbeautyfacts.org/data

Usage:
    python -m src.etl.open_beauty_fetch --output sources/raw/open-beauty-facts/obf_products.json
    python -m src.etl.open_beauty_fetch --output sources/raw/open-beauty-facts/obf_products.json -v
"""

import argparse
import csv
import gzip
import io
import json
import logging
import tempfile
from pathlib import Path

import requests

logger = logging.getLogger(__name__)

OBF_CSV_URL = "https://world.openbeautyfacts.org/data/en.openbeautyfacts.org.products.csv.gz"
USER_AGENT = "FessiWasteBot/1.0 (https://github.com/fessi-chatbot)"


def fetch_obf_products(output: Path, verbose: bool = False) -> int:
    """Download OBF CSV dump and convert to ProductInfo JSON.

    Filters for products with non-empty barcodes and at least a product name.
    Outputs a JSON array compatible with `python -m src.etl.products --input`.

    Args:
        output: Path for the output JSON file.
        verbose: Enable debug logging.

    Returns:
        Number of products written.
    """
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    logger.info("Downloading Open Beauty Facts CSV dump...")
    logger.info("  URL: %s", OBF_CSV_URL)

    try:
        response = session.get(OBF_CSV_URL, stream=True, timeout=120)
        response.raise_for_status()
    except requests.RequestException as e:
        logger.error("Failed to download OBF dump: %s", e)
        return 0

    # Download to temp file first (can be ~50MB compressed)
    content_length = response.headers.get("Content-Length")
    if content_length:
        logger.info("  Download size: %.1f MB", int(content_length) / 1024 / 1024)

    with tempfile.NamedTemporaryFile(suffix=".csv.gz", delete=False) as tmp:
        downloaded = 0
        for chunk in response.iter_content(chunk_size=8192):
            tmp.write(chunk)
            downloaded += len(chunk)
        tmp_path = tmp.name

    logger.info("  Downloaded %.1f MB → %s", downloaded / 1024 / 1024, tmp_path)
    session.close()

    # Parse CSV from gzip
    products = []
    skipped_no_barcode = 0
    skipped_no_name = 0

    logger.info("Parsing OBF CSV dump...")

    with gzip.open(tmp_path, "rt", encoding="utf-8", errors="replace") as gz:
        reader = csv.DictReader(gz, delimiter="\t")

        # Log available columns
        if reader.fieldnames:
            logger.debug("CSV columns (%d): %s", len(reader.fieldnames), reader.fieldnames[:20])

        for row in reader:
            barcode = (row.get("code") or "").strip()
            if not barcode or not barcode.isdigit():
                skipped_no_barcode += 1
                continue

            product_name = (row.get("product_name") or "").strip()
            if not product_name:
                skipped_no_name += 1
                continue

            # Build ProductInfo-compatible dict
            categories_tags_raw = row.get("categories_tags") or ""
            packaging_tags_raw = row.get("packaging_tags") or ""
            packaging_materials_raw = row.get("packaging_materials_tags") or ""

            product = {
                "barcode": barcode,
                "found": True,
                "source_api": "openbeautyfacts",
                "product_name": product_name,
                "product_name_de": (row.get("product_name_de") or "").strip(),
                "brands": (row.get("brands") or "").strip(),
                "quantity": (row.get("quantity") or "").strip(),
                "categories": (row.get("categories") or "").strip(),
                "categories_tags": [
                    t.strip()
                    for t in categories_tags_raw.split(",")
                    if t.strip()
                ],
                "packaging": (row.get("packaging") or "").strip(),
                "packaging_tags": [
                    t.strip()
                    for t in packaging_tags_raw.split(",")
                    if t.strip()
                ],
                "packaging_materials_tags": [
                    t.strip()
                    for t in packaging_materials_raw.split(",")
                    if t.strip()
                ],
                "packaging_components": [],
                "labels": (row.get("labels") or "").strip(),
                "image_url": (row.get("image_url") or "").strip(),
            }
            products.append(product)

    # Clean up temp file
    Path(tmp_path).unlink(missing_ok=True)

    logger.info(
        "Parsed %d products (%d skipped: %d no barcode, %d no name)",
        len(products),
        skipped_no_barcode + skipped_no_name,
        skipped_no_barcode,
        skipped_no_name,
    )

    # Write output
    output.parent.mkdir(parents=True, exist_ok=True)
    with open(output, "w") as f:
        json.dump(products, f, indent=2, ensure_ascii=False)

    logger.info("Wrote %d products → %s", len(products), output)
    return len(products)


def main():
    parser = argparse.ArgumentParser(
        description="Fetch Open Beauty Facts data and convert to ProductInfo JSON"
    )
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        default=Path("sources/raw/open-beauty-facts/obf_products.json"),
        help="Output JSON file path (default: sources/raw/open-beauty-facts/obf_products.json)",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose output",
    )
    args = parser.parse_args()

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    count = fetch_obf_products(output=args.output, verbose=args.verbose)
    print(f"\nOpen Beauty Facts fetch complete: {count} products → {args.output}")


if __name__ == "__main__":
    main()
