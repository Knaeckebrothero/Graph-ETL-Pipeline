"""
EU Ecolabel ECAT product importer.

Fetches eco-certified products from the EU Ecolabel Product Catalogue API
and imports them as Product nodes with eco-label properties.

Products are identified by EAN13 barcode, enabling cross-referencing with
Open Food Facts and other barcode-based sources via shared Product UIDs.

API docs: https://apps.data.env.service.ec.europa.eu/dataquery/docs

Usage:
    python -m src.etl.ecolabel                           # Fetch from API + import
    python -m src.etl.ecolabel --dry-run                 # Preview without DB changes
    python -m src.etl.ecolabel --input sources/raw/eu-ecolabel/ecolabel.json  # Import from local file
    python -m src.etl.ecolabel --fetch-only --output sources/raw/eu-ecolabel/ecolabel.json  # Download only
    python -m src.etl.ecolabel -v                        # Verbose output
"""

import argparse
import hashlib
import json
import logging
import time
from pathlib import Path

import requests

logger = logging.getLogger(__name__)

BATCH_SIZE = 500
SOURCE_NAME = "eu-ecolabel-ecat"
SOURCE_TYPE = "open-data"
SOURCE_URL = "https://data.europa.eu/data/datasets/eu-ecolabel-products"

# EU Ecolabel Data Query API v2
API_BASE = "https://apps.data.env.service.ec.europa.eu/dataquery/v2/ecolabel/products"
API_FIELDS = "ean13,product_name,group_name,group_category_name,licence_holder,licence_holder_country"
API_PAGE_SIZE = 1000  # max allowed by API
API_DELAY = 0.5  # seconds between requests (be polite)


def generate_uid(value: str) -> str:
    """Generate deterministic UID from value (SHA256[:16])."""
    return hashlib.sha256(value.encode()).hexdigest()[:16]


def _batched(items: list, size: int):
    """Yield successive batches from a list."""
    for i in range(0, len(items), size):
        yield items[i : i + size]


def fetch_ecolabel_products(output: Path | None = None) -> list[dict]:
    """Fetch all products with EAN13 from the EU Ecolabel API.

    Paginates through the full dataset, filtering for non-null EAN13.

    Args:
        output: If provided, save fetched data as JSON to this path.

    Returns:
        List of product dicts with API field names.
    """
    session = requests.Session()
    session.headers.update({"Accept": "application/json"})

    all_products = []
    offset = 0

    logger.info("Fetching EU Ecolabel products from API (EAN13 only)...")

    while True:
        params = {
            "fields": API_FIELDS,
            "ean13__isnull": "false",
            "limit": API_PAGE_SIZE,
            "offset": offset,
            "order_by": "licence_number",
        }

        try:
            response = session.get(API_BASE, params=params, timeout=30)
            response.raise_for_status()
        except requests.RequestException as e:
            logger.error("API request failed at offset %d: %s", offset, e)
            break

        data = response.json()
        products = data.get("data", [])
        meta = data.get("meta", {})
        total = meta.get("total", 0)

        if not products:
            break

        all_products.extend(products)
        offset += len(products)

        logger.info(
            "  Fetched %d / %d products (%.0f%%)",
            len(all_products),
            total,
            (len(all_products) / total * 100) if total else 0,
        )

        if offset >= total:
            break

        time.sleep(API_DELAY)

    session.close()
    logger.info("Fetched %d products with EAN13 barcodes", len(all_products))

    if output:
        output.parent.mkdir(parents=True, exist_ok=True)
        with open(output, "w") as f:
            json.dump(all_products, f, indent=2, ensure_ascii=False)
        logger.info("Saved to %s", output)

    return all_products


def import_ecolabel(
    filepath: Path | None = None,
    products: list[dict] | None = None,
    dry_run: bool = False,
    verbose: bool = False,
) -> dict:
    """Import EU Ecolabel products into Neo4j.

    Creates or enriches Product nodes with eco-label properties.
    Uses EAN-based UIDs for automatic cross-referencing with other sources.

    Args:
        filepath: Path to JSON file with fetched ecolabel data.
        products: Pre-fetched list of product dicts (alternative to filepath).
        dry_run: If True, log what would happen without DB changes.
        verbose: If True, enable debug logging.

    Returns:
        Stats dictionary with counts.
    """
    if verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    if products is None:
        if filepath is None:
            raise ValueError("Either filepath or products must be provided")
        with open(filepath) as f:
            products = json.load(f)
        logger.info("Loaded %d products from %s", len(products), filepath.name)

    stats = {
        "products_loaded": len(products),
        "products_merged": 0,
        "provenance_edges": 0,
        "skipped_no_ean": 0,
        "skipped_invalid_ean": 0,
        "dry_run": dry_run,
    }

    # Filter and prepare entries
    entries = []
    seen_eans = set()
    for p in products:
        ean = (p.get("ean13") or "").strip()
        if not ean:
            stats["skipped_no_ean"] += 1
            continue
        if not ean.isdigit() or len(ean) < 8:
            stats["skipped_invalid_ean"] += 1
            logger.debug("Invalid EAN: %s (%s)", ean, p.get("product_name", ""))
            continue
        if ean in seen_eans:
            continue  # deduplicate
        seen_eans.add(ean)

        entries.append(
            {
                "uid": generate_uid(ean),
                "ean": ean,
                "name": (p.get("product_name") or "").strip(),
                "product_group": (p.get("group_name") or "").strip(),
                "category": (p.get("group_category_name") or "").strip(),
                "licence_holder": (p.get("licence_holder") or "").strip(),
                "country": (p.get("licence_holder_country") or "").strip(),
            }
        )

    logger.info(
        "Prepared %d unique products (%d skipped: %d no EAN, %d invalid EAN)",
        len(entries),
        stats["skipped_no_ean"] + stats["skipped_invalid_ean"],
        stats["skipped_no_ean"],
        stats["skipped_invalid_ean"],
    )

    if dry_run:
        stats["products_merged"] = len(entries)
        stats["provenance_edges"] = len(entries)

        logger.info("DRY RUN — would create/enrich:")
        logger.info("  %d Product nodes", stats["products_merged"])
        logger.info("  %d DERIVED_FROM provenance edges", stats["provenance_edges"])

        # Show category distribution
        cats: dict[str, int] = {}
        for e in entries:
            cat = e["product_group"] or "(unknown)"
            cats[cat] = cats.get(cat, 0) + 1
        logger.info("Product groups:")
        for cat, count in sorted(cats.items(), key=lambda x: -x[1])[:15]:
            logger.info("    %s: %d", cat, count)

        return stats

    # Real import
    from src.db import neo4j_db

    with neo4j_db.session() as session:
        # Pass 1: Create Source node
        source_uid = generate_uid(SOURCE_NAME)
        session.run(
            """
            MERGE (s:Source {uid: $uid})
            ON CREATE SET s.name = $name, s.type = $type,
                          s.url = $url, s.import_date = datetime()
            ON MATCH SET s.last_import = datetime()
            """,
            uid=source_uid,
            name=SOURCE_NAME,
            type=SOURCE_TYPE,
            url=SOURCE_URL,
        )
        logger.info("Source node: %s", SOURCE_NAME)

        # Pass 2: MERGE Product nodes with eco-label properties
        for batch in _batched(entries, BATCH_SIZE):
            result = session.run(
                """
                UNWIND $entries AS e
                MERGE (p:Product {uid: e.uid})
                ON CREATE SET p.ean = e.ean,
                              p.name = e.name,
                              p.eco_label = "EU Ecolabel",
                              p.ecolabel_product_group = e.product_group,
                              p.ecolabel_category = e.category,
                              p.ecolabel_holder = e.licence_holder,
                              p.ecolabel_country = e.country,
                              p.source_api = "eu-ecolabel",
                              p.created_at = datetime()
                ON MATCH SET  p.eco_label = "EU Ecolabel",
                              p.ecolabel_product_group = e.product_group,
                              p.ecolabel_category = e.category,
                              p.ecolabel_holder = e.licence_holder,
                              p.ecolabel_country = e.country,
                              p.updated_at = datetime()
                RETURN count(*) AS cnt
                """,
                entries=batch,
            )
            stats["products_merged"] += result.single()["cnt"]
        logger.info("Merged %d Product nodes", stats["products_merged"])

        # Pass 3: Provenance edges
        provenance_entries = [{"uid": e["uid"]} for e in entries]
        for batch in _batched(provenance_entries, BATCH_SIZE):
            result = session.run(
                """
                UNWIND $entries AS e
                MATCH (p:Product {uid: e.uid})
                MATCH (s:Source {uid: $source_uid})
                MERGE (p)-[:DERIVED_FROM]->(s)
                RETURN count(*) AS cnt
                """,
                entries=batch,
                source_uid=source_uid,
            )
            stats["provenance_edges"] += result.single()["cnt"]
        logger.info(
            "Linked %d products to source: %s",
            stats["provenance_edges"],
            SOURCE_NAME,
        )

    logger.info("EU Ecolabel import complete: %s", stats)
    return stats


def main():
    parser = argparse.ArgumentParser(
        description="Import EU Ecolabel products into Neo4j"
    )
    parser.add_argument(
        "--input",
        type=Path,
        help="Import from local JSON file (skip API fetch)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Save fetched data to JSON file",
    )
    parser.add_argument(
        "--fetch-only",
        action="store_true",
        help="Only fetch data, do not import to Neo4j (requires --output)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview import without database changes",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="Enable verbose output",
    )
    args = parser.parse_args()

    if args.fetch_only and not args.output:
        parser.error("--fetch-only requires --output")

    if args.input:
        # Import from local file
        stats = import_ecolabel(
            filepath=args.input,
            dry_run=args.dry_run,
            verbose=args.verbose,
        )
    else:
        # Fetch from API
        products = fetch_ecolabel_products(output=args.output)

        if args.fetch_only:
            print(f"\nFetched {len(products)} products → {args.output}")
            return

        # Import fetched data
        stats = import_ecolabel(
            products=products,
            dry_run=args.dry_run,
            verbose=args.verbose,
        )

    if not args.dry_run:
        print(f"\nEU Ecolabel import results:")
        print(f"  Products loaded:   {stats['products_loaded']}")
        print(f"  Products merged:   {stats['products_merged']}")
        print(f"  Provenance edges:  {stats['provenance_edges']}")
        if stats["skipped_no_ean"]:
            print(f"  Skipped (no EAN):  {stats['skipped_no_ean']}")
        if stats["skipped_invalid_ean"]:
            print(f"  Skipped (invalid): {stats['skipped_invalid_ean']}")


if __name__ == "__main__":
    main()
