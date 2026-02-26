"""
opengtindb.org bulk fetch script.

Queries the opengtindb.org API for a list of EAN barcodes and saves
results as ProductInfo JSON compatible with src/etl/products.py.

Requires OPENGTINDB_QUERY_ID environment variable (free registration).
Rate-limited to ~1 request/second.

Usage:
    # From a text file (one EAN per line)
    python -m src.etl.opengtindb_fetch --eans data/ean_list.txt --output data/opengtindb_products.json

    # From Product nodes in the graph that are missing names
    python -m src.etl.opengtindb_fetch --from-graph --output data/opengtindb_products.json

    # Limit number of lookups (for testing or rate limit budget)
    python -m src.etl.opengtindb_fetch --eans data/ean_list.txt --limit 50 --output data/out.json
"""

import argparse
import json
import logging
from pathlib import Path

from src.barcode.opengtindb import OpenGTINDBClient

logger = logging.getLogger(__name__)


def load_eans_from_file(filepath: Path) -> list[str]:
    """Load EAN barcodes from a text file (one per line)."""
    eans = []
    with open(filepath) as f:
        for line in f:
            ean = line.strip()
            if ean and ean.isdigit():
                eans.append(ean)
    logger.info("Loaded %d EANs from %s", len(eans), filepath)
    return eans


def load_eans_from_graph() -> list[str]:
    """Load EANs from Product nodes in Neo4j that are missing names."""
    from src.db import neo4j_db

    results = neo4j_db.query(
        """
        MATCH (p:Product)
        WHERE (p.name IS NULL OR p.name = '')
          AND p.ean IS NOT NULL AND p.ean <> ''
        RETURN p.ean AS ean
        """
    )
    eans = [r["ean"] for r in results]
    logger.info("Found %d Product nodes missing names in graph", len(eans))
    return eans


def fetch_opengtindb(
    eans: list[str],
    output: Path,
    limit: int | None = None,
) -> int:
    """Fetch product data from opengtindb for a list of EANs.

    Args:
        eans: List of EAN barcode strings to look up.
        output: Path for the output JSON file.
        limit: Maximum number of lookups (None = all).

    Returns:
        Number of products found and written.
    """
    client = OpenGTINDBClient()

    if not client.query_id:
        logger.error(
            "OPENGTINDB_QUERY_ID not set. Register at https://opengtindb.org/userid.php"
        )
        return 0

    if limit:
        eans = eans[:limit]

    products = []
    not_found = 0

    logger.info("Looking up %d EANs on opengtindb.org (~1/sec)...", len(eans))

    for i, ean in enumerate(eans, 1):
        product = client.lookup(ean)

        if product.found:
            products.append(product.model_dump())
            logger.debug(
                "  [%d/%d] %s → %s (%s)",
                i,
                len(eans),
                ean,
                product.product_name,
                product.brands,
            )
        else:
            not_found += 1
            logger.debug("  [%d/%d] %s → not found", i, len(eans), ean)

        if i % 100 == 0:
            logger.info(
                "  Progress: %d/%d (found: %d, not found: %d)",
                i,
                len(eans),
                len(products),
                not_found,
            )

    client.close()

    logger.info(
        "Lookup complete: %d found, %d not found (of %d total)",
        len(products),
        not_found,
        len(eans),
    )

    # Write output
    output.parent.mkdir(parents=True, exist_ok=True)
    with open(output, "w") as f:
        json.dump(products, f, indent=2, ensure_ascii=False)

    logger.info("Wrote %d products → %s", len(products), output)
    return len(products)


def main():
    parser = argparse.ArgumentParser(
        description="Bulk fetch product data from opengtindb.org"
    )
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument(
        "--eans",
        type=Path,
        help="Text file with EAN barcodes (one per line)",
    )
    source.add_argument(
        "--from-graph",
        action="store_true",
        help="Look up Product nodes in Neo4j that are missing names",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        default=Path("data/opengtindb_products.json"),
        help="Output JSON file (default: data/opengtindb_products.json)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Maximum number of lookups",
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

    if args.from_graph:
        eans = load_eans_from_graph()
    else:
        eans = load_eans_from_file(args.eans)

    if not eans:
        print("No EANs to look up.")
        return

    count = fetch_opengtindb(eans=eans, output=args.output, limit=args.limit)
    print(f"\nopengtindb fetch complete: {count} products → {args.output}")


if __name__ == "__main__":
    main()
