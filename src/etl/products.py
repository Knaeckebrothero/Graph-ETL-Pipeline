"""
Product importer ETL module.

Loads product data (from BarcodeResolver JSON dumps) into Neo4j:
  - Product nodes (EAN-identified commercial products)
  - PackagingComponent nodes + HAS_COMPONENT edges
  - MADE_OF edges (component → existing Material)
  - Source provenance

Requires: materials.py must run first (components link to existing Material nodes).

Usage:
    python -m src.etl.products --input data/products_sample.json
    python -m src.etl.products --input data/products_sample.json --dry-run
    python -m src.etl.products --input data/products_sample.json -v
"""

import argparse
import hashlib
import json
import logging
from pathlib import Path

from src.barcode.models import ProductInfo

logger = logging.getLogger(__name__)

BATCH_SIZE = 500
SOURCE_NAME = "openfoodfacts"
SOURCE_TYPE = "api"


def generate_uid(value: str) -> str:
    """Generate deterministic UID from value (SHA256[:16])."""
    return hashlib.sha256(value.encode()).hexdigest()[:16]


def _batched(items: list, size: int):
    """Yield successive batches from a list."""
    for i in range(0, len(items), size):
        yield items[i : i + size]


def load_products(filepath: Path) -> list[ProductInfo]:
    """Load product definitions from JSON file.

    Expects an array of serialized ProductInfo objects.
    Only products with found=True are returned.
    """
    with open(filepath) as f:
        data = json.load(f)

    products = [ProductInfo(**item) for item in data]
    found = [p for p in products if p.found]
    logger.info(
        "Loaded %d products from %s (%d found, %d skipped)",
        len(products),
        filepath.name,
        len(found),
        len(products) - len(found),
    )
    return found


def import_products(
    filepath: Path | None = None,
    products: list[ProductInfo] | None = None,
    dry_run: bool = False,
    verbose: bool = False,
) -> dict:
    """Import products into Neo4j.

    Creates Product nodes, PackagingComponent nodes, and edges for
    HAS_COMPONENT, MADE_OF, and DERIVED_FROM relationships.

    Args:
        filepath: Path to JSON file with ProductInfo array.
        products: Pre-loaded list of ProductInfo (alternative to filepath).
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
        products = load_products(filepath)
    else:
        products = [p for p in products if p.found]
        logger.info("Received %d found products programmatically", len(products))

    stats = {
        "products_loaded": len(products),
        "products_created": 0,
        "components_created": 0,
        "has_component_edges": 0,
        "made_of_edges": 0,
        "provenance_edges": 0,
        "skipped_no_barcode": 0,
        "skipped_unknown_material": 0,
        "dry_run": dry_run,
    }

    # Filter out products with empty barcodes
    valid = []
    for p in products:
        if not p.barcode:
            stats["skipped_no_barcode"] += 1
            logger.warning("Skipping product with empty barcode: %s", p.product_name)
        else:
            valid.append(p)

    if dry_run:
        stats["products_created"] = len(valid)
        total_components = sum(len(p.packaging_components) for p in valid)
        stats["components_created"] = total_components
        stats["has_component_edges"] = total_components
        stats["made_of_edges"] = sum(
            1
            for p in valid
            for c in p.packaging_components
            if c.material
        )
        stats["provenance_edges"] = len(valid)

        logger.info("DRY RUN — would create:")
        logger.info("  %d Product nodes", stats["products_created"])
        logger.info("  %d PackagingComponent nodes", stats["components_created"])
        logger.info("  %d HAS_COMPONENT edges", stats["has_component_edges"])
        logger.info("  %d MADE_OF edges (max)", stats["made_of_edges"])
        logger.info("  %d DERIVED_FROM provenance edges", stats["provenance_edges"])

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
                          s.import_date = datetime()
            """,
            uid=source_uid,
            name=SOURCE_NAME,
            type=SOURCE_TYPE,
        )
        logger.info("Source node: %s", SOURCE_NAME)

        # Pass 2: Create Product nodes (batched)
        product_entries = [
            {
                "uid": generate_uid(p.barcode),
                "ean": p.barcode,
                "name": p.product_name or p.product_name_de,
                "name_de": p.product_name_de,
                "brand": p.brands,
                "quantity": p.quantity,
                "categories_tags": p.categories_tags,
                "image_url": p.image_url,
                "source_api": p.source_api,
            }
            for p in valid
        ]

        for batch in _batched(product_entries, BATCH_SIZE):
            result = session.run(
                """
                UNWIND $entries AS e
                MERGE (p:Product {uid: e.uid})
                ON CREATE SET p.ean = e.ean, p.name = e.name, p.name_de = e.name_de,
                              p.brand = e.brand, p.quantity = e.quantity,
                              p.categories_tags = e.categories_tags,
                              p.image_url = e.image_url, p.source_api = e.source_api
                ON MATCH SET  p.name = e.name, p.name_de = e.name_de,
                              p.brand = e.brand, p.quantity = e.quantity,
                              p.categories_tags = e.categories_tags,
                              p.image_url = e.image_url
                RETURN count(*) AS cnt
                """,
                entries=batch,
            )
            stats["products_created"] += result.single()["cnt"]
        logger.info("Created/updated %d Product nodes", stats["products_created"])

        # Pass 3: Create PackagingComponent nodes + HAS_COMPONENT edges (batched)
        component_entries = []
        for p in valid:
            for idx, comp in enumerate(p.packaging_components):
                component_entries.append(
                    {
                        "uid": generate_uid(f"{p.barcode}:{idx}"),
                        "product_uid": generate_uid(p.barcode),
                        "shape": comp.shape,
                        "material_tag": comp.material,
                        "recycling_hint": comp.recycling,
                        "number_of_units": comp.number_of_units,
                        "weight_g": comp.weight_measured,
                        "food_contact": comp.food_contact,
                    }
                )

        for batch in _batched(component_entries, BATCH_SIZE):
            result = session.run(
                """
                UNWIND $entries AS e
                MERGE (c:PackagingComponent {uid: e.uid})
                ON CREATE SET c.shape = e.shape, c.recycling_hint = e.recycling_hint,
                              c.number_of_units = e.number_of_units,
                              c.weight_g = e.weight_g, c.food_contact = e.food_contact
                ON MATCH SET  c.shape = e.shape, c.recycling_hint = e.recycling_hint,
                              c.number_of_units = e.number_of_units,
                              c.weight_g = e.weight_g, c.food_contact = e.food_contact
                WITH c, e
                MATCH (p:Product {uid: e.product_uid})
                MERGE (p)-[r:HAS_COMPONENT]->(c)
                ON CREATE SET r.qty = e.number_of_units, r.weight_g = e.weight_g
                RETURN count(DISTINCT c) AS comp_cnt, count(r) AS edge_cnt
                """,
                entries=batch,
            )
            row = result.single()
            stats["components_created"] += row["comp_cnt"]
            stats["has_component_edges"] += row["edge_cnt"]
        logger.info(
            "Created %d PackagingComponent nodes, %d HAS_COMPONENT edges",
            stats["components_created"],
            stats["has_component_edges"],
        )

        # Pass 4: Create MADE_OF edges (component → existing Material)
        material_links = [
            {
                "component_uid": e["uid"],
                "material_tag": e["material_tag"],
            }
            for e in component_entries
            if e["material_tag"]
        ]

        for batch in _batched(material_links, BATCH_SIZE):
            result = session.run(
                """
                UNWIND $links AS l
                MATCH (c:PackagingComponent {uid: l.component_uid})
                MATCH (m:Material {tag: l.material_tag})
                MERGE (c)-[:MADE_OF]->(m)
                RETURN count(*) AS cnt
                """,
                links=batch,
            )
            stats["made_of_edges"] += result.single()["cnt"]

        # Count unknown materials (links attempted minus edges created)
        attempted = len(material_links)
        stats["skipped_unknown_material"] = attempted - stats["made_of_edges"]
        if stats["skipped_unknown_material"] > 0:
            logger.warning(
                "%d components reference unknown materials (no matching Material node)",
                stats["skipped_unknown_material"],
            )
        logger.info("Created %d MADE_OF edges", stats["made_of_edges"])

        # Pass 5: Create DERIVED_FROM provenance edges (batched)
        provenance_entries = [{"uid": generate_uid(p.barcode)} for p in valid]

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
        logger.info("Linked %d products to source: %s", stats["provenance_edges"], SOURCE_NAME)

    logger.info("Product import complete: %s", stats)
    return stats


def main():
    parser = argparse.ArgumentParser(
        description="Import products into Neo4j from BarcodeResolver JSON"
    )
    parser.add_argument(
        "--input",
        type=Path,
        required=True,
        help="Path to JSON file with ProductInfo array",
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

    stats = import_products(
        filepath=args.input,
        dry_run=args.dry_run,
        verbose=args.verbose,
    )

    if not args.dry_run:
        print(f"\nProduct import results:")
        print(f"  Products:          {stats['products_created']}")
        print(f"  Components:        {stats['components_created']}")
        print(f"  HAS_COMPONENT:     {stats['has_component_edges']}")
        print(f"  MADE_OF:           {stats['made_of_edges']}")
        print(f"  Provenance:        {stats['provenance_edges']}")
        if stats["skipped_no_barcode"]:
            print(f"  Skipped (no EAN):  {stats['skipped_no_barcode']}")
        if stats["skipped_unknown_material"]:
            print(f"  Unknown materials: {stats['skipped_unknown_material']}")


if __name__ == "__main__":
    main()
