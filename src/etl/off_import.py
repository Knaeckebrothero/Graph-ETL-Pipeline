"""
Open Food Facts bulk Parquet importer.

Loads the full OFF Parquet dump (~4.29M products) into Neo4j:
  - Product nodes (EAN-identified commercial products)
  - PackagingComponent nodes + HAS_COMPONENT edges
  - MADE_OF edges (component → existing Material)
  - Source provenance

Requires: materials.py must run first (components link to existing Material nodes).

Usage:
    python -m src.etl.off_import
    python -m src.etl.off_import --dry-run
    python -m src.etl.off_import --limit 10000
    python -m src.etl.off_import --limit 10000 --dry-run
    python -m src.etl.off_import -v
"""

import argparse
import hashlib
import logging
import time
from pathlib import Path

import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

# Default input path
DATA_FILE = Path(__file__).resolve().parent.parent.parent / "data" / "off_dump" / "food.parquet"

# Source provenance
SOURCE_NAME = "openfoodfacts-parquet"
SOURCE_TYPE = "dump"

# Columns to read from Parquet
PARQUET_COLUMNS = [
    "code",
    "product_name",
    "brands",
    "quantity",
    "categories_tags",
    "labels_tags",
    "countries_tags",
    "packaging_tags",
    "packagings",
    "images",
]

# Batch sizes
READ_CHUNK_SIZE = 50_000
NEO4J_BATCH_SIZE = 5_000
LOG_INTERVAL = 100_000

# Image URL config
IMAGE_BASE_URL = "https://images.openfoodfacts.org/images/products"
IMAGE_SIZE = "400"

# Name extraction priority
NAME_LANG_PRIORITY = ("de", "en", "main")

# Image key priority
IMAGE_KEY_PRIORITY = ("front_de", "front_en", "front")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def generate_uid(value: str) -> str:
    """Generate deterministic UID from value (SHA256[:16])."""
    return hashlib.sha256(value.encode()).hexdigest()[:16]


def _batched(items: list, size: int):
    """Yield successive batches from a list."""
    for i in range(0, len(items), size):
        yield items[i : i + size]


# ---------------------------------------------------------------------------
# Transform functions
# ---------------------------------------------------------------------------

def extract_name(product_name: list[dict] | None) -> str:
    """Extract best product name from multilingual list.

    Priority: de > en > main > first available.
    """
    if not product_name:
        return ""

    by_lang = {}
    for entry in product_name:
        if entry.get("text"):
            by_lang[entry["lang"]] = entry["text"]

    for lang in NAME_LANG_PRIORITY:
        if lang in by_lang:
            return by_lang[lang]

    # Fallback: first non-empty text
    for entry in product_name:
        if entry.get("text"):
            return entry["text"]

    return ""


def _off_image_path(code: str) -> str:
    """Convert barcode to OFF image directory path.

    Codes with 9+ digits: split as XXX/XXX/XXX/rest.
    Codes with <=8 digits: use as-is.
    """
    if len(code) <= 8:
        return code
    return "/".join([code[0:3], code[3:6], code[6:9], code[9:]])


def construct_image_url(code: str, images: list[dict] | None) -> str:
    """Construct OFF image URL from the images metadata list.

    Looks for front_de > front_en > front > any front_* key.
    """
    if not images or not code:
        return ""

    by_key = {img["key"]: img for img in images if img.get("key")}

    target = None
    for preferred in IMAGE_KEY_PRIORITY:
        if preferred in by_key:
            target = by_key[preferred]
            break

    if target is None:
        for key, img in by_key.items():
            if key.startswith("front_"):
                target = img
                break

    if target is None or target.get("rev") is None:
        return ""

    path = _off_image_path(code)
    return f"{IMAGE_BASE_URL}/{path}/{target['key']}.{target['rev']}.{IMAGE_SIZE}.jpg"


def transform_product(row: dict) -> dict | None:
    """Transform a Parquet row into a Product node entry.

    Returns None if the row has no usable barcode.
    """
    code = (row.get("code") or "").strip()
    if not code:
        return None

    return {
        "uid": generate_uid(code),
        "ean": code,
        "name": extract_name(row.get("product_name")),
        "brand": row.get("brands") or "",
        "quantity": row.get("quantity") or "",
        "categories_tags": row.get("categories_tags") or [],
        "labels_tags": row.get("labels_tags") or [],
        "countries_tags": row.get("countries_tags") or [],
        "packaging_tags": row.get("packaging_tags") or [],
        "image_url": construct_image_url(code, row.get("images")),
    }


def extract_components(code: str, packagings: list[dict] | None) -> list[dict]:
    """Extract PackagingComponent entries from the packagings array.

    Each component gets a deterministic UID based on "{code}:{index}".
    """
    if not packagings:
        return []

    product_uid = generate_uid(code)
    components = []

    for idx, pkg in enumerate(packagings):
        components.append({
            "uid": generate_uid(f"{code}:{idx}"),
            "product_uid": product_uid,
            "shape": pkg.get("shape") or "",
            "material_tag": pkg.get("material") or "",
            "recycling_hint": pkg.get("recycling") or "",
            "number_of_units": pkg.get("number_of_units") or 1,
            "weight_g": pkg.get("weight_measured"),
        })

    return components


# ---------------------------------------------------------------------------
# Cypher queries
# ---------------------------------------------------------------------------

SOURCE_CYPHER = """
MERGE (s:Source {uid: $uid})
ON CREATE SET s.name = $name, s.type = $type,
              s.import_date = datetime()
"""

PRODUCT_CYPHER = """
UNWIND $entries AS e
MERGE (p:Product {uid: e.uid})
ON CREATE SET p.ean = e.ean, p.name = e.name,
              p.brand = e.brand, p.quantity = e.quantity,
              p.categories_tags = e.categories_tags,
              p.labels_tags = e.labels_tags,
              p.countries_tags = e.countries_tags,
              p.packaging_tags = e.packaging_tags,
              p.image_url = e.image_url
ON MATCH SET  p.name = e.name, p.brand = e.brand,
              p.quantity = e.quantity,
              p.categories_tags = e.categories_tags,
              p.labels_tags = e.labels_tags,
              p.countries_tags = e.countries_tags,
              p.packaging_tags = e.packaging_tags,
              p.image_url = e.image_url
RETURN count(*) AS cnt
"""

COMPONENT_CYPHER = """
UNWIND $entries AS e
MERGE (c:PackagingComponent {uid: e.uid})
ON CREATE SET c.shape = e.shape, c.recycling_hint = e.recycling_hint,
              c.number_of_units = e.number_of_units, c.weight_g = e.weight_g
ON MATCH SET  c.shape = e.shape, c.recycling_hint = e.recycling_hint,
              c.number_of_units = e.number_of_units, c.weight_g = e.weight_g
WITH c, e
MATCH (p:Product {uid: e.product_uid})
MERGE (p)-[r:HAS_COMPONENT]->(c)
ON CREATE SET r.qty = e.number_of_units, r.weight_g = e.weight_g
RETURN count(DISTINCT c) AS comp_cnt, count(r) AS edge_cnt
"""

MADE_OF_CYPHER = """
UNWIND $links AS l
MATCH (c:PackagingComponent {uid: l.component_uid})
MATCH (m:Material {tag: l.material_tag})
MERGE (c)-[:MADE_OF]->(m)
RETURN count(*) AS cnt
"""

PROVENANCE_CYPHER = """
UNWIND $entries AS e
MATCH (p:Product {uid: e.uid})
MATCH (s:Source {uid: $source_uid})
MERGE (p)-[:DERIVED_FROM]->(s)
RETURN count(*) AS cnt
"""


# ---------------------------------------------------------------------------
# Main import
# ---------------------------------------------------------------------------

def import_off_products(
    filepath: Path = DATA_FILE,
    dry_run: bool = False,
    verbose: bool = False,
    limit: int | None = None,
) -> dict:
    """Import Open Food Facts products from Parquet dump into Neo4j.

    Streams the Parquet file in chunks, processing each chunk through
    all passes (Products, Components+HAS_COMPONENT, MADE_OF, DERIVED_FROM)
    before moving to the next chunk.

    Args:
        filepath: Path to food.parquet file.
        dry_run: If True, scan and transform without database writes.
        verbose: If True, enable debug logging.
        limit: If set, stop after importing this many products.

    Returns:
        Stats dictionary with counts and timing.
    """
    if verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    stats = {
        "total_rows_scanned": 0,
        "products_created": 0,
        "products_skipped_no_code": 0,
        "components_created": 0,
        "has_component_edges": 0,
        "made_of_edges": 0,
        "made_of_attempted": 0,
        "skipped_unknown_material": 0,
        "provenance_edges": 0,
        "dry_run": dry_run,
        "elapsed_seconds": 0.0,
    }

    t_start = time.monotonic()

    pf = pq.ParquetFile(filepath)
    logger.info(
        "Opened %s: %s rows, %s row groups",
        filepath.name,
        f"{pf.metadata.num_rows:,}",
        pf.metadata.num_row_groups,
    )

    if dry_run:
        stats = _dry_run(pf, stats, limit)
        stats["elapsed_seconds"] = time.monotonic() - t_start
        return stats

    # Real import
    from src.db import neo4j_db

    products_processed = 0

    with neo4j_db.session() as session:
        # Create Source node once
        source_uid = generate_uid(SOURCE_NAME)
        session.run(SOURCE_CYPHER, uid=source_uid, name=SOURCE_NAME, type=SOURCE_TYPE)
        logger.info("Source node: %s", SOURCE_NAME)

        for batch in pf.iter_batches(batch_size=READ_CHUNK_SIZE, columns=PARQUET_COLUMNS):
            rows = batch.to_pylist()
            stats["total_rows_scanned"] += len(rows)

            # --- Transform ---
            product_entries = []
            all_components = []

            for row in rows:
                entry = transform_product(row)
                if entry is None:
                    stats["products_skipped_no_code"] += 1
                    continue
                product_entries.append(entry)
                all_components.extend(
                    extract_components(row["code"], row.get("packagings"))
                )
                if limit and len(product_entries) + products_processed >= limit:
                    product_entries = product_entries[: limit - products_processed]
                    break

            if not product_entries:
                continue

            # --- Pass 1: Products ---
            for neo4j_batch in _batched(product_entries, NEO4J_BATCH_SIZE):
                result = session.run(PRODUCT_CYPHER, entries=neo4j_batch)
                stats["products_created"] += result.single()["cnt"]

            # --- Pass 2: Components + HAS_COMPONENT ---
            if all_components:
                for neo4j_batch in _batched(all_components, NEO4J_BATCH_SIZE):
                    result = session.run(COMPONENT_CYPHER, entries=neo4j_batch)
                    row = result.single()
                    stats["components_created"] += row["comp_cnt"]
                    stats["has_component_edges"] += row["edge_cnt"]

            # --- Pass 3: MADE_OF ---
            material_links = [
                {"component_uid": c["uid"], "material_tag": c["material_tag"]}
                for c in all_components
                if c["material_tag"]
            ]
            stats["made_of_attempted"] += len(material_links)
            if material_links:
                for neo4j_batch in _batched(material_links, NEO4J_BATCH_SIZE):
                    result = session.run(MADE_OF_CYPHER, links=neo4j_batch)
                    stats["made_of_edges"] += result.single()["cnt"]

            # --- Pass 4: DERIVED_FROM ---
            provenance = [{"uid": e["uid"]} for e in product_entries]
            for neo4j_batch in _batched(provenance, NEO4J_BATCH_SIZE):
                result = session.run(
                    PROVENANCE_CYPHER, entries=neo4j_batch, source_uid=source_uid,
                )
                stats["provenance_edges"] += result.single()["cnt"]

            # --- Progress ---
            products_processed += len(product_entries)
            if products_processed % LOG_INTERVAL < len(product_entries):
                elapsed = time.monotonic() - t_start
                rate = products_processed / elapsed if elapsed > 0 else 0
                logger.info(
                    "Progress: %s products, %s components | %.0f prod/sec | %.1f min",
                    f"{products_processed:,}",
                    f"{stats['components_created']:,}",
                    rate,
                    elapsed / 60,
                )

            if limit and products_processed >= limit:
                break

    # Final stats
    stats["skipped_unknown_material"] = stats["made_of_attempted"] - stats["made_of_edges"]
    elapsed = time.monotonic() - t_start
    stats["elapsed_seconds"] = elapsed

    if stats["skipped_unknown_material"] > 0:
        logger.warning(
            "%d components reference unknown materials",
            stats["skipped_unknown_material"],
        )

    logger.info("OFF import complete in %.1f min: %s", elapsed / 60, stats)
    return stats


def _dry_run(pf: pq.ParquetFile, stats: dict, limit: int | None) -> dict:
    """Scan Parquet and count what would be created, without touching DB."""
    products_seen = 0

    for batch in pf.iter_batches(batch_size=READ_CHUNK_SIZE, columns=PARQUET_COLUMNS):
        rows = batch.to_pylist()
        stats["total_rows_scanned"] += len(rows)

        for row in rows:
            entry = transform_product(row)
            if entry is None:
                stats["products_skipped_no_code"] += 1
                continue

            products_seen += 1
            comps = extract_components(row["code"], row.get("packagings"))
            stats["components_created"] += len(comps)
            stats["made_of_attempted"] += sum(1 for c in comps if c["material_tag"])

            if limit and products_seen >= limit:
                break

        if limit and products_seen >= limit:
            break

    stats["products_created"] = products_seen
    stats["has_component_edges"] = stats["components_created"]
    stats["provenance_edges"] = products_seen

    logger.info("DRY RUN — would create:")
    logger.info("  %s Product nodes", f"{stats['products_created']:,}")
    logger.info("  %s PackagingComponent nodes", f"{stats['components_created']:,}")
    logger.info("  %s MADE_OF edges (max)", f"{stats['made_of_attempted']:,}")
    logger.info("  %s DERIVED_FROM edges", f"{stats['provenance_edges']:,}")

    return stats


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Import Open Food Facts products from Parquet into Neo4j"
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=DATA_FILE,
        help="Path to food.parquet (default: data/off_dump/food.parquet)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Scan and transform without database writes",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose (DEBUG) output",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Stop after importing N products (for testing)",
    )
    args = parser.parse_args()

    stats = import_off_products(
        filepath=args.input,
        dry_run=args.dry_run,
        verbose=args.verbose,
        limit=args.limit,
    )

    print(f"\nOFF Import {'(DRY RUN) ' if stats['dry_run'] else ''}Results ({stats['elapsed_seconds']:.1f}s):")
    print(f"  Products:          {stats['products_created']:,}")
    print(f"  Components:        {stats['components_created']:,}")
    print(f"  HAS_COMPONENT:     {stats['has_component_edges']:,}")
    print(f"  MADE_OF (max):     {stats['made_of_attempted']:,}")
    if not stats["dry_run"]:
        print(f"  MADE_OF (actual):  {stats['made_of_edges']:,}")
    print(f"  Provenance:        {stats['provenance_edges']:,}")
    if stats["products_skipped_no_code"]:
        print(f"  Skipped (no code): {stats['products_skipped_no_code']:,}")
    if stats.get("skipped_unknown_material"):
        print(f"  Unknown materials: {stats['skipped_unknown_material']:,}")


if __name__ == "__main__":
    main()
