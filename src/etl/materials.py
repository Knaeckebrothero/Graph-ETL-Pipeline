"""
Material taxonomy importer.

Loads material reference data from data/materials.json into Neo4j:
  - Material nodes (~85 tags from Open Food Facts)
  - IS_A hierarchy edges (e.g., en:pp-5-polypropylene IS_A en:plastic)
  - DEFAULT_STREAM edges (each material → its WasteStream)
  - Source provenance

Usage:
    python -m src.etl.materials              # Import materials
    python -m src.etl.materials --dry-run    # Preview without changes
    python -m src.etl.materials -v           # Verbose output
"""

import argparse
import hashlib
import json
import logging
import sys
from pathlib import Path

logger = logging.getLogger(__name__)

DATA_FILE = Path(__file__).resolve().parent.parent.parent / "data" / "materials.json"
SOURCE_NAME = "off-packaging-materials"
SOURCE_TYPE = "reference"


def generate_uid(tag: str) -> str:
    """Generate deterministic UID from material tag."""
    return hashlib.sha256(tag.encode()).hexdigest()[:16]


def load_materials(filepath: Path = DATA_FILE) -> list[dict]:
    """Load material definitions from JSON seed file."""
    with open(filepath) as f:
        data = json.load(f)
    materials = data["materials"]
    logger.info("Loaded %d material definitions from %s", len(materials), filepath.name)
    return materials


def import_materials(
    filepath: Path = DATA_FILE,
    dry_run: bool = False,
    verbose: bool = False,
) -> dict:
    """Import material taxonomy into Neo4j.

    Creates Material nodes, IS_A hierarchy, and DEFAULT_STREAM edges.

    Args:
        filepath: Path to materials.json seed file.
        dry_run: If True, log what would happen without DB changes.
        verbose: If True, enable debug logging.

    Returns:
        Stats dictionary with counts.
    """
    if verbose:
        logging.basicConfig(level=logging.DEBUG)
    else:
        logging.basicConfig(level=logging.INFO)

    materials = load_materials(filepath)

    stats = {
        "materials_loaded": len(materials),
        "nodes_created": 0,
        "hierarchy_edges": 0,
        "stream_edges": 0,
        "dry_run": dry_run,
    }

    if dry_run:
        # Count what would be created
        stats["nodes_created"] = len(materials)
        stats["hierarchy_edges"] = sum(1 for m in materials if m.get("parent"))
        stats["stream_edges"] = sum(1 for m in materials if m.get("default_stream"))

        logger.info("DRY RUN — would create:")
        logger.info("  %d Material nodes", stats["nodes_created"])
        logger.info("  %d IS_A hierarchy edges", stats["hierarchy_edges"])
        logger.info("  %d DEFAULT_STREAM edges", stats["stream_edges"])

        # Show stream distribution
        stream_counts: dict[str, int] = {}
        for m in materials:
            stream = m.get("default_stream") or "(none)"
            stream_counts[stream] = stream_counts.get(stream, 0) + 1
        for stream, count in sorted(stream_counts.items(), key=lambda x: -x[1]):
            logger.info("    %s: %d materials", stream, count)

        return stats

    # Real import
    from src.db import neo4j_db

    with neo4j_db.session() as session:
        # Pass 1: Create Source node for provenance
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

        # Pass 2: Create Material nodes
        entries = [
            {
                "uid": generate_uid(m["tag"]),
                "tag": m["tag"],
                "name": m["name"],
                "name_de": m["name_de"],
            }
            for m in materials
        ]
        result = session.run(
            """
            UNWIND $entries AS e
            MERGE (m:Material {uid: e.uid})
            ON CREATE SET m.tag = e.tag, m.name = e.name, m.name_de = e.name_de
            ON MATCH SET m.name = e.name, m.name_de = e.name_de
            RETURN count(*) AS cnt
            """,
            entries=entries,
        )
        stats["nodes_created"] = result.single()["cnt"]
        logger.info("Created/updated %d Material nodes", stats["nodes_created"])

        # Pass 3: IS_A hierarchy edges
        hierarchy = [
            {
                "child_uid": generate_uid(m["tag"]),
                "parent_uid": generate_uid(m["parent"]),
            }
            for m in materials
            if m.get("parent")
        ]
        if hierarchy:
            result = session.run(
                """
                UNWIND $edges AS e
                MATCH (child:Material {uid: e.child_uid})
                MATCH (parent:Material {uid: e.parent_uid})
                MERGE (child)-[:IS_A]->(parent)
                RETURN count(*) AS cnt
                """,
                edges=hierarchy,
            )
            stats["hierarchy_edges"] = result.single()["cnt"]
            logger.info("Created %d IS_A hierarchy edges", stats["hierarchy_edges"])

        # Pass 4: DEFAULT_STREAM edges (Material → WasteStream)
        stream_links = [
            {
                "material_uid": generate_uid(m["tag"]),
                "stream_name": m["default_stream"],
            }
            for m in materials
            if m.get("default_stream")
        ]
        if stream_links:
            result = session.run(
                """
                UNWIND $links AS l
                MATCH (m:Material {uid: l.material_uid})
                MATCH (s:WasteStream {name: l.stream_name})
                MERGE (m)-[:DEFAULT_STREAM]->(s)
                RETURN count(*) AS cnt
                """,
                links=stream_links,
            )
            stats["stream_edges"] = result.single()["cnt"]
            logger.info("Created %d DEFAULT_STREAM edges", stats["stream_edges"])

        # Pass 5: Provenance edges
        all_uids = [{"uid": generate_uid(m["tag"])} for m in materials]
        session.run(
            """
            UNWIND $entries AS e
            MATCH (m:Material {uid: e.uid})
            MATCH (s:Source {uid: $source_uid})
            MERGE (m)-[:DERIVED_FROM]->(s)
            """,
            entries=all_uids,
            source_uid=source_uid,
        )
        logger.info("Linked all materials to source: %s", SOURCE_NAME)

    logger.info("Material import complete: %s", stats)
    return stats


def main():
    parser = argparse.ArgumentParser(
        description="Import material taxonomy into Neo4j"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview import without database changes",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose output",
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=DATA_FILE,
        help="Path to materials.json (default: data/materials.json)",
    )
    args = parser.parse_args()

    stats = import_materials(
        filepath=args.input,
        dry_run=args.dry_run,
        verbose=args.verbose,
    )

    if not args.dry_run:
        print(f"\nMaterial import results:")
        print(f"  Nodes:           {stats['nodes_created']}")
        print(f"  IS_A edges:      {stats['hierarchy_edges']}")
        print(f"  Stream edges:    {stats['stream_edges']}")


if __name__ == "__main__":
    main()
