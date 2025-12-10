"""
ETL: Import disposal facilities from disposal_map_db.json into Neo4j.

Creates Facility nodes with properties:
    - uid: Deterministic hash of facility name
    - name: Facility name
    - address: Street address
    - opening_hours: Operating hours
    - contact: Contact information
    - additional_info: Extra notes
    - link: URL if available

Usage:
    python -m src.etl.facilities
    python -m src.etl.facilities --dry-run
    python -m src.etl.facilities -v
"""

import argparse
import hashlib
import json
import logging
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.db import neo4j_db

logger = logging.getLogger(__name__)

# Default data path
DATA_FILE = Path(__file__).parent.parent.parent / "data" / "disposal_map_db.json"


def generate_uid(name: str) -> str:
    """Generate deterministic UID from facility name."""
    return hashlib.sha256(name.encode()).hexdigest()[:16]


def load_facilities(filepath: Path) -> list[dict]:
    """
    Load and deduplicate facilities from JSON file.

    The JSON structure is:
        {
            "uuid": [
                {"name": "...", "address": "...", ...},
                ...
            ]
        }

    Multiple UUIDs can reference the same facility, so we dedupe by name.
    """
    with open(filepath, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Flatten all facility entries and dedupe by name
    seen = {}
    for uuid_key, facilities in data.items():
        for facility in facilities:
            name = facility.get('name', '').strip()
            if not name:
                continue

            # Keep the most complete entry (one with most non-empty fields)
            if name not in seen:
                seen[name] = facility
            else:
                # Merge: prefer non-empty values
                existing = seen[name]
                for key, value in facility.items():
                    if value and not existing.get(key):
                        existing[key] = value

    return list(seen.values())


def import_facilities(filepath: Path = None, dry_run: bool = False) -> dict:
    """
    Import facilities into Neo4j.

    Args:
        filepath: Path to JSON file (defaults to data/disposal_map_db.json)
        dry_run: If True, only log what would be done without writing

    Returns:
        Dict with import statistics
    """
    filepath = filepath or DATA_FILE

    logger.info(f"Loading facilities from {filepath}")
    facilities = load_facilities(filepath)
    logger.info(f"Found {len(facilities)} unique facilities")

    if dry_run:
        logger.info("DRY RUN - no changes will be made")
        for f in facilities:
            logger.info(f"  Would create: {f.get('name')}")
        return {'loaded': len(facilities), 'created': 0, 'dry_run': True}

    # Import to Neo4j
    created = 0
    with neo4j_db.session() as session:
        for facility in facilities:
            name = facility.get('name', '').strip()
            uid = generate_uid(name)

            result = session.run("""
                MERGE (f:Facility {uid: $uid})
                ON CREATE SET
                    f.name = $name,
                    f.address = $address,
                    f.opening_hours = $opening_hours,
                    f.contact = $contact,
                    f.additional_info = $additional_info,
                    f.link = $link,
                    f.created_at = datetime()
                ON MATCH SET
                    f.address = CASE WHEN $address <> '' THEN $address ELSE f.address END,
                    f.opening_hours = CASE WHEN $opening_hours <> '' THEN $opening_hours ELSE f.opening_hours END,
                    f.contact = CASE WHEN $contact <> '' THEN $contact ELSE f.contact END,
                    f.additional_info = CASE WHEN $additional_info <> '' THEN $additional_info ELSE f.additional_info END,
                    f.link = CASE WHEN $link <> '' THEN $link ELSE f.link END,
                    f.updated_at = datetime()
                RETURN f.uid AS uid, f.name AS name
            """, {
                'uid': uid,
                'name': name,
                'address': facility.get('address', ''),
                'opening_hours': facility.get('opening_hours', ''),
                'contact': facility.get('contact', ''),
                'additional_info': facility.get('additional_info', ''),
                'link': facility.get('link', ''),
            })

            record = result.single()
            if record:
                created += 1
                logger.debug(f"Created/updated: {record['name']} ({record['uid']})")

    logger.info(f"Import complete: {created} facilities created/updated")
    return {'loaded': len(facilities), 'created': created, 'dry_run': False}


def main():
    parser = argparse.ArgumentParser(
        description='Import disposal facilities into Neo4j'
    )
    parser.add_argument(
        '--file', '-f',
        type=Path,
        default=DATA_FILE,
        help='Path to disposal_map_db.json'
    )
    parser.add_argument(
        '--dry-run', '-n',
        action='store_true',
        help='Show what would be imported without making changes'
    )
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Enable verbose output'
    )

    args = parser.parse_args()

    # Configure logging
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    try:
        stats = import_facilities(args.file, dry_run=args.dry_run)
        print(f"\nImport Statistics:")
        print(f"  Facilities loaded: {stats['loaded']}")
        print(f"  Facilities created/updated: {stats['created']}")
        if stats['dry_run']:
            print("  (dry run - no changes made)")
    except FileNotFoundError:
        logger.error(f"File not found: {args.file}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Import failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
