"""
ETL: Import waste items from Abfall-ABC_new.csv into Neo4j.

Creates:
    - WasteItem nodes with name and uid
    - WasteStream nodes for bin types (Restabfalltonne, Biotonne, etc.)
    - DISPOSED_AT relationships to Facility nodes
    - DISPOSED_IN relationships to WasteStream nodes

Usage:
    python -m src.etl.waste_items
    python -m src.etl.waste_items --dry-run
    python -m src.etl.waste_items -v
"""

import argparse
import csv
import hashlib
import logging
import re
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.db import neo4j_db

logger = logging.getLogger(__name__)

# Default data path
DATA_FILE = Path(__file__).parent.parent.parent / "data" / "Abfall-ABC_new.csv"

# Bin types that should be WasteStream nodes (not Facility nodes)
# These are the standard Frankfurt FES waste streams
WASTE_STREAMS = {
    'Restabfalltonne',
    'Biotonne',
    'Altpapiertonne',
    'Verpackungstonne',
    'Verpackungstonne (Gelbe Tonne)',
}

# Facility name normalization mapping
# Maps CSV variants to canonical names in the database
FACILITY_NAME_MAP = {
    'Fachhandel/Hersteller': 'Fachhandel / Hersteller',
    'Fachhandel / Herstelle': 'Fachhandel / Hersteller',  # Typo in CSV
    'Mobile Elektrokleingerätesam-mlung': 'Mobile Elektrokleingerätesammlung',
    'Abfallumladeanlage FES': 'FES-Abfallumladeanlage',
    'Abfallumladeanlage (FES)': 'FES-Abfallumladeanlage',
    'Abfallumladeanlage': 'FES-Abfallumladeanlage',
    'Abfallumladeanlage \tFES': 'FES-Abfallumladeanlage',
    'Schadstoffsammlung FES': 'Schadstoffsammlung',
    'Schadstoffsammlung \tFES': 'Schadstoffsammlung',
    'Schadstoffsammlung\t FES': 'Schadstoffsammlung',
    'Schadstoffmobil FES': 'Schadstoffsammlung',
    'Restmülltonne': 'Restabfalltonne',  # Synonym
}


def generate_uid(name: str) -> str:
    """Generate deterministic UID from waste item name."""
    return hashlib.sha256(name.encode()).hexdigest()[:16]


def normalize_facility_name(name: str) -> str:
    """Normalize facility name to match database entries."""
    name = name.strip()
    return FACILITY_NAME_MAP.get(name, name)


def is_valid_facility_name(name: str) -> bool:
    """
    Check if a string looks like a valid facility name vs a note/comment.

    Returns False for things like:
    - "Laut FES:" (source notes)
    - "Hinweis" (hint markers)
    - "1 Stück = Sperrmüll" (quantity notes)
    - Sentences with equals signs
    """
    name = name.strip()

    # Skip empty or short strings
    if not name or len(name) < 3:
        return False

    # Skip notes and hints
    skip_patterns = [
        'laut ',
        'hinweis',
        ' = ',  # Quantity/condition notes
        'stück',
        'mengen',
        'kartons',
        'polizei',  # Not a waste facility
        'elektrische zahnbürste',  # Notes about specific items
        'sonst ',
        'selbstgebaut',
        'aus dem handel',
        'haushaltsübliche',
        'saubere ',
        'größere ',
        'kleinere ',
    ]

    name_lower = name.lower()
    for pattern in skip_patterns:
        if pattern in name_lower:
            return False

    # Skip if it starts with certain patterns
    if name_lower.startswith(('laut', 'ab ', 'bis ', 'lauut')):
        return False

    # Skip combined facility names (should be handled elsewhere or broken apart)
    if ' oder ' in name_lower:
        return False

    return True


def parse_disposal_targets(disposal_text: str) -> list[str]:
    """
    Parse the Entsorgungsweg column to extract disposal target names.

    The column may contain:
    - Newline-separated names (in quoted cells)
    - Space-separated names (for simple cases)
    - Notes and hints that should be filtered out

    Returns a list of normalized, validated target names.
    """
    if not disposal_text or disposal_text.strip() == '-':
        return []

    targets = []

    # First try splitting by newlines (most reliable for multi-line cells)
    if '\n' in disposal_text:
        parts = disposal_text.split('\n')
    else:
        # For single-line cells, we need to split by known facility names
        # This is trickier because names can contain spaces
        parts = [disposal_text]

    for part in parts:
        part = part.strip()
        if not part or part == '-':
            continue

        # Handle space-separated facilities in single line
        # We need to match against known facility patterns
        if '\n' not in disposal_text and len(part) > 30:
            # Likely concatenated facilities - try to split smartly
            extracted = extract_facilities_from_concat(part)
            for name in extracted:
                if is_valid_facility_name(name):
                    targets.append(normalize_facility_name(name))
        else:
            # Check if this looks like a valid facility name
            if is_valid_facility_name(part):
                targets.append(normalize_facility_name(part))
            else:
                # Try to extract any valid facility names from the text
                extracted = extract_facilities_from_concat(part)
                for name in extracted:
                    if is_valid_facility_name(name):
                        targets.append(normalize_facility_name(name))

    return list(set(targets))  # Deduplicate


def extract_facilities_from_concat(text: str) -> list[str]:
    """
    Extract facility names from a concatenated string.

    Example input: "Wertstoffhof Nord Wertstoffhof West Schadstoffsammlung"
    Expected output: ["Wertstoffhof Nord", "Wertstoffhof West", "Schadstoffsammlung"]
    """
    # Known facility name patterns (order matters - longer patterns first)
    known_patterns = [
        r'Altkleidercontainer im öffentlichen Straßenraum',
        r'Self Service am Wertstoffhof Nord',
        r'Mobile Elektrokleingerätesam-mlung',
        r'Mobile Elektrokleingerätesammlung',
        r'Verpackungstonne \(Gelbe Tonne\)',
        r'Öffentliche Gebäude / Einzelhandel',
        r'Öffentliche Gebäude/Einzelhandel',
        r'Fachhandel / Hersteller',
        r'Fachhandel/Hersteller',
        r'Abfallumladeanlage FES',
        r'FES-Abfallumladeanlage',
        r'Altpapiersortieranlage',
        r'FES-Aktenvernichtung',
        r'Deponiepark Wicker',
        r'Rhein-Main-Deponie',
        r'FES-Servicecenter',
        r'Containergestellung',
        r'Schadstoffsammlung',
        r'Wertstoffhof Nord',
        r'Wertstoffhof West',
        r'Wertstoffhof Süd',
        r'Wertstoffhof Ost',
        r'Kofferraumservice',
        r'Recyclingzentrum',
        r'Verpackungstonne',
        r'Altglascontainer',
        r'Restabfalltonne',
        r'Altpapiertonne',
        r'Kleiderspende',
        r'Möbelspende',
        r'Sachspende',
        r'Wertstoffinsel',
        r'Altölverordnung',
        r'Klamoddekurier',
        r'Betriebshöfe FES',
        r'Auf Anfrage',
        r'Sperrmüll',
        r'GWR GmbH',
        r'RMB GmbH',
        r'FFR GmbH',
        r'Biotonne',
        r'easi',
    ]

    results = []
    remaining = text

    for pattern in known_patterns:
        matches = re.findall(pattern, remaining, re.IGNORECASE)
        for match in matches:
            results.append(normalize_facility_name(match))
            # Remove matched text to avoid double-matching
            remaining = remaining.replace(match, ' ', 1)

    return results


def is_section_marker(row: dict) -> bool:
    """Check if row is a section marker (single letter like A, B, C...)."""
    abfallart = row.get('Abfallart', '').strip()
    entsorgungsweg = row.get('Entsorgungsweg', '').strip()

    # Section markers are single letters with empty other columns
    if len(abfallart) == 1 and abfallart.isalpha() and not entsorgungsweg:
        return True
    return False


def load_waste_items(filepath: Path) -> list[dict]:
    """
    Load waste items from CSV file.

    Returns list of dicts with keys:
        - name: Waste item name
        - disposal_targets: List of facility/stream names
    """
    items = []

    with open(filepath, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)

        for row in reader:
            # Skip section markers (A, B, C, ...)
            if is_section_marker(row):
                continue

            name = row.get('Abfallart', '').strip()
            if not name:
                continue

            disposal_text = row.get('Entsorgungsweg', '')
            targets = parse_disposal_targets(disposal_text)

            items.append({
                'name': name,
                'disposal_targets': targets,
            })

    return items


def classify_target(target_name: str, existing_facilities: set[str]) -> tuple[str, str]:
    """
    Classify a disposal target as either 'facility' or 'stream'.

    Returns tuple of (type, normalized_name)
    """
    # Check if it's a known waste stream type
    if target_name in WASTE_STREAMS:
        return ('stream', target_name)

    # Check if it matches an existing facility
    if target_name in existing_facilities:
        return ('facility', target_name)

    # Default to facility (will be created if doesn't exist)
    return ('facility', target_name)


def get_existing_facilities() -> set[str]:
    """Get set of existing facility names from database."""
    results = neo4j_db.query("MATCH (f:Facility) RETURN f.name AS name")
    return {r['name'] for r in results}


def import_waste_items(filepath: Path = None, dry_run: bool = False) -> dict:
    """
    Import waste items into Neo4j.

    Args:
        filepath: Path to CSV file (defaults to data/Abfall-ABC_new.csv)
        dry_run: If True, only log what would be done without writing

    Returns:
        Dict with import statistics
    """
    filepath = filepath or DATA_FILE

    logger.info(f"Loading waste items from {filepath}")
    items = load_waste_items(filepath)
    logger.info(f"Found {len(items)} waste items")

    # Get existing facilities for classification
    existing_facilities = get_existing_facilities()
    logger.info(f"Found {len(existing_facilities)} existing facilities in database")

    if dry_run:
        logger.info("DRY RUN - no changes will be made")
        streams_needed = set()
        unmatched_facilities = set()

        for item in items:
            logger.debug(f"Would create WasteItem: {item['name']}")
            for target in item['disposal_targets']:
                target_type, target_name = classify_target(target, existing_facilities)
                if target_type == 'stream':
                    streams_needed.add(target_name)
                elif target_name not in existing_facilities:
                    unmatched_facilities.add(target_name)

        logger.info(f"WasteStream nodes needed: {streams_needed}")
        if unmatched_facilities:
            logger.warning(f"Unmatched facilities (will be created): {unmatched_facilities}")

        return {
            'items_loaded': len(items),
            'items_created': 0,
            'streams_needed': len(streams_needed),
            'relationships_created': 0,
            'dry_run': True,
        }

    # Import to Neo4j
    items_created = 0
    streams_created = 0
    relationships_created = 0

    with neo4j_db.session() as session:
        for item in items:
            name = item['name']
            uid = generate_uid(name)

            # Create WasteItem node
            session.run("""
                MERGE (w:WasteItem {name: $name})
                ON CREATE SET
                    w.uid = $uid,
                    w.created_at = datetime()
                ON MATCH SET
                    w.updated_at = datetime()
            """, {'name': name, 'uid': uid})
            items_created += 1
            logger.debug(f"Created WasteItem: {name}")

            # Create relationships to disposal targets
            for target in item['disposal_targets']:
                target_type, target_name = classify_target(target, existing_facilities)

                if target_type == 'stream':
                    # Create WasteStream node and DISPOSED_IN relationship
                    result = session.run("""
                        MATCH (w:WasteItem {name: $item_name})
                        MERGE (s:WasteStream {name: $stream_name})
                        ON CREATE SET
                            s.uid = $stream_uid,
                            s.created_at = datetime()
                        MERGE (w)-[r:DISPOSED_IN]->(s)
                        ON CREATE SET r.created_at = datetime()
                        RETURN s.name AS stream, type(r) AS rel_type
                    """, {
                        'item_name': name,
                        'stream_name': target_name,
                        'stream_uid': generate_uid(target_name),
                    })
                    record = result.single()
                    if record:
                        streams_created += 1
                        relationships_created += 1
                        logger.debug(f"  -> DISPOSED_IN -> {target_name}")
                else:
                    # Create DISPOSED_AT relationship to existing Facility
                    result = session.run("""
                        MATCH (w:WasteItem {name: $item_name})
                        MATCH (f:Facility {name: $facility_name})
                        MERGE (w)-[r:DISPOSED_AT]->(f)
                        ON CREATE SET r.created_at = datetime()
                        RETURN f.name AS facility, type(r) AS rel_type
                    """, {
                        'item_name': name,
                        'facility_name': target_name,
                    })
                    record = result.single()
                    if record:
                        relationships_created += 1
                        logger.debug(f"  -> DISPOSED_AT -> {target_name}")
                    else:
                        logger.warning(f"  Could not link to facility: {target_name}")

    logger.info(f"Import complete: {items_created} items, {relationships_created} relationships")

    return {
        'items_loaded': len(items),
        'items_created': items_created,
        'streams_created': streams_created,
        'relationships_created': relationships_created,
        'dry_run': False,
    }


def main():
    parser = argparse.ArgumentParser(
        description='Import waste items from Abfall-ABC_new.csv into Neo4j'
    )
    parser.add_argument(
        '--file', '-f',
        type=Path,
        default=DATA_FILE,
        help='Path to Abfall-ABC_new.csv'
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
        stats = import_waste_items(args.file, dry_run=args.dry_run)
        print(f"\nImport Statistics:")
        print(f"  Items loaded: {stats['items_loaded']}")
        print(f"  Items created/updated: {stats['items_created']}")
        if 'streams_created' in stats:
            print(f"  WasteStream nodes created: {stats['streams_created']}")
        print(f"  Relationships created: {stats['relationships_created']}")
        if stats['dry_run']:
            print("  (dry run - no changes made)")
    except FileNotFoundError:
        logger.error(f"File not found: {args.file}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Import failed: {e}")
        raise


if __name__ == '__main__':
    main()
