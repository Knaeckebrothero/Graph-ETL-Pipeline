"""
ETL: Import AVV codes from CSV into Neo4j.

Imports the Abfallverzeichnis-Verordnung (European Waste Catalogue) codes
as a 3-level hierarchy:
    Level 1: Chapter (XX)        — 20 chapters by waste origin
    Level 2: Subgroup (XX XX)    — industry-typical generation
    Level 3: Specific type (XX XX XX) — concrete waste type

Creates AVVCode nodes with properties:
    - code: Unique code string (e.g., "200301")
    - name: German description
    - level: Hierarchy level (1, 2, or 3)
    - is_hazardous: True if code ends with * (gA = gefährlicher Abfall)
    - status: Classification status (gA, ngA, Spiegel, or empty)

Creates HAS_PARENT relationships:
    (Level3)-[:HAS_PARENT]->(Level2)-[:HAS_PARENT]->(Level1)

Usage:
    python -m src.etl.avv_codes
    python -m src.etl.avv_codes --dry-run
    python -m src.etl.avv_codes -v
"""

import argparse
import csv
import hashlib
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.db import neo4j_db

logger = logging.getLogger(__name__)

DATA_FILE = Path(__file__).parent.parent.parent / "data" / "Abgabe_Gruppe2_AVV_mit_AbfallABC.csv"

# Chapter names from the AVV standard
CHAPTER_NAMES = {
    "01": "Abfälle aus dem Abbau von Bodenschätzen",
    "02": "Abfälle aus Landwirtschaft, Gartenbau, Nahrungsmittel",
    "03": "Abfälle aus der Holzbearbeitung und Papierherstellung",
    "04": "Abfälle aus der Leder-, Pelz- und Textilindustrie",
    "05": "Abfälle aus der Erdölraffination und Kohlepyrolyse",
    "06": "Abfälle aus anorganisch-chemischen Prozessen",
    "07": "Abfälle aus organisch-chemischen Prozessen",
    "08": "Abfälle aus Farben, Lacken, Klebstoffen und Druckfarben",
    "09": "Abfälle aus der fotografischen Industrie",
    "10": "Abfälle aus thermischen Prozessen",
    "11": "Abfälle aus der chemischen Oberflächenbearbeitung",
    "12": "Abfälle aus der mechanischen Formgebung",
    "13": "Ölabfälle und Abfälle aus flüssigen Brennstoffen",
    "14": "Abfälle aus organischen Lösemitteln und Kühlmitteln",
    "15": "Verpackungsabfall, Aufsaugmassen, Wischtücher, Filtermaterialien",
    "16": "Abfälle, die nicht anderswo im Verzeichnis aufgeführt sind",
    "17": "Bau- und Abbruchabfälle",
    "18": "Abfälle aus der humanmedizinischen oder tierärztlichen Versorgung",
    "19": "Abfälle aus Abfallbehandlungsanlagen und Abwasserbehandlung",
    "20": "Siedlungsabfälle (Haushaltsabfälle und ähnliche Abfälle)",
}


def generate_uid(code: str) -> str:
    """Generate deterministic UID from AVV code."""
    return hashlib.sha256(code.encode()).hexdigest()[:16]


def parse_avv_csv(filepath: Path) -> dict:
    """
    Parse AVV CSV into hierarchical structure.

    Returns:
        Dict with keys 'level1', 'level2', 'level3' mapping
        code -> {name, is_hazardous, parent_code, status}.
    """
    level1_nodes = {}
    level2_nodes = {}
    level3_nodes = {}

    with open(filepath, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f, delimiter=";")
        for row in reader:
            l1 = row["AVV_Level1"].strip()
            l2 = row["AVV_Level2"].strip()
            l3 = row["AVV_Level3"].strip()
            is_hazardous = row.get("AVV_gA", "").strip() == "*"
            status = row.get("AVV_Status", "").strip()
            short_name = row.get("Level 3", "").strip()
            subgroup_name = row.get("Level 2+3", "").strip()
            description = row.get("AVV_Beschreibung", "").strip()

            if not l1 or not l3:
                continue

            code1 = l1.zfill(2)
            code2 = l2.zfill(4)
            code3 = l3.zfill(6)

            # Level 1 (chapter)
            if code1 not in level1_nodes:
                level1_nodes[code1] = {
                    "name": CHAPTER_NAMES.get(code1, f"Kapitel {code1}"),
                    "is_hazardous": False,
                }

            # Level 2 (subgroup) — extract name from hierarchical description
            if code2 not in level2_nodes:
                sub_name = ""
                if subgroup_name and " - " in subgroup_name:
                    sub_name = subgroup_name.split(" - ")[0]
                elif description and " - " in description:
                    parts = description.split(" - ")
                    sub_name = parts[1] if len(parts) > 1 else parts[0]
                level2_nodes[code2] = {
                    "name": sub_name or f"Untergruppe {code2}",
                    "parent_code": code1,
                    "is_hazardous": False,
                }

            # Level 3 (specific type)
            level3_nodes[code3] = {
                "name": short_name or description,
                "parent_code": code2,
                "is_hazardous": is_hazardous,
                "status": status,
            }

            # Propagate hazardous flag upward
            if is_hazardous:
                level2_nodes[code2]["is_hazardous"] = True
                level1_nodes[code1]["is_hazardous"] = True

    return {"level1": level1_nodes, "level2": level2_nodes, "level3": level3_nodes}


def import_avv_codes(filepath: Path = None, dry_run: bool = False) -> dict:
    """
    Import AVV codes into Neo4j.

    Returns dict with import statistics.
    """
    filepath = filepath or DATA_FILE

    logger.info(f"Loading AVV codes from {filepath}")
    avv = parse_avv_csv(filepath)

    counts = {k: len(v) for k, v in avv.items()}
    total = sum(counts.values())
    logger.info(
        f"Parsed {total} AVV codes: "
        f"{counts['level1']} chapters, {counts['level2']} subgroups, {counts['level3']} types"
    )

    if dry_run:
        logger.info("DRY RUN - no changes will be made")
        hazardous = sum(1 for v in avv["level3"].values() if v["is_hazardous"])
        logger.info(f"  Hazardous codes: {hazardous}")
        return {**counts, "total": total, "created": 0, "relationships": 0, "dry_run": True}

    created = 0
    relationships = 0

    with neo4j_db.session() as session:
        # Level 1: Chapters
        for code, props in avv["level1"].items():
            session.run(
                """
                MERGE (a:AVVCode {code: $code})
                ON CREATE SET
                    a.uid = $uid, a.name = $name, a.level = 1,
                    a.is_hazardous = $is_hazardous, a.created_at = datetime()
                ON MATCH SET
                    a.name = $name, a.is_hazardous = $is_hazardous,
                    a.updated_at = datetime()
                """,
                {"code": code, "uid": generate_uid(code),
                 "name": props["name"], "is_hazardous": props["is_hazardous"]},
            )
            created += 1

        # Level 2: Subgroups (with HAS_PARENT → Level 1)
        for code, props in avv["level2"].items():
            session.run(
                """
                MERGE (a:AVVCode {code: $code})
                ON CREATE SET
                    a.uid = $uid, a.name = $name, a.level = 2,
                    a.is_hazardous = $is_hazardous, a.created_at = datetime()
                ON MATCH SET
                    a.name = $name, a.is_hazardous = $is_hazardous,
                    a.updated_at = datetime()
                WITH a
                MATCH (parent:AVVCode {code: $parent_code})
                MERGE (a)-[:HAS_PARENT]->(parent)
                """,
                {"code": code, "uid": generate_uid(code), "name": props["name"],
                 "is_hazardous": props["is_hazardous"], "parent_code": props["parent_code"]},
            )
            created += 1
            relationships += 1

        # Level 3: Specific types (with HAS_PARENT → Level 2)
        for code, props in avv["level3"].items():
            session.run(
                """
                MERGE (a:AVVCode {code: $code})
                ON CREATE SET
                    a.uid = $uid, a.name = $name, a.level = 3,
                    a.is_hazardous = $is_hazardous, a.status = $status,
                    a.created_at = datetime()
                ON MATCH SET
                    a.name = $name, a.is_hazardous = $is_hazardous,
                    a.status = $status, a.updated_at = datetime()
                WITH a
                MATCH (parent:AVVCode {code: $parent_code})
                MERGE (a)-[:HAS_PARENT]->(parent)
                """,
                {"code": code, "uid": generate_uid(code), "name": props["name"],
                 "is_hazardous": props["is_hazardous"], "status": props.get("status", ""),
                 "parent_code": props["parent_code"]},
            )
            created += 1
            relationships += 1

    logger.info(f"Import complete: {created} codes, {relationships} HAS_PARENT relationships")

    return {
        **counts, "total": total,
        "created": created, "relationships": relationships, "dry_run": False,
    }


def main():
    parser = argparse.ArgumentParser(description="Import AVV codes into Neo4j")
    parser.add_argument("--file", "-f", type=Path, default=DATA_FILE, help="Path to AVV CSV")
    parser.add_argument("--dry-run", "-n", action="store_true", help="No DB changes")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    try:
        stats = import_avv_codes(args.file, dry_run=args.dry_run)
        print(f"\nAVV Import Statistics:")
        print(f"  Chapters (L1):   {stats['level1']}")
        print(f"  Subgroups (L2):  {stats['level2']}")
        print(f"  Types (L3):      {stats['level3']}")
        print(f"  Total codes:     {stats['total']}")
        print(f"  Created/updated: {stats['created']}")
        print(f"  HAS_PARENT rels: {stats['relationships']}")
        if stats["dry_run"]:
            print("  (dry run - no changes made)")
    except FileNotFoundError:
        logger.error(f"File not found: {args.file}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Import failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
