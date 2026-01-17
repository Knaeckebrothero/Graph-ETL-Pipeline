"""
CLI entry point for the database loader.

Usage:
    # Load from orchestrator JSON output
    python -m src.llm_etl.load --input extracted.json

    # Load with custom source info
    python -m src.llm_etl.load --input extracted.json --source-name "manual.pdf"

    # Dry-run mode (no database changes)
    python -m src.llm_etl.load --input extracted.json --dry-run

    # Load directly from file (runs full pipeline)
    python -m src.llm_etl.load --file data/document.txt
"""

import argparse
import json
import logging
import sys
from pathlib import Path

from src.llm_etl.loader import EntityLoader, LoadResult, load_from_orchestrator_result
from src.llm_etl.schemas import SourceExtract, get_schema


def setup_logging(verbose: bool = False) -> None:
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


def load_from_json(json_path: Path, source: SourceExtract, dry_run: bool) -> LoadResult:
    """
    Load entities from a JSON file.

    Expects JSON format from orchestrator output:
    {
        "entities": [
            {"schema_name": "WasteItem", "entity": {...}},
            ...
        ]
    }
    """
    data = json.loads(json_path.read_text(encoding="utf-8"))

    entities = []
    for item in data.get("entities", []):
        schema_name = item.get("schema_name", "")
        entity_data = item.get("entity", {})

        schema_class = get_schema(schema_name)
        if schema_class:
            try:
                entity = schema_class(**entity_data)
                entities.append(entity)
            except Exception as e:
                logging.warning(f"Failed to parse {schema_name}: {e}")

    loader = EntityLoader()
    return loader.load(entities, source, dry_run)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Load extracted entities into Neo4j database",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Load from orchestrator JSON output
  python -m src.llm_etl.load --input extracted.json

  # Load with custom source info
  python -m src.llm_etl.load --input extracted.json --source-name "manual.pdf" --source-type pdf

  # Dry-run mode (no database changes)
  python -m src.llm_etl.load --input extracted.json --dry-run

  # Process and load a file directly (runs orchestrator + loader)
  python -m src.llm_etl.load --file data/document.txt
        """,
    )

    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument(
        "--input",
        "-i",
        type=Path,
        help="Path to JSON file with extracted entities",
    )
    input_group.add_argument(
        "--file",
        "-f",
        type=Path,
        help="Path to source document (runs full extraction + load pipeline)",
    )

    parser.add_argument(
        "--source-name",
        help="Source document name for provenance (default: filename)",
    )
    parser.add_argument(
        "--source-type",
        default="json",
        choices=["pdf", "docx", "csv", "json", "txt", "web"],
        help="Source document type (default: json)",
    )
    parser.add_argument(
        "--source-path",
        help="Original source file path for provenance tracking",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulate loading without database changes",
    )

    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        help="Output JSON file for load results",
    )

    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose logging",
    )

    args = parser.parse_args()
    setup_logging(args.verbose)
    logger = logging.getLogger(__name__)

    # Build source info
    if args.input:
        source_name = args.source_name or args.input.name
        source_path = args.source_path or str(args.input)
    elif args.file:
        source_name = args.source_name or args.file.name
        source_path = args.source_path or str(args.file)
        # Detect file type from extension
        ext = args.file.suffix.lower().lstrip(".")
        args.source_type = ext if ext in ["pdf", "docx", "csv", "json", "txt"] else "txt"
    else:
        source_name = args.source_name or "unknown"
        source_path = args.source_path or ""

    source = SourceExtract(
        name=source_name,
        file_type=args.source_type,
        file_path=source_path,
    )

    # Process input
    try:
        if args.input:
            # Load from JSON
            if not args.input.exists():
                logger.error(f"Input file not found: {args.input}")
                return 1

            logger.info(f"Loading entities from: {args.input}")
            result = load_from_json(args.input, source, args.dry_run)

        elif args.file:
            # Run full pipeline (orchestrator + loader)
            if not args.file.exists():
                logger.error(f"File not found: {args.file}")
                return 1

            logger.info(f"Processing file: {args.file}")

            from src.llm_etl.orchestrator import Orchestrator

            orchestrator = Orchestrator()
            orch_result = orchestrator.process_file(args.file)

            if not orch_result.success:
                logger.error(f"Extraction failed: {orch_result.error}")
                return 1

            logger.info(
                f"Extracted {len(orch_result.extracted_entities)} entities, loading..."
            )
            result = load_from_orchestrator_result(orch_result, source, args.dry_run)

        else:
            logger.error("No input specified")
            return 1

    except Exception as e:
        logger.error(f"Loading failed: {e}")
        return 1

    # Format output
    output_data = {
        "success": result.success,
        "dry_run": result.dry_run,
        "source_uid": result.source_uid,
        "summary": {
            "processed": result.entities_processed,
            "created": result.entities_created,
            "updated": result.entities_updated,
            "enriched": result.entities_enriched,
            "skipped": result.entities_skipped,
        },
        "entities": [
            {
                "type": e.entity_type,
                "name": e.name,
                "uid": e.uid,
                "action": e.action,
                "changes": e.changes,
                "error": e.error,
            }
            for e in result.loaded_entities
        ],
        "errors": result.errors,
    }

    output_json = json.dumps(output_data, indent=2, ensure_ascii=False)

    # Write output
    if args.output:
        args.output.write_text(output_json, encoding="utf-8")
        logger.info(f"Wrote results to {args.output}")
    else:
        print(output_json)

    # Summary
    logger.info(
        f"{'[DRY-RUN] ' if result.dry_run else ''}"
        f"Complete: {result.entities_created} created, "
        f"{result.entities_updated} updated, "
        f"{result.entities_enriched} enriched, "
        f"{result.entities_skipped} skipped"
    )

    if result.errors:
        logger.warning(f"{len(result.errors)} errors occurred during loading")
        for error in result.errors[:5]:
            logger.warning(f"  - {error}")

    return 0 if result.success else 1


if __name__ == "__main__":
    sys.exit(main())
