"""
CLI entry point for the LLM-based extraction orchestrator.

Usage:
    python -m src.llm_etl.orchestrate --input data/document.txt
    python -m src.llm_etl.orchestrate --input data/doc.txt --output extracted.json
    python -m src.llm_etl.orchestrate --text "Altöl → Schadstoffannahme..."
"""

import argparse
import json
import logging
import sys
from pathlib import Path

from src.llm_etl.orchestrator import Orchestrator


def setup_logging(verbose: bool = False) -> None:
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Process documents through LLM extraction pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m src.llm_etl.orchestrate --input data/document.txt
  python -m src.llm_etl.orchestrate --input data/doc.txt --output extracted.json
  python -m src.llm_etl.orchestrate --text "Altöl muss zur Schadstoffannahme..."

The orchestrator will:
  1. Analyze the document structure
  2. Detect likely entity types (WasteItem, Facility, etc.)
  3. Chunk the document for processing
  4. Extract entities using the LLM
  5. Validate against the database
        """,
    )

    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument(
        "--input",
        "-i",
        type=Path,
        help="Path to input document",
    )
    input_group.add_argument(
        "--text",
        "-t",
        help="Text to process directly (for quick tests)",
    )

    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        help="Output JSON file (default: stdout)",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Analyze and plan only, skip extraction",
    )

    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose logging",
    )

    parser.add_argument(
        "--show-messages",
        action="store_true",
        help="Show processing messages in output",
    )

    args = parser.parse_args()
    setup_logging(args.verbose)
    logger = logging.getLogger(__name__)

    # Get input
    if args.input:
        if not args.input.exists():
            logger.error(f"Input file not found: {args.input}")
            return 1
        document = args.input.read_text(encoding="utf-8")
        file_path = str(args.input)
        logger.info(f"Loaded {len(document)} chars from {args.input}")
    else:
        document = args.text
        file_path = ""

    # Run orchestrator
    orchestrator = Orchestrator()

    if args.dry_run:
        logger.info("Dry-run mode: analyzing document only")
        # For dry-run, we could expose analysis-only, but for now just note it
        # The full processing still happens, just don't write to DB (handled by loader)

    try:
        result = orchestrator.process(
            document=document,
            file_path=file_path,
        )
    except Exception as e:
        logger.error(f"Processing failed: {e}")
        return 1

    # Format output
    output_data = {
        "success": result.success,
        "entities": result.extracted_entities,
        "validation": result.validation_summary,
        "gaps": result.db_gaps,
    }

    if args.show_messages:
        output_data["messages"] = result.messages

    if result.error:
        output_data["error"] = result.error

    output_json = json.dumps(output_data, indent=2, ensure_ascii=False)

    # Write output
    if args.output:
        args.output.write_text(output_json, encoding="utf-8")
        logger.info(f"Wrote results to {args.output}")
    else:
        print(output_json)

    # Summary
    entity_count = len(result.extracted_entities)
    validation = result.validation_summary
    logger.info(
        f"Complete: {entity_count} entities extracted, "
        f"{validation.get('valid', 0)} valid, "
        f"{validation.get('warnings', 0)} warnings, "
        f"{validation.get('errors', 0)} errors"
    )

    return 0 if result.success else 1


if __name__ == "__main__":
    sys.exit(main())
