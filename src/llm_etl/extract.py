"""
CLI entry point for LLM-based extraction.

Usage:
    python -m src.llm_etl.extract --schema WasteItem --input data/chunk.txt
    python -m src.llm_etl.extract --schema Facility --input data/doc.txt --output extracted.json
    python -m src.llm_etl.extract --schema DisposalRule --text "Farbreste: flüssig → Schadstoffannahme"
"""

import argparse
import json
import logging
import sys
from pathlib import Path

from src.llm_etl.extraction_chain import ExtractionChain
from src.llm_etl.schemas import SCHEMA_REGISTRY


def setup_logging(verbose: bool = False) -> None:
    """Configure logging."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Extract structured entities from text using LLM",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m src.llm_etl.extract --schema WasteItem --input chunk.txt
  python -m src.llm_etl.extract --schema Facility --text "Wertstoffhof Nord..."
  python -m src.llm_etl.extract --schema DisposalRule --input doc.txt --output rules.json

Available schemas: {}
        """.format(
            ", ".join(SCHEMA_REGISTRY.keys())
        ),
    )

    parser.add_argument(
        "--schema",
        "-s",
        required=True,
        choices=list(SCHEMA_REGISTRY.keys()),
        help="Entity type to extract",
    )

    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument(
        "--input",
        "-i",
        type=Path,
        help="Path to input text file",
    )
    input_group.add_argument(
        "--text",
        "-t",
        help="Text to extract from (for quick tests)",
    )

    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        help="Output JSON file (default: stdout)",
    )

    parser.add_argument(
        "--prompt",
        "-p",
        default="",
        help="Additional extraction instructions",
    )

    parser.add_argument(
        "--context",
        "-c",
        default="",
        help="Context hints (known entities, etc.)",
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

    # Get input text
    if args.input:
        if not args.input.exists():
            logger.error(f"Input file not found: {args.input}")
            return 1
        text = args.input.read_text(encoding="utf-8")
        logger.info(f"Loaded {len(text)} chars from {args.input}")
    else:
        text = args.text

    # Get schema class
    schema_class = SCHEMA_REGISTRY[args.schema]
    logger.info(f"Extracting {args.schema} entities...")

    # Run extraction
    chain = ExtractionChain()

    try:
        entities = chain.extract(
            text=text,
            output_schema=schema_class,
            extraction_prompt=args.prompt,
            context=args.context,
        )
    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        return 1

    # Format output
    output_data = {
        "schema": args.schema,
        "count": len(entities),
        "entities": [entity.model_dump() for entity in entities],
    }

    output_json = json.dumps(output_data, indent=2, ensure_ascii=False)

    # Write output
    if args.output:
        args.output.write_text(output_json, encoding="utf-8")
        logger.info(f"Wrote {len(entities)} entities to {args.output}")
    else:
        print(output_json)

    logger.info(f"Extraction complete: {len(entities)} {args.schema} entities")
    return 0


if __name__ == "__main__":
    sys.exit(main())
