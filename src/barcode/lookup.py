"""CLI entry point for barcode lookup.

Usage:
    python -m src.barcode.lookup --number 4006381333931
    python -m src.barcode.lookup --image path/to/photo.jpg
    python -m src.barcode.lookup --number 4006381333931 -v
    python -m src.barcode.lookup --number 4006381333931 --output result.json
"""

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.barcode.models import LookupResult
from src.barcode.resolver import BarcodeResolver
from src.barcode.scanner import BarcodeScanner


def setup_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )


def _output_result(result: LookupResult, output_path: Path | None) -> None:
    """Write result as JSON to file or stdout."""
    output_json = json.dumps(
        result.model_dump(), indent=2, ensure_ascii=False
    )

    if output_path:
        output_path.write_text(output_json, encoding="utf-8")
        logging.getLogger(__name__).info("Wrote result to %s", output_path)
    else:
        print(output_json)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Look up product information by barcode number or image",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m src.barcode.lookup --number 3017620422003
  python -m src.barcode.lookup --image photo_of_barcode.jpg
  python -m src.barcode.lookup --number 3017620422003 --output result.json
  python -m src.barcode.lookup --image photo.jpg -v
        """,
    )

    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument(
        "--number",
        "-n",
        help="Barcode number (EAN-13, UPC-A, etc.)",
    )
    input_group.add_argument(
        "--image",
        "-i",
        type=Path,
        help="Path to image file containing a barcode",
    )

    parser.add_argument(
        "--output",
        "-o",
        type=Path,
        help="Output JSON file (default: stdout)",
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

    resolver = BarcodeResolver()
    scan_result = None

    # Determine barcode number
    if args.image:
        if not args.image.exists():
            logger.error("Image file not found: %s", args.image)
            return 1

        logger.info("Scanning image: %s", args.image)
        scanner = BarcodeScanner()
        scan_result = scanner.scan(args.image)

        if scan_result.error:
            logger.error("Scan failed: %s", scan_result.error)
            result = LookupResult(scan=scan_result, error=scan_result.error)
            _output_result(result, args.output)
            return 1

        if not scan_result.has_barcodes:
            logger.warning("No barcodes found in image")
            result = LookupResult(
                scan=scan_result,
                error="No barcodes found in image",
            )
            _output_result(result, args.output)
            return 1

        if scan_result.count > 1:
            logger.info(
                "Multiple barcodes found (%d), using first: %s",
                scan_result.count,
                scan_result.barcodes[0],
            )

        barcode = scan_result.barcodes[0]
        logger.info(
            "Scanned barcode: %s (%s)",
            barcode,
            scan_result.formats[0].value,
        )
    else:
        barcode = args.number

    # Look up product
    logger.info("Looking up barcode: %s", barcode)
    product = resolver.lookup(barcode)

    if scan_result and scan_result.formats:
        product.barcode_format = scan_result.formats[0]

    result = LookupResult(
        product=product,
        scan=scan_result,
        success=product.found,
        error=""
        if product.found
        else f"Barcode {barcode} not found in product databases",
    )

    _output_result(result, args.output)

    if product.found:
        name = product.product_name or product.product_name_de
        logger.info("Product found: %s", name)
        logger.info("  Brand: %s", product.brands)
        logger.info("  Packaging: %s", product.packaging)
        if product.packaging_materials_tags:
            logger.info(
                "  Materials: %s",
                ", ".join(product.packaging_materials_tags),
            )
    else:
        logger.warning(
            "Barcode %s not found in any product database", barcode
        )

    resolver.close()
    return 0 if product.found else 1


if __name__ == "__main__":
    sys.exit(main())
