"""Barcode scanner: extract barcode numbers from images using pyzbar.

Requires system package: libzbar0 (apt) or zbar (brew)
Requires Python packages: pyzbar, Pillow
"""

import logging
from pathlib import Path

from src.barcode.models import BarcodeFormat, ScanResult

logger = logging.getLogger(__name__)

# Map pyzbar type strings to our BarcodeFormat enum
_PYZBAR_FORMAT_MAP = {
    "EAN13": BarcodeFormat.EAN_13,
    "EAN8": BarcodeFormat.EAN_8,
    "UPCA": BarcodeFormat.UPC_A,
    "UPCE": BarcodeFormat.UPC_E,
    "CODE128": BarcodeFormat.CODE_128,
    "CODE39": BarcodeFormat.CODE_39,
    "QRCODE": BarcodeFormat.QR_CODE,
}


class BarcodeScanner:
    """Scan images for barcodes using pyzbar.

    Usage:
        scanner = BarcodeScanner()
        result = scanner.scan("photo.jpg")
        if result.has_barcodes:
            print(result.barcodes[0])  # "4006381333931"
    """

    def scan(self, image_path: str | Path) -> ScanResult:
        """Scan an image file for barcodes.

        Args:
            image_path: Path to the image file (JPEG, PNG, etc.)

        Returns:
            ScanResult with extracted barcode(s) or error.
        """
        image_path = Path(image_path)

        if not image_path.exists():
            return ScanResult(
                image_path=str(image_path),
                error=f"Image file not found: {image_path}",
            )

        try:
            from PIL import Image
            from pyzbar.pyzbar import decode
        except ImportError as e:
            return ScanResult(
                image_path=str(image_path),
                error=(
                    f"Missing dependency: {e}. "
                    "Install with: pip install pyzbar Pillow "
                    "(and system package libzbar0)"
                ),
            )

        try:
            image = Image.open(image_path)
            decoded = decode(image)
        except Exception as e:
            logger.error("Failed to scan image %s: %s", image_path, e)
            return ScanResult(
                image_path=str(image_path),
                error=f"Failed to scan image: {e}",
            )

        if not decoded:
            logger.info("No barcodes found in %s", image_path)
            return ScanResult(image_path=str(image_path))

        barcodes = []
        formats = []
        for barcode in decoded:
            data = barcode.data.decode("utf-8")
            fmt = _PYZBAR_FORMAT_MAP.get(barcode.type, BarcodeFormat.UNKNOWN)
            barcodes.append(data)
            formats.append(fmt)
            logger.info("Found barcode: %s (format: %s)", data, fmt.value)

        return ScanResult(
            barcodes=barcodes,
            formats=formats,
            image_path=str(image_path),
        )
