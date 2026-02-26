"""Barcode scanner and product lookup for the Fessi waste disposal chatbot.

This package provides:
- BarcodeScanner: Extract barcode numbers from images (pyzbar)
- BarcodeResolver: Look up product info by barcode (Open Food Facts API)
- ProductInfo: Pydantic model with structured product data
- LookupResult: Combined scan + resolve result

Usage:
    from src.barcode import BarcodeResolver, ProductInfo

    resolver = BarcodeResolver()
    product = resolver.lookup("3017620422003")
    if product.found:
        print(product.packaging_materials_tags)
"""

from src.barcode.models import (
    BarcodeFormat,
    LookupResult,
    ProductInfo,
    ScanResult,
)
from src.barcode.resolver import BarcodeResolver
from src.barcode.scanner import BarcodeScanner

__all__ = [
    "BarcodeFormat",
    "BarcodeResolver",
    "BarcodeScanner",
    "LookupResult",
    "ProductInfo",
    "ScanResult",
]
