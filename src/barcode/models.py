"""Pydantic models for barcode lookup results.

These models provide structured product data suitable for
passing to a chatbot agent for waste disposal classification.
"""

from enum import Enum

from pydantic import BaseModel, Field


class BarcodeFormat(str, Enum):
    """Supported barcode formats."""

    EAN_13 = "EAN-13"
    EAN_8 = "EAN-8"
    UPC_A = "UPC-A"
    UPC_E = "UPC-E"
    CODE_128 = "CODE-128"
    CODE_39 = "CODE-39"
    QR_CODE = "QR-CODE"
    UNKNOWN = "UNKNOWN"


class PackagingComponentInfo(BaseModel):
    """Single packaging component from Open Food Facts structured data.

    OFF provides a per-component breakdown in the `packagings` array,
    with material, shape, recycling hint, and weight for each physical
    part of the product packaging.
    """

    material: str = Field(
        default="",
        description="Material tag (e.g., 'en:clear-glass', 'en:pp-5-polypropylene')",
    )
    shape: str = Field(
        default="",
        description="Shape tag (e.g., 'en:pot', 'en:lid', 'en:bottle')",
    )
    recycling: str = Field(
        default="",
        description="Recycling instruction tag (e.g., 'en:recycle-in-glass-bin')",
    )
    number_of_units: int = Field(default=1, description="Number of this component")
    weight_measured: float | None = Field(
        default=None, description="Measured weight in grams"
    )
    food_contact: bool | None = Field(
        default=None, description="Whether this component contacts food"
    )


class ProductInfo(BaseModel):
    """Structured product information from barcode lookup.

    Contains the fields most relevant for waste disposal classification:
    product name, brand, categories, and especially packaging materials.
    """

    barcode: str = Field(description="The barcode number (EAN/UPC)")
    barcode_format: BarcodeFormat = Field(
        default=BarcodeFormat.UNKNOWN,
        description="Barcode format detected from image scan",
    )
    found: bool = Field(
        default=False,
        description="Whether the barcode was found in the API",
    )
    source_api: str = Field(
        default="",
        description="API that provided the data (e.g., 'openfoodfacts')",
    )

    # Product identification
    product_name: str = Field(
        default="", description="Product name (best available language)"
    )
    product_name_de: str = Field(
        default="", description="Product name in German, if available"
    )
    brands: str = Field(default="", description="Brand name(s)")
    quantity: str = Field(
        default="", description="Product quantity (e.g., '400 g', '1.5 L')"
    )

    # Classification (most relevant for waste disposal)
    categories: str = Field(
        default="", description="Product categories (comma-separated)"
    )
    categories_tags: list[str] = Field(
        default_factory=list,
        description="Normalized category tags (e.g., 'en:beverages')",
    )

    # Packaging (critical for waste stream routing)
    packaging: str = Field(
        default="", description="Packaging description (free text)"
    )
    packaging_tags: list[str] = Field(
        default_factory=list,
        description="Normalized packaging tags (e.g., 'en:plastic')",
    )
    packaging_materials_tags: list[str] = Field(
        default_factory=list,
        description="Packaging material codes (e.g., 'en:clear-glass', 'en:pp-5-polypropylene')",
    )
    packaging_components: list[PackagingComponentInfo] = Field(
        default_factory=list,
        description="Structured per-component packaging breakdown from OFF `packagings` array",
    )

    # Environmental scores
    ecoscore_grade: str = Field(
        default="", description="Eco-Score grade (a-e or 'unknown')"
    )
    nutriscore_grade: str = Field(
        default="", description="Nutri-Score grade (a-e)"
    )
    labels: str = Field(
        default="", description="Product labels (e.g., 'Vegetarian, Bio')"
    )

    # Media
    image_url: str = Field(default="", description="Product image URL")

    # Metadata
    lookup_timestamp: str = Field(
        default="",
        description="ISO timestamp of when the lookup was performed",
    )


class ScanResult(BaseModel):
    """Result of scanning an image for barcodes."""

    barcodes: list[str] = Field(
        default_factory=list,
        description="Barcode numbers extracted from image",
    )
    formats: list[BarcodeFormat] = Field(
        default_factory=list,
        description="Barcode formats detected (parallel to barcodes list)",
    )
    image_path: str = Field(default="", description="Path to the scanned image")
    error: str = Field(
        default="", description="Error message if scanning failed"
    )

    @property
    def count(self) -> int:
        return len(self.barcodes)

    @property
    def has_barcodes(self) -> bool:
        return len(self.barcodes) > 0


class LookupResult(BaseModel):
    """Full result of a barcode lookup operation (scan + resolve)."""

    product: ProductInfo | None = Field(
        default=None,
        description="Product info if found, None if barcode not in database",
    )
    scan: ScanResult | None = Field(
        default=None,
        description="Scan result if input was an image, None if barcode was provided directly",
    )
    error: str = Field(
        default="", description="Error message if the operation failed"
    )
    success: bool = Field(
        default=False, description="Whether the lookup succeeded"
    )
