"""Barcode resolver: look up product information by barcode number via APIs.

Primary API: Open Food Facts (free, no key required)
"""

import logging
from datetime import datetime, timezone

import requests

from src.barcode.models import PackagingComponentInfo, ProductInfo

logger = logging.getLogger(__name__)

OFF_API_BASE = "https://world.openfoodfacts.org/api/v2/product"
OFF_USER_AGENT = "FessiWasteBot/1.0 (https://github.com/fessi-chatbot)"
OFF_TIMEOUT = 10  # seconds

# Request only the fields we need to reduce payload size
OFF_FIELDS = ",".join([
    "product_name",
    "product_name_de",
    "brands",
    "quantity",
    "categories",
    "categories_tags",
    "packaging",
    "packaging_tags",
    "packaging_materials_tags",
    "ecoscore_grade",
    "nutriscore_grade",
    "labels",
    "image_url",
    "image_front_url",
    "packagings",
])


class BarcodeResolver:
    """Resolve barcode numbers to product information via APIs.

    Usage:
        resolver = BarcodeResolver()
        product = resolver.lookup("3017620422003")
        if product.found:
            print(product.product_name)   # "Nutella"
            print(product.packaging_materials_tags)

    Programmatic usage by agents:
        from src.barcode import BarcodeResolver
        resolver = BarcodeResolver()
        info = resolver.lookup(barcode)
    """

    def __init__(
        self,
        timeout: int = OFF_TIMEOUT,
        user_agent: str = OFF_USER_AGENT,
    ):
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": user_agent})

    def lookup(self, barcode: str) -> ProductInfo:
        """Look up product information by barcode number.

        Tries Open Food Facts first. Returns ProductInfo with found=False
        if the barcode is not in any database.

        Args:
            barcode: Barcode number string (EAN-13, UPC-A, etc.)

        Returns:
            ProductInfo with product data (found=True) or empty (found=False).
        """
        barcode = barcode.strip()
        if not barcode:
            logger.warning("Empty barcode provided")
            return ProductInfo(barcode="", found=False)

        logger.info("Looking up barcode: %s", barcode)

        result = self._lookup_openfoodfacts(barcode)
        if result.found:
            return result

        # Future fallback APIs can be added here:
        # result = self._lookup_open_ean_db(barcode)
        # if result.found:
        #     return result

        logger.info("Barcode %s not found in any API", barcode)
        return ProductInfo(
            barcode=barcode,
            found=False,
            lookup_timestamp=datetime.now(timezone.utc).isoformat(),
        )

    def _lookup_openfoodfacts(self, barcode: str) -> ProductInfo:
        """Query Open Food Facts API.

        API docs: https://openfoodfacts.github.io/openfoodfacts-server/api/
        Rate limit: ~100 requests/minute.
        """
        url = f"{OFF_API_BASE}/{barcode}"
        params = {"fields": OFF_FIELDS}

        try:
            response = self.session.get(
                url, params=params, timeout=self.timeout
            )
        except requests.Timeout:
            logger.warning(
                "Open Food Facts API timeout for barcode %s", barcode
            )
            return ProductInfo(barcode=barcode, found=False)
        except requests.RequestException as e:
            logger.warning(
                "Open Food Facts API error for barcode %s: %s", barcode, e
            )
            return ProductInfo(barcode=barcode, found=False)

        if response.status_code != 200:
            logger.debug(
                "Open Food Facts returned status %d", response.status_code
            )
            return ProductInfo(barcode=barcode, found=False)

        data = response.json()
        product = data.get("product")

        if not product:
            logger.debug(
                "No product data in Open Food Facts response for %s", barcode
            )
            return ProductInfo(barcode=barcode, found=False)

        timestamp = datetime.now(timezone.utc).isoformat()

        # Parse structured per-component packaging data
        packaging_components = []
        for comp in product.get("packagings") or []:
            packaging_components.append(
                PackagingComponentInfo(
                    material=comp.get("material") or "",
                    shape=comp.get("shape") or "",
                    recycling=comp.get("recycling") or "",
                    number_of_units=comp.get("number_of_units") or 1,
                    weight_measured=comp.get("weight_measured"),
                    food_contact=comp.get("food_contact"),
                )
            )

        return ProductInfo(
            barcode=barcode,
            found=True,
            source_api="openfoodfacts",
            product_name=product.get("product_name", ""),
            product_name_de=product.get("product_name_de", ""),
            brands=product.get("brands", ""),
            quantity=product.get("quantity", ""),
            categories=product.get("categories", ""),
            categories_tags=product.get("categories_tags") or [],
            packaging=product.get("packaging", ""),
            packaging_tags=product.get("packaging_tags") or [],
            packaging_materials_tags=product.get("packaging_materials_tags")
            or [],
            packaging_components=packaging_components,
            ecoscore_grade=product.get("ecoscore_grade", ""),
            nutriscore_grade=product.get("nutriscore_grade", ""),
            labels=product.get("labels", ""),
            image_url=product.get("image_url")
            or product.get("image_front_url", ""),
            lookup_timestamp=timestamp,
        )

    def close(self):
        """Close the HTTP session."""
        self.session.close()
