"""
opengtindb.org barcode resolver.

German open EAN database with ~50K products. Returns product name, category,
vendor, and bitwise flags for packaging material and contents.

API docs: https://opengtindb.org/api.php
License: GNU FDL (free, open data)
Registration: https://opengtindb.org/userid.php (free, required for queryid)

Response format: key=value plain text, NOT JSON.

Usage:
    client = OpenGTINDBClient(query_id="400000000")
    product = client.lookup("4006381333931")
    if product.found:
        print(product.product_name, product.brands)
"""

import logging
import os
import time
from datetime import datetime, timezone

import requests

from src.barcode.models import ProductInfo

logger = logging.getLogger(__name__)

API_URL = "https://opengtindb.org/"
RATE_LIMIT_DELAY = 1.0  # seconds between requests
TIMEOUT = 10

# Packaging material bitwise flags → waste-relevant tags
PACK_FLAGS = {
    1: "pack:plastic",
    2: "pack:composite",
    4: "pack:paper-cardboard",
    8: "pack:glass-ceramic",
    16: "pack:metal",
    32: "pack:unpackaged",
    64: "pack:plastic-free",
    128: "pack:over-packaged",
    256: "pack:minimal-packaging",
    512: "pack:returnable-reusable",
}

# Contents bitwise flags
CONTENTS_FLAGS = {
    1: "contents:lactose-free",
    2: "contents:caffeine-free",
    4: "contents:dietary",
    8: "contents:gluten-free",
    16: "contents:fructose-free",
    32: "contents:organic",
    64: "contents:fair-trade",
    128: "contents:vegetarian",
    256: "contents:vegan",
    512: "contents:microplastic-warning",
    1024: "contents:mineral-oil-warning",
    2048: "contents:nicotine-warning",
}

# 26 main categories (1-indexed to match API)
MAINCAT = {
    1: "Baby/Kind",
    2: "Backwaren",
    3: "Brotaufstriche",
    4: "Desserts",
    5: "Eier",
    6: "Elektro",
    7: "Elektronik",
    8: "Fertiggerichte",
    9: "Fleisch/Fisch",
    10: "Früchte",
    11: "Gemüse",
    12: "Getränke/Alkohol",
    13: "Haushalt",
    14: "Kochen/Backen",
    15: "Konfitüre",
    16: "Kosmetik",
    17: "Milchprodukte",
    18: "Nahrungsergänzung",
    19: "Tabak",
    20: "Sojaprodukte",
    21: "Süßwaren/Snacks",
    22: "Getreide",
    23: "Tierbedarf",
    24: "Reinigung",
    25: "Zusatzstoffe",
    26: "Wohnen/Freizeit",
}


def _decode_flags(value: int, flag_map: dict[int, str]) -> list[str]:
    """Decode bitwise OR-combined flags into tag list."""
    tags = []
    for bit, tag in sorted(flag_map.items()):
        if value & bit:
            tags.append(tag)
    return tags


def _parse_response(text: str) -> dict[str, str]:
    """Parse key=value plain text response into a dict.

    Multiple results separated by '---'; we take the first (most relevant).
    """
    fields = {}
    for line in text.strip().splitlines():
        line = line.strip()
        if line == "---":
            break  # only first result
        if "=" in line:
            key, _, value = line.partition("=")
            fields[key.strip()] = value.strip()
    return fields


class OpenGTINDBClient:
    """Client for the opengtindb.org barcode lookup API."""

    def __init__(
        self,
        query_id: str | None = None,
        rate_limit: float = RATE_LIMIT_DELAY,
    ):
        self.query_id = query_id or os.getenv("OPENGTINDB_QUERY_ID", "")
        self.rate_limit = rate_limit
        self.session = requests.Session()
        self.session.headers.update(
            {"User-Agent": "FessiWasteBot/1.0 (https://github.com/fessi-chatbot)"}
        )
        self._last_request = 0.0

    def _wait_rate_limit(self):
        """Enforce rate limiting between requests."""
        elapsed = time.time() - self._last_request
        if elapsed < self.rate_limit:
            time.sleep(self.rate_limit - elapsed)
        self._last_request = time.time()

    def lookup(self, barcode: str) -> ProductInfo:
        """Look up a single EAN barcode.

        Args:
            barcode: EAN-8 or EAN-13 barcode string.

        Returns:
            ProductInfo with found=True if product exists, found=False otherwise.
        """
        if not self.query_id:
            logger.warning("OPENGTINDB_QUERY_ID not set, skipping opengtindb lookup")
            return ProductInfo(barcode=barcode, found=False)

        self._wait_rate_limit()

        params = {
            "ean": barcode,
            "cmd": "query",
            "queryid": self.query_id,
        }

        try:
            response = self.session.get(API_URL, params=params, timeout=TIMEOUT)
        except requests.Timeout:
            logger.warning("opengtindb timeout for barcode %s", barcode)
            return ProductInfo(barcode=barcode, found=False)
        except requests.RequestException as e:
            logger.warning("opengtindb error for barcode %s: %s", barcode, e)
            return ProductInfo(barcode=barcode, found=False)

        if response.status_code != 200:
            logger.debug("opengtindb returned status %d", response.status_code)
            return ProductInfo(barcode=barcode, found=False)

        fields = _parse_response(response.text)

        # Check for API errors (error=0 means success)
        error_code = fields.get("error", "1")
        if error_code != "0":
            logger.debug(
                "opengtindb error %s for barcode %s", error_code, barcode
            )
            return ProductInfo(barcode=barcode, found=False)

        # Parse packaging and contents flags
        pack_value = int(fields.get("pack", "0") or "0")
        contents_value = int(fields.get("contents", "0") or "0")
        pack_tags = _decode_flags(pack_value, PACK_FLAGS)
        contents_tags = _decode_flags(contents_value, CONTENTS_FLAGS)

        # Build category tags from maincat
        categories_tags = []
        maincat = fields.get("maincat", "")
        subcat = fields.get("subcat", "")
        if maincat:
            categories_tags.append(f"opengtindb:{maincat}")
        if subcat:
            categories_tags.append(f"opengtindb:{subcat}")

        timestamp = datetime.now(timezone.utc).isoformat()

        return ProductInfo(
            barcode=barcode,
            found=True,
            source_api="opengtindb",
            product_name=fields.get("detailname") or fields.get("name", ""),
            product_name_de=fields.get("name", ""),
            brands=fields.get("vendor", ""),
            categories=maincat,
            categories_tags=categories_tags + contents_tags,
            packaging_tags=pack_tags,
            labels=fields.get("origin", ""),
            lookup_timestamp=timestamp,
        )

    def close(self):
        """Close the HTTP session."""
        self.session.close()
