"""
Web search tools for the orchestrator agent.

Provides optional web search enrichment capabilities.
This is a placeholder - implement with actual search API when needed.
"""

import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class SearchResult:
    """A web search result."""

    title: str
    url: str
    snippet: str


def web_search(
    query: str,
    max_results: int = 5,
) -> list[SearchResult]:
    """
    Search the web for information.

    This is a placeholder implementation. To enable:
    1. Add a search API (e.g., SerpAPI, Google Custom Search, DuckDuckGo)
    2. Configure API key in .env
    3. Implement the actual search logic

    Args:
        query: Search query string
        max_results: Maximum number of results to return

    Returns:
        List of SearchResult objects (empty in placeholder mode)
    """
    logger.warning(
        "web_search is a placeholder. Implement with actual search API for enrichment."
    )
    return []


def search_avv_code(waste_item_name: str) -> str | None:
    """
    Search for AVV code for a waste item.

    AVV (Abfallverzeichnis-Verordnung) codes are German waste classification codes.

    Args:
        waste_item_name: Name of the waste item in German

    Returns:
        AVV code string or None if not found
    """
    logger.info(f"Searching AVV code for: {waste_item_name}")
    return None


def search_facility_info(facility_name: str) -> dict | None:
    """
    Search for additional facility information.

    Args:
        facility_name: Name of the facility

    Returns:
        Dictionary with facility info or None
    """
    logger.info(f"Searching facility info for: {facility_name}")
    return None
