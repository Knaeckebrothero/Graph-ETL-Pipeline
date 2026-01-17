"""
Agent tools for the LLM-based ETL orchestrator.

This package provides tools that the orchestrator agent can use:
- db_tools: Query Neo4j, validate extractions against schema
- doc_tools: Document chunking and parsing
- web_tools: Web search for enrichment (optional)
"""

from src.llm_etl.tools.db_tools import (
    get_existing_entities,
    query_db,
    validate_extraction,
)
from src.llm_etl.tools.doc_tools import (
    chunk_document,
    detect_document_type,
    estimate_entity_types,
)
from src.llm_etl.tools.web_tools import web_search

__all__ = [
    # DB tools
    "query_db",
    "get_existing_entities",
    "validate_extraction",
    # Doc tools
    "chunk_document",
    "detect_document_type",
    "estimate_entity_types",
    # Web tools
    "web_search",
]
