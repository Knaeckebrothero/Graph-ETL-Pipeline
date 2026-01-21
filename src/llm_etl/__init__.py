"""
LLM-based ETL Pipeline for extracting structured data from unstructured documents.

This package provides:
- ExtractionChain: Generic, schema-driven extraction using LangChain
- Orchestrator: LangGraph-based document processing pipeline
- EntityLoader: Database loader with provenance tracking
- Pydantic schemas for all entity types (WasteItem, Facility, etc.)
- WasteStream reference data for Frankfurt FES bin types
- Schema utility functions for dynamic schema handling
- Tools for database queries, document processing, and web search
"""

from src.llm_etl.extraction_chain import ExtractionChain
from src.llm_etl.loader import EntityLoader, LoadedEntity, LoadResult
from src.llm_etl.orchestrator import Orchestrator, OrchestratorResult
from src.llm_etl.schemas import (
    # Schema registry
    SCHEMA_REGISTRY,
    # Extraction schemas
    DisposalRuleExtract,
    FacilityExtract,
    RelationshipExtract,
    SourceExtract,
    WasteItemExtract,
    WasteStreamExtract,
    # WasteStream reference data
    WASTE_STREAM_REFERENCE,
    WasteStreamReference,
    WasteStreamType,
    get_known_waste_stream_names,
    lookup_waste_stream,
    # Schema utilities
    get_field_descriptions,
    get_field_names,
    get_optional_fields,
    get_required_fields,
    get_schema,
    get_schema_names,
    schema_to_prompt_hint,
)

__all__ = [
    # Core extraction
    "ExtractionChain",
    # Orchestrator
    "Orchestrator",
    "OrchestratorResult",
    # Database loader
    "EntityLoader",
    "LoadResult",
    "LoadedEntity",
    # Extraction schemas
    "WasteItemExtract",
    "DisposalRuleExtract",
    "FacilityExtract",
    "WasteStreamExtract",
    "RelationshipExtract",
    "SourceExtract",
    "SCHEMA_REGISTRY",
    # WasteStream reference data
    "WasteStreamType",
    "WasteStreamReference",
    "WASTE_STREAM_REFERENCE",
    "get_known_waste_stream_names",
    "lookup_waste_stream",
    # Schema utilities
    "get_schema_names",
    "get_schema",
    "get_field_names",
    "get_required_fields",
    "get_optional_fields",
    "get_field_descriptions",
    "schema_to_prompt_hint",
]
