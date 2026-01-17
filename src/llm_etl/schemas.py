"""
Pydantic schemas for LLM-based extraction.

These models mirror the Neo4j graph schema and are used for:
1. Structured LLM output via LangChain's with_structured_output()
2. Validation of extracted data before database insertion
3. Type hints and documentation

The schema IS the prompt - Pydantic field descriptions guide LLM extraction.
"""

from dataclasses import dataclass
from enum import Enum

from pydantic import BaseModel, Field


# =============================================================================
# WasteStream Reference Data (Frankfurt FES)
# =============================================================================


class WasteStreamType(str, Enum):
    """Known waste stream types in Frankfurt."""

    RESTABFALL = "Restabfalltonne"
    VERPACKUNG = "Verpackungstonne"
    ALTPAPIER = "Altpapiertonne"
    BIO = "Biotonne"
    ALTGLAS = "Altglascontainer"


@dataclass(frozen=True)
class WasteStreamReference:
    """Reference data for a waste stream type."""

    name: str
    english_name: str
    lid_color: str
    body_color: str
    available_sizes: tuple[str, ...]


# Frankfurt FES bin type reference data
WASTE_STREAM_REFERENCE: dict[WasteStreamType, WasteStreamReference] = {
    WasteStreamType.RESTABFALL: WasteStreamReference(
        name="Restabfalltonne",
        english_name="Residual Waste",
        lid_color="gray/black",
        body_color="gray/black",
        available_sizes=("80L", "120L", "240L", "770L", "1.1m³", "2.5m³", "5m³"),
    ),
    WasteStreamType.VERPACKUNG: WasteStreamReference(
        name="Verpackungstonne",
        english_name="Packaging",
        lid_color="yellow",
        body_color="gray/black",
        available_sizes=("120L", "240L", "770L"),
    ),
    WasteStreamType.ALTPAPIER: WasteStreamReference(
        name="Altpapiertonne",
        english_name="Paper/Cardboard",
        lid_color="blue/green",
        body_color="gray",
        available_sizes=("120L", "240L", "770L"),
    ),
    WasteStreamType.BIO: WasteStreamReference(
        name="Biotonne",
        english_name="Organic Waste",
        lid_color="brown",
        body_color="brown",
        available_sizes=("60L", "120L", "240L"),
    ),
    WasteStreamType.ALTGLAS: WasteStreamReference(
        name="Altglascontainer",
        english_name="Glass",
        lid_color="various",
        body_color="blue",
        available_sizes=("public containers only",),
    ),
}


def get_known_waste_stream_names() -> list[str]:
    """Get list of all known waste stream names."""
    return [ref.name for ref in WASTE_STREAM_REFERENCE.values()]


def lookup_waste_stream(name: str) -> WasteStreamReference | None:
    """Look up reference data for a waste stream by name."""
    for ref in WASTE_STREAM_REFERENCE.values():
        if ref.name.lower() == name.lower():
            return ref
    return None


# =============================================================================
# Pydantic Extraction Schemas
# =============================================================================


class WasteItemExtract(BaseModel):
    """Extracted waste item with disposal information."""

    name: str = Field(description="Name of the waste item in German")
    synonyms: list[str] = Field(
        default_factory=list, description="Alternative names or spellings"
    )
    disposal_targets: list[str] = Field(
        description="Facility or WasteStream names where this item can be disposed"
    )
    conditions: list[str] = Field(
        default_factory=list,
        description="Conditional disposal notes (e.g., 'if liquid → Schadstoffannahme')",
    )


class DisposalRuleExtract(BaseModel):
    """Conditional disposal rule (the 'Paint Can' problem)."""

    waste_item: str = Field(description="The waste item this rule applies to")
    condition: str = Field(
        description="The condition to check (e.g., 'liquid', 'dry', 'empty')"
    )
    if_true_target: str = Field(
        description="Disposal target if condition is true (facility or bin type)"
    )
    if_false_target: str = Field(
        default="", description="Disposal target if condition is false"
    )


class FacilityExtract(BaseModel):
    """Extracted facility information."""

    name: str = Field(description="Official facility name in German")
    address: str = Field(default="", description="Street address with postal code")
    opening_hours: str = Field(default="", description="Operating hours")
    contact: str = Field(default="", description="Phone number or email")
    additional_info: str = Field(
        default="", description="Holiday closures, special notes"
    )
    link: str = Field(default="", description="Website URL")
    accepted_items: list[str] = Field(
        default_factory=list, description="Waste types accepted at this facility"
    )


class WasteStreamExtract(BaseModel):
    """Extracted waste stream (bin type) with metadata."""

    name: str = Field(description="German name of the bin type (e.g., 'Restabfalltonne')")
    english_name: str = Field(default="", description="English translation")
    lid_color: str = Field(
        default="", description="Lid color (e.g., 'yellow', 'brown', 'gray')"
    )
    body_color: str = Field(default="", description="Body/container color")
    description: str = Field(default="", description="What goes in this bin")
    emptying_frequency: str = Field(
        default="", description="How often emptied (e.g., 'weekly', 'bi-weekly')"
    )
    available_sizes: list[str] = Field(
        default_factory=list, description="Available container sizes (e.g., '120L', '240L')"
    )


class RelationshipExtract(BaseModel):
    """Extracted relationship between entities."""

    source_type: str = Field(
        description="Source node label (e.g., 'WasteItem', 'Facility')"
    )
    source_name: str = Field(description="Source node name")
    relationship: str = Field(
        description="Relationship type (e.g., 'DISPOSED_AT', 'DISPOSED_IN')"
    )
    target_type: str = Field(description="Target node label")
    target_name: str = Field(description="Target node name")


class SourceExtract(BaseModel):
    """Source document for provenance tracking."""

    name: str = Field(description="Filename or document title")
    file_type: str = Field(
        description="File type (e.g., 'pdf', 'docx', 'csv', 'json', 'web')"
    )
    file_path: str = Field(default="", description="Path to source file")
    url: str = Field(default="", description="Source URL if web-scraped")


# Mapping of schema names to classes for CLI
SCHEMA_REGISTRY: dict[str, type[BaseModel]] = {
    "WasteItem": WasteItemExtract,
    "DisposalRule": DisposalRuleExtract,
    "Facility": FacilityExtract,
    "WasteStream": WasteStreamExtract,
    "Relationship": RelationshipExtract,
    "Source": SourceExtract,
}


# =============================================================================
# Schema Utilities
# =============================================================================


def get_schema_names() -> list[str]:
    """Get list of all registered schema names."""
    return list(SCHEMA_REGISTRY.keys())


def get_schema(name: str) -> type[BaseModel] | None:
    """Get schema class by name (case-insensitive)."""
    # Try exact match first
    if name in SCHEMA_REGISTRY:
        return SCHEMA_REGISTRY[name]
    # Try case-insensitive match
    name_lower = name.lower()
    for key, schema in SCHEMA_REGISTRY.items():
        if key.lower() == name_lower:
            return schema
    return None


def get_field_names(schema: type[BaseModel]) -> list[str]:
    """Get all field names for a schema."""
    return list(schema.model_fields.keys())


def get_required_fields(schema: type[BaseModel]) -> list[str]:
    """Get required field names for a schema (fields without defaults)."""
    return [
        name
        for name, field in schema.model_fields.items()
        if field.is_required()
    ]


def get_optional_fields(schema: type[BaseModel]) -> list[str]:
    """Get optional field names for a schema (fields with defaults)."""
    return [
        name
        for name, field in schema.model_fields.items()
        if not field.is_required()
    ]


def get_field_descriptions(schema: type[BaseModel]) -> dict[str, str]:
    """Get field descriptions for a schema (used in LLM prompts)."""
    return {
        name: field.description or ""
        for name, field in schema.model_fields.items()
    }


def schema_to_prompt_hint(schema: type[BaseModel]) -> str:
    """Generate a prompt hint from schema for LLM context."""
    lines = [f"Entity type: {schema.__name__}"]
    if schema.__doc__:
        lines.append(f"Description: {schema.__doc__.strip()}")
    lines.append("Fields:")
    for name, field in schema.model_fields.items():
        required = "(required)" if field.is_required() else "(optional)"
        desc = field.description or "no description"
        lines.append(f"  - {name} {required}: {desc}")
    return "\n".join(lines)
