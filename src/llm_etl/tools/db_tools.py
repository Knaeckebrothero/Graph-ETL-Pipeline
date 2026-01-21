"""
Database tools for the orchestrator agent.

Provides tools to query Neo4j and validate extractions.
"""

import logging
from typing import Any

from pydantic import BaseModel

from src.db import neo4j_db
from src.llm_etl.schemas import SCHEMA_REGISTRY

logger = logging.getLogger(__name__)


def query_db(cypher: str, params: dict | None = None) -> list[dict]:
    """
    Execute a Cypher query against Neo4j.

    Args:
        cypher: Cypher query string
        params: Optional query parameters

    Returns:
        List of result dictionaries
    """
    try:
        results = neo4j_db.query(cypher, params or {})
        logger.debug(f"Query returned {len(results)} results")
        return results
    except Exception as e:
        logger.error(f"Database query failed: {e}")
        return []


def get_existing_entities(entity_type: str, limit: int = 1000) -> list[str]:
    """
    Get names of existing entities of a given type.

    Useful for providing context to the extraction chain about
    what entities already exist in the database.

    Args:
        entity_type: Node label (e.g., 'WasteItem', 'Facility', 'WasteStream')
        limit: Maximum number of entities to return

    Returns:
        List of entity names
    """
    cypher = f"MATCH (n:{entity_type}) RETURN n.name AS name LIMIT $limit"
    results = query_db(cypher, {"limit": limit})
    return [r["name"] for r in results if r.get("name")]


def get_entity_by_name(entity_type: str, name: str) -> dict | None:
    """
    Get a single entity by type and name.

    Args:
        entity_type: Node label
        name: Entity name

    Returns:
        Entity properties dict or None if not found
    """
    cypher = f"MATCH (n:{entity_type} {{name: $name}}) RETURN n LIMIT 1"
    results = query_db(cypher, {"name": name})
    if results:
        return dict(results[0]["n"])
    return None


def find_gaps(
    entity_type: str,
    extracted_names: list[str],
) -> dict[str, list[str]]:
    """
    Compare extracted entity names against database to find gaps.

    Args:
        entity_type: Node label to check
        extracted_names: List of names extracted from document

    Returns:
        Dictionary with:
        - 'new': Names not in database (to be created)
        - 'existing': Names already in database (for enrichment/linking)
        - 'db_only': Names in database but not in extraction (for reference)
    """
    existing_names = set(get_existing_entities(entity_type))
    extracted_set = set(extracted_names)

    return {
        "new": list(extracted_set - existing_names),
        "existing": list(extracted_set & existing_names),
        "db_only": list(existing_names - extracted_set),
    }


def validate_extraction(
    entity: BaseModel,
    entity_type: str,
) -> dict[str, Any]:
    """
    Validate an extracted entity against database constraints.

    Checks:
    - Required fields are present
    - Referenced entities exist (e.g., disposal targets)
    - No duplicates would be created

    Args:
        entity: Extracted Pydantic model instance
        entity_type: Entity type name (e.g., 'WasteItem')

    Returns:
        Validation result with:
        - 'valid': bool - whether entity passes validation
        - 'warnings': list of non-critical issues
        - 'errors': list of critical issues
    """
    warnings = []
    errors = []

    # Get entity data
    entity_data = entity.model_dump()
    name = entity_data.get("name", "")

    # Check if entity already exists
    existing = get_entity_by_name(entity_type, name)
    if existing:
        warnings.append(f"{entity_type} '{name}' already exists in database")

    # Entity-specific validation
    if entity_type == "WasteItem":
        _validate_waste_item(entity_data, warnings, errors)
    elif entity_type == "Facility":
        _validate_facility(entity_data, warnings, errors)
    elif entity_type == "DisposalRule":
        _validate_disposal_rule(entity_data, warnings, errors)

    return {
        "valid": len(errors) == 0,
        "warnings": warnings,
        "errors": errors,
    }


def _validate_waste_item(data: dict, warnings: list, errors: list) -> None:
    """Validate WasteItem-specific constraints."""
    disposal_targets = data.get("disposal_targets", [])

    if not disposal_targets:
        errors.append("WasteItem has no disposal targets")
        return

    # Check if disposal targets exist
    existing_facilities = set(get_existing_entities("Facility"))
    existing_streams = set(get_existing_entities("WasteStream"))
    all_targets = existing_facilities | existing_streams

    for target in disposal_targets:
        if target not in all_targets:
            warnings.append(f"Disposal target '{target}' not found in database")


def _validate_facility(data: dict, warnings: list, errors: list) -> None:
    """Validate Facility-specific constraints."""
    name = data.get("name", "")
    if not name:
        errors.append("Facility must have a name")

    # Check for common facility name patterns
    if name and len(name) < 3:
        warnings.append(f"Facility name '{name}' seems too short")


def _validate_disposal_rule(data: dict, warnings: list, errors: list) -> None:
    """Validate DisposalRule-specific constraints."""
    waste_item = data.get("waste_item", "")
    if_true_target = data.get("if_true_target", "")

    if not waste_item:
        errors.append("DisposalRule must specify a waste_item")
    if not if_true_target:
        errors.append("DisposalRule must have an if_true_target")

    # Check if referenced waste item exists
    if waste_item:
        existing = get_entity_by_name("WasteItem", waste_item)
        if not existing:
            warnings.append(f"Referenced WasteItem '{waste_item}' not in database")


def get_database_stats() -> dict:
    """
    Get current database statistics.

    Returns:
        Dictionary with node counts and relationship count
    """
    try:
        return neo4j_db.get_stats()
    except Exception as e:
        logger.error(f"Failed to get database stats: {e}")
        return {"node_counts": {}, "relationship_count": 0, "total_nodes": 0}


def get_orphaned_items() -> list[str]:
    """
    Find WasteItems with no disposal relationships.

    Returns:
        List of orphaned WasteItem names
    """
    cypher = """
    MATCH (w:WasteItem)
    WHERE NOT (w)-[:DISPOSED_IN|DISPOSED_AT]->()
    RETURN w.name AS name
    """
    results = query_db(cypher)
    return [r["name"] for r in results if r.get("name")]


def get_items_per_stream() -> dict[str, int]:
    """
    Get count of WasteItems per WasteStream.

    Returns:
        Dictionary mapping stream name to item count
    """
    cypher = """
    MATCH (w:WasteItem)-[:DISPOSED_IN]->(s:WasteStream)
    RETURN s.name AS stream, count(w) AS count
    ORDER BY count DESC
    """
    results = query_db(cypher)
    return {r["stream"]: r["count"] for r in results}
