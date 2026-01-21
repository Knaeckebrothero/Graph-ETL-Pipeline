"""
Database loader for extracted entities with provenance tracking.

Handles insertion of extracted entities into Neo4j with:
- MERGE pattern for idempotent insertion
- Provenance tracking via Source nodes and DERIVED_FROM relationships
- Enrichment strategy for existing Facility/WasteStream nodes
- Validation and error handling

Usage:
    loader = EntityLoader()
    result = loader.load(
        entities=[...],
        source=SourceExtract(name="document.pdf", file_type="pdf"),
        dry_run=False,
    )
"""

import hashlib
import logging
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from pydantic import BaseModel

from src.db import neo4j_db
from src.llm_etl.schemas import (
    DisposalRuleExtract,
    FacilityExtract,
    RelationshipExtract,
    SourceExtract,
    WasteItemExtract,
    WasteStreamExtract,
)

logger = logging.getLogger(__name__)


# =============================================================================
# Result Types
# =============================================================================


@dataclass
class LoadedEntity:
    """Result of loading a single entity."""

    entity_type: str
    name: str
    uid: str
    action: str  # 'created', 'updated', 'enriched', 'skipped'
    changes: dict = field(default_factory=dict)
    error: str = ""


@dataclass
class LoadResult:
    """Result of the load operation."""

    success: bool
    source_uid: str
    entities_processed: int
    entities_created: int
    entities_updated: int
    entities_enriched: int
    entities_skipped: int
    errors: list[str]
    loaded_entities: list[LoadedEntity]
    dry_run: bool = False


# =============================================================================
# UID Generation
# =============================================================================


def generate_uid(name: str) -> str:
    """
    Generate a deterministic UID from a name.

    Uses SHA256 hash of the canonical (lowercase, stripped) name.

    Args:
        name: Entity name

    Returns:
        16-character hex string UID
    """
    canonical = name.strip().lower()
    return hashlib.sha256(canonical.encode()).hexdigest()[:16]


def generate_source_uid(source: SourceExtract) -> str:
    """
    Generate a UID for a source document.

    Uses combination of name, file_type, and file_path.

    Args:
        source: Source document info

    Returns:
        16-character hex string UID
    """
    canonical = f"{source.name}|{source.file_type}|{source.file_path}".lower()
    return hashlib.sha256(canonical.encode()).hexdigest()[:16]


# =============================================================================
# Entity Loader
# =============================================================================


class EntityLoader:
    """
    Load extracted entities into Neo4j with provenance tracking.

    Supports:
    - WasteItem, Facility, WasteStream, DisposalRule entities
    - Source nodes for provenance tracking
    - DERIVED_FROM relationships linking entities to sources
    - Enrichment of existing nodes (update properties without duplicating)
    - Dry-run mode for testing without database changes
    """

    def __init__(self):
        """Initialize the entity loader."""
        self._db = neo4j_db

    def load(
        self,
        entities: list[BaseModel],
        source: SourceExtract,
        dry_run: bool = False,
    ) -> LoadResult:
        """
        Load extracted entities into Neo4j.

        Args:
            entities: List of extracted Pydantic model instances
            source: Source document for provenance tracking
            dry_run: If True, simulate but don't execute database changes

        Returns:
            LoadResult with statistics and details
        """
        logger.info(
            f"Loading {len(entities)} entities from source: {source.name} "
            f"{'(dry-run)' if dry_run else ''}"
        )

        errors: list[str] = []
        loaded_entities: list[LoadedEntity] = []
        created = 0
        updated = 0
        enriched = 0
        skipped = 0

        # Create source node first
        source_uid = generate_source_uid(source)
        if not dry_run:
            try:
                self._create_source_node(source, source_uid)
                logger.info(f"Created/updated source node: {source_uid}")
            except Exception as e:
                logger.error(f"Failed to create source node: {e}")
                errors.append(f"Source node creation failed: {e}")

        # Process each entity
        for entity in entities:
            try:
                result = self._load_entity(entity, source_uid, dry_run)
                loaded_entities.append(result)

                if result.error:
                    errors.append(result.error)
                    skipped += 1
                elif result.action == "created":
                    created += 1
                elif result.action == "updated":
                    updated += 1
                elif result.action == "enriched":
                    enriched += 1
                else:
                    skipped += 1

            except Exception as e:
                logger.error(f"Failed to load entity: {e}")
                errors.append(str(e))
                skipped += 1

        success = len(errors) == 0

        result = LoadResult(
            success=success,
            source_uid=source_uid,
            entities_processed=len(entities),
            entities_created=created,
            entities_updated=updated,
            entities_enriched=enriched,
            entities_skipped=skipped,
            errors=errors,
            loaded_entities=loaded_entities,
            dry_run=dry_run,
        )

        logger.info(
            f"Load complete: {created} created, {updated} updated, "
            f"{enriched} enriched, {skipped} skipped"
        )

        return result

    def _create_source_node(self, source: SourceExtract, source_uid: str) -> None:
        """
        Create or update a Source node for provenance tracking.

        Args:
            source: Source document info
            source_uid: Generated UID for the source
        """
        cypher = """
        MERGE (s:Source {uid: $uid})
        SET s.name = $name,
            s.file_type = $file_type,
            s.file_path = $file_path,
            s.url = $url,
            s.import_date = datetime()
        RETURN s
        """
        self._db.query(
            cypher,
            {
                "uid": source_uid,
                "name": source.name,
                "file_type": source.file_type,
                "file_path": source.file_path,
                "url": source.url,
            },
        )

    def _load_entity(
        self,
        entity: BaseModel,
        source_uid: str,
        dry_run: bool,
    ) -> LoadedEntity:
        """
        Load a single entity into the database.

        Args:
            entity: Pydantic model instance
            source_uid: UID of the source document
            dry_run: If True, simulate but don't execute

        Returns:
            LoadedEntity with action result
        """
        # Dispatch based on entity type
        if isinstance(entity, WasteItemExtract):
            return self._load_waste_item(entity, source_uid, dry_run)
        elif isinstance(entity, FacilityExtract):
            return self._load_facility(entity, source_uid, dry_run)
        elif isinstance(entity, WasteStreamExtract):
            return self._load_waste_stream(entity, source_uid, dry_run)
        elif isinstance(entity, DisposalRuleExtract):
            return self._load_disposal_rule(entity, source_uid, dry_run)
        elif isinstance(entity, RelationshipExtract):
            return self._load_relationship(entity, source_uid, dry_run)
        else:
            entity_type = type(entity).__name__
            return LoadedEntity(
                entity_type=entity_type,
                name="unknown",
                uid="",
                action="skipped",
                error=f"Unsupported entity type: {entity_type}",
            )

    def _load_waste_item(
        self,
        entity: WasteItemExtract,
        source_uid: str,
        dry_run: bool,
    ) -> LoadedEntity:
        """Load a WasteItem entity."""
        uid = generate_uid(entity.name)
        action = "created"
        changes = {}

        # Check if exists (only if not dry-run)
        if not dry_run:
            existing = self._get_existing_node("WasteItem", uid)
            if existing:
                action = "updated"

        # Build changes dict for output
        changes = {
            "synonyms": entity.synonyms,
            "disposal_targets": entity.disposal_targets,
            "conditions": entity.conditions,
        }

        if not dry_run:
            # Create/update WasteItem node
            cypher = """
            MERGE (w:WasteItem {uid: $uid})
            SET w.name = $name
            WITH w
            OPTIONAL MATCH (s:Source {uid: $source_uid})
            MERGE (w)-[:DERIVED_FROM]->(s)
            RETURN w
            """
            self._db.query(
                cypher,
                {
                    "uid": uid,
                    "name": entity.name,
                    "source_uid": source_uid,
                },
            )

            # Handle synonyms (stored as property)
            if entity.synonyms:
                self._db.query(
                    "MATCH (w:WasteItem {uid: $uid}) SET w.synonyms = $synonyms",
                    {"uid": uid, "synonyms": entity.synonyms},
                )

            # Create relationships to disposal targets
            for target in entity.disposal_targets:
                self._create_disposal_relationship(uid, target)

        return LoadedEntity(
            entity_type="WasteItem",
            name=entity.name,
            uid=uid,
            action=action,
            changes=changes,
        )

    def _load_facility(
        self,
        entity: FacilityExtract,
        source_uid: str,
        dry_run: bool,
    ) -> LoadedEntity:
        """Load or enrich a Facility entity."""
        uid = generate_uid(entity.name)
        action = "created"
        changes = {}
        existing = None

        # Build changes dict for output
        changes = {
            "address": entity.address,
            "opening_hours": entity.opening_hours,
            "contact": entity.contact,
            "accepted_items": entity.accepted_items,
        }

        # Check if exists - if so, we enrich instead of create (only if not dry-run)
        if not dry_run:
            existing = self._get_existing_node("Facility", uid)
            if existing:
                action = "enriched"

            if existing:
                # Enrich existing facility with extracted info
                cypher = """
                MATCH (f:Facility {uid: $uid})
                SET f.address = CASE WHEN $address <> '' THEN $address ELSE f.address END,
                    f.opening_hours = CASE WHEN $opening_hours <> '' THEN $opening_hours ELSE f.opening_hours END,
                    f.contact = CASE WHEN $contact <> '' THEN $contact ELSE f.contact END,
                    f.additional_info = CASE WHEN $additional_info <> '' THEN $additional_info ELSE f.additional_info END,
                    f.link = CASE WHEN $link <> '' THEN $link ELSE f.link END
                WITH f
                OPTIONAL MATCH (s:Source {uid: $source_uid})
                MERGE (f)-[:DERIVED_FROM]->(s)
                RETURN f
                """
            else:
                # Create new facility
                cypher = """
                MERGE (f:Facility {uid: $uid})
                SET f.name = $name,
                    f.address = $address,
                    f.opening_hours = $opening_hours,
                    f.contact = $contact,
                    f.additional_info = $additional_info,
                    f.link = $link
                WITH f
                OPTIONAL MATCH (s:Source {uid: $source_uid})
                MERGE (f)-[:DERIVED_FROM]->(s)
                RETURN f
                """

            self._db.query(
                cypher,
                {
                    "uid": uid,
                    "name": entity.name,
                    "address": entity.address,
                    "opening_hours": entity.opening_hours,
                    "contact": entity.contact,
                    "additional_info": entity.additional_info,
                    "link": entity.link,
                    "source_uid": source_uid,
                },
            )

        return LoadedEntity(
            entity_type="Facility",
            name=entity.name,
            uid=uid,
            action=action,
            changes=changes,
        )

    def _load_waste_stream(
        self,
        entity: WasteStreamExtract,
        source_uid: str,
        dry_run: bool,
    ) -> LoadedEntity:
        """Load or enrich a WasteStream entity."""
        uid = generate_uid(entity.name)
        action = "created"
        existing = None

        # Build changes dict for output
        changes = {
            "english_name": entity.english_name,
            "lid_color": entity.lid_color,
            "body_color": entity.body_color,
            "description": entity.description,
            "emptying_frequency": entity.emptying_frequency,
            "available_sizes": entity.available_sizes,
        }

        # Check if exists - if so, we enrich (only if not dry-run)
        if not dry_run:
            existing = self._get_existing_node("WasteStream", uid)
            if existing:
                action = "enriched"

            if existing:
                # Enrich existing waste stream
                cypher = """
                MATCH (s:WasteStream {uid: $uid})
                SET s.english_name = CASE WHEN $english_name <> '' THEN $english_name ELSE s.english_name END,
                    s.lid_color = CASE WHEN $lid_color <> '' THEN $lid_color ELSE s.lid_color END,
                    s.body_color = CASE WHEN $body_color <> '' THEN $body_color ELSE s.body_color END,
                    s.description = CASE WHEN $description <> '' THEN $description ELSE s.description END,
                    s.emptying_frequency = CASE WHEN $emptying_frequency <> '' THEN $emptying_frequency ELSE s.emptying_frequency END,
                    s.available_sizes = CASE WHEN size($available_sizes) > 0 THEN $available_sizes ELSE s.available_sizes END
                WITH s
                OPTIONAL MATCH (src:Source {uid: $source_uid})
                MERGE (s)-[:DERIVED_FROM]->(src)
                RETURN s
                """
            else:
                # Create new waste stream
                cypher = """
                MERGE (s:WasteStream {uid: $uid})
                SET s.name = $name,
                    s.english_name = $english_name,
                    s.lid_color = $lid_color,
                    s.body_color = $body_color,
                    s.description = $description,
                    s.emptying_frequency = $emptying_frequency,
                    s.available_sizes = $available_sizes
                WITH s
                OPTIONAL MATCH (src:Source {uid: $source_uid})
                MERGE (s)-[:DERIVED_FROM]->(src)
                RETURN s
                """

            self._db.query(
                cypher,
                {
                    "uid": uid,
                    "name": entity.name,
                    "english_name": entity.english_name,
                    "lid_color": entity.lid_color,
                    "body_color": entity.body_color,
                    "description": entity.description,
                    "emptying_frequency": entity.emptying_frequency,
                    "available_sizes": entity.available_sizes,
                    "source_uid": source_uid,
                },
            )

        return LoadedEntity(
            entity_type="WasteStream",
            name=entity.name,
            uid=uid,
            action=action,
            changes=changes,
        )

    def _load_disposal_rule(
        self,
        entity: DisposalRuleExtract,
        source_uid: str,
        dry_run: bool,
    ) -> LoadedEntity:
        """Load a DisposalRule entity."""
        # Create a UID from the combination of waste_item and condition
        uid = generate_uid(f"{entity.waste_item}|{entity.condition}")
        action = "created"

        # Build changes dict for output
        changes = {
            "condition": entity.condition,
            "if_true_target": entity.if_true_target,
            "if_false_target": entity.if_false_target,
        }

        # Check if exists and execute database operations (only if not dry-run)
        if not dry_run:
            existing = self._get_existing_node("DisposalRule", uid)
            if existing:
                action = "updated"

            cypher = """
            MERGE (r:DisposalRule {uid: $uid})
            SET r.waste_item = $waste_item,
                r.condition = $condition,
                r.if_true_target = $if_true_target,
                r.if_false_target = $if_false_target
            WITH r
            OPTIONAL MATCH (s:Source {uid: $source_uid})
            MERGE (r)-[:DERIVED_FROM]->(s)
            RETURN r
            """
            self._db.query(
                cypher,
                {
                    "uid": uid,
                    "waste_item": entity.waste_item,
                    "condition": entity.condition,
                    "if_true_target": entity.if_true_target,
                    "if_false_target": entity.if_false_target,
                    "source_uid": source_uid,
                },
            )

            # Link to WasteItem if exists
            self._db.query(
                """
                MATCH (r:DisposalRule {uid: $rule_uid})
                MATCH (w:WasteItem {name: $waste_item})
                MERGE (w)-[:HAS_RULE]->(r)
                """,
                {"rule_uid": uid, "waste_item": entity.waste_item},
            )

            # Link to targets (WasteStream or Facility)
            for target, rel_type in [
                (entity.if_true_target, "IF_TRUE"),
                (entity.if_false_target, "IF_FALSE"),
            ]:
                if target:
                    self._db.query(
                        """
                        MATCH (r:DisposalRule {uid: $rule_uid})
                        OPTIONAL MATCH (ws:WasteStream {name: $target})
                        OPTIONAL MATCH (f:Facility {name: $target})
                        WITH r, COALESCE(ws, f) AS target
                        WHERE target IS NOT NULL
                        MERGE (r)-[:ROUTES_TO {condition: $rel_type}]->(target)
                        """,
                        {"rule_uid": uid, "target": target, "rel_type": rel_type},
                    )

        return LoadedEntity(
            entity_type="DisposalRule",
            name=f"{entity.waste_item} ({entity.condition})",
            uid=uid,
            action=action,
            changes=changes,
        )

    def _load_relationship(
        self,
        entity: RelationshipExtract,
        source_uid: str,
        dry_run: bool,
    ) -> LoadedEntity:
        """Load a relationship between existing entities."""
        uid = generate_uid(
            f"{entity.source_type}:{entity.source_name}|"
            f"{entity.relationship}|"
            f"{entity.target_type}:{entity.target_name}"
        )
        action = "created"

        if not dry_run:
            # Dynamically create relationship between nodes
            # Note: This uses APOC-free approach with specific relationship types
            rel_type = entity.relationship.upper().replace(" ", "_")

            # Build query based on known relationship types
            if rel_type == "DISPOSED_AT":
                cypher = """
                MATCH (source:WasteItem {name: $source_name})
                MATCH (target:Facility {name: $target_name})
                MERGE (source)-[:DISPOSED_AT]->(target)
                """
            elif rel_type == "DISPOSED_IN":
                cypher = """
                MATCH (source:WasteItem {name: $source_name})
                MATCH (target:WasteStream {name: $target_name})
                MERGE (source)-[:DISPOSED_IN]->(target)
                """
            elif rel_type == "HAS_RULE":
                cypher = """
                MATCH (source:WasteItem {name: $source_name})
                MATCH (target:DisposalRule {name: $target_name})
                MERGE (source)-[:HAS_RULE]->(target)
                """
            elif rel_type == "ROUTES_TO":
                cypher = """
                MATCH (source:DisposalRule {uid: $source_name})
                OPTIONAL MATCH (ws:WasteStream {name: $target_name})
                OPTIONAL MATCH (f:Facility {name: $target_name})
                WITH source, COALESCE(ws, f) AS target
                WHERE target IS NOT NULL
                MERGE (source)-[:ROUTES_TO]->(target)
                """
            else:
                return LoadedEntity(
                    entity_type="Relationship",
                    name=f"{entity.source_name} -> {entity.target_name}",
                    uid=uid,
                    action="skipped",
                    error=f"Unknown relationship type: {rel_type}",
                )

            try:
                self._db.query(
                    cypher,
                    {
                        "source_name": entity.source_name,
                        "target_name": entity.target_name,
                    },
                )
            except Exception as e:
                return LoadedEntity(
                    entity_type="Relationship",
                    name=f"{entity.source_name} -> {entity.target_name}",
                    uid=uid,
                    action="skipped",
                    error=str(e),
                )

        return LoadedEntity(
            entity_type="Relationship",
            name=f"{entity.source_name} -[{entity.relationship}]-> {entity.target_name}",
            uid=uid,
            action=action,
        )

    def _get_existing_node(self, label: str, uid: str) -> dict | None:
        """Check if a node exists by label and UID."""
        cypher = f"MATCH (n:{label} {{uid: $uid}}) RETURN n LIMIT 1"
        results = self._db.query(cypher, {"uid": uid})
        if results:
            return dict(results[0]["n"])
        return None

    def _create_disposal_relationship(self, waste_item_uid: str, target: str) -> None:
        """
        Create a disposal relationship from WasteItem to target.

        Determines if target is a WasteStream or Facility and creates
        the appropriate relationship type.
        """
        # Check if target is a WasteStream
        ws_result = self._db.query(
            "MATCH (s:WasteStream {name: $name}) RETURN s",
            {"name": target},
        )
        if ws_result:
            self._db.query(
                """
                MATCH (w:WasteItem {uid: $uid})
                MATCH (s:WasteStream {name: $target})
                MERGE (w)-[:DISPOSED_IN]->(s)
                """,
                {"uid": waste_item_uid, "target": target},
            )
            return

        # Check if target is a Facility
        f_result = self._db.query(
            "MATCH (f:Facility {name: $name}) RETURN f",
            {"name": target},
        )
        if f_result:
            self._db.query(
                """
                MATCH (w:WasteItem {uid: $uid})
                MATCH (f:Facility {name: $target})
                MERGE (w)-[:DISPOSED_AT]->(f)
                """,
                {"uid": waste_item_uid, "target": target},
            )
            return

        # Target doesn't exist yet - log warning
        logger.warning(f"Disposal target not found in database: {target}")


# =============================================================================
# Convenience Functions
# =============================================================================


def load_from_orchestrator_result(
    result: Any,
    source: SourceExtract,
    dry_run: bool = False,
) -> LoadResult:
    """
    Load entities from an OrchestratorResult.

    Convenience function that extracts entities from orchestrator output
    and loads them into the database.

    Args:
        result: OrchestratorResult from the orchestrator
        source: Source document info
        dry_run: If True, simulate but don't execute

    Returns:
        LoadResult with statistics
    """
    from src.llm_etl.schemas import SCHEMA_REGISTRY, get_schema

    entities = []

    for item in result.extracted_entities:
        schema_name = item.get("schema_name", "")
        entity_data = item.get("entity", {})

        schema_class = get_schema(schema_name)
        if schema_class:
            try:
                entity = schema_class(**entity_data)
                entities.append(entity)
            except Exception as e:
                logger.warning(f"Failed to reconstruct {schema_name}: {e}")

    loader = EntityLoader()
    return loader.load(entities, source, dry_run)
