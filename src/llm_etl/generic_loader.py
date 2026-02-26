"""
Generic entity loader for arbitrary Pydantic schemas.

Persists any BaseModel as a Neo4j node without requiring a hardcoded
handler in EntityLoader. Used as a fallback when the schema is not
in SCHEMA_REGISTRY (i.e., custom/user-defined schemas).

Design:
- Node label: schema class name (e.g., ChemicalExtract → :ChemicalExtract)
- UID: SHA256 of `name` field if present, else content-hash of full model
- Properties: all Pydantic fields → Neo4j properties
- Provenance: DERIVED_FROM → Source node
- MERGE-based, idempotent
"""

import json
import logging
from dataclasses import dataclass, field

from pydantic import BaseModel

from src.db import neo4j_db
from src.llm_etl.loader import LoadedEntity, LoadResult, generate_source_uid, generate_uid
from src.llm_etl.schemas import SourceExtract

logger = logging.getLogger(__name__)


class GenericEntityLoader:
    """
    Load arbitrary Pydantic entities into Neo4j as labeled nodes.

    Usage:
        loader = GenericEntityLoader()
        result = loader.load(entities, source, dry_run=False)
    """

    def __init__(self):
        self._db = neo4j_db

    def load(
        self,
        entities: list[BaseModel],
        source: SourceExtract,
        dry_run: bool = False,
    ) -> LoadResult:
        """
        Load entities into Neo4j using generic MERGE pattern.

        Args:
            entities: Pydantic model instances to persist.
            source: Provenance metadata.
            dry_run: If True, simulate without DB writes.

        Returns:
            LoadResult with statistics and per-entity details.
        """
        logger.info(
            f"Generic loader: {len(entities)} entities from {source.name} "
            f"{'(dry-run)' if dry_run else ''}"
        )

        source_uid = generate_source_uid(source)
        if not dry_run:
            self._create_source_node(source, source_uid)

        errors: list[str] = []
        loaded: list[LoadedEntity] = []
        created = 0
        updated = 0
        skipped = 0

        for entity in entities:
            try:
                result = self._load_entity(entity, source_uid, dry_run)
                loaded.append(result)

                if result.error:
                    errors.append(result.error)
                    skipped += 1
                elif result.action == "created":
                    created += 1
                elif result.action == "updated":
                    updated += 1
                else:
                    skipped += 1
            except Exception as e:
                logger.error(f"Generic load failed for {type(entity).__name__}: {e}")
                errors.append(str(e))
                skipped += 1

        return LoadResult(
            success=len(errors) == 0,
            source_uid=source_uid,
            entities_processed=len(entities),
            entities_created=created,
            entities_updated=updated,
            entities_enriched=0,
            entities_skipped=skipped,
            errors=errors,
            loaded_entities=loaded,
            dry_run=dry_run,
        )

    def _load_entity(
        self,
        entity: BaseModel,
        source_uid: str,
        dry_run: bool,
    ) -> LoadedEntity:
        """Load a single entity as a Neo4j node."""
        label = type(entity).__name__
        uid = self._get_uid(entity)
        name = self._get_name(entity)
        properties = self._serialize_properties(entity)

        action = "created"

        if not dry_run:
            # Check existence
            existing = self._db.query(
                f"MATCH (n:{label} {{uid: $uid}}) RETURN n LIMIT 1",
                {"uid": uid},
            )
            if existing:
                action = "updated"

            # Build and run MERGE query
            cypher, params = self._build_merge_cypher(
                label, uid, properties, source_uid
            )
            self._db.query(cypher, params)

        return LoadedEntity(
            entity_type=label,
            name=name,
            uid=uid,
            action=action,
            changes=properties,
        )

    def _create_source_node(self, source: SourceExtract, source_uid: str) -> None:
        """Create or update provenance Source node."""
        self._db.query(
            """
            MERGE (s:Source {uid: $uid})
            SET s.name = $name,
                s.file_type = $file_type,
                s.file_path = $file_path,
                s.url = $url,
                s.import_date = datetime()
            """,
            {
                "uid": source_uid,
                "name": source.name,
                "file_type": source.file_type,
                "file_path": source.file_path,
                "url": source.url,
            },
        )

    def _get_uid(self, entity: BaseModel) -> str:
        """Generate UID from name field or content hash."""
        data = entity.model_dump()
        if "name" in data and data["name"]:
            return generate_uid(str(data["name"]))
        # Fallback: hash the full model content
        return generate_uid(entity.model_dump_json())

    def _get_name(self, entity: BaseModel) -> str:
        """Get a display name from the entity."""
        data = entity.model_dump()
        if "name" in data and data["name"]:
            return str(data["name"])
        # Fallback: first non-empty string field
        for value in data.values():
            if isinstance(value, str) and value.strip():
                return value.strip()[:80]
        return type(entity).__name__

    def _serialize_properties(self, entity: BaseModel) -> dict:
        """Convert Pydantic fields to Neo4j-compatible properties."""
        data = entity.model_dump()
        properties = {}

        for key, value in data.items():
            if value is None:
                continue
            elif isinstance(value, (str, int, float, bool)):
                properties[key] = value
            elif isinstance(value, list):
                if not value:
                    properties[key] = []
                elif all(isinstance(v, (str, int, float, bool)) for v in value):
                    properties[key] = value  # Neo4j native list
                else:
                    properties[key] = json.dumps(value, ensure_ascii=False)
            elif isinstance(value, dict):
                properties[key] = json.dumps(value, ensure_ascii=False)
            else:
                properties[key] = str(value)

        return properties

    def _build_merge_cypher(
        self,
        label: str,
        uid: str,
        properties: dict,
        source_uid: str,
    ) -> tuple[str, dict]:
        """Build a dynamic MERGE + SET Cypher query."""
        params = {"uid": uid, "source_uid": source_uid}

        set_clauses = []
        for key, value in properties.items():
            param_name = f"p_{key}"
            set_clauses.append(f"n.{key} = ${param_name}")
            params[param_name] = value

        set_clause = ", ".join(set_clauses) if set_clauses else "n.uid = $uid"

        cypher = f"""
        MERGE (n:{label} {{uid: $uid}})
        SET {set_clause}
        WITH n
        OPTIONAL MATCH (s:Source {{uid: $source_uid}})
        FOREACH (_ IN CASE WHEN s IS NOT NULL THEN [1] ELSE [] END |
            MERGE (n)-[:DERIVED_FROM]->(s)
        )
        RETURN n
        """
        return cypher, params
