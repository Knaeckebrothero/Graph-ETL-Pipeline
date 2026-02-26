"""
Bulk import of dictionary concept clusters into Neo4j.

Reads concepts.jsonl (produced by dictionary_parser) and creates
DictionaryEntry nodes with SYNONYM_OF, TRANSLATION_OF, IS_A, and
DERIVED_FROM relationships using batched UNWIND queries.

No CLI — called from dictionary.py.
"""

import hashlib
import json
import logging
import time
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.db import neo4j_db

logger = logging.getLogger(__name__)


def _generate_uid(name: str) -> str:
    """Generate deterministic UID from name (SHA256[:16])."""
    return hashlib.sha256(name.encode()).hexdigest()[:16]


def _load_clusters(input_path: Path) -> list[dict]:
    """Load concept clusters from JSONL file."""
    clusters = []
    with open(input_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                clusters.append(json.loads(line))
    return clusters


def _batched(items: list, size: int):
    """Yield successive batches from a list."""
    for i in range(0, len(items), size):
        yield items[i : i + size]


# =============================================================================
# Schema setup
# =============================================================================


def _ensure_schema(session) -> None:
    """Create DictionaryEntry constraints and indexes if they don't exist."""
    statements = [
        "CREATE CONSTRAINT dict_entry_uid IF NOT EXISTS FOR (d:DictionaryEntry) REQUIRE d.uid IS UNIQUE",
        "CREATE CONSTRAINT dict_entry_name IF NOT EXISTS FOR (d:DictionaryEntry) REQUIRE d.name IS UNIQUE",
        "CREATE INDEX dict_entry_lang IF NOT EXISTS FOR (d:DictionaryEntry) ON (d.lang)",
        "CREATE INDEX dict_entry_synset IF NOT EXISTS FOR (d:DictionaryEntry) ON (d.synset_id)",
    ]
    for stmt in statements:
        session.run(stmt)
    logger.info("Schema constraints/indexes ensured")


# =============================================================================
# Pass 0: Source node
# =============================================================================


def _create_source_node(session) -> str:
    """Create Source node for dictionary provenance. Returns source UID."""
    source_uid = _generate_uid("dictionary-baseline")
    session.run(
        """
        MERGE (s:Source {uid: $uid})
        SET s.name = 'dictionary-baseline',
            s.type = 'dictionary',
            s.description = 'Merged vocabulary from kaikki.org Wiktionary + OdeNet WordNet'
        """,
        uid=source_uid,
    )
    logger.info(f"Source node created: dictionary-baseline ({source_uid})")
    return source_uid


# =============================================================================
# Pass 1: Canonical nodes
# =============================================================================


def _import_canonical_nodes(session, clusters: list[dict], batch_size: int) -> int:
    """Create canonical DictionaryEntry nodes. Returns count."""
    entries = []
    for c in clusters:
        entries.append({
            "uid": c["uid"],
            "name": c["canonical"],
            "lang": c.get("lang", "de"),
            "synset_id": c.get("synset_id"),
            "glosses": c.get("glosses", []),
            "categories": c.get("categories", []),
            "sources": c.get("sources", []),
        })

    created = 0
    for batch in _batched(entries, batch_size):
        result = session.run(
            """
            UNWIND $entries AS e
            MERGE (d:DictionaryEntry {uid: e.uid})
            SET d.name = e.name,
                d.lang = e.lang,
                d.pos = 'noun',
                d.synset_id = e.synset_id,
                d.glosses = e.glosses,
                d.categories = e.categories,
                d.sources = e.sources
            RETURN count(*) AS cnt
            """,
            entries=batch,
        )
        created += result.single()["cnt"]
        if created % 50000 < batch_size:
            logger.info(f"  Pass 1 progress: {created:,} canonical nodes")

    return created


# =============================================================================
# Pass 2: Synonym edges
# =============================================================================


def _import_synonym_edges(session, clusters: list[dict], batch_size: int) -> int:
    """Create synonym nodes + SYNONYM_OF edges. Returns edge count."""
    pairs = []
    for c in clusters:
        canonical_uid = c["uid"]
        for syn_name in c.get("synonyms_de", []):
            syn_uid = _generate_uid(syn_name)
            pairs.append({
                "canonical_uid": canonical_uid,
                "syn_name": syn_name,
                "syn_uid": syn_uid,
            })

    if not pairs:
        return 0

    created = 0
    for batch in _batched(pairs, batch_size):
        result = session.run(
            """
            UNWIND $pairs AS p
            MATCH (canonical:DictionaryEntry {uid: p.canonical_uid})
            MERGE (syn:DictionaryEntry {uid: p.syn_uid})
            ON CREATE SET syn.name = p.syn_name, syn.lang = 'de', syn.pos = 'noun'
            MERGE (syn)-[:SYNONYM_OF]->(canonical)
            RETURN count(*) AS cnt
            """,
            pairs=batch,
        )
        created += result.single()["cnt"]
        if created % 50000 < batch_size:
            logger.info(f"  Pass 2 progress: {created:,} synonym edges")

    return created


# =============================================================================
# Pass 3: Translation edges
# =============================================================================


def _import_translation_edges(session, clusters: list[dict], batch_size: int) -> int:
    """Create English translation nodes + TRANSLATION_OF edges. Returns edge count."""
    pairs = []
    for c in clusters:
        de_uid = c["uid"]
        for en_name in c.get("translations_en", []):
            en_uid = _generate_uid(en_name)
            pairs.append({
                "de_uid": de_uid,
                "en_name": en_name,
                "en_uid": en_uid,
            })

    if not pairs:
        return 0

    created = 0
    for batch in _batched(pairs, batch_size):
        result = session.run(
            """
            UNWIND $pairs AS p
            MATCH (de:DictionaryEntry {uid: p.de_uid})
            MERGE (en:DictionaryEntry {uid: p.en_uid})
            ON CREATE SET en.name = p.en_name, en.lang = 'en'
            MERGE (en)-[:TRANSLATION_OF]->(de)
            RETURN count(*) AS cnt
            """,
            pairs=batch,
        )
        created += result.single()["cnt"]
        if created % 50000 < batch_size:
            logger.info(f"  Pass 3 progress: {created:,} translation edges")

    return created


# =============================================================================
# Pass 4: Hypernym (IS_A) edges
# =============================================================================


def _import_hypernym_edges(session, clusters: list[dict], batch_size: int) -> int:
    """Create IS_A edges (child -> parent hypernym). Returns edge count."""
    pairs = []
    for c in clusters:
        child_uid = c["uid"]
        for parent_name in c.get("hypernyms", []):
            parent_uid = _generate_uid(parent_name)
            pairs.append({
                "child_uid": child_uid,
                "parent_name": parent_name,
                "parent_uid": parent_uid,
            })

    if not pairs:
        return 0

    created = 0
    for batch in _batched(pairs, batch_size):
        result = session.run(
            """
            UNWIND $pairs AS p
            MATCH (child:DictionaryEntry {uid: p.child_uid})
            MERGE (parent:DictionaryEntry {uid: p.parent_uid})
            ON CREATE SET parent.name = p.parent_name, parent.lang = 'de', parent.pos = 'noun'
            MERGE (child)-[:IS_A]->(parent)
            RETURN count(*) AS cnt
            """,
            pairs=batch,
        )
        created += result.single()["cnt"]
        if created % 50000 < batch_size:
            logger.info(f"  Pass 4 progress: {created:,} IS_A edges")

    return created


# =============================================================================
# Pass 5: Provenance edges
# =============================================================================


def _import_provenance_edges(
    session, clusters: list[dict], source_uid: str, batch_size: int
) -> int:
    """Link canonical nodes to Source via DERIVED_FROM. Returns edge count."""
    uids = [c["uid"] for c in clusters]

    created = 0
    for batch in _batched(uids, batch_size):
        result = session.run(
            """
            UNWIND $uids AS uid
            MATCH (d:DictionaryEntry {uid: uid})
            MATCH (s:Source {uid: $source_uid})
            MERGE (d)-[:DERIVED_FROM]->(s)
            RETURN count(*) AS cnt
            """,
            uids=batch,
            source_uid=source_uid,
        )
        created += result.single()["cnt"]
        if created % 50000 < batch_size:
            logger.info(f"  Pass 5 progress: {created:,} provenance edges")

    return created


# =============================================================================
# Main entry point
# =============================================================================


def import_dictionary_entries(
    input_path: Path,
    batch_size: int = 500,
    dry_run: bool = False,
) -> dict:
    """Import dictionary concept clusters into Neo4j.

    Performs a 5-pass import:
      1. Canonical DictionaryEntry nodes
      2. Synonym nodes + SYNONYM_OF edges
      3. English translation nodes + TRANSLATION_OF edges
      4. Hypernym nodes + IS_A edges
      5. Source node + DERIVED_FROM edges

    All passes use MERGE for idempotency.

    Args:
        input_path: Path to concepts.jsonl.
        batch_size: Number of records per UNWIND batch.
        dry_run: If True, compute stats but execute no Cypher.

    Returns:
        Dict with import statistics.
    """
    logger.info(f"Loading clusters from {input_path}...")
    t0 = time.time()
    clusters = _load_clusters(input_path)
    load_time = time.time() - t0
    logger.info(f"Loaded {len(clusters):,} clusters in {load_time:.1f}s")

    # Compute dry-run stats
    total_synonyms = sum(len(c.get("synonyms_de", [])) for c in clusters)
    total_translations = sum(len(c.get("translations_en", [])) for c in clusters)
    total_hypernyms = sum(len(c.get("hypernyms", [])) for c in clusters)

    stats = {
        "clusters_loaded": len(clusters),
        "canonical_nodes": len(clusters),
        "synonym_edges": total_synonyms,
        "translation_edges": total_translations,
        "hypernym_edges": total_hypernyms,
        "provenance_edges": len(clusters),
        "dry_run": dry_run,
    }

    if dry_run:
        logger.info("DRY RUN — no database changes")
        logger.info(f"  Would create {len(clusters):,} canonical nodes")
        logger.info(f"  Would create {total_synonyms:,} SYNONYM_OF edges")
        logger.info(f"  Would create {total_translations:,} TRANSLATION_OF edges")
        logger.info(f"  Would create {total_hypernyms:,} IS_A edges")
        logger.info(f"  Would create {len(clusters):,} DERIVED_FROM edges")
        return stats

    # Full import
    t_start = time.time()

    with neo4j_db.session() as session:
        _ensure_schema(session)

        source_uid = _create_source_node(session)

        logger.info("Pass 1/5: Creating canonical nodes...")
        t1 = time.time()
        stats["canonical_nodes"] = _import_canonical_nodes(
            session, clusters, batch_size
        )
        logger.info(
            f"  Pass 1 done: {stats['canonical_nodes']:,} nodes in {time.time()-t1:.1f}s"
        )

        logger.info("Pass 2/5: Creating synonym edges...")
        t2 = time.time()
        stats["synonym_edges"] = _import_synonym_edges(
            session, clusters, batch_size
        )
        logger.info(
            f"  Pass 2 done: {stats['synonym_edges']:,} edges in {time.time()-t2:.1f}s"
        )

        logger.info("Pass 3/5: Creating translation edges...")
        t3 = time.time()
        stats["translation_edges"] = _import_translation_edges(
            session, clusters, batch_size
        )
        logger.info(
            f"  Pass 3 done: {stats['translation_edges']:,} edges in {time.time()-t3:.1f}s"
        )

        logger.info("Pass 4/5: Creating IS_A edges...")
        t4 = time.time()
        stats["hypernym_edges"] = _import_hypernym_edges(
            session, clusters, batch_size
        )
        logger.info(
            f"  Pass 4 done: {stats['hypernym_edges']:,} edges in {time.time()-t4:.1f}s"
        )

        logger.info("Pass 5/5: Creating provenance edges...")
        t5 = time.time()
        stats["provenance_edges"] = _import_provenance_edges(
            session, clusters, source_uid, batch_size
        )
        logger.info(
            f"  Pass 5 done: {stats['provenance_edges']:,} edges in {time.time()-t5:.1f}s"
        )

    total_time = time.time() - t_start
    logger.info(f"Import complete in {total_time:.1f}s")

    return stats
