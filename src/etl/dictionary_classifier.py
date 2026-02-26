"""
LLM-based classification of DictionaryEntry nodes as physical/non-physical.

Three-phase approach:
  1. Separate entries into base forms vs inflected forms (by gloss pattern)
  2. Classify base forms via LLM (batched, with resume support)
  3. Inherit classifications for inflected forms from their base form

No CLI — called from dictionary.py via the 'classify' subcommand.
"""

import json
import logging
import re
import time
from enum import Enum
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pydantic import BaseModel

from src.db import neo4j_db
from src.llm_etl.config import get_llm, LLM_MODEL

logger = logging.getLogger(__name__)


# =============================================================================
# Pydantic schemas for structured LLM output
# =============================================================================


class PhysicalCategory(str, Enum):
    OBJECT = "object"
    SUBSTANCE = "substance"
    FOOD = "food"
    ORGANISM = "organism"
    STRUCTURE = "structure"
    NONPHYSICAL = "nonphysical"
    PERSON = "person"
    PLACE = "place"
    EVENT = "event"
    UNCERTAIN = "uncertain"


class ItemClassification(BaseModel):
    index: int
    is_physical: bool
    disposal_relevant: bool
    category: PhysicalCategory


class BatchClassificationResult(BaseModel):
    classifications: list[ItemClassification]


# =============================================================================
# Inflection detection
# =============================================================================

# Matches glosses like:
#   "Nominativ Plural des Substantivs Schokolade"
#   "Genitiv Singular der schwachen Deklination des Substantivs Batterie"
#   "Dativ Plural der starken Deklination des Substantivs Zipfsches Gesetz"
_INFLECTION_RE = re.compile(
    r"(?:Nominativ|Genitiv|Dativ|Akkusativ)\s+"
    r"(?:Singular|Plural)\s+"
    r".*?(?:des Substantivs|des (?:neutralen|femininen|männlichen) Substantivs)\s+"
    r"(.+)"
)


def _extract_base_form(glosses: list[str]) -> str | None:
    """Extract base form name from inflection gloss.

    Returns the base form name if any gloss matches the inflection pattern,
    or None if this entry is not an inflected form.
    """
    for g in glosses:
        m = _INFLECTION_RE.search(g)
        if m:
            return m.group(1).strip()
    return None


# =============================================================================
# Data loading
# =============================================================================


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
# Resume support
# =============================================================================


def _get_classified_uids(session) -> set[str]:
    """Query Neo4j for UIDs that already have is_physical set."""
    result = session.run(
        "MATCH (d:DictionaryEntry) WHERE d.is_physical IS NOT NULL "
        "RETURN d.uid AS uid"
    )
    return {r["uid"] for r in result}


# =============================================================================
# LLM classification
# =============================================================================


SYSTEM_PROMPT = """\
Du bist ein Klassifikationssystem fuer deutsche Substantive.
Bestimme fuer jedes Wort, ob es ein physisches Objekt beschreibt,
das als Abfall entsorgt werden koennte.

Regeln:
- is_physical=true: greifbare Dinge (Gegenstaende, Stoffe, Lebensmittel, Materialien)
- is_physical=false: abstrakte Begriffe, Personen, Orte, Ereignisse, Zeitraeume
- disposal_relevant=true: Dinge die realistisch im Hausmüll oder Wertstoff landen \
(Verpackungen, Lebensmittelreste, Papier, Elektrogeraete, Chemikalien etc.)
- disposal_relevant=false: Gebaeude, lebende Tiere, grosse Strukturen, Fahrzeuge, \
Naturphaenomene

Kategorien:
- object: Gegenstaende (Flasche, Stuhl, Batterie, Kleidung)
- substance: Stoffe und Materialien (Farbe, Altoel, Plastik, Klebstoff)
- food: Lebensmittel (Apfel, Brot, Kaffeesatz, Speiseoel)
- organism: Lebewesen (Eiche, Hund, Bakterie) — meistens nicht disposal_relevant
- structure: Gebaeude und grosse Strukturen (Haus, Bruecke) — nicht disposal_relevant
- nonphysical: Abstrakte Begriffe (Liebe, Freiheit, Mathematik, Demokratie)
- person: Personen und Berufe (Arzt, Lehrer, Koenig)
- place: Orte und Regionen (Frankfurt, Europa, Insel)
- event: Ereignisse (Hochzeit, Konzert, Krieg)
- uncertain: Wenn die Kategorie unklar ist

Antworte IMMER fuer ALLE Eintraege in der Liste. Der index muss mit der \
Nummer des Eintrags uebereinstimmen."""


def _build_batch_prompt(items: list[dict]) -> str:
    """Build numbered list prompt for classification.

    Args:
        items: List of dicts with 'canonical' and 'glosses' keys.

    Returns:
        Formatted prompt string like:
        1. Schokolade — kakaohaltige Süßware
        2. Demokratie — Regierungsform
    """
    lines = []
    for i, item in enumerate(items, 1):
        name = item["canonical"]
        glosses = item.get("glosses", [])
        # Use first non-inflection gloss, truncated
        gloss_text = ""
        for g in glosses:
            if not _INFLECTION_RE.search(g):
                gloss_text = g[:100]
                break
        if not gloss_text and glosses:
            gloss_text = glosses[0][:100]

        if gloss_text:
            lines.append(f"{i}. {name} — {gloss_text}")
        else:
            lines.append(f"{i}. {name}")

    return "Klassifiziere die folgenden Substantive:\n" + "\n".join(lines)


def _classify_batch(
    llm, items: list[dict], delay: float, max_retries: int = 3
) -> list[ItemClassification]:
    """Call LLM with structured output for a batch of items.

    Retries on rate limit (429) with exponential backoff.

    Args:
        llm: LangChain chat model with structured output.
        items: List of dicts with 'canonical' and 'glosses'.
        delay: Base delay between calls in seconds.
        max_retries: Maximum retry attempts on rate limit errors.

    Returns:
        List of ItemClassification results.
    """
    from langchain_core.messages import HumanMessage, SystemMessage

    messages = [
        SystemMessage(content=SYSTEM_PROMPT),
        HumanMessage(content=_build_batch_prompt(items)),
    ]

    backoff_times = [15, 30, 60]

    for attempt in range(max_retries + 1):
        try:
            result = llm.invoke(messages)
            time.sleep(delay)
            return result.classifications
        except Exception as e:
            error_str = str(e).lower()
            is_rate_limit = "429" in error_str or "rate" in error_str
            if is_rate_limit and attempt < max_retries:
                wait = backoff_times[min(attempt, len(backoff_times) - 1)]
                logger.warning(
                    f"Rate limit hit, waiting {wait}s (attempt {attempt + 1}/{max_retries})"
                )
                time.sleep(wait)
            else:
                raise

    return []  # unreachable but satisfies type checker


# =============================================================================
# Neo4j write-back
# =============================================================================


def _write_classifications(
    session, entries: list[dict], batch_size: int = 500
) -> int:
    """Batched UNWIND to set classification properties on DictionaryEntry nodes.

    Args:
        session: Neo4j session.
        entries: List of dicts with uid, is_physical, disposal_relevant, category.
        batch_size: Records per UNWIND batch.

    Returns:
        Number of nodes updated.
    """
    updated = 0
    model_name = LLM_MODEL

    for batch in _batched(entries, batch_size):
        result = session.run(
            """
            UNWIND $entries AS e
            MATCH (d:DictionaryEntry {uid: e.uid})
            SET d.is_physical = e.is_physical,
                d.disposal_relevant = e.disposal_relevant,
                d.classification_category = e.category,
                d.classified_by = $model_name
            RETURN count(*) AS cnt
            """,
            entries=batch,
            model_name=model_name,
        )
        updated += result.single()["cnt"]
        if updated % 10000 < batch_size:
            logger.info(f"  Write progress: {updated:,} nodes updated")

    return updated


def _promote_to_waste_item(session) -> int:
    """Add :WasteItem label to physical + disposal_relevant DictionaryEntry nodes.

    Returns:
        Number of nodes promoted.
    """
    result = session.run(
        """
        MATCH (d:DictionaryEntry)
        WHERE d.is_physical = true
          AND d.disposal_relevant = true
          AND NOT d:WasteItem
        SET d:WasteItem
        RETURN count(*) AS promoted
        """
    )
    return result.single()["promoted"]


# =============================================================================
# Main orchestrator
# =============================================================================


def classify_dictionary_entries(
    input_path: Path,
    batch_size: int = 25,
    dry_run: bool = False,
    delay: float = 1.0,
) -> dict:
    """Classify DictionaryEntry nodes as physical/non-physical via LLM.

    Five-pass process:
      1. Load concepts.jsonl, separate base forms vs inflected forms
      2. LLM classify base forms (batched, with resume support)
      3. Inherit classifications for inflected forms
      4. Write all classifications to Neo4j
      5. Promote physical+disposal_relevant entries to :WasteItem

    Args:
        input_path: Path to concepts.jsonl.
        batch_size: Items per LLM call (default 25).
        dry_run: If True, compute stats but skip LLM calls and DB writes.
        delay: Seconds between LLM calls (default 1.0).

    Returns:
        Dict with classification statistics.
    """
    t_start = time.time()

    # ── Pass 1: Load and separate ────────────────────────────────────────
    logger.info(f"Pass 1/5: Loading clusters from {input_path}...")
    t1 = time.time()
    clusters = _load_clusters(input_path)
    logger.info(f"  Loaded {len(clusters):,} clusters in {time.time() - t1:.1f}s")

    # Build UID -> cluster lookup
    uid_to_cluster = {c["uid"]: c for c in clusters}

    # Build name lookup: canonical names + synonym names -> canonical name
    # This allows inflected forms to find their base even when the gloss
    # references a synonym rather than the canonical name.
    name_to_canonical_name: dict[str, str] = {}
    for c in clusters:
        name_to_canonical_name[c["canonical"]] = c["canonical"]
        for syn in c.get("synonyms_de", []):
            # Don't overwrite if already mapped (first canonical wins)
            if syn not in name_to_canonical_name:
                name_to_canonical_name[syn] = c["canonical"]

    # Separate base forms from inflected forms
    base_forms = []       # clusters to classify via LLM
    inflected_forms = []  # clusters that inherit from base
    # Map: inflected cluster UID -> resolved canonical name of base form
    inflected_to_base_canonical: dict[str, str] = {}

    for c in clusters:
        glosses = c.get("glosses", [])
        base_name = _extract_base_form(glosses)
        if base_name is not None:
            # Resolve base name through synonym lookup
            resolved = name_to_canonical_name.get(base_name)
            if resolved is not None:
                inflected_forms.append(c)
                inflected_to_base_canonical[c["uid"]] = resolved
            else:
                # Base form not in any cluster — treat as base form for LLM
                base_forms.append(c)
        else:
            base_forms.append(c)

    unresolvable = sum(
        1 for c in clusters
        if _extract_base_form(c.get("glosses", [])) is not None
        and c["uid"] not in inflected_to_base_canonical
    )
    logger.info(
        f"  Base forms: {len(base_forms):,} (incl. {unresolvable:,} "
        f"unresolvable inflections) | "
        f"Inflected forms: {len(inflected_forms):,}"
    )

    # ── Dry run stats ────────────────────────────────────────────────────
    stats = {
        "total_clusters": len(clusters),
        "base_forms": len(base_forms),
        "inflected_forms": len(inflected_forms),
        "llm_batches_needed": (len(base_forms) + batch_size - 1) // batch_size,
        "llm_classified": 0,
        "inherited": 0,
        "skipped_already_classified": 0,
        "skipped_no_base_match": 0,
        "written_to_db": 0,
        "promoted_to_waste_item": 0,
        "dry_run": dry_run,
    }

    if dry_run:
        logger.info("DRY RUN — no LLM calls or database changes")
        logger.info(f"  Would classify {len(base_forms):,} base forms via LLM")
        logger.info(
            f"  Would need ~{stats['llm_batches_needed']:,} LLM calls "
            f"(batch_size={batch_size})"
        )
        logger.info(
            f"  Would inherit classification for {len(inflected_forms):,} "
            f"inflected forms"
        )
        est_minutes = stats["llm_batches_needed"] * delay / 60
        logger.info(f"  Estimated time: ~{est_minutes:.0f} minutes")
        return stats

    # ── Pass 2: LLM classify base forms ─────────────────────────────────
    logger.info("Pass 2/5: Classifying base forms via LLM...")
    t2 = time.time()

    # Check which UIDs are already classified (resume support)
    with neo4j_db.session() as session:
        already_classified = _get_classified_uids(session)
    logger.info(f"  Already classified in DB: {len(already_classified):,}")

    # Filter out already-classified base forms
    to_classify = [c for c in base_forms if c["uid"] not in already_classified]
    stats["skipped_already_classified"] = len(base_forms) - len(to_classify)
    logger.info(
        f"  To classify: {len(to_classify):,} "
        f"(skipping {stats['skipped_already_classified']:,} already done)"
    )

    if not to_classify:
        logger.info("  All base forms already classified — skipping LLM pass")
    else:
        # Set up LLM with structured output
        llm = get_llm(temperature=0.0)
        structured_llm = llm.with_structured_output(BatchClassificationResult)

        # Classify in batches
        classifications: dict[str, dict] = {}  # uid -> classification dict
        total_batches = (len(to_classify) + batch_size - 1) // batch_size
        batch_num = 0

        for batch in _batched(to_classify, batch_size):
            batch_num += 1
            try:
                results = _classify_batch(structured_llm, batch, delay)

                for item_cls in results:
                    idx = item_cls.index - 1  # convert 1-based to 0-based
                    if 0 <= idx < len(batch):
                        uid = batch[idx]["uid"]
                        classifications[uid] = {
                            "uid": uid,
                            "is_physical": item_cls.is_physical,
                            "disposal_relevant": item_cls.disposal_relevant,
                            "category": item_cls.category.value,
                        }

                stats["llm_classified"] += len(results)

                if batch_num % 20 == 0 or batch_num == total_batches:
                    elapsed = time.time() - t2
                    rate = batch_num / elapsed * 60 if elapsed > 0 else 0
                    logger.info(
                        f"  Batch {batch_num}/{total_batches} — "
                        f"{stats['llm_classified']:,} classified — "
                        f"{rate:.0f} batches/min"
                    )
            except Exception as e:
                logger.error(
                    f"  Batch {batch_num} failed: {e} — "
                    f"skipping {len(batch)} items"
                )
                continue

        logger.info(
            f"  Pass 2 done: {stats['llm_classified']:,} items classified "
            f"in {time.time() - t2:.1f}s"
        )

    # ── Pass 3: Inherit classifications for inflected forms ──────────────
    logger.info("Pass 3/5: Inheriting classifications for inflected forms...")
    t3 = time.time()

    # Build canonical name -> classification lookup from DB + new results
    # First, load existing classifications from base forms already in DB
    name_to_classification: dict[str, dict] = {}

    # From newly classified base forms
    if to_classify:
        for c in base_forms:
            if c["uid"] in classifications:
                name_to_classification[c["canonical"]] = classifications[c["uid"]]

    # From previously classified base forms (in DB)
    if already_classified:
        with neo4j_db.session() as session:
            result = session.run(
                """
                MATCH (d:DictionaryEntry)
                WHERE d.is_physical IS NOT NULL AND d.lang = 'de'
                RETURN d.uid AS uid, d.name AS name,
                       d.is_physical AS is_physical,
                       d.disposal_relevant AS disposal_relevant,
                       d.classification_category AS category
                """
            )
            for r in result:
                name_to_classification[r["name"]] = {
                    "uid": r["uid"],
                    "is_physical": r["is_physical"],
                    "disposal_relevant": r["disposal_relevant"],
                    "category": r["category"],
                }

    # Now inherit for each inflected form
    inherited_entries: list[dict] = []
    no_base_match = 0

    for c in inflected_forms:
        if c["uid"] in already_classified:
            stats["skipped_already_classified"] += 1
            continue

        base_canonical = inflected_to_base_canonical[c["uid"]]
        base_cls = name_to_classification.get(base_canonical)

        if base_cls:
            inherited_entries.append({
                "uid": c["uid"],
                "is_physical": base_cls["is_physical"],
                "disposal_relevant": base_cls["disposal_relevant"],
                "category": base_cls["category"],
            })
            stats["inherited"] += 1
        else:
            no_base_match += 1

    stats["skipped_no_base_match"] = no_base_match
    logger.info(
        f"  Inherited: {stats['inherited']:,} | "
        f"No base match: {no_base_match:,} | "
        f"Already classified: {stats['skipped_already_classified']:,}"
    )
    logger.info(f"  Pass 3 done in {time.time() - t3:.1f}s")

    # ── Pass 4: Write all classifications to Neo4j ───────────────────────
    logger.info("Pass 4/5: Writing classifications to Neo4j...")
    t4 = time.time()

    # Combine new LLM classifications + inherited
    all_entries = []
    if to_classify:
        all_entries.extend(classifications.values())
    all_entries.extend(inherited_entries)

    if all_entries:
        with neo4j_db.session() as session:
            stats["written_to_db"] = _write_classifications(
                session, all_entries, batch_size=500
            )
        logger.info(
            f"  Pass 4 done: {stats['written_to_db']:,} nodes updated "
            f"in {time.time() - t4:.1f}s"
        )
    else:
        logger.info("  No new classifications to write")

    # ── Pass 5: Promote to WasteItem ─────────────────────────────────────
    logger.info("Pass 5/5: Promoting physical entries to :WasteItem...")
    t5 = time.time()

    with neo4j_db.session() as session:
        stats["promoted_to_waste_item"] = _promote_to_waste_item(session)

    logger.info(
        f"  Pass 5 done: {stats['promoted_to_waste_item']:,} nodes promoted "
        f"in {time.time() - t5:.1f}s"
    )

    total_time = time.time() - t_start
    logger.info(f"Classification complete in {total_time:.1f}s")

    return stats
