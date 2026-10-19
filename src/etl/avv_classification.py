"""
AVV Classification: Classify WasteItem nodes with 6-digit AVV codes using hierarchical LLM extraction.

Classifies each WasteItem in 3 steps (small context per call):
    Step 1: Chapter     — pick from 20 AVV chapters (~272 tokens)
    Step 2: Subgroup    — pick from ~5-30 subgroups within that chapter
    Step 3: Specific    — pick from ~5-15 codes within that subgroup

This avoids sending all 1,505 codes (~20K tokens) in a single call,
which fails on Groq due to rate limits and model capacity.

Prerequisites:
    - Neo4j running with WasteItem nodes imported
    - AVV codes imported (run: python -m src.etl.avv_codes)
    - .env configured with LLM_API_KEY

Usage:
    python -m src.etl.avv_classification --limit 5 --dry-run -v
    python -m src.etl.avv_classification --skip-classified --delay 2
    python -m src.etl.avv_classification -v
"""

import argparse
import logging
import sys
import time
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from pydantic import BaseModel, Field

from src.db import neo4j_db
from src.etl.avv_codes import CHAPTER_NAMES, DATA_FILE, parse_avv_csv
from src.llm_etl.extraction_chain import ExtractionChain

logger = logging.getLogger(__name__)


# =============================================================================
# Classification Schemas (one per step)
# =============================================================================


class ChapterClassification(BaseModel):
    """Step 1: Classify waste item to an AVV chapter."""

    chapter_code: str = Field(description="2-digit AVV chapter code (e.g. '20')")
    reasoning: str = Field(description="Why this chapter fits")


class SubgroupClassification(BaseModel):
    """Step 2: Classify waste item to a subgroup within the chapter."""

    subgroup_code: str = Field(description="4-digit AVV subgroup code (e.g. '2001')")
    reasoning: str = Field(description="Why this subgroup fits")


class AVVClassification(BaseModel):
    """Step 3: Classify waste item to a specific 6-digit AVV code."""

    avv_code: str = Field(description="6-digit AVV code (e.g. '200301')")
    is_hazardous: bool = Field(default=False)
    reasoning: str = Field(description="Final classification reasoning")


# =============================================================================
# AVV Data Helpers
# =============================================================================


def build_avv_hierarchy(filepath: Path = None) -> dict:
    """Load and structure AVV data for hierarchical lookups.

    Returns dict with keys: level1, level2, level3 (from parse_avv_csv),
    plus precomputed chapter_lines for step 1.
    """
    avv = parse_avv_csv(filepath or DATA_FILE)

    # Precompute chapter reference lines for step 1
    chapter_lines = []
    for code in sorted(avv["level1"]):
        name = avv["level1"][code]["name"]
        chapter_lines.append(f"{code} - {name}")

    avv["chapter_lines"] = "\n".join(chapter_lines)
    return avv


# =============================================================================
# 3-Step Classification
# =============================================================================


def classify_single_item(
    name: str,
    avv: dict,
    chain: ExtractionChain,
    verbose: bool = False,
) -> AVVClassification | None:
    """Classify a single waste item through 3 hierarchical steps.

    Returns AVVClassification on success, None on failure.
    """
    # --- Step 1: Chapter ---
    chapter_context = avv["chapter_lines"]
    chapter_prompt = (
        "Classify this German waste item to the most appropriate AVV chapter.\n"
        'Return JSON with exactly these keys: "chapter_code" (2-digit string) '
        'and "reasoning" (brief explanation).\n'
        'Example: {"chapter_code": "20", "reasoning": "Municipal household waste"}'
    )

    step1 = chain.extract_single(
        text=name,
        output_schema=ChapterClassification,
        extraction_prompt=chapter_prompt,
        context=f"AVV Chapters:\n{chapter_context}",
    )

    if step1 is None:
        logger.warning(f"Step 1 failed for '{name}': no result")
        return None

    chapter_code = step1.chapter_code.strip().zfill(2)
    if chapter_code not in avv["level1"]:
        logger.warning(f"Step 1 invalid for '{name}': chapter '{chapter_code}' not in AVV")
        return None

    if verbose:
        logger.debug(f"  Step 1: {name} -> chapter {chapter_code} ({step1.reasoning})")

    # --- Step 2: Subgroup ---
    subgroups = {
        code: props
        for code, props in avv["level2"].items()
        if props["parent_code"] == chapter_code
    }

    if not subgroups:
        logger.warning(f"Step 2 failed for '{name}': no subgroups in chapter {chapter_code}")
        return None

    subgroup_lines = "\n".join(
        f"{code} - {props['name']}" for code, props in sorted(subgroups.items())
    )
    chapter_name = avv["level1"][chapter_code]["name"]
    subgroup_prompt = (
        f"Within AVV chapter {chapter_code} ({chapter_name}), "
        f"classify this waste item to the most specific subgroup.\n"
        f'Return JSON with exactly these keys: "subgroup_code" (4-digit string) '
        f'and "reasoning" (brief explanation).\n'
        f'Example: {{"subgroup_code": "2001", "reasoning": "Separately collected fractions"}}'
    )

    step2 = chain.extract_single(
        text=name,
        output_schema=SubgroupClassification,
        extraction_prompt=subgroup_prompt,
        context=f"Available subgroups in chapter {chapter_code}:\n{subgroup_lines}",
    )

    if step2 is None:
        logger.warning(f"Step 2 failed for '{name}': no result")
        return None

    subgroup_code = step2.subgroup_code.strip().zfill(4)
    if subgroup_code not in subgroups:
        logger.warning(
            f"Step 2 invalid for '{name}': subgroup '{subgroup_code}' "
            f"not in chapter {chapter_code}"
        )
        return None

    if verbose:
        logger.debug(f"  Step 2: {name} -> subgroup {subgroup_code} ({step2.reasoning})")

    # --- Step 3: Specific code ---
    codes = {
        code: props
        for code, props in avv["level3"].items()
        if props["parent_code"] == subgroup_code
    }

    if not codes:
        logger.warning(
            f"Step 3 failed for '{name}': no codes in subgroup {subgroup_code}"
        )
        return None

    code_lines = "\n".join(
        f"{code}{'*' if props['is_hazardous'] else ''} - {props['name']}"
        for code, props in sorted(codes.items())
    )
    subgroup_name = subgroups[subgroup_code]["name"]
    code_prompt = (
        f"Within AVV subgroup {subgroup_code} ({subgroup_name}), "
        f"pick the single best matching 6-digit AVV code for this waste item.\n"
        f"Codes marked with * are hazardous (gefährlicher Abfall).\n"
        f"Codes ending with 99 are catch-all — avoid if a better match exists.\n"
        f"IMPORTANT: Return exactly ONE code as a JSON object (not a list).\n"
        f'Return JSON with exactly these keys: "avv_code" (6-digit string), '
        f'"is_hazardous" (boolean), and "reasoning" (brief explanation).\n'
        f'Example: {{"avv_code": "200301", "is_hazardous": false, "reasoning": "Mixed municipal waste"}}'
    )

    step3 = chain.extract_single(
        text=name,
        output_schema=AVVClassification,
        extraction_prompt=code_prompt,
        context=f"Available codes in subgroup {subgroup_code}:\n{code_lines}",
    )

    if step3 is None:
        logger.warning(f"Step 3 failed for '{name}': no result")
        return None

    avv_code = step3.avv_code.strip().zfill(6)
    if avv_code not in codes:
        logger.warning(
            f"Step 3 invalid for '{name}': code '{avv_code}' "
            f"not in subgroup {subgroup_code}"
        )
        return None

    if verbose:
        hz = "*" if step3.is_hazardous else ""
        logger.debug(f"  Step 3: {name} -> {avv_code}{hz} ({step3.reasoning})")

    return step3


# =============================================================================
# Main Classification Loop
# =============================================================================


def classify_waste_items(
    limit: int = 0,
    delay: float = 2.0,
    skip_classified: bool = False,
    dry_run: bool = False,
    verbose: bool = False,
) -> dict:
    """Classify WasteItem nodes with AVV codes using 3-step hierarchy.

    Args:
        limit: Max items to classify (0 = all).
        delay: Seconds to wait between items (rate limiting).
        skip_classified: Skip items that already have a CLASSIFIED_AS relationship.
        dry_run: If True, classify but don't write relationships.
        verbose: Enable debug logging for each step.

    Returns:
        Dict with classification statistics.
    """
    # 1. Load AVV hierarchy
    logger.info("Loading AVV hierarchy...")
    avv = build_avv_hierarchy()
    logger.info(
        f"Loaded {len(avv['level1'])} chapters, "
        f"{len(avv['level2'])} subgroups, "
        f"{len(avv['level3'])} codes"
    )

    # 2. Query WasteItem nodes
    if skip_classified:
        logger.info("Querying unclassified WasteItem nodes...")
        query = (
            "MATCH (w:WasteItem) "
            "WHERE NOT (w)-[:CLASSIFIED_AS]->() "
            "RETURN w.name AS name ORDER BY w.name"
        )
    else:
        logger.info("Querying all WasteItem nodes...")
        query = "MATCH (w:WasteItem) RETURN w.name AS name ORDER BY w.name"

    records = neo4j_db.query(query)
    item_names = [r["name"] for r in records if r["name"]]

    if limit > 0:
        item_names = item_names[:limit]

    total_items = len(item_names)
    logger.info(f"Found {total_items} WasteItems to classify (3 calls each)")

    if total_items == 0:
        logger.info("Nothing to classify.")
        return {
            "total_items": 0, "classified": 0, "failed": 0,
            "hazardous": 0, "created": 0, "skipped": 0,
            "chapter_counts": {}, "dry_run": dry_run,
            "classifications": [], "error_list": [],
        }

    # 3. Initialize ExtractionChain
    chain = ExtractionChain(temperature=0.0, structured_output_method="json_mode")

    # 4. Classify each item
    all_classifications: list[tuple[str, AVVClassification]] = []
    error_list: list[str] = []

    for i, name in enumerate(item_names, 1):
        logger.info(f"[{i}/{total_items}] Classifying: {name}")

        try:
            result = classify_single_item(name, avv, chain, verbose=verbose)

            if result is not None:
                all_classifications.append((name, result))
                hz = "*" if result.is_hazardous else ""
                logger.info(f"  -> {result.avv_code}{hz}")
            else:
                error_list.append(name)
                logger.warning(f"  -> FAILED (no valid classification)")

        except Exception as e:
            error_list.append(name)
            logger.error(f"  -> ERROR: {e}")

        # Rate limiting between items (skip after last)
        if delay > 0 and i < total_items:
            time.sleep(delay)

    # 5. Write relationships
    created = 0
    skipped = 0

    if not dry_run and all_classifications:
        logger.info("Writing CLASSIFIED_AS relationships...")
        with neo4j_db.session() as session:
            for name, c in all_classifications:
                code = c.avv_code.zfill(6)
                record = session.run(
                    """
                    MATCH (w:WasteItem {name: $name})
                    MATCH (a:AVVCode {code: $code})
                    MERGE (w)-[r:CLASSIFIED_AS]->(a)
                    ON CREATE SET
                        r.reasoning = $reasoning,
                        r.is_hazardous = $is_hazardous,
                        r.classified_by = 'llm_hierarchical',
                        r.created_at = datetime()
                    RETURN w.name AS name, a.code AS code
                    """,
                    {
                        "name": name,
                        "code": code,
                        "reasoning": c.reasoning,
                        "is_hazardous": c.is_hazardous,
                    },
                ).single()
                if record:
                    created += 1
                else:
                    skipped += 1

    # 6. Statistics
    chapter_counts = Counter(
        c.avv_code[:2] for _, c in all_classifications
    )
    hazardous_count = sum(1 for _, c in all_classifications if c.is_hazardous)

    return {
        "total_items": total_items,
        "classified": len(all_classifications),
        "failed": len(error_list),
        "hazardous": hazardous_count,
        "created": created,
        "skipped": skipped,
        "chapter_counts": dict(chapter_counts.most_common()),
        "dry_run": dry_run,
        "classifications": all_classifications,
        "error_list": error_list,
    }


# =============================================================================
# CLI
# =============================================================================


def main():
    parser = argparse.ArgumentParser(
        description="Classify WasteItem nodes with AVV codes (3-step hierarchical)"
    )
    parser.add_argument(
        "--dry-run", "-n", action="store_true",
        help="Classify but don't write CLASSIFIED_AS relationships",
    )
    parser.add_argument(
        "--limit", "-l", type=int, default=0,
        help="Max items to classify (default: 0 = all)",
    )
    parser.add_argument(
        "--delay", "-d", type=float, default=2.0,
        help="Seconds between items for rate limiting (default: 2.0)",
    )
    parser.add_argument(
        "--skip-classified", "-s", action="store_true",
        help="Only classify items without existing CLASSIFIED_AS relationship",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Enable verbose output (show each classification step)",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    try:
        stats = classify_waste_items(
            limit=args.limit,
            delay=args.delay,
            skip_classified=args.skip_classified,
            dry_run=args.dry_run,
            verbose=args.verbose,
        )

        print(f"\nAVV Classification Results (3-step hierarchical):")
        print(f"  WasteItems queried:  {stats['total_items']}")
        print(f"  Classified:          {stats['classified']}")
        print(f"  Failed:              {stats['failed']}")
        print(f"  Hazardous:           {stats['hazardous']}")

        if not stats["dry_run"]:
            print(f"  Relationships created: {stats['created']}")
            print(f"  Relationships skipped: {stats['skipped']}")
        else:
            print(f"  (dry run - no relationships written)")

        if stats["chapter_counts"]:
            print(f"\nBy AVV chapter:")
            for chapter, count in stats["chapter_counts"].items():
                chapter_name = CHAPTER_NAMES.get(chapter, "?")
                print(f"  {chapter} - {chapter_name}: {count}")

        if stats["error_list"]:
            print(f"\nFailed items ({len(stats['error_list'])}):")
            for name in stats["error_list"][:20]:
                print(f"  - {name}")
            if len(stats["error_list"]) > 20:
                print(f"  ... and {len(stats['error_list']) - 20} more")

        if stats["classifications"]:
            print(f"\nSample classifications:")
            for name, c in stats["classifications"][:15]:
                hz = "*" if c.is_hazardous else " "
                print(f"  {name:30s} -> {c.avv_code}{hz}  ({c.reasoning[:60]})")

    except Exception as e:
        logger.error(f"Classification failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
