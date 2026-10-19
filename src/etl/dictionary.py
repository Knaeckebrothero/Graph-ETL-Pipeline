"""
ETL: Dictionary data pipeline — parse, import, and classify concept clusters.

Subcommands:
    parse     Download raw data, parse German nouns, produce concepts.jsonl (default)
    import    Load concepts.jsonl into Neo4j as DictionaryEntry nodes
    classify  Classify DictionaryEntry nodes as physical/non-physical via LLM

Usage:
    python -m src.etl.dictionary                          # parse (default)
    python -m src.etl.dictionary parse -o custom.jsonl
    python -m src.etl.dictionary import --dry-run -v
    python -m src.etl.dictionary import --batch-size 1000
    python -m src.etl.dictionary classify --dry-run -v
    python -m src.etl.dictionary classify --delay 1.0
"""

import argparse
import hashlib
import json
import logging
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

logger = logging.getLogger(__name__)

# Default paths
DATA_DIR = Path(__file__).parent.parent.parent / "sources" / "large" / "dictionaries"
DEFAULT_OUTPUT = DATA_DIR / "concepts.jsonl"


def generate_uid(name: str) -> str:
    """Generate deterministic UID from concept name."""
    return hashlib.sha256(name.encode()).hexdigest()[:16]


def ensure_data_dir() -> Path:
    """Create sources/large/dictionaries/ directory if it doesn't exist.

    Returns:
        Path to the sources/large/dictionaries/ directory.
    """
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    gitignore = DATA_DIR / ".gitignore"
    if not gitignore.exists():
        gitignore.write_text("*\n!.gitignore\n")

    return DATA_DIR


KAIKKI_URL = "https://kaikki.org/dictionary/downloads/de/de-extract.jsonl.gz"
KAIKKI_CACHE = DATA_DIR / "de-extract.jsonl.gz"


def ensure_downloads() -> None:
    """Download and cache all required dictionary data.

    - kaikki.org: downloads de-extract.jsonl.gz (~280 MB)
    - wn: downloads OdeNet 1.4 and OEWN 2024 SQLite databases
    """
    import wn

    ensure_data_dir()
    logger.info("Ensuring dictionary data is downloaded...")

    # OdeNet (German WordNet)
    logger.info("  Downloading OdeNet 1.4 (if not cached)...")
    wn.download("odenet:1.4", progress_handler=None)

    # Open English WordNet (for ILI translations)
    logger.info("  Downloading OEWN 2024 (if not cached)...")
    wn.download("oewn:2024", progress_handler=None)

    # kaikki.org German extract — download ourselves since the kaikki-json
    # package's built-in URL is broken (404)
    if KAIKKI_CACHE.exists():
        size_mb = KAIKKI_CACHE.stat().st_size / (1024 * 1024)
        logger.info(f"  kaikki.org data already cached ({size_mb:.0f} MB)")
    else:
        import requests

        logger.info(f"  Downloading kaikki.org German extract (~280 MB)...")
        response = requests.get(KAIKKI_URL, stream=True)
        response.raise_for_status()

        total = int(response.headers.get("Content-Length", 0))
        downloaded = 0
        with open(KAIKKI_CACHE, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)
                downloaded += len(chunk)
                if total:
                    pct = downloaded * 100 // total
                    logger.info(f"    {pct}% ({downloaded // (1024*1024)} MB)")

        size_mb = KAIKKI_CACHE.stat().st_size / (1024 * 1024)
        logger.info(f"  Downloaded: {KAIKKI_CACHE} ({size_mb:.0f} MB)")

    logger.info("Downloads ready.")


def parse_dictionaries(output_path: Path, dry_run: bool = False) -> dict:
    """Parse dictionary data and produce concept cluster JSONL.

    Args:
        output_path: Path for the output JSONL file.
        dry_run: If True, compute stats but don't write the file.

    Returns:
        Dict with parsing statistics.
    """
    from src.etl.dictionary_parser import (
        build_odenet_index,
        cluster_concepts,
        extract_kaikki_nouns,
    )

    # Step 1: Extract kaikki nouns
    kaikki_entries = extract_kaikki_nouns()

    # Step 2: Build OdeNet index
    odenet_index = build_odenet_index()

    # Step 3: Cluster concepts
    clusters = cluster_concepts(kaikki_entries, odenet_index)

    # Add UIDs to clusters
    for cluster in clusters:
        cluster["uid"] = generate_uid(cluster["canonical"])

    # Compute stats
    synset_count = sum(1 for c in clusters if c.get("synset_id"))
    synonym_count = sum(1 for c in clusters if not c.get("synset_id"))
    with_translations = sum(1 for c in clusters if c.get("translations_en"))
    with_hypernyms = sum(1 for c in clusters if c.get("hypernyms"))
    total_synonyms = sum(len(c.get("synonyms_de", [])) for c in clusters)

    stats = {
        "kaikki_nouns": len(kaikki_entries),
        "total_clusters": len(clusters),
        "synset_clusters": synset_count,
        "synonym_clusters": synonym_count,
        "with_translations": with_translations,
        "with_hypernyms": with_hypernyms,
        "total_synonym_links": total_synonyms,
        "dry_run": dry_run,
    }

    if dry_run:
        logger.info("DRY RUN — no file written")
        logger.info(f"Would write {len(clusters):,} clusters to {output_path}")
        return stats

    # Write JSONL
    ensure_data_dir()
    logger.info(f"Writing {len(clusters):,} clusters to {output_path}...")

    with open(output_path, "w", encoding="utf-8") as f:
        for cluster in clusters:
            f.write(json.dumps(cluster, ensure_ascii=False) + "\n")

    logger.info(f"Output written: {output_path}")
    return stats


def _run_parse(args):
    """Execute the parse subcommand."""
    try:
        ensure_downloads()
        stats = parse_dictionaries(args.output, dry_run=args.dry_run)

        print(f"\nParsing Statistics:")
        print(f"  Kaikki nouns extracted:  {stats['kaikki_nouns']:,}")
        print(f"  Total concept clusters:  {stats['total_clusters']:,}")
        print(f"    Synset-based:          {stats['synset_clusters']:,}")
        print(f"    Synonym-based:         {stats['synonym_clusters']:,}")
        print(f"  With English translations: {stats['with_translations']:,}")
        print(f"  With hypernyms:          {stats['with_hypernyms']:,}")
        print(f"  Total synonym links:     {stats['total_synonym_links']:,}")
        if stats["dry_run"]:
            print("  (dry run — no file written)")
        else:
            print(f"  Output: {args.output}")
    except Exception as e:
        logger.error(f"Dictionary parsing failed: {e}")
        raise


def _run_import(args):
    """Execute the import subcommand."""
    from src.etl.dictionary_importer import import_dictionary_entries

    input_path = args.input
    if not input_path.exists():
        logger.error(f"Input file not found: {input_path}")
        logger.error("Run 'python -m src.etl.dictionary parse' first to generate it.")
        sys.exit(1)

    try:
        stats = import_dictionary_entries(
            input_path=input_path,
            batch_size=args.batch_size,
            dry_run=args.dry_run,
        )

        print(f"\nImport Statistics:")
        print(f"  Clusters loaded:       {stats['clusters_loaded']:,}")
        print(f"  Canonical nodes:       {stats['canonical_nodes']:,}")
        print(f"  SYNONYM_OF edges:      {stats['synonym_edges']:,}")
        print(f"  TRANSLATION_OF edges:  {stats['translation_edges']:,}")
        print(f"  IS_A edges:            {stats['hypernym_edges']:,}")
        print(f"  DERIVED_FROM edges:    {stats['provenance_edges']:,}")
        if stats["dry_run"]:
            print("  (dry run — no database changes)")
    except Exception as e:
        logger.error(f"Dictionary import failed: {e}")
        raise


def _run_classify(args):
    """Execute the classify subcommand."""
    from src.etl.dictionary_classifier import classify_dictionary_entries

    input_path = args.input
    if not input_path.exists():
        logger.error(f"Input file not found: {input_path}")
        logger.error("Run 'python -m src.etl.dictionary parse' first to generate it.")
        sys.exit(1)

    try:
        stats = classify_dictionary_entries(
            input_path=input_path,
            batch_size=args.batch_size,
            dry_run=args.dry_run,
            delay=args.delay,
        )

        print(f"\nClassification Statistics:")
        print(f"  Total clusters:         {stats['total_clusters']:,}")
        print(f"  Base forms:             {stats['base_forms']:,}")
        print(f"  Inflected forms:        {stats['inflected_forms']:,}")
        print(f"  LLM batches needed:     {stats['llm_batches_needed']:,}")
        if not stats["dry_run"]:
            print(f"  LLM classified:         {stats['llm_classified']:,}")
            print(f"  Inherited from base:    {stats['inherited']:,}")
            print(f"  Skipped (already done): {stats['skipped_already_classified']:,}")
            print(f"  No base match:          {stats['skipped_no_base_match']:,}")
            print(f"  Written to DB:          {stats['written_to_db']:,}")
            print(f"  Promoted to :WasteItem: {stats['promoted_to_waste_item']:,}")
        if stats["dry_run"]:
            print("  (dry run — no LLM calls or database changes)")
    except Exception as e:
        logger.error(f"Dictionary classification failed: {e}")
        raise


def main():
    parser = argparse.ArgumentParser(
        description="Dictionary ETL: parse raw data or import into Neo4j"
    )
    subparsers = parser.add_subparsers(dest="command")

    # --- parse subcommand ---
    parse_cmd = subparsers.add_parser(
        "parse",
        help="Download raw data and produce concepts.jsonl",
    )
    parse_cmd.add_argument(
        "--output", "-o",
        type=Path,
        default=DEFAULT_OUTPUT,
        help="Path for output JSONL (default: sources/large/dictionaries/concepts.jsonl)",
    )
    parse_cmd.add_argument(
        "--dry-run", "-n",
        action="store_true",
        help="Show stats without writing output file",
    )
    parse_cmd.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose output",
    )

    # --- import subcommand ---
    import_cmd = subparsers.add_parser(
        "import",
        help="Load concepts.jsonl into Neo4j as DictionaryEntry nodes",
    )
    import_cmd.add_argument(
        "--input", "-i",
        type=Path,
        default=DEFAULT_OUTPUT,
        help="Path to concepts.jsonl (default: sources/large/dictionaries/concepts.jsonl)",
    )
    import_cmd.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Records per UNWIND batch (default: 500)",
    )
    import_cmd.add_argument(
        "--dry-run", "-n",
        action="store_true",
        help="Compute stats without writing to database",
    )
    import_cmd.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose output",
    )

    # --- classify subcommand ---
    classify_cmd = subparsers.add_parser(
        "classify",
        help="Classify DictionaryEntry nodes as physical/non-physical via LLM",
    )
    classify_cmd.add_argument(
        "--input", "-i",
        type=Path,
        default=DEFAULT_OUTPUT,
        help="Path to concepts.jsonl (default: sources/large/dictionaries/concepts.jsonl)",
    )
    classify_cmd.add_argument(
        "--batch-size",
        type=int,
        default=25,
        help="Items per LLM call (default: 25)",
    )
    classify_cmd.add_argument(
        "--delay",
        type=float,
        default=1.0,
        help="Seconds between LLM calls (default: 1.0 for paid Groq tier)",
    )
    classify_cmd.add_argument(
        "--dry-run", "-n",
        action="store_true",
        help="Show breakdown without LLM calls or database changes",
    )
    classify_cmd.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose output",
    )

    args = parser.parse_args()

    # Default to parse when no subcommand given
    if args.command is None:
        args.command = "parse"
        args.output = DEFAULT_OUTPUT
        args.dry_run = False
        args.verbose = False

    # Configure logging
    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    if args.command == "parse":
        _run_parse(args)
    elif args.command == "import":
        _run_import(args)
    elif args.command == "classify":
        _run_classify(args)


if __name__ == "__main__":
    main()
