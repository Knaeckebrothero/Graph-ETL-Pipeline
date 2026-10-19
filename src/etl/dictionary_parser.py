"""
Pure parsing logic for dictionary data sources.

Extracts German nouns from kaikki.org Wiktionary data, cross-references
with OdeNet synsets, and clusters them into concept groups.

No CLI, no I/O beyond library APIs. Called by dictionary.py.
"""

import logging
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


# =============================================================================
# Data Classes
# =============================================================================


@dataclass
class KaikkiEntry:
    """A single noun entry extracted from kaikki.org data."""

    word: str
    glosses: list[str] = field(default_factory=list)
    synonyms: list[str] = field(default_factory=list)
    translations_en: list[str] = field(default_factory=list)
    hypernyms: list[str] = field(default_factory=list)
    hyponyms: list[str] = field(default_factory=list)
    categories: list[str] = field(default_factory=list)
    forms: list[str] = field(default_factory=list)


@dataclass
class OdeNetIndex:
    """Lookup tables built from OdeNet synsets."""

    word_to_synsets: dict[str, list[str]] = field(default_factory=dict)
    synset_to_lemmas: dict[str, list[str]] = field(default_factory=dict)
    synset_to_hypernyms: dict[str, list[str]] = field(default_factory=dict)
    synset_to_hyponyms: dict[str, list[str]] = field(default_factory=dict)
    synset_to_translations_en: dict[str, list[str]] = field(default_factory=dict)


# =============================================================================
# Kaikki Extraction
# =============================================================================


def _extract_link_words(linkage_list: list[dict] | None) -> list[str]:
    """Extract word strings from a kaikki linkage list.

    Linkages are list of dicts with a 'word' key. Filters out
    thesaurus links and empty entries.
    """
    if not linkage_list:
        return []
    words = []
    for item in linkage_list:
        if isinstance(item, dict):
            w = item.get("word", "")
        elif isinstance(item, str):
            w = item
        else:
            continue
        if w and not w.startswith("Thesaurus:"):
            words.append(w)
    return words


def _extract_translations_en(entry: dict) -> list[str]:
    """Extract English translations from a kaikki entry."""
    translations = []

    # Top-level translations
    for t in entry.get("translations", []):
        if isinstance(t, dict) and t.get("code") == "en":
            w = t.get("word", "")
            if w:
                translations.append(w)

    # Sense-level translations
    for sense in entry.get("senses", []):
        for t in sense.get("translations", []):
            if isinstance(t, dict) and t.get("code") == "en":
                w = t.get("word", "")
                if w:
                    translations.append(w)

    return _dedupe(translations)


def _extract_glosses(entry: dict) -> list[str]:
    """Extract gloss strings from all senses."""
    glosses = []
    for sense in entry.get("senses", []):
        for g in sense.get("glosses", []):
            if g:
                glosses.append(g)
    return glosses


def _extract_categories(entry: dict) -> list[str]:
    """Extract category names from entry and senses."""
    cats = []

    # Top-level categories (list of strings)
    for c in entry.get("categories", []):
        if isinstance(c, str):
            cats.append(c)
        elif isinstance(c, dict):
            name = c.get("name", "")
            if name:
                cats.append(name)

    # Sense-level categories (list of dicts)
    for sense in entry.get("senses", []):
        for c in sense.get("categories", []):
            if isinstance(c, dict):
                name = c.get("name", "")
                if name:
                    cats.append(name)
            elif isinstance(c, str):
                cats.append(c)

    return _dedupe(cats)


def _extract_forms(entry: dict) -> list[str]:
    """Extract inflectional form strings."""
    forms = []
    for f in entry.get("forms", []):
        if isinstance(f, dict):
            form = f.get("form", "")
            if form and form != entry.get("word", ""):
                forms.append(form)
    return _dedupe(forms)


def _collect_linkages(entry: dict, field_name: str) -> list[str]:
    """Collect linkage words from both top-level and sense-level."""
    words = _extract_link_words(entry.get(field_name))
    for sense in entry.get("senses", []):
        words.extend(_extract_link_words(sense.get(field_name)))
    return _dedupe(words)


def _iter_kaikki_items(gz_path: "Path") -> "Iterator[dict]":
    """Iterate over wiktextract JSONL entries from a gzipped file."""
    import gzip
    import json

    with gzip.open(gz_path, "rt", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


def extract_kaikki_nouns(gz_path: "Path | None" = None) -> list[KaikkiEntry]:
    """Extract all German noun entries from kaikki.org data.

    Args:
        gz_path: Path to de-extract.jsonl.gz. If None, uses the default
            cache location in sources/large/dictionaries/.

    Returns:
        List of KaikkiEntry instances (~149k entries).
    """
    if gz_path is None:
        from pathlib import Path

        gz_path = (
            Path(__file__).parent.parent.parent / "sources" / "large" / "dictionaries" / "de-extract.jsonl.gz"
        )

    if not gz_path.exists():
        raise FileNotFoundError(
            f"Kaikki data not found at {gz_path}. "
            "Run `python -m src.etl.dictionary` to download it first."
        )

    entries = []
    count = 0
    skipped = 0

    logger.info(f"Extracting German nouns from {gz_path.name}...")

    for item in _iter_kaikki_items(gz_path):
        count += 1
        if count % 50000 == 0:
            logger.info(f"  Processed {count:,} entries, {len(entries):,} nouns so far...")

        if item.get("lang_code") != "de" or item.get("pos") != "noun":
            skipped += 1
            continue

        word = item.get("word", "").strip()
        if not word:
            skipped += 1
            continue

        entry = KaikkiEntry(
            word=word,
            glosses=_extract_glosses(item),
            synonyms=_collect_linkages(item, "synonyms"),
            translations_en=_extract_translations_en(item),
            hypernyms=_collect_linkages(item, "hypernyms"),
            hyponyms=_collect_linkages(item, "hyponyms"),
            categories=_extract_categories(item),
            forms=_extract_forms(item),
        )
        entries.append(entry)

    logger.info(
        f"Kaikki extraction complete: {len(entries):,} nouns from {count:,} entries "
        f"({skipped:,} non-noun skipped)"
    )
    return entries


# =============================================================================
# OdeNet Index
# =============================================================================


def build_odenet_index() -> OdeNetIndex:
    """Build lookup tables from OdeNet German WordNet synsets.

    Requires wn with odenet:1.4 and oewn:2024 already downloaded.

    Returns:
        OdeNetIndex with word-to-synset and synset-to-* mappings.
    """
    import wn

    index = OdeNetIndex()
    de = wn.Wordnet("odenet:1.4")

    synsets = de.synsets(pos="n")
    logger.info(f"Building OdeNet index from {len(synsets):,} noun synsets...")

    for ss in synsets:
        ss_id = ss.id
        lemmas = ss.lemmas()

        index.synset_to_lemmas[ss_id] = lemmas

        # Map each lemma to its synsets
        for lemma in lemmas:
            if lemma not in index.word_to_synsets:
                index.word_to_synsets[lemma] = []
            if ss_id not in index.word_to_synsets[lemma]:
                index.word_to_synsets[lemma].append(ss_id)

        # Hypernyms
        hyper_lemmas = []
        for h in ss.hypernyms():
            hyper_lemmas.extend(h.lemmas())
        if hyper_lemmas:
            index.synset_to_hypernyms[ss_id] = _dedupe(hyper_lemmas)

        # Hyponyms
        hypo_lemmas = []
        for h in ss.hyponyms():
            hypo_lemmas.extend(h.lemmas())
        if hypo_lemmas:
            index.synset_to_hyponyms[ss_id] = _dedupe(hypo_lemmas)

        # English translations via ILI
        # Note: .translate() returns synsets whose .lemmas() is empty
        # due to wn wordnet scoping; re-lookup via wn.synset() works.
        en_lemmas = []
        try:
            for en_ss in ss.translate(lexicon="oewn:2024"):
                real_ss = wn.synset(en_ss.id)
                en_lemmas.extend(real_ss.lemmas())
        except Exception:
            pass  # Some synsets have no ILI link
        if en_lemmas:
            index.synset_to_translations_en[ss_id] = _dedupe(en_lemmas)

    logger.info(
        f"OdeNet index built: {len(index.word_to_synsets):,} words, "
        f"{len(index.synset_to_lemmas):,} synsets, "
        f"{len(index.synset_to_translations_en):,} with English translations"
    )
    return index


# =============================================================================
# OpenThesaurus (deferred)
# =============================================================================


def load_openthesaurus_clusters() -> dict[str, list[str]]:
    """Load synonym clusters from OpenThesaurus.

    Deferred — OdeNet was seeded from OpenThesaurus so core synonym
    coverage exists. Returns empty dict. Can be implemented as an
    enrichment pass later.

    Returns:
        Empty dict (stub).
    """
    return {}


# =============================================================================
# Concept Clustering
# =============================================================================


class _UnionFind:
    """Disjoint-set data structure for transitive synonym grouping."""

    def __init__(self):
        self._parent: dict[str, str] = {}
        self._rank: dict[str, int] = {}

    def find(self, x: str) -> str:
        if x not in self._parent:
            self._parent[x] = x
            self._rank[x] = 0
        if self._parent[x] != x:
            self._parent[x] = self.find(self._parent[x])
        return self._parent[x]

    def union(self, x: str, y: str) -> None:
        rx, ry = self.find(x), self.find(y)
        if rx == ry:
            return
        if self._rank[rx] < self._rank[ry]:
            rx, ry = ry, rx
        self._parent[ry] = rx
        if self._rank[rx] == self._rank[ry]:
            self._rank[rx] += 1

    def groups(self) -> dict[str, list[str]]:
        """Return root -> list of all members."""
        result: dict[str, list[str]] = {}
        for x in self._parent:
            root = self.find(x)
            if root not in result:
                result[root] = []
            result[root].append(x)
        return result


def _select_canonical(
    words: list[str],
    odenet_lemmas: list[str] | None,
    hypernym_set: set[str],
) -> str:
    """Select the canonical word from a cluster.

    Heuristic:
        (a) A word that's a hypernym of others in the cluster.
        (b) First OdeNet lemma (lemma ordering reflects frequency).
        (c) Shortest single-word form.
    """
    # (a) Prefer a word that's a hypernym of others
    for w in words:
        if w in hypernym_set:
            return w

    # (b) First OdeNet lemma
    if odenet_lemmas:
        for lemma in odenet_lemmas:
            if lemma in words:
                return lemma

    # (c) Shortest single-word form
    single_words = [w for w in words if " " not in w and "-" not in w]
    if single_words:
        return min(single_words, key=len)

    return min(words, key=len)


def _dedupe(items: list[str]) -> list[str]:
    """Remove duplicates while preserving order."""
    seen: set[str] = set()
    result = []
    for item in items:
        if item not in seen:
            seen.add(item)
            result.append(item)
    return result


def cluster_concepts(
    kaikki_entries: list[KaikkiEntry],
    odenet_index: OdeNetIndex,
) -> list[dict]:
    """Cluster kaikki entries into concept groups using OdeNet synsets.

    Phase 1: Group entries by OdeNet synset_id (word lookup).
    Phase 2: Ungrouped entries clustered via union-find on synonym links.
    Phase 3: Build output dicts merging kaikki + OdeNet data.

    Args:
        kaikki_entries: Noun entries from extract_kaikki_nouns().
        odenet_index: Index from build_odenet_index().

    Returns:
        List of concept cluster dicts sorted by canonical name.
    """
    # Build word -> KaikkiEntry lookup
    word_to_entry: dict[str, KaikkiEntry] = {}
    for entry in kaikki_entries:
        word_to_entry[entry.word] = entry

    noun_set = set(word_to_entry.keys())

    # Phase 1: Group by OdeNet synset
    logger.info("Phase 1: Grouping entries by OdeNet synset...")
    synset_words: dict[str, list[str]] = {}  # synset_id -> [words]
    grouped_words: set[str] = set()

    for entry in kaikki_entries:
        synset_ids = odenet_index.word_to_synsets.get(entry.word, [])
        for ss_id in synset_ids:
            if ss_id not in synset_words:
                synset_words[ss_id] = []
            if entry.word not in synset_words[ss_id]:
                synset_words[ss_id].append(entry.word)
            grouped_words.add(entry.word)

    logger.info(
        f"  {len(grouped_words):,} words matched to {len(synset_words):,} synsets"
    )

    # Phase 2: Union-find for ungrouped entries
    logger.info("Phase 2: Clustering ungrouped entries via synonym links...")
    ungrouped = [e for e in kaikki_entries if e.word not in grouped_words]
    uf = _UnionFind()

    for entry in ungrouped:
        uf.find(entry.word)  # Ensure every ungrouped word is in the union-find
        for syn in entry.synonyms:
            if syn in noun_set and syn not in grouped_words:
                uf.union(entry.word, syn)

    uf_groups = uf.groups()
    logger.info(
        f"  {len(ungrouped):,} ungrouped words -> {len(uf_groups):,} synonym clusters"
    )

    # Phase 3: Build output clusters
    logger.info("Phase 3: Building concept clusters...")
    clusters = []

    # Collect all hypernyms for canonical selection
    all_hypernyms: set[str] = set()
    for hypers in odenet_index.synset_to_hypernyms.values():
        all_hypernyms.update(hypers)

    # 3a: Synset-based clusters
    for ss_id, words in synset_words.items():
        # Merge kaikki data for all words in the synset
        all_glosses = []
        all_synonyms = set(words)
        all_translations_en = []
        all_categories = []

        for w in words:
            entry = word_to_entry.get(w)
            if entry:
                all_glosses.extend(entry.glosses)
                all_synonyms.update(entry.synonyms)
                all_translations_en.extend(entry.translations_en)
                all_categories.extend(entry.categories)

        # Add OdeNet data
        odenet_lemmas = odenet_index.synset_to_lemmas.get(ss_id, [])
        all_synonyms.update(odenet_lemmas)
        odenet_trans = odenet_index.synset_to_translations_en.get(ss_id, [])
        all_translations_en.extend(odenet_trans)
        odenet_hypers = odenet_index.synset_to_hypernyms.get(ss_id, [])
        odenet_hypos = odenet_index.synset_to_hyponyms.get(ss_id, [])

        canonical = _select_canonical(
            list(all_synonyms), odenet_lemmas, all_hypernyms
        )
        all_synonyms.discard(canonical)

        sources = ["kaikki-de"]
        if odenet_lemmas:
            sources.append("odenet")

        clusters.append({
            "canonical": canonical,
            "lang": "de",
            "synonyms_de": sorted(all_synonyms),
            "translations_en": _dedupe(all_translations_en),
            "hypernyms": odenet_hypers,
            "hyponyms": odenet_hypos,
            "synset_id": ss_id,
            "categories": _dedupe(all_categories),
            "glosses": _dedupe(all_glosses),
            "sources": sources,
        })

    # 3b: Union-find-based clusters (no synset)
    for root, members in uf_groups.items():
        all_glosses = []
        all_synonyms = set(members)
        all_translations_en = []
        all_categories = []
        all_hypernyms_entry = []
        all_hyponyms_entry = []

        for w in members:
            entry = word_to_entry.get(w)
            if entry:
                all_glosses.extend(entry.glosses)
                all_synonyms.update(entry.synonyms)
                all_translations_en.extend(entry.translations_en)
                all_categories.extend(entry.categories)
                all_hypernyms_entry.extend(entry.hypernyms)
                all_hyponyms_entry.extend(entry.hyponyms)

        canonical = _select_canonical(members, None, all_hypernyms)
        all_synonyms.discard(canonical)

        clusters.append({
            "canonical": canonical,
            "lang": "de",
            "synonyms_de": sorted(all_synonyms),
            "translations_en": _dedupe(all_translations_en),
            "hypernyms": _dedupe(all_hypernyms_entry),
            "hyponyms": _dedupe(all_hyponyms_entry),
            "synset_id": None,
            "categories": _dedupe(all_categories),
            "glosses": _dedupe(all_glosses),
            "sources": ["kaikki-de"],
        })

    # Sort by canonical name for deterministic output
    clusters.sort(key=lambda c: c["canonical"])

    logger.info(
        f"Clustering complete: {len(clusters):,} concept clusters "
        f"({len(synset_words):,} synset-based, {len(uf_groups):,} synonym-based)"
    )
    return clusters
