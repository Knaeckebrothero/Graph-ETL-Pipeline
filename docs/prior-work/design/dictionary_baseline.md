# Dictionary Baseline: Top-Down Graph Seeding

> **Status:** Milestones 1–4 complete, Milestone 5 next
> **Last updated:** 2026-02-26

## Motivation

The Fessi knowledge graph answers one core question: **"How do I dispose of X?"** This requires item nodes as anchors that the chatbot agent traverses to find disposal routes. The previous graph prototype had 548 items imported from a single CSV (Abfall-ABC), but this covered only a fraction of what people actually throw away. The 500+ researched sources we have contain disposal *rules* and *conditions*, not item definitions. Running them through the LLM pipeline without a broad anchor layer means entity creation and entity resolution happen simultaneously — a well-documented anti-pattern in knowledge graph construction.

This document argues for a **top-down, ontology-first** approach: seed the graph with structured dictionary data before running the LLM extraction pipeline. We start from a clean database — the dictionary import is the first phase of the new graph.

### Two-Label Strategy

Dictionary entries are imported as `:DictionaryEntry` nodes, not directly as `:WasteItem`. After LLM classification validates that an entry represents a physical, disposable object, it gets promoted to `:WasteItem`. This keeps the curated waste-item namespace clean while the full dictionary vocabulary remains available for agent lookup and synonym matching.

```
:DictionaryEntry  →  LLM classifies is_physical=true  →  add :WasteItem label
                  →  LLM classifies is_physical=false  →  stays :DictionaryEntry only
```

The agent queries both labels: `:WasteItem` for disposal routing, `:DictionaryEntry` for "I know this term but it's not a waste item" responses.

---

## The Case Against Bottom-Up Construction

The bottom-up approach — letting the LLM pipeline create nodes as it encounters items in source documents — has five structural problems.

### 1. Duplicate Explosion

Source A mentions "Schokolade", source B "Schokoriegel", source C "chocolate bar". Without pre-existing anchors, the pipeline creates three separate nodes for what is semantically one item. Each source independently invents its own terminology, and the graph accumulates redundant nodes proportionally to the number of sources.

### 2. Inconsistent Granularity

One source discusses "Batterie", another "Knopfzelle", a third "AA-Batterie". Without a taxonomy, the pipeline cannot decide whether these are synonyms, siblings, or a parent-child hierarchy. The result is a flat graph where structurally related items have no relationship.

### 3. Deduplication is Destructive

Merging nodes after the fact requires fuzzy matching across names, synonyms, and languages. Every merge is a destructive operation that risks losing relationship edges and breaks provenance links. Prevention is categorically cheaper than retroactive cleanup.

### 4. Recall is Limited to Source Vocabulary

A student searches "Tafelschokolade" but the graph only contains "Schokolade" because that's what appeared in the source PDFs. Without upfront synonym seeding, the graph's recall is bounded by the vocabulary of whichever documents were processed.

### 5. The LLM Has Two Competing Tasks

The extraction model must simultaneously (a) understand document content and extract disposal rules, and (b) decide whether each entity is new or matches an existing node. These are fundamentally different tasks. Combining them degrades both. Ontology-grounded construction separates vocabulary building from knowledge attachment.

---

## The Case For Ontology-First Construction

Recent research validates this approach. A 2025 study comparing ontology-grounded vs. extraction-first knowledge graph construction for RAG found that **ontology-first KGs achieved 90% answer accuracy**, compared to 60% for vector-RAG baselines. The key finding: "aligning a Knowledge Graph with an ontology extracted from a static [structured] source yields performance comparable to ontologies derived from dynamic text corpora" while "substantially reducing computational costs."

The dictionary baseline applies this principle: use structured lexical databases as the stable ontological layer, then attach domain knowledge from unstructured sources.

### What Changes

| Aspect | Bottom-Up | Top-Down (Dictionary) |
|--------|-----------|----------------------|
| Entity creation | LLM creates nodes on-the-fly | Pre-seeded from dictionary as `:DictionaryEntry`, promoted to `:WasteItem` after validation |
| Entity resolution | Fuzzy match + merge (destructive) | Direct match against existing nodes |
| Synonym coverage | Only what sources mention | Full dictionary coverage (150k+ German nouns) |
| Taxonomy | Flat, no hierarchy | WordNet hypernym/hyponym trees |
| Multilingual support | Only if sources are multilingual | Built-in English nodes with `TRANSLATION_OF` edges |
| LLM task complexity | Extraction + resolution | Extraction + matching (simpler) |
| Agent recall | Source-vocabulary-bounded | Dictionary-vocabulary-bounded |

### Separation of Concerns

The top-down approach cleanly separates two phases:

1. **Vocabulary building** (dictionary import): deterministic, structured, no LLM needed. Creates the noun inventory with synonyms, translations, and taxonomy.
2. **Knowledge attachment** (LLM pipeline): processes 500+ sources and attaches disposal rules, conditions, and facility links to existing nodes. The LLM focuses on what it's good at — understanding unstructured text — without the burden of entity resolution.

### Graceful Coverage of Unknown Items

A student who searches "Flugzeug" in the waste chatbot should get a helpful "this item cannot be disposed at the university" response, not silence. The two-label strategy makes this explicit: "Flugzeug" exists as a `:DictionaryEntry` (the agent recognizes the term) but was not promoted to `:WasteItem` (no disposal route). The agent can distinguish three states:

1. **`:WasteItem` with disposal edges** — "Dispose of Schokolade in Biotonne"
2. **`:WasteItem` without disposal edges** — "I know this is a waste item but I don't have disposal info yet"
3. **`:DictionaryEntry` only** — "This isn't something you'd dispose of at the university"
4. **No match** — "I don't recognize this term"

### LLM Cleanup is Cheap

Small models (Llama 8B, GPT-OSS-20B) via Groq can classify and clean up 100k+ nodes for under $5 total. Post-import tasks like physicality classification, category assignment, and near-duplicate merging are straightforward classification problems that small models handle well.

---

## Data Sources

### Tier 1: Primary Sources

#### Kaikki.org / Wiktextract (Breadth)

Structured data extracted from the **German** Wiktionary (de.wiktionary.org) using [wiktextract](https://github.com/tatuylonen/wiktextract). Provides the raw noun inventory. The German Wiktionary has broader German-language coverage than the English Wiktionary edition.

| Property | Value |
|----------|-------|
| German nouns extracted | **355,660** (filtered by `lang_code == "de"` and `pos == "noun"`) |
| Total entries in dump (all languages/POS) | 1,297,268 |
| Format | JSONL (one JSON object per word sense) |
| Raw file size | ~280 MB compressed (`de-extract.jsonl.gz`) |
| Download URL | `https://kaikki.org/dictionary/downloads/de/de-extract.jsonl.gz` |
| License | CC-BY-SA 3.0 + GFDL |
| Python access | Direct download + `gzip`/`json` parsing (see implementation notes) |

Each JSONL entry contains:
- `word`: headword
- `lang_code`: ISO 639-1 code
- `pos`: part of speech
- `senses`: array of definitions with glosses
- `translations`: cross-language equivalents by sense
- Linkages: `synonyms`, `antonyms`, `hypernyms`, `hyponyms`, `meronyms`, `holonyms`
- `categories`: semantic categories
- `forms`: inflectional variants
- `derived`: derived terms

**Note:** The postprocessed JSONL files on kaikki.org are deprecated. The pipeline uses the raw `de-extract.jsonl.gz` from the [raw data page](https://kaikki.org/dictionary/rawdata.html). The German Wiktionary dump contains entries for **all** languages defined in de.wiktionary.org, so filtering by `lang_code == "de"` is essential.

**Implementation note:** The `kaikki-json` Python package was evaluated but its built-in download URL is broken (404). The pipeline downloads the file directly via `requests` streaming and parses it with `gzip` + `json.loads` line-by-line.

#### OdeNet — Open German WordNet (Structure)

German WordNet providing synset-based taxonomy with interlingual links to English.

| Property | Value |
|----------|-------|
| Synsets | ~36,000 |
| Version | 1.4 |
| License | CC-BY-SA 4.0 (data), MIT (code) |
| Origin | Hochschule Darmstadt (Sprachtechnologie) |
| Install | `pip install wn && python -m wn download odenet:1.4` |
| Built from | OpenThesaurus synonym clusters + Princeton WordNet |

OdeNet provides what Kaikki.org lacks: a proper hypernym/hyponym tree. "Schokoriegel" → "Schokolade" → "Suesswaren" → "Lebensmittel". The `wn` Python package enables unified queries across OdeNet and the English WordNet via the Collaborative Interlingual Index (ILI).

```python
import wn
wn.download('odenet:1.4')
wn.download('oewn:2024')

# German synsets for "Batterie"
de_synsets = wn.synsets('Batterie', lang='de')

# Hypernym traversal
for ss in de_synsets:
    print(ss.hypernyms())

# Interlingual: German → English
for ss in de_synsets:
    for ili_ss in ss.translate('oewn:2024'):
        print(ili_ss.lemmas())
```

#### Open English WordNet (English Taxonomy)

| Property | Value |
|----------|-------|
| Version | 2024 edition |
| Synsets | ~120,000 |
| License | MIT |
| Install | `python -m wn download oewn:2024` |

Provides the English-side taxonomy accessible through the same `wn` API as OdeNet. Used for English translations and hypernym trees when OdeNet coverage is sparse.

### Tier 2: Enrichment Sources

#### OpenThesaurus (German Synonym Depth)

| Property | Value |
|----------|-------|
| Synonyms | 280,000+ |
| Dump formats | Text (1.57 MB zipped), MySQL (6.30 MB) |
| Last dump | 2026-02-24 |
| License | CC-BY-SA 4.0 or LGPL 2.1 (user's choice) |
| Download | [openthesaurus.de/about/download](https://www.openthesaurus.de/about/download) |
| Python access | `pip install py-openthesaurus` |

Catches colloquial and regional German synonyms that Wiktionary misses. "Muell" / "Abfall" / "Unrat" / "Kehricht" are all in the same cluster. OdeNet was originally seeded from OpenThesaurus, but OpenThesaurus has continued growing independently.

#### Wikidata (Instance-Level Items)

Structured items with multilingual labels and "instance of" / "subclass of" hierarchies. Accessed via SPARQL or imported into Neo4j using Neosemantics (n10s). Useful for brand-to-category mappings ("Milka" → instance of "chocolate brand" → subclass of "confectionery"). CC0 licensed.

### Tier 3: Optional (License-Dependent)

| Source | Value | Access |
|--------|-------|--------|
| **GermaNet** | Gold-standard German WordNet. GermaNet 20.0 (Nov 2025). | Free academic license — must be requested by thesis advisor, not individual students. Uni Tubingen. |
| **BabelNet** | 23M synsets, 600 languages. Integrates WordNet + Wiktionary + Wikipedia. | Academic download, 45 GB, approval required. |

### Source Selection Rationale

| Decision | Rationale |
|----------|-----------|
| Kaikki.org over Duden/PONS | Open license, JSONL bulk download, no scraping needed. Duden is copyrighted. |
| OdeNet over GermaNet | No license application required, same `wn` API, CC-BY-SA. GermaNet needs advisor signature. |
| Raw JSONL over postprocessed | Postprocessed files are deprecated on kaikki.org. Raw `de-extract.jsonl` is the supported path. |
| `requests` + `wn` packages | `kaikki-json` download URL is broken (404); replaced with direct `requests` download. `wn` works well for OdeNet/OEWN access. |

---

## Graph Schema Extensions

The current schema (`src/db/schema.cql`) has no label for dictionary entries, no relationship types for synonyms, translations, or taxonomic hierarchy. The dictionary import requires a new node label, three new relationship types, and updated constraints.

### New Node Label: DictionaryEntry

All dictionary-sourced nodes are imported as `:DictionaryEntry`. After LLM classification, validated physical items receive an additional `:WasteItem` label (multi-label node). This means a promoted node is both `:DictionaryEntry` AND `:WasteItem`, queryable via either label.

```
(:DictionaryEntry)                          — abstract noun, not disposable
(:DictionaryEntry:WasteItem)                — promoted: physical, disposable item
```

### New Relationships

```
(:DictionaryEntry)-[:SYNONYM_OF]->(:DictionaryEntry)
```
Synonym link within the same language. Both nodes represent the same concept. The canonical node is the one with the shortest, most generic German name (heuristic: pick the hypernym among synonyms, or the shortest lemma).

```
(:DictionaryEntry)-[:TRANSLATION_OF]->(:DictionaryEntry)
```
Cross-language link. Connects "chocolate" (`lang: "en"`) to "Schokolade" (`lang: "de"`). Direction: English node points to the German canonical. English entries are full `:DictionaryEntry` nodes, enabling the agent to handle English queries natively via graph traversal.

```
(:DictionaryEntry)-[:IS_A]->(:DictionaryEntry)
```
Taxonomic hierarchy (hyponym → hypernym). "Schokoriegel" IS_A "Schokolade" IS_A "Suesswaren" IS_A "Lebensmittel". The agent traverses upward to find the nearest node with disposal information attached.

### Node Properties

```cypher
(:DictionaryEntry {
    uid: "abc123...",
    name: "Schokolade",
    lang: "de",                    // ISO 639-1 language code
    source_dict: "kaikki-de",     // Dictionary source identifier
    pos: "noun",                   // Part of speech (always "noun" for this import)
    synset_id: "odenet-12345",    // OdeNet/WordNet synset ID (cross-source dedup key)
    is_physical: true              // LLM-classified: can this be a physical waste item?
})
```

### New Constraints and Indexes

```cypher
// DictionaryEntry constraints
CREATE CONSTRAINT dict_entry_uid IF NOT EXISTS FOR (d:DictionaryEntry) REQUIRE d.uid IS UNIQUE;
CREATE CONSTRAINT dict_entry_name IF NOT EXISTS FOR (d:DictionaryEntry) REQUIRE d.name IS UNIQUE;

// Indexes for dictionary queries
CREATE INDEX dict_entry_lang IF NOT EXISTS FOR (d:DictionaryEntry) ON (d.lang);
CREATE INDEX dict_entry_source IF NOT EXISTS FOR (d:DictionaryEntry) ON (d.source_dict);
CREATE INDEX dict_entry_synset IF NOT EXISTS FOR (d:DictionaryEntry) ON (d.synset_id);
CREATE INDEX dict_entry_physical IF NOT EXISTS FOR (d:DictionaryEntry) ON (d.is_physical);
```

### Promotion Query

After LLM classification sets `is_physical = true`, promote to `:WasteItem`:

```cypher
MATCH (d:DictionaryEntry {is_physical: true})
WHERE NOT d:WasteItem
SET d:WasteItem
```

This is a single Cypher query that adds the `:WasteItem` label to all classified physical items. The existing `WasteItem` constraints (uid, name uniqueness) apply automatically after promotion.

---

## Import Pipeline

### Overview

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  Phase 0    │     │  Phase 1    │     │  Phase 2    │     │  Phase 3    │     │  Phase 3b   │
│  Download   │────>│  Parse &    │────>│  LLM        │────>│  Neo4j      │────>│  Promote    │
│  & Cache    │     │  Filter     │     │  Classify   │     │  Import     │     │  to WasteItem│
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
    Local               Local             Groq API            Local               Local
    ~3 GB               ~150k nouns       ~$0.50         :DictionaryEntry      :WasteItem label
```

### Phase 0: Download and Cache

```
kaikki.org de-extract.jsonl.gz (280 MB)  ─┐
kaikki.org en-extract.jsonl.gz           ─┤
OdeNet 1.4 (via `wn download`)           ├──> data/dictionaries/
Open English WordNet (via `wn download`)  ─┤
OpenThesaurus text dump (1.57 MB)        ─┘
```

All downloads are cached locally. The `wn` package manages its own SQLite database. Kaikki.org files are downloaded once and reprocessed as needed.

### Phase 1: Parse and Filter

```
Raw JSONL (de-extract.jsonl.gz, 280 MB)
        │
        ▼
  Filter: lang_code == "de" AND pos == "noun" (355,660 entries from 1.3M)
        │
        ▼
  Extract per entry: word, glosses, synonyms, translations, hypernyms, categories
        │
        ▼
  Cross-reference with OdeNet synsets (synset_id as dedup key)
        │
        ▼
  Group into concept clusters:
    Phase 1: Synset-based grouping (16,001 clusters from 28,816 matched words)
    Phase 2: Union-find synonym clustering (313,230 clusters from remaining words)
    Phase 3: Build output records with canonical selection
        │
        ▼
  Output: concepts.jsonl (329,231 clusters, 177 MB)
```

A **concept cluster** groups a canonical term with its synonyms, translations, and taxonomy position:

```json
{
    "canonical": "Schokolade",
    "lang": "de",
    "synonyms_de": ["Schokoriegel", "Tafelschokolade", "Konfekt"],
    "translations_en": ["chocolate", "chocolate bar"],
    "hypernyms": ["Suesswaren", "Lebensmittel"],
    "hyponyms": ["Vollmilchschokolade", "Zartbitterschokolade"],
    "synset_id": "odenet-08573",
    "categories": ["Food", "Sweets"],
    "sources": ["kaikki-de", "odenet", "openthesaurus"]
}
```

**Canonical selection heuristic:** Among a synonym cluster, pick the term that (a) appears as a hypernym of the others in OdeNet, or failing that, (b) is the shortest single-word lemma. This prefers "Schokolade" over "Schokoriegel" and "Batterie" over "Knopfzelle".

### Phase 2: LLM Classification (Recommended)

Run concept clusters through a small model (Llama 8B / GPT-OSS-20B via Groq) in batch:

| Field | Question | Examples |
|-------|----------|----------|
| `is_physical` | Can this noun refer to a physical object? | "Schokolade" → true, "Demokratie" → false |
| `disposal_relevant` | Could someone plausibly need to dispose of this? | "Batterie" → true, "Ozean" → false |
| `category` | Broad waste category | food, electronics, chemicals, packaging, textiles, furniture, construction, hazardous, other |

This step is optional — non-physical items without disposal routes attached are harmless in the graph. But classification improves agent response quality and enables category-based browsing.

**Expected yield:** ~150k nouns → ~30-60k classified as `is_physical=true` and later promoted to `:WasteItem`. The remaining ~90-120k stay as `:DictionaryEntry` only. The exact split depends on how aggressively the model filters abstract nouns, place names, and proper nouns that slipped through POS filtering.

### Phase 3: Neo4j Import

```cypher
// 1. Create canonical DictionaryEntry node
MERGE (d:DictionaryEntry {name: $canonical})
ON CREATE SET
    d.uid = $uid,
    d.lang = $lang,
    d.source_dict = $source,
    d.synset_id = $synset_id,
    d.created_at = datetime()

// 2. Create synonym nodes + SYNONYM_OF edges
UNWIND $synonyms AS syn
MERGE (s:DictionaryEntry {name: syn})
ON CREATE SET s.uid = $syn_uid, s.lang = $lang, s.source_dict = $source
MERGE (s)-[:SYNONYM_OF]->(d)

// 3. Create English translation nodes + TRANSLATION_OF edges
UNWIND $translations AS trans
MERGE (t:DictionaryEntry {name: trans})
ON CREATE SET t.uid = $trans_uid, t.lang = "en", t.source_dict = $source
MERGE (t)-[:TRANSLATION_OF]->(d)

// 4. Create hypernym edges (IS_A)
UNWIND $hypernyms AS hyper
MERGE (h:DictionaryEntry {name: hyper})
ON CREATE SET h.uid = $hyper_uid, h.lang = $lang, h.source_dict = $source
MERGE (d)-[:IS_A]->(h)

// 5. Provenance
MERGE (src:Source {name: $source_name, type: "dictionary"})
MERGE (d)-[:DERIVED_FROM]->(src)
```

All operations use `MERGE` for idempotent re-runs, consistent with the existing project pattern.

### Phase 3b: LLM Classification and Promotion

After import, run the LLM classification batch (Phase 2 output), then promote:

```cypher
// Set classification results from LLM batch
MATCH (d:DictionaryEntry {name: $name})
SET d.is_physical = $is_physical,
    d.category = $category

// Promote all physical items to WasteItem
MATCH (d:DictionaryEntry {is_physical: true})
WHERE NOT d:WasteItem
SET d:WasteItem
```

This can be run incrementally — reclassify and re-promote as needed without affecting existing edges.

### Phase 4: Post-Import LLM Cleanup

After the dictionary baseline is loaded, run cheap LLM passes to:

1. **Merge near-duplicates** that escaped deduplication (e.g., compound noun variants, inflectional forms that POS filtering didn't catch).
2. **Promote/demote** edges: if two synonyms turn out to have different disposal paths once the LLM pipeline attaches rules, promote the synonym to a full node with its own `IS_A` edge.
3. **Prune orphan chains**: remove hypernym chains that lead nowhere useful (e.g., abstract top-level synsets like "Entitaet" or "Ding").
4. **Reclassify edge cases**: re-run classification on nodes the LLM was uncertain about.

---

## Integration with Existing Pipeline

We start from a clean graph. The dictionary baseline is the **first import phase**, followed by reference data, product data, and then LLM extraction:

```
Phase 0: Schema + Ref    (init_db.py --schema)          ← DONE — v5.0 constraints + 5 WasteStream nodes
Phase 1: Dictionary      (src/etl/dictionary.py)        ← DONE (M1+M2) — 329k clusters → 424k nodes + 589k edges
Phase 1b: Promotion      (LLM classification)            ← DONE (M3) — 67,912 get :WasteItem label
Phase 2: Materials       (src/etl/materials.py)          ← DONE — 90 Material nodes + IS_A + DEFAULT_STREAM
Phase 3: Products        (src/etl/products.py)                   — 4.29M Open Food Facts → Product + PackagingComponent
Phase 4: LLM Extraction  (src/llm_etl/*)                         — attaches disposal rules from 500+ sources
```

**Dependency chain:** Schema → WasteStream → Materials → Products. Materials link to WasteStreams via `DEFAULT_STREAM`; Products link to Materials via `MADE_OF`.

After the dictionary baseline, the LLM pipeline benefits from:
- **50-100x more anchor nodes** to match against
- **Synonym coverage** for fuzzy matching (incoming "Tafelschokolade" hits existing "Schokolade" via `SYNONYM_OF`)
- **Taxonomic structure** for inheritance-based reasoning (disposal info on "Batterie" applies to all hyponyms)
- **Bilingual vocabulary** (English nodes with `TRANSLATION_OF` edges enable native English query handling)

The existing `WasteItemExtract` schema already has a `synonyms` field (`src/llm_etl/schemas.py:106`). The LLM pipeline's entity matching can be enhanced to query both `name` and `SYNONYM_OF`-linked nodes.

### Material Taxonomy Coverage Analysis

The material reference data (`data/materials.json`) was validated against the full 4.29M Open Food Facts parquet dump (`data/off_dump/food.parquet`). Analysis date: 2026-02-26.

**Key finding:** 90 curated material tags cover 98.9% of all packaging components.

| Metric | Value |
|--------|-------|
| Products in parquet | 4,290,376 |
| Products with packaging data | 400,831 (9.3%) |
| Total packaging components | 465,072 |
| Unique material tags in data | 2,430 |
| **Curated tags (materials.json)** | **90** |
| **Components covered** | **459,859 (98.9%)** |
| Components not covered | 5,213 (1.1%) |

The uncovered 1.1% (5,213 components across 2,340 tags) consists of:
- **Localized duplicates:** `fr:papier adhésif` (167), `de:Folie` (77), `es:Cartón` (41), `nb:plast` (51) — non-English tags for materials already in the curated set
- **Typos/malformed:** `fr:Aluminim` (37), `en:Aluminum` (28) — misspellings of existing tags
- **Compound tags:** `en:plastic,en:metal` (40) — comma-separated values in a single field
- **Niche composites:** `xx:81-c-ldpe` (35), `xx:85-c-pap` (19) — recycling code variants not in the curated C/PAP set
- **Brand-specific:** `en:sig-combibloc` (21), `en:tetra-recart` (24), `en:italpack` (20)

The long tail (2,290 tags beyond the top 50 missing) averages 1.6 uses each — data quality noise, not taxonomy gaps. Components referencing unknown tags still get `PackagingComponent` nodes but no `MADE_OF` edge, which is correct behavior. A future cleanup pass could normalize the obvious localized/typo variants.

**Top 10 material tags by frequency (covers 86% of components):**

| Rank | Tag | Components | Stream |
|------|-----|-----------|--------|
| 1 | `en:plastic` | 182,132 (39.2%) | Verpackungstonne |
| 2 | `en:cardboard` | 52,150 (11.2%) | Altpapiertonne |
| 3 | `en:glass` | 48,439 (10.4%) | Altglascontainer |
| 4 | `en:metal` | 29,180 (6.3%) | Verpackungstonne |
| 5 | `en:paper` | 24,507 (5.3%) | Altpapiertonne |
| 6 | `en:pp-5-polypropylene` | 21,700 (4.7%) | Verpackungstonne |
| 7 | `en:aluminium` | 15,325 (3.3%) | Verpackungstonne |
| 8 | `en:pet-1-polyethylene-terephthalate` | 15,171 (3.3%) | Verpackungstonne |
| 9 | `en:paperboard` | 6,424 (1.4%) | Altpapiertonne |
| 10 | `en:non-corrugated-cardboard` | 6,060 (1.3%) | Altpapiertonne |

---

## Expected Scale

| Metric | Estimate | Actual (M1) | Notes |
|--------|----------|-------------|-------|
| German nouns from Kaikki.org | 149,849 | **355,660** | German Wiktionary has broader coverage than English Wiktionary |
| OdeNet noun synsets | ~36,000 | **25,767** | Noun synsets only (total OdeNet is ~36k across all POS) |
| After dedup + concept clustering | ~80-120k | **329,231** | 16,001 synset-based + 313,230 synonym-based |
| Synsets with English translations | — | **13,149** | Via ILI links to OEWN 2024 |
| Clusters with English translations | ~40-80k | **11,456** | Lower than estimate; many clusters lack OdeNet synsets |
| Clusters with hypernyms | — | **61,098** | From OdeNet + kaikki hypernym data |
| Total synonym links | — | **141,437** | Union-find transitive closure |
| Total DictionaryEntry nodes (de + en) | ~120-200k | **423,716** | 403,213 de + 20,503 en |
| Promoted to :WasteItem | ~30-60k | **67,912** | Physical + disposal_relevant entries (M3) |
| SYNONYM_OF edges | ~100-200k | **122,960** | |
| TRANSLATION_OF edges | ~40-80k | **24,738** | Lower than estimate; many clusters lack OdeNet synsets for ILI translation |
| IS_A (hypernym) edges | ~30-50k | **115,489** | Higher than estimate; kaikki hypernym data adds significantly beyond OdeNet |
| DERIVED_FROM edges | — | **325,937** | Provenance links to Source node |

**Total graph after dictionary import:** 423,717 nodes (423,716 DictionaryEntry + 1 Source), 589,124 edges. Of these, **67,912** nodes carry both `:DictionaryEntry` and `:WasteItem` labels after M3 classification.

## Cost Estimate

| Step | Model | Cost | Status |
|------|-------|------|--------|
| Dictionary download + parsing | None (local processing) | Free | **Done** |
| LLM classification (~329k clusters) | Llama 8B via Groq | ~$0.30 | **Done** (M3) |
| LLM cleanup pass | GPT-OSS-20B via Groq | ~$1-2 | Planned (M5) |
| Neo4j import | None (local) | Free | Planned (M2) |

**Total: under $5** for the entire dictionary baseline.

---

## Decision Log

| Decision | Rationale |
|----------|-----------|
| Top-down over bottom-up | Prevention of duplicates is cheaper than deduplication. Backed by 2025 arXiv study showing 90% accuracy for ontology-first vs 60% for vector-RAG. |
| Fresh graph start | Old CSV-imported data was limited (548 items). Dictionary provides broader, cleaner foundation. Existing ETL modules are adapted to match against dictionary nodes. |
| `:DictionaryEntry` label with promotion | Keeps validated `:WasteItem` namespace clean. Agent can still query `:DictionaryEntry` for vocabulary recognition. Promotion is a single Cypher query. |
| English as full nodes | English `:DictionaryEntry` nodes with `TRANSLATION_OF` edges enable native English query handling via graph traversal, not just text matching. Doubles node count but adds real bilingual capability. |
| Kaikki.org over Duden/PONS | Open license, JSONL bulk download, no scraping needed. Duden is copyrighted. |
| OdeNet over GermaNet | No license application required, same `wn` API, CC-BY-SA. GermaNet needs advisor signature. |
| Raw JSONL over postprocessed | Postprocessed files are deprecated on kaikki.org (Feb 2026). Raw `de-extract.jsonl` is the supported path. |
| Small LLMs for classification | Groq makes Llama 8B / GPT-OSS-20B essentially free at this scale (< $5 total). |
| Separate phases (seed → classify → promote → cleanup) | Each phase is independently verifiable and re-runnable via MERGE patterns. |

---

## Implementation Roadmap

### Pipeline Reuse Strategy

The dictionary pipeline reuses existing `llm_etl` components where they fit and adds new code only where necessary. The LangGraph orchestrator is **not** reused — it's designed for single-document processing, while dictionary classification is a batch job over ~80-120k structured records.

| Existing Component | Reuse | Notes |
|--------------------|-------|-------|
| `llm_etl/config.py` (`get_llm()`) | **As-is** | Already supports Groq provider |
| `llm_etl/schemas.py` | **Extend** | Add `DictionaryClassification` schema to `SCHEMA_REGISTRY` |
| `llm_etl/extraction_chain.py` | **As-is** | `with_structured_output()` works for classification — swap prompts at call site |
| `llm_etl/orchestrator.py` | **Skip** | Overkill for batch classification; replaced by a simple loop in `dictionary.py` |
| `llm_etl/loader.py` | **Extend** | Add `_load_dictionary_entry()` method with `:DictionaryEntry` Cypher + new relationship types |

### Milestone 1: Download and Parse (local only) — COMPLETE

**Goal:** Produce an intermediate JSONL of concept clusters from raw dictionary data.

**Status:** Complete. Output: `data/dictionaries/concepts.jsonl` (329,231 clusters, 177 MB).

**Files created:**
| File | Purpose |
|------|---------|
| `src/etl/dictionary.py` | CLI orchestrator: download, parse, write JSONL |
| `src/etl/dictionary_parser.py` | Kaikki.org JSONL parsing + OdeNet cross-referencing + concept clustering |
| `data/dictionaries/.gitignore` | Ignores cached downloads and output files |

**Tasks (all complete):**
1. ~~Add `wn` and `kaikki-json` to `requirements.txt`~~ → Added `wn>=0.9.0` and `requests>=2.28.0` (`kaikki-json` replaced due to broken download URL)
2. ~~Create `data/dictionaries/` directory with `.gitignore`~~ → Done
3. ~~Implement download functions in `dictionary.py`~~ → Done:
   - Fetch `de-extract.jsonl.gz` from kaikki.org (280 MB, cached via `requests` streaming)
   - Download OdeNet 1.4 and OEWN 2024 via `wn.download()`
   - OpenThesaurus deferred (OdeNet was seeded from OpenThesaurus; core synonym coverage exists)
4. ~~Implement `dictionary_parser.py`~~ → Done:
   - Filter kaikki JSONL for `lang_code == "de"` and `pos == "noun"` (355,660 entries)
   - Extract per entry: word, glosses, synonyms, translations, hypernyms, categories
   - Cross-reference with OdeNet synsets (synset_id as dedup key)
   - Canonical selection heuristic (hypernym preference > first OdeNet lemma > shortest single-word)
   - Group into concept clusters via synset grouping + union-find synonym clustering
5. ~~CLI~~ → `python -m src.etl.dictionary [--dry-run] [--verbose] [--output PATH]`

**Actual results:**
| Metric | Value |
|--------|-------|
| German nouns extracted | 355,660 (from 1,297,268 total entries) |
| Concept clusters | 329,231 (16,001 synset-based + 313,230 synonym-based) |
| With English translations | 11,456 |
| With hypernyms | 61,098 |
| Total synonym links | 141,437 |
| Pipeline runtime | ~29 seconds (after initial download) |

**Implementation notes:**
- The German Wiktionary (`de-extract.jsonl.gz`) contains entries for **all** languages, not just German. Filtering by `lang_code == "de"` is required.
- The `kaikki-json` package's download URL hardcodes a 404. Replaced with direct `requests` streaming download.
- The `wn` package's `.translate()` method returns synset objects whose `.lemmas()` is empty due to wordnet scoping. Workaround: re-lookup each translated synset via `wn.synset(en_ss.id)`.
- `wn.Synset.id` is a property, not a method (no parentheses).
- `wn.download()` uses `progress_handler` parameter, not `progress`.

**Acceptance criteria:** ~~Running `parse` produces a JSONL where each line is a concept cluster with `canonical`, `synonyms_de`, `translations_en`, `hypernyms`, `synset_id`, and `sources` fields. No LLM or database needed.~~ **Met.** Each JSONL line contains all required fields plus `glosses`, `categories`, `uid`, and `lang`.

### Milestone 2: Schema Extensions and Neo4j Import — COMPLETE

**Goal:** Import concept clusters as `:DictionaryEntry` nodes with `SYNONYM_OF`, `TRANSLATION_OF`, and `IS_A` edges.

**Status:** Complete. Import runs in ~20 seconds for 329k clusters.

**Architecture decision:** Custom 5-pass UNWIND batching in `dictionary_importer.py` instead of routing through `EntityLoader`. The EntityLoader is tightly coupled to single-entity LLM extraction dispatch — bulk dictionary import with UNWIND is simpler and ~100x faster.

**Tasks (all complete):**
1. ~~Add `:DictionaryEntry` constraints and indexes to `src/db/schema.cql`~~ → Done (4 constraints/indexes + relationship pattern docs)
2. ~~Create `src/etl/dictionary_importer.py`~~ → Done (~250 lines, 5-pass UNWIND batch import)
3. ~~Refactor `dictionary.py` to subcommands~~ → Done (`parse`/`import`, backward compatible — no subcommand defaults to `parse`)
4. ~~Update `src/etl/__init__.py`~~ → Done (added `import_dictionary_entries` export)
5. ~~Update `CLAUDE.md`~~ → Done (schema layers, commands, ETL phases table)
6. ~~CLI~~ → `python -m src.etl.dictionary import [--input/-i] [--batch-size] [--dry-run/-n] [-v]`

**Import passes:**
1. Canonical DictionaryEntry nodes (unconditional `SET` — canonical properties always win)
2. Synonym nodes + `SYNONYM_OF` edges (`ON CREATE SET` for sparse targets)
3. English translation nodes + `TRANSLATION_OF` edges (`ON CREATE SET`)
4. Hypernym nodes + `IS_A` edges (`ON CREATE SET`)
5. Source node + `DERIVED_FROM` provenance edges

**Actual results:**
| Metric | Estimate | Actual |
|--------|----------|--------|
| DictionaryEntry nodes (de) | ~80-120k | **403,213** |
| DictionaryEntry nodes (en) | ~40-80k | **20,503** |
| **Total DictionaryEntry nodes** | ~120-200k | **423,716** |
| Source nodes | 1 | **1** |
| SYNONYM_OF edges | ~100-200k | **122,960** |
| TRANSLATION_OF edges | ~40-80k | **24,738** |
| IS_A edges | ~30-50k | **115,489** |
| DERIVED_FROM edges | ~329k | **325,937** |
| **Total edges** | ~200-400k | **589,124** |
| Import runtime | ~5-15 min | **~20 seconds** |

**Implementation notes:**
- Skipped extending `EntityLoader` and `schemas.py` — not needed for bulk import. These may be revisited if other ETL modules need to create DictionaryEntry nodes.
- Node counts exceed cluster count (329k) because Passes 2-4 create additional nodes for synonyms, translations, and hypernyms that don't exist as canonicals.
- DERIVED_FROM count (325,937) is slightly below cluster count (329,231) because some clusters share UIDs after deduplication.
- Running with `-v` produces excessive Neo4j driver debug logs (~182 MB). Use INFO level (no `-v`) for normal runs.
- Idempotency verified: re-running import produces identical database state.

**~~Acceptance criteria:~~** **Met.** Neo4j contains 403k German and 20k English `:DictionaryEntry` nodes with all four edge types. All canonical nodes have `DERIVED_FROM` → `Source` provenance. Re-running is idempotent.

### Milestone 3: LLM Classification and Promotion — COMPLETE

**Goal:** Classify dictionary entries as physical/non-physical using cheap LLMs via Groq, then promote physical items to `:WasteItem`.

**Status:** Complete. 67,912 nodes promoted to `:WasteItem`. Run 1 took ~98 minutes; Run 2 (fix + re-run) took ~11 minutes.

**Architecture decision:** Self-contained `dictionary_classifier.py` with Pydantic structured output instead of routing through `ExtractionChain`. The classifier uses `llm.with_structured_output(BatchClassificationResult)` directly — simpler for batch classification than the single-document extraction chain.

**Key optimization — Inflection inheritance:**

Of the 329k concept clusters, ~237k are inflected forms (genitives, plurals, datives) whose glosses follow the pattern:

> "Nominativ Plural des Substantivs Schokolade"

These inherit their classification from the base form without LLM calls, reducing LLM batches from ~13k to ~4k.

**Three-phase classification:**
1. **Separate** entries into base forms vs inflected forms (regex on gloss)
2. **Classify** base forms via LLM (batched, 25 items/call, structured output)
3. **Inherit** inflected forms from their base form's classification

**Tasks (all complete):**
1. ~~Create `src/etl/dictionary_classifier.py`~~ → Done (~310 lines):
   - `PhysicalCategory` enum: object, substance, food, organism, structure, nonphysical, person, place, event, uncertain
   - `BatchClassificationResult` Pydantic model for structured LLM output
   - Inflection detection via `_INFLECTION_RE` regex
   - 5-pass orchestrator with resume support
   - German system prompt for classification
   - Exponential backoff on rate limits (15s, 30s, 60s)
2. ~~Add `classify` subcommand to `dictionary.py`~~ → Done (--input, --batch-size, --delay, --dry-run, --verbose)
3. ~~Update `src/etl/__init__.py`~~ → Done (added `classify_dictionary_entries` export)
4. ~~Update `CLAUDE.md`~~ → Done (classify command docs, Phase D+ row)
5. ~~Write classifications to Neo4j~~ → Done (batched UNWIND, sets `is_physical`, `disposal_relevant`, `classification_category`, `classified_by`)
6. ~~Promote physical+disposal_relevant to `:WasteItem`~~ → Done

**The inflection-to-base resolution bug and fix:**

Run 1 showed 104k inflected forms with "No base match" — their glosses referenced a base form name (e.g., "0,5-Liter-Flasche") that wasn't a *canonical* name in the clusters but existed as a synonym. The original code only looked up canonical names.

Analysis of the 236,820 inflected forms:
| Resolution | Count | Notes |
|------------|-------|-------|
| Base is a canonical name | 163,649 | Direct match — worked from the start |
| Base is a synonym name | 56,267 | **Bug:** missed in Run 1, fixed in Run 2 |
| Base not in any cluster | 16,904 | Suffixes (`-algie`, `-stan`), rare compounds |

**Fix:** Build a `name_to_canonical_name` lookup that maps both canonical names AND synonym names back to their cluster's canonical. Inflected forms now resolve through synonyms. The 16,904 truly unresolvable entries (base form doesn't exist anywhere in the data) are reclassified as base forms and sent to the LLM.

After the fix, re-running `classify` uses resume support to skip the 92k already-classified base forms, LLM-classifies the 16,902 unresolvable inflections (~677 batches, ~11 min), and inherits for the 56,267 newly-resolvable forms.

**Run 1 results (before fix):**
| Metric | Value |
|--------|-------|
| LLM classified | 76,184 |
| Inherited from base | 132,823 |
| No base match | 103,997 |
| Written to DB | 206,355 |
| Promoted to :WasteItem | 67,912 |
| Runtime | ~98 minutes |
| Cost (llama-3.1-8b-instant via Groq) | ~$0.30 |

**Classification breakdown (Run 1):**
| Category | Physical+Disposal | Physical only | Non-physical | Total |
|----------|-------------------|---------------|-------------|-------|
| object | 54,595 | 28,579 | 12,731 | ~96k |
| person | 368 | 18,781 | 11,275 | ~30k |
| nonphysical | — | 3 | 23,090 | ~23k |
| substance | 8,871 | 6,588 | 3,117 | ~19k |
| event | 4 | 94 | 15,145 | ~15k |
| structure | 140 | 5,513 | 366 | ~6k |
| organism | 59 | 4,980 | 129 | ~5k |
| place | 51 | 2,386 | 2,130 | ~5k |
| food | 3,745 | 27 | 7 | ~4k |
| uncertain | 79 | 164 | 3,158 | ~3k |

**~~Acceptance criteria:~~** **Met.** 67,912 nodes carry both `:DictionaryEntry` and `:WasteItem` labels. Classification results stored as node properties (`is_physical`, `disposal_relevant`, `classification_category`, `classified_by`). Re-running skips already-classified nodes via resume support.

### Milestone 4: Adapt Existing ETL Modules — COMPLETE

**Goal:** Make existing structured ETL (facilities, waste_items CSV) work against the dictionary-seeded graph.

**Status:** Complete. `waste_items.py` pre-promotes matching DictionaryEntry nodes before the main import loop, preventing duplicate nodes.

**Approach:** Instead of changing MERGE to MATCH (which would require two code paths), the fix adds a pre-promotion step: before the main loop, all CSV item names that exist as `:DictionaryEntry` (but not yet `:WasteItem`) get the `:WasteItem` label added. Then the existing `MERGE (w:WasteItem {name: ...})` finds them naturally via ON MATCH.

**Tasks (all complete):**
1. ~~Update `src/etl/waste_items.py`~~ → Done:
   - Added `_get_dictionary_match_stats()` — classifies CSV names into three buckets (already promoted / dict-only / not in dict)
   - Added `_pre_promote_dictionary_entries()` — adds `:WasteItem` label to matching DictionaryEntry nodes before main loop
   - Added `_create_csv_source_node()` — creates Source node for CSV provenance (`abfall-abc-csv`)
   - Added `DERIVED_FROM` provenance link for every imported WasteItem
   - Dictionary match breakdown displayed in both dry-run and full import output
2. `src/etl/facilities.py` works unchanged (Facility nodes are independent of dictionary)
3. Schema constraints/indexes already done in Milestone 2

**Why this works:**
- `MERGE (w:WasteItem {name: "Backofen"})` only searches the `:WasteItem` label index
- Before pre-promotion: "Backofen" is `:DictionaryEntry` only → MERGE creates a duplicate `:WasteItem`
- After pre-promotion: "Backofen" is `:DictionaryEntry:WasteItem` → MERGE finds it, ON MATCH fires
- For items not in dictionary: MERGE creates new standalone `:WasteItem` as before
- Idempotent: `WHERE NOT d:WasteItem` makes re-runs no-ops

**Acceptance criteria:** ~~Running `facilities.py` then `waste_items.py` against the dictionary-seeded graph attaches disposal relationships to existing `:DictionaryEntry:WasteItem` nodes.~~ **Met.** Pre-promotion prevents duplicates; provenance tracking via `DERIVED_FROM` → `Source(abfall-abc-csv)` links all CSV-imported items.

### Milestone 5: Post-Import Cleanup

**Goal:** Use LLM passes to clean up the graph after all imports.

**Tasks:**
1. Merge near-duplicates (compound noun variants, inflectional forms)
2. Prune orphan hypernym chains (abstract top-level synsets like "Entitaet", "Ding")
3. Reclassify edge cases the LLM was uncertain about
4. Generate quality report: node counts by label, orphan nodes, disconnected subgraphs
5. CLI: `python -m src.etl.dictionary cleanup [--dry-run]`

**Acceptance criteria:** Quality report shows no obvious duplicate clusters, orphan chain depth is bounded, and all `:WasteItem` nodes have at least one relationship (synonym, taxonomy, or disposal).

### Milestone Summary

| # | Milestone | Dependencies | LLM | DB | Status |
|---|-----------|-------------|-----|-----|--------|
| 1 | Download and Parse | None | No | No | **Complete** |
| 2 | Schema + Neo4j Import | M1 | No | Yes | **Complete** |
| 3 | LLM Classification + Promotion | M2 | Groq | Yes | **Complete** |
| 4 | Adapt Existing ETL | M2 | No | Yes | **Complete** |
| 5 | Post-Import Cleanup | M3, M4 | Groq | Yes | Not started |

Milestones 1-4 are complete. Milestone 5 runs last.

### New and Modified Files

| File | Status | Purpose |
|------|--------|---------|
| `src/etl/dictionary.py` | **Done (M1+M2)** | CLI orchestrator with `parse`/`import` subcommands |
| `src/etl/dictionary_parser.py` | **Done (M1)** | Kaikki.org JSONL parsing + OdeNet cross-referencing + concept clustering |
| `src/etl/dictionary_importer.py` | **Done (M2)** | 5-pass UNWIND batch import into Neo4j (~250 lines) |
| `data/dictionaries/.gitignore` | **Done (M1)** | Ignores cached dictionary downloads and output files |
| `requirements.txt` | **Done (M1)** | Added `wn>=0.9.0`, `requests>=2.28.0` |
| `src/etl/dictionary_classifier.py` | **Done (M3)** | LLM classification + inflection inheritance + promotion (~310 lines) |
| `src/etl/__init__.py` | **Done (M1+M2+M3)** | Exports `parse_dictionaries` + `import_dictionary_entries` + `classify_dictionary_entries` |
| `src/db/schema.cql` | **Done (M2)** | Added `:DictionaryEntry` constraints, indexes, relationship pattern docs |
| `src/llm_etl/schemas.py` | Planned (M3) | Add `DictionaryClassification` schema |
| `src/etl/waste_items.py` | **Done (M4)** | Pre-promotes DictionaryEntry nodes, adds CSV provenance tracking |

The module follows existing patterns from `src/etl/facilities.py` and `src/etl/waste_items.py`:
- CLI with `--dry-run`, `--verbose` flags
- `MERGE`-based idempotent import
- Deterministic UID generation via SHA256
- Provenance tracking via `Source` + `DERIVED_FROM`
- Runnable as `python -m src.etl.dictionary`

---

## References

- Kaikki.org raw data downloads: https://kaikki.org/dictionary/rawdata.html
- Wiktextract (parser): https://github.com/tatuylonen/wiktextract
- `kaikki-json` PyPI package: https://pypi.org/project/kaikki-json/
- OdeNet (Open German WordNet): https://github.com/hdaSprachtechnologie/odenet
- `wn` Python package: https://pypi.org/project/wn/
- Open English WordNet: https://github.com/globalwordnet/english-wordnet
- OpenThesaurus download: https://www.openthesaurus.de/about/download
- Ontology-first vs extraction-first KG construction (2025 study): https://arxiv.org/html/2511.05991v1
- Neo4j ontology integration with Neosemantics: https://neo4j.com/blog/knowledge-graph/ontologies-in-neo4j-semantics-and-knowledge-graphs/
