# 6. Creating the Dictionary Baseline

The research described in Section 5 solved the data quantity problem: we had identified 68 evaluated sources spanning millions of potential entities, with structured APIs, database dumps, and PDF documents covering every layer of the schema. The natural next step would have been to start building ETL pipelines for the highest-priority sources — import Open Food Facts products, parse AVV codes from the legal text, extract disposal rules from municipal PDFs — and let the graph grow incrementally as each pipeline delivered its data.

We did not take this approach. Before importing any domain-specific data, we needed to address a structural problem that would have compounded with every subsequent import: the graph had no vocabulary foundation. The 548 Abfall-ABC items described in Section 3 covered a fraction of the terms people use when describing waste. Running the LLM extraction pipeline against hundreds of source documents without a broad anchor layer would mean that entity creation and entity resolution would happen simultaneously — the pipeline would need to both understand a document and decide whether each entity it encountered was new or already existed in the graph. These are fundamentally different tasks, and combining them degrades both.

## 6.1 The Case for Top-Down Construction

The conventional approach to building a knowledge graph from heterogeneous sources is bottom-up: process each document, extract entities and relationships, merge duplicates after the fact. This approach has five structural problems that become acute at scale.

First, duplicate explosion. Source A mentions "Schokolade," source B "Schokoriegel," source C "chocolate bar." Without pre-existing anchor nodes, the pipeline creates three separate nodes for what is semantically one item. Second, inconsistent granularity. One source discusses "Batterie," another "Knopfzelle," a third "AA-Batterie." Without a taxonomy, the pipeline cannot determine whether these are synonyms, siblings, or a parent-child hierarchy. Third, deduplication is destructive. Merging nodes after the fact requires fuzzy matching and risks losing relationship edges and breaking provenance links. Fourth, limited recall. A student searches "Tafelschokolade" but the graph only contains "Schokolade" because that term appeared in the source PDFs. Without upfront synonym seeding, the graph's recall is bounded by the vocabulary of whichever documents have been processed. Fifth, the LLM carries two competing tasks: understanding document content and resolving entities against the existing graph.

A 2025 study comparing ontology-grounded and extraction-first knowledge graph construction for RAG found that ontology-first graphs achieved 90% answer accuracy, compared to 60% for vector-RAG baselines. The key finding was that aligning a knowledge graph with an ontology from a structured source yields performance comparable to ontologies derived from dynamic text corpora, while substantially reducing computational cost. This validated our decision to build the vocabulary layer first.

The approach cleanly separates two phases. Vocabulary building — the dictionary import — is deterministic, structured, and requires no LLM involvement. It creates the noun inventory with synonyms, translations, and taxonomic relationships. Knowledge attachment — the subsequent LLM extraction pipeline — processes source documents and attaches disposal rules, conditions, and facility links to existing anchor nodes. The LLM focuses on what it does well (understanding unstructured text) without the burden of entity resolution.

## 6.2 The Two-Label Strategy

Section 4.3 described the two-label strategy at a conceptual level and its four resulting agent response states. The implementation relies on Neo4j's multi-label capability: a single node can carry both `:DictionaryEntry` and `:WasteItem` labels simultaneously, queryable via either. Dictionary entries are imported exclusively as `:DictionaryEntry` nodes. Promotion to `:WasteItem` happens only after LLM classification confirms that an entry represents a physical, disposable object — a separate phase that can be re-run as classification models improve or criteria change.

The practical consequence is that promotion is cheap and reversible. Adding a label is a single Cypher `SET` operation; removing one is equally trivial. This means the boundary between "recognized vocabulary" and "actionable waste item" can be adjusted without re-importing any data, and without affecting the synonym, translation, or taxonomy edges attached to the underlying DictionaryEntry node.

## 6.3 Data Sources

We selected three primary sources, prioritizing open licensing, structured bulk access, and complementary coverage.

**Kaikki.org / Wiktextract** provides the raw noun inventory. The project extracts structured data from Wiktionary using the `wiktextract` library and publishes it as JSONL files. We used the German Wiktionary extraction (`de-extract.jsonl.gz`, approximately 280 MB compressed), which contains 1.3 million entries across all languages and parts of speech defined in the German Wiktionary. Filtering for German-language nouns (`lang_code == "de"` and `pos == "noun"`) yielded 355,660 entries. Each entry carries the headword, definitions (glosses), same-language synonyms, cross-language translations, hypernyms, hyponyms, and semantic categories. The data is licensed under CC-BY-SA 3.0 and GFDL.

**OdeNet (Open German WordNet)** provides taxonomic structure. Developed at Hochschule Darmstadt, OdeNet organizes approximately 36,000 synsets (of which 25,767 are noun synsets) into hypernym-hyponym trees: "Schokoriegel" IS_A "Schokolade" IS_A "Suesswaren" IS_A "Lebensmittel." The `wn` Python package provides a unified query interface for both OdeNet and the English WordNet, and critically, OdeNet includes interlingual links (ILI) to the Open English WordNet that enable cross-language translation at the synset level. OdeNet was originally seeded from OpenThesaurus synonym clusters and Princeton WordNet, providing a bridge between colloquial German synonyms and formal taxonomic structure. Licensed under CC-BY-SA 4.0.

**Open English WordNet (2024 edition)** provides the English-side taxonomy, with approximately 120,000 synsets accessible through the same `wn` API. English DictionaryEntry nodes linked via `TRANSLATION_OF` edges to their German counterparts enable the chatbot agent to handle English queries natively through graph traversal rather than runtime translation.

Source selection was driven by practical constraints. We chose Kaikki.org over Duden or PONS because of its open license and bulk JSONL download — Duden is copyrighted and scraping would violate terms of service. We chose OdeNet over GermaNet because GermaNet, while considered the gold standard, requires a license application signed by a thesis advisor at Universität Tübingen — a bureaucratic dependency we could not afford in a semester project. OdeNet provides the same `wn` API, the same taxonomic structure, and a permissive CC-BY-SA license.

## 6.4 The Import Pipeline

The pipeline proceeds in four phases, each independently verifiable and re-runnable.

**Phase 0: Download and Cache.** The Kaikki.org JSONL file is downloaded via HTTP streaming and cached locally. OdeNet and the Open English WordNet are installed through the `wn` package, which manages its own SQLite database. All downloads are cached in `data/dictionaries/` and reprocessed as needed. The `kaikki-json` Python package was evaluated but its built-in download URL returned a 404; we replaced it with direct `requests` streaming.

**Phase 1: Parse and Filter.** The raw JSONL (1.3 million entries) is filtered to German nouns (355,660 entries). For each entry, the pipeline extracts the headword, glosses, synonyms, translations, hypernyms, and semantic categories. These are cross-referenced against OdeNet synsets using synset IDs as deduplication keys.

The central processing step is concept clustering, which groups multiple surface forms into a single canonical concept. This proceeds in two passes. First, synset-based grouping: entries that share an OdeNet synset ID are merged into a single cluster. This produced 16,001 clusters from 28,816 matched words — entries like "Schokoriegel" and "Tafelschokolade" that OdeNet already groups under the same synset. Second, union-find synonym clustering: for the remaining entries (those without OdeNet synset matches), a union-find data structure groups words that share synonyms transitively. If word A lists word B as a synonym, and word B lists word C, all three are merged into one cluster. This second pass produced 313,230 additional clusters from the remaining entries.

For each cluster, a canonical term is selected using a heuristic: prefer the term that appears as a hypernym of the others in OdeNet; failing that, prefer the shortest single-word lemma. This favors "Schokolade" over "Schokoriegel" and "Batterie" over "Knopfzelle." The total output is 329,231 concept clusters written to an intermediate JSONL file (177 MB), each containing the canonical term, its synonyms, English translations, hypernyms, hyponyms, semantic categories, and source provenance. The pipeline runs in approximately 29 seconds after the initial download.

**Phase 2: Neo4j Import.** Rather than routing through the LLM extraction pipeline's EntityLoader — which is designed for single-entity dispatch from document processing — we implemented a dedicated five-pass UNWIND batch importer. Pass 1 creates canonical DictionaryEntry nodes. Pass 2 creates synonym nodes and `SYNONYM_OF` edges. Pass 3 creates English translation nodes and `TRANSLATION_OF` edges. Pass 4 creates hypernym nodes and `IS_A` edges. Pass 5 creates the Source node and `DERIVED_FROM` provenance edges. All operations use `MERGE` for idempotent re-runs. The import completes in approximately 20 seconds for all 329,231 clusters — roughly 100 times faster than routing through the single-entity loader would have been.

**Phase 3: LLM Classification and Promotion.** The 329,231 concept clusters are classified by a small language model (Llama 3.1 8B via Groq) using structured output. For each entry, the model determines whether the noun refers to a physical object (`is_physical`), whether someone could plausibly need to dispose of it (`disposal_relevant`), and assigns a broad category (object, substance, food, organism, structure, person, place, event, or nonphysical).

A key optimization reduced the number of LLM calls by approximately 70%. Of the 329,231 clusters, roughly 237,000 are inflected forms — genitives, plurals, and datives whose dictionary glosses follow patterns like "Nominativ Plural des Substantivs Schokolade." These forms inherit their classification from the base form without requiring an LLM call. The classifier detects inflections via regex, classifies the approximately 92,000 base forms through the LLM in batches of 25, then propagates results to all associated inflected forms.

An initial implementation bug caused approximately 104,000 inflected forms to fail base-form resolution: their glosses referenced a base form (such as "0,5-Liter-Flasche") that existed only as a synonym in another cluster, not as a canonical name. The fix was straightforward — extend the lookup table to map synonym names, not just canonical names, back to their cluster — but it affected a third of all entries. Because the classifier includes resume support (re-running skips already-classified entries), the fix required only an 11-minute incremental run rather than repeating the full 98-minute initial classification. The total cost for classifying all 329,231 clusters was approximately $0.30 via Groq.

After classification, promotion is a single Cypher query: `MATCH (d:DictionaryEntry {is_physical: true}) WHERE NOT d:WasteItem SET d:WasteItem`. This adds the `:WasteItem` label to all entries classified as physical and disposal-relevant, making them instantly visible to all existing queries and tools that operate on the WasteItem label.

## 6.5 Results

The dictionary import produced the following graph:

| Metric | Value |
|--------|-------|
| DictionaryEntry nodes (German) | 403,213 |
| DictionaryEntry nodes (English) | 20,503 |
| **Total nodes** | **423,717** (incl. 1 Source node) |
| SYNONYM_OF edges | 122,960 |
| TRANSLATION_OF edges | 24,738 |
| IS_A (taxonomy) edges | 115,489 |
| DERIVED_FROM (provenance) edges | 325,937 |
| **Total edges** | **589,124** |
| Promoted to :WasteItem | **67,912** |

The node count exceeds the cluster count (329,231) because synonym, translation, and hypernym passes create additional nodes for terms that do not appear as canonicals in any cluster but are referenced as synonyms, translations, or hypernyms by other entries. The DERIVED_FROM count (325,937) is slightly below the cluster count because some clusters share UIDs after deduplication.

The 67,912 promoted WasteItem nodes represent a 124-fold increase over the previous graph's 548 items — transforming a sparse vocabulary into a broad foundation that covers the vast majority of physical objects a German-speaking user might ask about. The classification breakdown shows the distribution: approximately 55,000 objects, 9,000 substances, 3,700 food items, and smaller counts for organisms, structures, and other physical categories.

The taxonomy structure provides inheritance-based reasoning that the previous flat list could not support. A disposal rule attached to "Batterie" automatically applies to all hyponyms — "Knopfzelle," "AA-Batterie," "Lithium-Ionen-Akku" — through IS_A traversal. The 122,960 synonym edges ensure that the chatbot agent can resolve colloquial, regional, and informal terms to their canonical forms. The 24,738 cross-language translation edges enable English-language queries to reach German disposal knowledge through graph traversal rather than runtime translation.

The entire pipeline — from raw download through classification and promotion — cost under $0.50 in LLM API fees and ran in under two hours of wall-clock time, most of which was the LLM classification phase. The deterministic steps (download, parsing, Neo4j import) complete in under a minute combined. This makes the dictionary baseline reproducible from scratch at negligible cost, an important property for a pipeline that may need to be re-run as dictionary sources update or classification criteria are refined.

## 6.6 Integration with Existing ETL

The dictionary baseline was designed as Phase 1 of the new graph — the first import after schema initialization. This required adapting the existing structured ETL modules to work against a dictionary-seeded graph rather than an empty one.

The facility import (`src/etl/facilities.py`) required no changes, since Facility nodes are independent of the vocabulary layer. The waste item import (`src/etl/waste_items.py`), however, posed a subtle problem. The existing pipeline uses `MERGE (w:WasteItem {name: ...})` to create or match waste items. Before the dictionary import, this worked correctly — every MERGE created a new node. After the dictionary import, "Backofen" already exists in the graph as a `:DictionaryEntry` node, but without the `:WasteItem` label, so the MERGE does not find it and creates a duplicate.

The solution was a pre-promotion step: before the main import loop, all CSV item names that exist as `:DictionaryEntry` nodes (but not yet as `:WasteItem`) receive the `:WasteItem` label. Then the existing MERGE finds them via the WasteItem label index and fires the ON MATCH branch, which updates the existing node with disposal relationships rather than creating a duplicate. For items not present in the dictionary, the MERGE creates new standalone `:WasteItem` nodes as before. We also added `DERIVED_FROM` provenance edges linking every CSV-imported item to a Source node, bringing the waste item importer in line with the provenance tracking pattern established by the dictionary pipeline.

This approach preserved backward compatibility — the same import code handles both dictionary-seeded and empty graphs — while preventing the duplicate nodes that would have undermined the entire rationale for top-down construction.
