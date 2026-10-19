# 9. Data Imports

The research described in Section 5 identified 68 evaluated sources spanning millions of potential entities. The pipeline architecture described in Section 7 provided the machinery to process them. The dictionary baseline described in Section 6 laid the vocabulary foundation. What remained was the actual work of importing data into the graph. Due to time and resource constraints, we could not assess — let alone import — all sources. Instead, we focused on building a foundation that later semesters could extend: the dictionary baseline for the vocabulary layer, Open Food Facts for the product layer, the AVV code hierarchy for the legal classification layer, and a set of substitutes for the office items domain where our preferred data source proved unavailable. This section describes the practical experience of each import — the numbers, the challenges, and the decisions that shaped the final graph.

## 9.1 Dictionary Baseline

The dictionary import is described in detail in Section 6. In summary: we parsed 355,660 German nouns from the Kaikki.org Wiktionary extraction, clustered them into 329,231 concepts using OdeNet synsets and union-find synonym grouping, imported them as DictionaryEntry nodes with synonym, translation, and taxonomy edges, and classified them using Llama 3.1 8B via Groq. The result was 423,717 nodes, 589,124 edges, and 67,912 entries promoted to the WasteItem label — a 124-fold increase over the previous graph's 548 items. The entire pipeline ran in under two hours at a cost of approximately $0.30 in LLM API fees.

## 9.2 Open Food Facts

Open Food Facts was the largest single import and the primary data source for the product layer. The dataset is available in two formats: a JSONL dump (2.6 GB, 719,174 products — a curated higher-completeness subset) and a Parquet file on Hugging Face (4.42 GB, 4,290,376 products — the complete database). We chose the Parquet format because it contains six times more products, supports columnar access through PyArrow without loading the entire file into memory, and includes image metadata and structured packaging data that the JSONL subset omits.

The import extracted 11 of the Parquet file's 111 columns. Product identity fields (barcode, name, brand, quantity) became Product node properties, with multilingual name resolution prioritizing German over English over the main language. Category tags, label tags, and country tags were stored as list properties for downstream filtering. The most valuable field for our use case was the structured `packagings` array: each element represents a physical packaging component (jar, lid, film, can) with its material, shape, recycling hint, unit count, and measured weight. These became PackagingComponent nodes linked to their parent Product via HAS_COMPONENT edges and to Material nodes via MADE_OF edges — enabling the full decomposition chain from barcode scan to per-component waste stream routing.

The import processed all 4.29 million products in 9 minutes by streaming the Parquet file in batches of 50,000 rows and writing to Neo4j in transactions of 5,000 records across four passes: Product nodes first, then PackagingComponent nodes with HAS_COMPONENT edges, then MADE_OF edges to the pre-imported material taxonomy, and finally DERIVED_FROM provenance edges. The material taxonomy (~90 Material nodes mapping OFF's packaging material tags to waste streams) had to be imported separately before the main OFF import, since the MADE_OF edges match on existing Material nodes.

The result: 4,290,312 Product nodes, 555,227 PackagingComponent nodes, 459,850 MADE_OF edges, and 385,542 products tagged as available in Germany. Of the packaging components, 5,213 (1.1%) referenced material tags not present in our 90-material taxonomy — these were flagged for future extension rather than silently dropped. The 100 skipped Parquet columns (nutrition facts, ingredient lists, eco-scores, editorial metadata) remain available in the source file for future LLM-based enrichment without re-downloading.

## 9.3 Office Items and the BMEcat Gap

Section 5.6 identified the absence of an open office supply database as the most significant data gap. German universities procure office items through framework contracts with B2B distributors who deliver product catalogs in the BMEcat XML format — a rich data source containing EAN barcodes, product descriptions, eCl@ss classification codes, manufacturer information, and links to Safety Data Sheets. However, BMEcat catalogs are confidential B2B exchange documents. No openly downloadable catalog exists, and access requires an institutional procurement account. A partnership with the university's procurement department (Vergabestelle) would be the most practical path to obtaining this data, but was not feasible within the semester.

We evaluated 16 public barcode and product databases as potential substitutes and found that none provides structured material composition data for non-food everyday products. UPCitemdb (686 million barcodes) and EAN-Search.org (1.07 billion) have enormous coverage but store only product names and categories — not what the product is made of or how to dispose of it. Open Products Facts (the non-food counterpart to Open Food Facts) has the right schema but is effectively a ghost town with only 40,000 sparsely populated entries.

Instead, we imported two partial substitutes that collectively cover a subset of the office items domain. The EU Ecolabel product database, accessible through its public API, yielded 16,333 Product nodes covering eco-certified categories relevant to office environments: tissue paper products, hard surface coverings, cleaning products, paints and varnishes, printed paper products, and furniture. Open Beauty Facts, the cosmetics sibling of Open Food Facts, contributed 40,033 Product nodes covering personal care items (soaps, shampoos, cleaning agents) that overlap with university facility supplies. Both imports follow the same pattern as the main product import: deterministic UIDs from EAN barcodes, MERGE-based idempotency, and DERIVED_FROM provenance tracking.

The combined 56,000 product nodes from these substitutes are cross-referenceable by EAN with all other product sources — a Nivea hand soap that appears in both Open Beauty Facts and a future BMEcat catalog would merge into a single enriched Product node rather than creating a duplicate. This was a deliberate design choice: build the infrastructure for product data even when the ideal source is unavailable, so that future imports slot in without migration.

## 9.4 AVV Code Hierarchy

Section 5.2 described the Abfallverzeichnis-Verordnung as a three-level hierarchy of 20 chapters, approximately 113 subgroups, and roughly 840 specific waste codes. Our import source — a CSV prepared by the previous semester that combines the standard AVV with university-specific entries from the Frankfurt UAS campus — extends this to 22 chapters and 1,505 codes, adding items like soldering paste, gas cylinders, and binder clips that do not appear in the standard catalog.

The import proceeded in two stages. The first was a straightforward structured ETL: the extended CSV was parsed into 1,640 AVVCode nodes with HAS_PARENT relationships forming the three-level tree.

The second stage — classifying WasteItem nodes against the AVV hierarchy using LLMs — proved more instructive. The naive approach would be to send each waste item name along with all 1,505 AVV codes as context and ask the model to pick the best match. This failed for two reasons. First, the full code list requires approximately 20,000 tokens of context, which exceeded Groq's rate limits after roughly 12 calls (250,000 tokens per minute). Second, the 8B model we used (Llama 3.1 8B Instant) simply could not handle the task at that context size — it either returned empty results or echoed input words as malformed JSON.

The solution was hierarchical classification in three steps. Step one presents only the 22 chapter descriptions (~272 tokens) and asks the model to select the correct chapter. Step two presents only the subgroups within that chapter (~5–30 codes). Step three presents only the specific codes within that subgroup (~5–15 codes). Each step validates that the returned code exists in the filtered set; if any step fails, the item is skipped. This reduced the context per call from 20,000 tokens to roughly 300–500, making it feasible for the 8B model and eliminating rate limit issues entirely.

Two additional Groq-specific fixes were required. Groq's function calling (tool use) API rejected valid JSON that its own model generated — the model output was correct, but Groq's validation layer could not parse it back. We worked around this by switching from function calling to JSON mode (`response_format: {"type": "json_object"}`). Groq then required the literal word "json" to appear somewhere in the prompt text when JSON mode is enabled — a requirement that LangChain's structured output abstraction does not handle automatically. Adding "and return them as JSON" to the system prompt resolved this.

A first batch of 500 items yielded a 99.8% success rate (499 classified, 1 failure) in 18 minutes at negligible cost. The pipeline mechanics — hierarchical narrowing, validation at each step, resume support for interrupted runs — proved robust. Classification quality, however, was limited by the input data: the current WasteItem population consists primarily of dictionary-promoted entries (words like "Diamant" and "Verwandtschaft" that are technically physical objects but not meaningful waste items), which biased results toward catch-all codes. Quality will improve substantially when the pipeline is run against real waste items from curated sources rather than the dictionary baseline's broad vocabulary.

## 9.5 Import Summary

The following table summarizes the current state of the knowledge graph after all imports:

| Import | Nodes | Key Edges | Runtime | Status |
|--------|-------|-----------|---------|--------|
| Dictionary baseline | 423,717 | 589,124 (synonym, translation, taxonomy, provenance) | ~2 hours | Complete |
| Open Food Facts | 4,290,312 Products + 555,227 Components | 5,305,389 (HAS_COMPONENT, MADE_OF, DERIVED_FROM) | 9 min | Complete |
| EU Ecolabel | 16,333 Products | 16,333 DERIVED_FROM | ~10 min | Complete |
| Open Beauty Facts | 40,033 Products | 40,033 DERIVED_FROM | < 5 min | Complete |
| AVV codes | 1,640 AVVCode nodes | 2,265 HAS_PARENT | < 1 min | Complete |
| AVV classification | — | 499 CLASSIFIED_AS (first batch) | 18 min (500 items) | Proof of concept |
| Facilities | 38 Facility nodes | — | < 1 min | Complete |
| Waste items (Abfall-ABC) | 548 WasteItem + 5 WasteStream | 1,067 (DISPOSED_AT, DISPOSED_IN) | < 1 min | Complete |
| Materials taxonomy | ~90 Material nodes | IS_A + DEFAULT_STREAM | < 1 min | Complete |

The graph now contains over 5.3 million nodes and approximately 5.95 million edges. The vast majority of nodes are Product nodes from Open Food Facts — the vocabulary and disposal logic layers are comparatively small but carry disproportionate value for the chatbot's core function of answering "How do I dispose of X?" The product layer's value will be realized in a future phase when an agent pipeline connects products to waste items through their packaging components, enabling the barcode-to-disposal-route chain described in the schema (Section 4).
