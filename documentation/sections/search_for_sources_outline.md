# Section 5: The Search for New Sources — Outline (v2)

## Target Length
~140-170 lines (7-9 pages). Longer than the schema section, but still narrative-driven.
High-level throughout; specific sources serve as examples, not exhaustive catalogs.

## Narrative Arc
Sparse knowledge base (Section 3) + multi-layer schema (Section 4) → we need data.
Schema layers become the organizing framework for the search.
Systematic research across 19 topic areas produces 68+ evaluated sources.
We find far more data than expected → distillation is the real challenge, not acquisition.
Priority matrix + identified gaps → informs what we actually build pipelines for.

---

## 5.1 From Schema to Search Strategy

**Purpose:** Bridge from the schema section. The schema tells us *what* the graph needs; this section is about finding the data to fill it.

**Content:**
- Opening: The schema defined 5 layers and ~15 node types. But after importing the Abfall-ABC and facility data (Section 3), most of those layers were empty. The vocabulary, product, rules, and infrastructure layers had no data at all.
- The schema itself suggested the search categories. Each layer implies a class of data source:
  - Layer 0 (Vocabulary) → dictionaries, thesauri, multilingual databases
  - Layer 1 (Core) → municipal waste directories (Abfall-ABCs), AVV legal codes, waste stream definitions
  - Layer 2 (Products) → product databases with barcodes, packaging materials, material taxonomies
  - Layer 3 (Rules) → regulatory documents, municipal statutes, hazard classification systems
  - Layer 4 (Infrastructure) → facility databases, OpenStreetMap, municipal collection APIs
- We formalized this into 19 research topics, organized into four priority tiers:
  - Tier 1 (5 topics): Core data with highest impact — food, electronics, chemicals, municipal rules, AVV codes
  - Tier 2 (5 topics): Domain-specific categories — construction, packaging, batteries, wood/furniture, facilities
  - Tier 3 (5 topics): Enrichment data — material properties, taxonomies, multilingual synonyms, textiles, disassembly
  - Tier 4 (4 topics): Advanced features — computer vision, transport regulations, medical waste, office supplies
- For each topic, we defined structured research criteria: what data we needed, how it should map to the schema, and what formats/licenses were acceptable.
- Produced detailed reports for each topic, evaluating sources on a consistent rubric (relevance, coverage, data quality, access method, licensing).

## 5.2 The Six Core Domains

**Purpose:** Walk through the six domains that yielded the most significant findings. Each gets a narrative paragraph (not a source table) with 1-2 concrete examples and the key takeaway for our project.

### Food & Grocery Products (8 sources evaluated)
- Most common waste query category. The disposal question for food items is rarely about the food itself (→ Biotonne) but about the *packaging*.
- Open Food Facts emerged as the standout source: 4M+ products globally, 800K+ with German market data. Crucially, its `packagings` array provides per-component breakdowns (material, shape, weight, recycling hints) — exactly the decomposition data our product layer (Section 4.4) was designed to consume.
- Also evaluated: Open Beauty Facts (cosmetics), Open Pet Food Facts, GS1 GEPIR (barcode verification).
- Key takeaway: OFF alone could populate the entire product layer for food items. Bulk data available as Parquet dump — no API rate limits.

### Consumer Electronics & E-Waste (10 sources evaluated)
- Electronics disposal is governed by the ElektroG, which classifies devices into 6 collection groups based on physical dimensions and type. Stiftung EAR (the official German producer registry) publishes this taxonomy as structured data.
- The critical question for electronics disposal is often whether a battery can be removed. iFixit's teardown database serves as a proxy: if disassembly difficulty is "Low" or "Moderate," the chatbot can instruct battery removal; if "Very Hard," the entire device goes to e-waste collection.
- Also evaluated: Icecat (product specs), EPREL (EU energy labels), ECHA SCIP (hazardous substances in products), Open Repair Alliance.
- Key takeaway: Electronics disposal logic maps well to our conditional rule model, but requires combining multiple sources (EAR for categories, Icecat for specs, iFixit for disassembly).

### Chemicals & Hazardous Materials (15 sources evaluated)
- The most data-rich domain by far. PubChem alone contains 111 million compounds with GHS hazard classifications. Obviously, we don't need all of them — but for a university with chemistry and biology labs, even a small subset (common solvents, acids, reagents) requires reliable hazard data.
- GESTIS (German occupational safety database, 8,800 substances) is the gold standard for German lab contexts: it provides storage classes (Lagerklassen) critical for waste segregation, and references to TRGS regulations.
- A significant windfall: the BAM dangerous goods database (UN numbers, ADR transport codes) transitioned from a paid subscription to free public access in July 2025, eliminating the need for expensive logistics software.
- Also evaluated: ECHA C&L Inventory (5M classifications), CAS Common Chemistry, EPA CompTox, SDS parsing tools.
- Key takeaway: The challenge here is not finding data but filtering it. Our graph needs perhaps 500-2,000 common lab chemicals, not 111 million compounds.

### German Municipal Waste Rules (9 sources evaluated)
- This domain produced the most surprising structural finding: the "SaaS Oligopoly." Three platforms — Abfall+ (abfall.io), MyMüll (JumoMind), and Müllmax — power the waste management apps for most German municipalities. Rather than building custom scrapers for 400+ districts, integrating with these three providers would cover the majority of university towns.
- An open-source Home Assistant integration (`hacs_waste_collection_schedule`) has already reverse-engineered these APIs and provides pre-configured keys and municipality IDs for hundreds of cities — a ready-made bootstrap.
- Berlin's BSR Open Data emerged as a vocabulary gold standard: 2,000+ waste items with synonyms, avoidance tips, and recycling instructions. Because the items themselves are universal (a pizza box exists everywhere), BSR's vocabulary can serve as a base ontology with city-specific disposal rules overlaid.
- Key takeaway: Municipal data is less fragmented than it appears. Three SaaS providers + one open-source project could provide broad coverage.

### AVV/EWC Legal Codes (12 sources evaluated)
- The Abfallverzeichnis-Verordnung implements the European Waste Catalogue as German law: 20 chapters, ~113 subgroups, ~840 specific waste codes. The authoritative source is Gesetze-im-Internet (the German federal legal portal), available as HTML and PDF.
- The most complex aspect is *mirror entries* (Spiegeleinträge): pairs of codes for the same waste, distinguished by whether hazardous substances exceed concentration thresholds. Wood with hazardous substances is 20 01 37*; wood without is 20 01 38. The graph must link these pairs and trigger clarifying questions in the chatbot — which is exactly what the Condition nodes in our schema (Section 4.2) were designed for.
- The SEPA Waste Thesaurus (from Scotland's environmental agency) maps thousands of common-language terms to EWC codes — "acetone" → 14 06 03*, "pallets" → 15 01 03. This is a natural-language gold mine for the vocabulary layer.
- Also evaluated: EU Vocabularies (SKOS format), UK WM3 guidance (mirror entry thresholds), LAGA technical guidelines, university-specific Abfall-ABCs from ETH Zürich and LMU Munich.
- Key takeaway: AVV codes are the regulatory backbone. Mirror entries validate our conditional logic model; the SEPA thesaurus provides the synonym bridge between user language and legal classification.

### Construction Materials (14 sources evaluated)
- Construction and demolition waste is Germany's largest waste stream by mass, but the least well-served by open data. There is no "Open Rubble Facts" equivalent.
- The critical data challenge is *density*. A container of solid concrete has a density of ~2,400 kg/m³; the same volume of crushed rubble is ~1,500 kg/m³; mixed construction waste can be as low as 200 kg/m³. Using the wrong density factor causes massive logistics and cost errors. The graph needs to store density values per AVV code and distinguish solid (Rohdichte) from bulk (Schüttdichte) density.
- ÖKOBAUDAT (the German government's building material LCA database) and LAGA M20 (pollutant limit classification) are the primary structured sources. For density data, we had to combine values from multiple regional sources (Bavarian statistics office, City of Vienna conversion factors, South Tyrolean recycling facilities).
- Key takeaway: Construction waste is important for campus renovation contexts but requires federation of multiple incomplete sources.

## 5.3 Additional Research Domains

**Purpose:** Brief paragraph acknowledging the remaining 13 topics without deep-diving each one. Groups them thematically.

- Beyond the six core domains, we investigated 13 additional topic areas at lower priority.
- **Material-specific categories:** Packaging and recycling codes (the international 01-84 numbering system), batteries and accumulators (chemistry types, UN transport numbers, BattG compliance), wood and furniture (the German Altholzverordnung's A1-A4 contamination classification), and textiles (fiber composition databases, recyclability by material blend).
- **Enrichment and cross-cutting sources:** Universal product taxonomies (UNSPSC with 157,000 codes, ECLASS for German industry), multilingual synonym databases (Wikidata properties for material, recycling code, and labels in 300+ languages), material property databases (density, toxicity, flammability).
- **Advanced and future-looking:** Computer vision datasets for image-based waste recognition (TACO, TrashNet — small but usable for prototyping), ADR transport regulations, medical and laboratory waste (AVV Chapter 18, LAGA M18 guidelines), and office supplies/stationery (where we discovered the "Open Office Facts" gap).
- These domains are documented in our research reports but were not prioritized for immediate ETL implementation.

## 5.4 Key Cross-Cutting Discoveries

**Purpose:** Four structural insights that emerged *across* domains and shaped our overall strategy. These aren't domain-specific findings but architectural observations.

1. **The SaaS Oligopoly** — Municipal waste data appears hopelessly fragmented (400+ German districts, each with its own rules), but three SaaS platforms consolidate most of it. This transforms a seemingly intractable scraping problem into a manageable API integration task. [1-2 sentences of narrative]

2. **Mirror Entries Validate Conditional Logic** — The AVV mirror entry pattern (same waste, two codes depending on contamination) appeared independently in chemicals, construction, electronics, and municipal waste research. Every domain has items where the disposal route depends on a condition. This confirmed that our schema's Condition-based branching model was not overengineered — it reflects a fundamental structure in waste classification law. [1-2 sentences]

3. **Regional Variation Is Real but Bounded** — The "pizza box paradox" (Altpapier in some cities, Restmüll in others if greasy) and bioplastics consensus (most German processors ban them from the Biotonne) show that rules vary by municipality, but not arbitrarily. The variations cluster around a small number of genuinely ambiguous items. A graph with city-level scope overrides (as our schema supports) can model this without per-city duplication of the entire rule set. [1-2 sentences]

4. **The Scale Mismatch** — The raw data landscape exceeds 100 million potential entities. PubChem alone has 111 million compounds; Icecat has 15 million product datasheets; ECHA's classification inventory contains 5 million entries. But a university waste disposal chatbot does not need to know about every compound ever synthesized. The challenge was not finding data — it was determining how little of it we actually needed. [Transition to 5.5]

## 5.5 From 100 Million Entities to a Practical Graph

**Purpose:** The distillation problem. How we reasoned about graph size and what to actually import.

**Content:**
- Table: raw data landscape (source → order-of-magnitude entity count)
  - PubChem: 111M, Icecat: 15M, ECHA: 5M, OFF (DE): 800K, EAR: 50K, SEPA: 10K, GESTIS: 8.8K, BSR: 2K, AVV: 840, Frankfurt facilities: ~50
  - Naive total: 100M+ nodes
- But the chatbot answers "How do I dispose of X?" — it doesn't need every product variant or chemical isomer.
- Three practical scenarios:
  - MVP (~25K nodes): waste vocabulary, synonyms, AVV codes, disposal rules, facilities
  - With barcode scanning (~150K): add product layer from OFF
  - With full chemistry (~500K): add lab chemical subset from PubChem/GESTIS
- MVP layer breakdown table (layer → purpose → estimated nodes → primary source)
- Strategy: build the foundation (MVP), extend on demand. API connectors for PubChem and OFF can resolve queries at runtime without bulk import of millions of unused records.

## 5.6 Priority Matrix and Data Gaps

**Purpose:** What we prioritized for implementation and what gaps remain.

**Content:**
- Three-tier priority system with brief rationale:
  - Tier 1 (Must Have): Local Abfall-ABC, Open Food Facts, AVV codes, PubChem/GESTIS, Wikidata/dictionaries
  - Tier 2 (Should Have): ÖKOBAUDAT, Icecat, OpenStreetMap, ADR/BAM
  - Tier 3 (Nice to Have): UNSPSC/ECLASS, CV datasets, iFixit
- Data quality varied enormously: government databases (ÖKOBAUDAT, AVV) are authoritative and well-structured; crowdsourced data (OFF, Wikidata) has broad coverage but inconsistent completeness; several sources have no API at all and require PDF parsing or web scraping.
- Notable gaps we identified:
  - **No "Open Office Facts":** No open database for everyday office/stationery items comparable to OFF for food. EU Ecolabel and Blauer Engel cover eco-certified products but not the full range.
  - **Closed procurement catalogs:** German university B2B catalogs (BMEcat format, supplied through platforms like Simple System or JAGGAER) would contain exactly the items the university purchases, but these are proprietary. A partnership with the university's procurement department would be the practical path forward.
  - **Municipal rule fragmentation:** Even with the SaaS oligopoly shortcut, extracting *disposal rules* (not just collection schedules) from municipal sources requires parsing PDFs, FAQs, and Sortierhilfen that are not available via API.
  - **Language coverage:** The university's student body queries in German and English, but also in Turkish, Arabic, and other languages. Wikidata aliases and LLM-based translation can bridge this gap.
- We deliberately excluded university-specific sources (campus bin locations, room assignments, building-specific rules) from this research phase. Previous semesters had attempted this with mixed results, as it requires manual surveying, interviews, and access to internal university systems. We focused instead on building the foundational data layers that would benefit any deployment, not just the Frankfurt campus.

## 5.7 Implications for the Data Strategy

**Purpose:** Short closing that bridges to the subsequent sections (dictionary baseline, LLM pipelines, data imports).

**Content:**
- The research produced 68 evaluated and prioritized sources across 19 topic areas, documented in detailed reports.
- The data landscape falls into two clear categories: *structured sources* (APIs, databases, CSV/JSON dumps) that can be imported via deterministic ETL pipelines, and *unstructured sources* (PDFs, legal texts, municipal guidelines) that require LLM-based extraction.
- This duality directly motivates the two parallel pipeline architectures described in subsequent sections: the structured ETL system for sources like Open Food Facts, AVV code hierarchies, and facility databases; and the LLM extraction pipeline for regulatory documents, municipal statutes, and German-language PDFs.
- Of the ~68 sources, we selected a subset for implementation within the semester: the dictionary baseline (vocabulary layer), Open Food Facts (product layer), AVV code hierarchy (core layer), and facility/waste item data from existing municipal sources. The remaining sources represent a prioritized backlog for future semesters — the research itself is a deliverable that outlives this semester's implementation.

---

## Structure Improvements over v1

1. **Added methodology depth (5.1):** Describes the systematic research approach — structured topics, priority tiers, consistent evaluation rubric — without mentioning "deep research agents."
2. **Expanded domain subsections (5.2):** Each domain gets a full narrative paragraph with concrete examples, not just bullet points. This is where the section earns its length.
3. **Added "Additional Research Domains" (5.3):** Acknowledges the remaining 13 topics without deep-diving — grouped thematically in one paragraph. Prevents the section from feeling like it ignores 2/3 of the research.
4. **Renamed and expanded "Key Discoveries" (5.4):** Reframed as "cross-cutting" insights that appeared across multiple domains. Each gets a narrative explanation of *why* it matters for the project, not just a bullet point.
5. **Strengthened the distillation section (5.5):** Concrete scenario table (MVP / barcode / chemistry) makes the sizing reasoning tangible.
6. **Separated gaps from priorities (5.6):** Data quality assessment added. University-specific exclusion is now framed as a deliberate strategic choice, not an afterthought.
7. **Stronger closing (5.7):** The research itself is positioned as a deliverable — the prioritized source catalog outlives the semester's implementation. Bridges cleanly to subsequent sections.

## Style Notes
- Don't mention "deep research agents" — frame as "systematic research," "structured investigation," "topic-based evaluation"
- Use specific sources as examples to make claims concrete, but don't reproduce the full source tables
- Tables for: raw data landscape, MVP layer breakdown, priority tiers
- Reference Section 3 (data scarcity motivation), Section 4 (schema layers as search framework)
- Academic but readable, first-person plural, honest about limitations
