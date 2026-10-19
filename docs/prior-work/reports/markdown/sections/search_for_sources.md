# 5. The Search for New Sources

The five-layer schema described in the previous section defined what the knowledge graph should contain: waste items, disposal rules, conditions, facilities, products, materials, AVV codes, dictionary entries. But after importing the Abfall-ABC and facility data described in Section 3 — 548 waste items and 38 facilities — most of those layers were empty. The vocabulary layer had no entries. The product layer had no products. The rules layer had no conditional logic. The infrastructure layer had only the 38 facilities from the previous semester's dataset. The schema was a blueprint for a building with no materials. This section describes how we searched for those materials.

## 5.1 From Schema to Search Strategy

Rather than searching for waste disposal data in general, we used the schema layers as an organizing framework. Each layer implies a category of data source: the vocabulary layer needs dictionaries and thesauri; the core layer needs municipal waste directories and legal code hierarchies; the product layer needs databases with barcodes, packaging breakdowns, and material taxonomies; the rules layer needs regulatory documents and hazard classification systems; the infrastructure layer needs facility databases and municipal collection APIs.

We formalized this mapping into 19 research topics, organized into four priority tiers. Tier 1 (five topics) targeted the highest-impact data categories: food and grocery products, consumer electronics, chemicals and hazardous materials, German municipal waste rules, and AVV legal codes. Tier 2 (five topics) covered domain-specific categories: construction materials, packaging and recycling codes, batteries, wood and furniture, and facility infrastructure. Tier 3 (five topics) addressed enrichment data: material properties, universal product taxonomies, multilingual synonyms, textiles, and disassembly instructions. Tier 4 (four topics) covered advanced features such as computer vision datasets, transport regulations, medical waste, and office supplies.

For each topic, we defined structured research criteria specifying what data we needed, how it should map to the schema, and what formats and licenses were acceptable. Each topic was investigated systematically, producing a detailed report that evaluated every discovered source on a consistent rubric: relevance to the knowledge graph (scored 1–5), data coverage and completeness, format and access method (free API, registration required, paid, PDF requiring extraction), and licensing constraints. The initial sweep identified several hundred potential sources; after deduplication and relevance filtering, 68 sources across all 19 topics received detailed evaluation. The resulting reports became the project's reference catalog for all subsequent data sourcing decisions.

## 5.2 The Six Core Domains

Six research domains produced the most significant findings. Each is summarized below with representative examples; the full evaluations are documented in the project's research reports.

### Food and Grocery Products

Food items are among the most common waste disposal queries, but the disposal question is rarely about the food itself — organic waste goes in the Biotonne. The real complexity lies in the packaging: a single product can involve glass, plastic, cardboard, and metal components, each routed to a different bin. Open Food Facts emerged as the standout source for this domain: over 4 million products globally, more than 800,000 with German market data, and — crucially — a structured `packagings` array that provides per-component breakdowns with material, shape, weight, and recycling hints. This is exactly the decomposition data our product layer (Section 4.4) was designed to consume. The entire dataset is available as a Parquet bulk dump, eliminating API rate limit concerns for large-scale import. We also evaluated Open Beauty Facts for cosmetics, Open Pet Food Facts for pet products, and GS1 GEPIR for barcode verification, but Open Food Facts alone could populate the product layer for food items.

### Consumer Electronics and E-Waste

Electronics disposal in Germany is governed by the ElektroG (Elektro- und Elektronikgerätegesetz), which classifies devices into six collection groups based on physical dimensions and device type — ranging from heat exchangers and screens to small IT and telecom equipment, with a key threshold at 50 cm (any dimension above this classifies a device as "large equipment"). Stiftung EAR, the official German producer registry, publishes this taxonomy as structured CSV data — the authoritative source for mapping devices to their legal collection group.

A recurring question in electronics disposal is whether a battery can be removed before the device is discarded. iFixit's teardown database serves as a practical proxy: if disassembly difficulty is rated "Low" or "Moderate," the chatbot can instruct the user to remove the battery; if "Very Hard," the entire device should go to an e-waste collection point. We also evaluated Icecat for product specifications, EPREL for EU energy labels, ECHA's SCIP database for hazardous substances in products, and the Open Repair Alliance for failure mode statistics. Electronics disposal logic maps well to our conditional rule model but requires combining multiple sources — EAR for legal categories, Icecat for physical specifications, and iFixit for disassembly guidance.

### Chemicals and Hazardous Materials

This domain offered the largest volume of available data by far. PubChem alone contains 111 million compounds with GHS hazard classifications, synonyms in multiple languages, and physical properties such as flash point and boiling point. For a university with chemistry and biology laboratories, even a small subset — common solvents, acids, bases, reagents — requires reliable hazard data to determine whether a substance qualifies as hazardous waste. GESTIS, the German occupational safety database maintained by the DGUV, emerged as the gold standard for the German lab context: its 8,800 substance entries include storage classes (Lagerklassen) critical for waste segregation, handling instructions, and references to TRGS regulations that govern laboratory waste classification.

A significant discovery in this domain was that the BAM dangerous goods database — which provides UN transport numbers, ADR classification codes, and packing group assignments for 4,500 substances — transitioned from a paid subscription service (previously costing several hundred euros per year) to free public access in July 2025. This eliminated the need for expensive logistics software or manual ADR lookups. We also evaluated the ECHA Classification and Labelling Inventory (5 million classifications), CAS Common Chemistry, EPA CompTox databases, and tools for automated Safety Data Sheet parsing. The challenge in this domain is not finding data but filtering it: our graph needs perhaps 500 to 2,000 common lab chemicals, not 111 million compounds.

### German Municipal Waste Rules

This domain produced the most surprising structural finding. German municipal waste management appears hopelessly fragmented — over 400 districts, each with its own waste management company, its own Abfall-ABC, and its own sorting rules. Building custom scrapers or parsers for each district would be intractable. But the research revealed that three SaaS platforms consolidate most of this landscape: Abfall+ (abfall.io), which dominates in southern and western Germany; MyMüll (operated by JumoMind), which covers much of NRW, Hesse, and western districts; and Müllmax, a legacy system used by several large cities. Rather than 400 custom integrations, three API integrations would cover the majority of German university towns.

This discovery was amplified by a second finding: an open-source Home Assistant integration (`hacs_waste_collection_schedule`) has already reverse-engineered these APIs and provides pre-configured tenant keys and municipality IDs for hundreds of cities. This represents a ready-made bootstrap for any waste data integration project.

We also identified Berlin's BSR Open Data as a vocabulary gold standard: over 2,000 waste items with synonyms, avoidance tips, and recycling instructions, published under a CC BY license. Because the waste items themselves are universal — a pizza box exists in every German city — BSR's vocabulary can serve as a base ontology with city-specific disposal rules overlaid as scoped overrides.

### AVV/EWC Legal Codes

The Abfallverzeichnis-Verordnung implements the European Waste Catalogue as German law: 20 chapters organized by waste origin, approximately 113 subgroups, and roughly 840 specific six-digit waste codes. Codes marked with an asterisk denote hazardous waste. The authoritative source is Gesetze-im-Internet, the German federal legal portal, which provides the complete AVV as HTML and PDF.

The most complex aspect of the AVV is its mirror entries (Spiegeleinträge): pairs of codes for the same waste, distinguished by whether hazardous substances exceed concentration thresholds. Wood containing hazardous substances is classified as 20 01 37\*; wood without hazardous substances is 20 01 38. Soil contaminated with dangerous substances is 17 05 03\*; clean soil is 17 05 04. The graph must link these pairs and trigger clarifying questions — which is exactly what the Condition nodes in our schema (Section 4.2) were designed for. The mirror entry pattern appeared independently in every domain we researched: chemicals, construction materials, electronics, and municipal waste all contain items where the disposal route depends on a condition that must be evaluated.

A particularly valuable source for bridging user language and legal classification is the SEPA Waste Thesaurus, published by Scotland's environmental agency. It maps thousands of common-language terms directly to EWC codes — "acetone" to 14 06 03\*, "pallets" to 15 01 03, "fluorescent tubes" to 20 01 21\* — providing the synonym layer that connects how people talk about waste to how the law classifies it. We also evaluated EU Vocabularies in SKOS format (natively compatible with knowledge graphs), UK WM3 guidance for mirror entry threshold calculations, LAGA technical guidelines, and university-specific chemical waste guides from ETH Zürich and LMU Munich.

### Construction Materials

Construction and demolition waste is Germany's largest waste stream by mass, but the least well-served by open data. There is no "Open Rubble Facts" equivalent — no single database that provides comprehensive material classifications, densities, and contamination indicators for construction materials the way Open Food Facts does for food products.

The critical data challenge in this domain is density. A cubic meter of solid concrete has a density of approximately 2,400 kg/m³. The same volume of crushed concrete rubble has a bulk density of roughly 1,500 kg/m³. Mixed construction waste — rubble combined with wood, plastics, and wallpaper — can be as low as 200 kg/m³. Using the wrong density factor causes massive errors in logistics planning and disposal cost estimation. The graph must distinguish solid density (Rohdichte) from bulk density (Schüttdichte) and select the correct factor based on the AVV code. For density data, we had to federate values from multiple regional sources: the Bavarian statistics office, the City of Vienna's disposal conversion factors, and weighbridge measurements from South Tyrolean recycling facilities.

ÖKOBAUDAT, the German government's building material LCA database, provides life-cycle assessment data for over 1,400 building products, including end-of-life scenarios. LAGA M20 defines pollutant concentration limits that determine whether construction waste can be freely reused (class Z0), reused with restrictions (Z1/Z2), or must be treated as hazardous waste. Both are well-structured sources, but neither alone is sufficient — construction waste data requires combining regulatory classification, material science, and operational logistics sources.

## 5.3 Additional Research Domains

Beyond the six core domains, the remaining 13 research topics covered material-specific categories, enrichment sources, and advanced capabilities. For packaging and recycling codes, we documented the complete international numbering system (codes 01–84 covering plastics, paper, metals, glass, and composites) and identified Open Food Facts' material tags as a practical implementation of this taxonomy. For batteries, we cataloged chemistry types (lithium-ion, NiMH, lead-acid, NiCd), their UN transport numbers, and BattG compliance requirements. For wood and furniture, we documented the German Altholzverordnung's contamination classification from A I (untreated, freely recyclable) through A IV (preservative-treated, hazardous waste) — another domain where condition-based disposal routing is essential.

Enrichment sources included Wikidata, which provides material properties, recycling codes, and labels in over 300 languages through its SPARQL endpoint; UNSPSC, a product taxonomy with 157,000 codes available as a free PDF; and ECLASS, the German industry classification standard (licensed, not free). For computer vision, we evaluated TACO (1,500 annotated images, 60 waste categories) and TrashNet (2,527 images, 6 categories) — small datasets useful for prototyping but insufficient for production deployment. Medical and laboratory waste (AVV Chapter 18, LAGA M18 guidelines) is particularly relevant for the university context but involves regulatory complexity that exceeds what we could address within the semester. Office supplies proved to be the domain with the largest open-data gap: no equivalent to Open Food Facts exists for everyday stationery and office items. The EU Ecolabel database and Blauer Engel product registry cover eco-certified products but not the full range of items a university procures.

These domains are fully documented in the project's research reports and represent a prioritized backlog for future implementation.

## 5.4 Cross-Cutting Discoveries

Four structural insights emerged across the domain-specific research and shaped our overall data strategy.

**The SaaS Oligopoly.** The discovery that three SaaS platforms consolidate most German municipal waste management (described in Section 5.2) has implications beyond the municipal rules domain. It means that facility data, collection schedules, and even Abfall-ABC vocabularies can be sourced through a small number of API integrations rather than hundreds of municipal scrapers. Combined with the pre-configured keys available through the open-source Home Assistant integration, this transforms what appeared to be the most fragmented data landscape into one of the most accessible.

**Conditional logic as a universal pattern.** The AVV mirror entry pattern — same waste, two codes depending on contamination — is not a legal curiosity confined to the AVV chapter. It appeared independently in every domain we researched. Electronics may or may not contain hazardous components. Wood may or may not be treated with preservatives. Paint may or may not still be liquid. The pattern is the same: a disposal route that branches on a condition. This confirmed that the schema's Condition-based branching model (Section 4.2) was not overengineered for edge cases but reflects a fundamental structure in waste classification law.

**Regional variation is real but bounded.** The pizza box paradox and bioplastics consensus described in Section 5.2 illustrate that disposal rules vary by municipality, but the variations are not arbitrary. They cluster around a small number of genuinely ambiguous items. A graph with city-level scope overrides, as our schema supports through the DisposalRule's `scope` property, can model this without duplicating the entire rule set for every municipality.

**The scale mismatch.** The raw data landscape exceeds 100 million potential entities, but a university waste disposal chatbot does not need to know about every compound ever synthesized or every product ever manufactured. The challenge we faced was not data acquisition — it was distillation. The following section describes how we reasoned about what to actually import.

## 5.5 From 100 Million Entities to a Practical Graph

To appreciate the distillation challenge, consider the data landscape we surveyed:

| Source | Entity Count | Domain |
|--------|-------------|--------|
| PubChem | 111,000,000 | Chemical compounds |
| Icecat | 15,000,000 | Consumer products |
| ECHA C&L Inventory | 5,000,000 | Chemical classifications |
| Open Food Facts (DE) | 800,000 | Food products |
| Stiftung EAR | 50,000 | Electronics producers |
| SEPA Waste Thesaurus | 10,000 | Waste term synonyms |
| GESTIS | 8,800 | Lab safety substances |
| BSR Abfall-ABC | 2,000 | Waste item vocabulary |
| AVV Codes | 840 | Legal waste codes |
| Frankfurt Facilities | ~50 | Local disposal locations |

Importing everything naively would produce a graph of over 100 million nodes — most of which would never be queried. A university chatbot that answers "How do I dispose of X?" needs a curated foundation, not a data lake.

We defined three practical scenarios for graph sizing:

| Scenario | Nodes | Use Case |
|----------|-------|----------|
| MVP | ~25,000 | Text-based disposal queries |
| With barcode scanning | ~150,000 | Product lookups via EAN codes |
| With full chemistry | ~500,000 | Lab waste advisor for university context |

The MVP graph breaks down by layer: approximately 5,000 waste vocabulary items (primarily from the BSR Abfall-ABC and dictionary baseline), 8,000 synonym entries, 1,000 AVV codes, 10,000 disposal rules (the most labor-intensive layer, requiring extraction from municipal documents and statutes), 200 facilities, and a small number of container and building nodes. The product and chemistry layers would extend the graph on demand — via API connectors that resolve barcode queries against Open Food Facts or chemical name queries against PubChem at runtime, caching results as new graph nodes without requiring bulk import of millions of unused records.

## 5.6 Priority Matrix and Data Gaps

Based on the research, we organized the evaluated sources into three priority tiers:

**Tier 1 (Must Have):** The local Abfall-ABC as the authoritative waste vocabulary; Open Food Facts for food product packaging data; AVV codes from Gesetze-im-Internet for legal classification; PubChem and GESTIS for lab chemical hazard data; and Wikidata or dictionary sources for multilingual synonyms and taxonomic relationships.

**Tier 2 (Should Have):** ÖKOBAUDAT for construction material data; Icecat for electronics specifications; OpenStreetMap for Wertstoffhof locations; and the BAM dangerous goods database for ADR transport codes.

**Tier 3 (Nice to Have):** UNSPSC and ECLASS for product taxonomy enrichment; material property databases for density and safety data; computer vision datasets for image recognition; and iFixit for disassembly guidance.

Data quality varied considerably across sources. Government databases such as ÖKOBAUDAT and the AVV are authoritative and well-structured but update infrequently. Crowdsourced platforms like Open Food Facts and Wikidata offer broad coverage and real-time updates but with inconsistent completeness — not every product has its packaging materials tagged, not every Wikidata item has German-language labels. Several important sources have no API at all: GESTIS prohibits scraping, LAGA guidelines are published as PDFs, and many municipal Sortierhilfen exist only as downloadable documents. This format heterogeneity — JSON APIs alongside PDF legal texts alongside SPARQL endpoints — directly motivated our decision to build two parallel ETL architectures.

The research also revealed notable gaps. No open database for everyday office and stationery items exists that is comparable to Open Food Facts for food. The EU Ecolabel ECAT database and Blauer Engel's product registry cover eco-certified products, but the majority of items a university procures — pens, printer cartridges, adhesives, cleaning agents — fall outside their scope. German university procurement catalogs, which would contain exactly these items, follow the BMEcat standard but are proprietary and accessible only through B2B platforms. A partnership with the university's procurement department would be the most practical path to closing this gap.

We deliberately excluded university-specific sources from this research phase — campus bin locations, per-room container assignments, building-specific waste rules, and Schadstoffmobil schedules. Previous semesters had attempted to collect this data through manual surveying and interviews with facilities management, with mixed results. We chose instead to focus on building foundational data layers that would benefit any deployment of the system, not just the Frankfurt campus. University-specific data can be layered on top of the foundation without schema changes, using the campus-scoped override mechanism described in Section 4.2.

## 5.7 Implications for the Data Strategy

The systematic research produced 68 evaluated and prioritized sources across 19 topic areas, each documented with access methods, format specifications, licensing constraints, and relevance scores. This catalog is itself a deliverable — a reference that outlives this semester's implementation and provides a prioritized backlog for future work.

The discovered sources fall into two distinct categories that shaped our pipeline architecture. Structured sources — APIs, database dumps, CSV and JSON files with known schemas — can be imported through deterministic ETL pipelines: parse the input, map fields to graph properties, merge nodes and relationships. Open Food Facts, AVV code hierarchies, and facility databases fall into this category. Unstructured sources — PDFs, legal texts, municipal guidelines, German-language regulatory documents — require LLM-based extraction: analyze the document, identify entities and relationships, validate against the schema, and load the results with provenance tracking.

This duality directly motivated the two parallel pipeline architectures described in subsequent sections. Of the 68 sources, we selected a subset for implementation within the semester: the dictionary baseline for the vocabulary layer, Open Food Facts for the product layer, the AVV code hierarchy for the core layer, and the existing facility and waste item data from municipal sources. The remaining sources — chemical databases, municipal SaaS APIs, construction material registries, and the many PDF-based regulatory documents — represent a prioritized roadmap that can be executed incrementally using the pipeline infrastructure we built.
