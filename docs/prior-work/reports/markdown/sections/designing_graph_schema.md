# 4. Designing the Graph Database Schema

The chatbot's knowledge graph must answer a deceptively layered question: "How do I dispose of X?" The answer is rarely a single fact. It depends on what the item is made of, what state it is in, which regulations apply, and where the nearest appropriate collection point is located. A banana peel goes in the Biotonne — one lookup, one answer. A paint can requires knowing whether it still contains liquid paint (hazardous waste collection) or is empty and dry (packaging bin), and in either case the user needs the address and opening hours of the relevant facility. An electric toothbrush on campus might follow different rules than the same item at home, because the university operates its own e-waste collection point that overrides the city-level default.

These three cases impose distinct requirements on the schema. The banana peel requires only a direct lookup: item → stream. The paint can requires conditional branching: item → rule → condition → stream. The toothbrush requires scope-aware override logic: item → campus rule (which supersedes city rule) → stream → campus collection point. A flat data model — a table of items and their disposal routes — cannot express any of these patterns beyond the first. The schema we designed had to encode all three, and it had to do so in a way that a language model could traverse through natural-language tool calls. This section describes the resulting ontology and the two major extensions it underwent as our data strategy evolved.

## 4.1 Design Principles

We developed the schema collaboratively with GPT-4, using it as a domain modeling partner: we described the problem space, discussed the entity types and relationships we anticipated, and iteratively refined the structure over several sessions. The result was documented in a 16-page schema specification (v4.0) that served as the blueprint for all subsequent implementation. Three principles guided the design.

**Layer separation.** The schema separates *what* something is (core entities), *how* it should be disposed of (rules and conditions), and *where* disposal physically happens (infrastructure). This separation mirrors how a knowledgeable human reasons about waste disposal: first identify the item, then determine the applicable rule, then locate the collection point. It also means that changes to campus infrastructure — a new bin location, a relocated collection point — do not require modifying any disposal rules, and vice versa.

**LLM-friendliness.** Since the chatbot agent traverses the graph through natural-language tool calls, the schema had to be legible to a language model. This meant few, semantically unambiguous relationship types (`HAS_RULE`, `REQUIRES_CHECK`, `LEADS_TO`, `LOCATED_AT`) rather than many specialized ones, and short, descriptive node labels that a model can reference in its reasoning. The schema was designed so that a system prompt listing the node types and their key relationships would be sufficient for the agent to formulate useful Cypher queries — no specialized query templates needed.

**Core versus professional layers.** Not every node type is needed for a basic waste disposal answer. The minimum viable graph requires only `WasteItem`, `WasteStream`, `DisposalRule`, and `Facility`. More advanced features — material composition, AVV legal codes, hazard classifications, per-room container inventories — were designated as "professional layer" extensions that could be added incrementally without disrupting the core traversal pattern.

## 4.2 The Three-Layer Ontology

The initial schema (v4.0) organized the graph into three layers, each answering a distinct class of question.

### Core Entities: What Is It?

The **WasteItem** is the central node in the graph — the entry point for every query. It represents a waste category as a user would name it: "Bananenschale," "Farbdose," "elektrische Zahnbürste." Each WasteItem carries a canonical German name (unique-constrained), an array of synonyms for fuzzy matching, and an optional description. The chatbot agent's first action on any query is to resolve the user's input to a WasteItem node.

**WasteStream** represents the logical disposal pathway — a bin type or waste fraction. Frankfurt operates five standard streams: Restabfalltonne (residual waste), Biotonne (organic), Verpackungstonne (packaging), Altpapiertonne (paper), and Altglascontainer (glass). Each stream carries metadata such as lid color, body color, and emptying frequency. Streams also distinguish between collection modes: "hol" (curbside pickup), "bring" (drop-off), and "service" (scheduled collection).

**AVVCode** encodes the legal classification under the Abfallverzeichnis-Verordnung (European Waste Catalogue). The AVV is a three-level hierarchy of 20 chapters, approximately 113 subgroups, and 1,505 specific waste types, where codes marked with an asterisk denote hazardous waste. Connecting WasteItems to AVV codes via `CLASSIFIED_AS` edges anchors the graph in the regulatory framework — a distinction that matters when the chatbot must explain *why* an item requires special handling, not just *where* to bring it.

### Logic and Rules: How Should It Be Disposed Of?

This is the layer that solves the "Paint Can" problem described in Section 1.2. Rather than storing conflicting disposal instructions as flat text, the schema models conditional logic as a traversable decision tree.

A **DisposalRule** is the entry point for disposal logic. Each WasteItem can have multiple rules — one for the city-level default, another for campus-specific overrides — connected via `HAS_RULE` edges. Rules carry a `scope` property ("city," "campus," "generic") and a numeric `priority`, enabling the agent to determine which rule applies in a given context. A campus-scoped rule can explicitly override its city-level counterpart via an `OVERRIDES` relationship.

For items with straightforward disposal, the rule connects directly to a WasteStream via `LEADS_TO` — no conditions required. For conditional items, the rule connects to a **Condition** node via `REQUIRES_CHECK`. The Condition carries a human-readable question ("Ist die Farbe noch flüssig?") and a machine-readable key ("aggregatzustand"). The branching is binary: `IF_TRUE` and `IF_FALSE` edges route to either a WasteStream (terminating the traversal) or another DisposalRule (which may carry its own Condition, enabling multi-level decision trees). The paint can, for instance, first branches on whether the contents are paint or lacquer, then on whether the substance is liquid or dry — two chained conditions, each resolved by a single yes-or-no clarification from the user.

**Instructions** attach imperative handling steps to rules: "Verpackung ausspülen," "Deckel abtrennen," "Nicht in den Ausguss kippen." These do not branch — they are practical guidance that the chatbot should communicate alongside the disposal route. **Tips** serve a similar role at a broader scope, attaching sorting hints, exceptions, or best practices to WasteStreams, WasteItems, or Containers (e.g., "Blue glass goes in the green container").

### Infrastructure: Where Does It Go?

The infrastructure layer maps the abstract disposal routes to physical locations. **Facility** nodes represent external collection points — Wertstoffhöfe, Schadstoffmobile, glass container sites — with addresses, opening hours, and contact information. On campus, the hierarchy is finer-grained: **Building** → **Room** → **Container**, where each Container is typed by its WasteStream (e.g., "Biotonne_240L") and placed at a specific Room.

The key relationship is `AVAILABLE_AT`, which connects a WasteStream to the Facilities where it can be deposited. Combined with the disposal rules, this creates the full traversal path the agent follows: identify the item, determine the applicable rule, evaluate any conditions, arrive at a WasteStream, and find the nearest Facility or Container where that stream is collected.

## 4.3 Schema Evolution: The Vocabulary Layer

The three-layer ontology was designed to model disposal knowledge. It did not, however, address the *discovery* problem: how does the chatbot agent find the right WasteItem node when a user asks about "Tafelschokolade" but the graph only contains "Schokolade"? The original 548 items from the Abfall-ABC covered only a fraction of the terms users might employ.

This gap became acute when we developed the dictionary baseline strategy described later in this document. The idea was to seed the graph with structured dictionary data — tens of thousands of German nouns with synonyms, translations, and taxonomic relationships — as anchor nodes for the LLM extraction pipeline. But these dictionary entries are not all waste items: "Verwandtschaft" and "Demokratie" are valid German nouns that should never receive disposal routes.

We solved this with a two-label strategy. Dictionary entries are imported as **DictionaryEntry** nodes — a new Layer 0 (Vocabulary) beneath the original three layers. After LLM classification determines whether an entry represents a physical, disposable object, qualifying entries receive an additional `:WasteItem` label. This gives the agent four distinct response states:

1. A node with both `:WasteItem` and disposal edges — full disposal guidance
2. A node with `:WasteItem` but no disposal edges — "I know this is waste but don't have routing yet"
3. A node with only `:DictionaryEntry` — "This isn't something you'd dispose of"
4. No match at all — "I don't recognize this term"

The vocabulary layer also introduced `SYNONYM_OF` edges (same-language synonyms), `TRANSLATION_OF` edges (cross-language links between German and English entries), and `IS_A` edges (hypernym–hyponym hierarchies from WordNet). These relationships dramatically expand the agent's recall without polluting the curated WasteItem namespace.

## 4.4 Schema Evolution: The Product Decomposition Layer

The second major extension arose when we began working with real product data from Open Food Facts. The original schema assumed a one-to-one mapping between things people throw away and waste categories: a chocolate bar is "Schokolade," which goes in the Biotonne. But this model breaks down for packaged products. A jar of Nutella is not one waste item — it is at least three: a glass jar (Altglascontainer), a polypropylene lid (Verpackungstonne), and a cardboard label (Altpapiertonne), each routed to a different bin. Under the original schema, the chatbot agent would have had to infer this decomposition from its parametric knowledge, with no structured data to support it.

The Open Food Facts API confirmed that this decomposition data was readily available. Its `packagings` array returns per-component breakdowns for products — material, shape, weight, and recycling hints for each physical part of the packaging. For Nutella (EAN 3017620422003), the API returns four distinct components: a clear glass pot (199.94 g), a polypropylene lid (6.8 g), a non-corrugated cardboard backing (~2.2 g), and a paper-aluminum seal (~0.4 g). Each component carries a material tag that maps deterministically to a waste stream.

We introduced three new node types as **Layer 2 (Products)**, sitting between the core entities and the rules layer:

- **Product** represents a real-world commercial product identified by its EAN barcode, carrying properties like name, brand, quantity, and category tags.
- **PackagingComponent** represents a physical part of the packaging — the jar, the lid, the label — with shape, weight, and recycling hint properties.
- **Material** represents a standardized material type from a taxonomy of approximately 90 entries derived from Open Food Facts tags (e.g., `en:clear-glass`, `en:pp-5-polypropylene`). Materials are organized in an `IS_A` hierarchy (polypropylene IS_A plastic) that enables fallback routing: if a specific sub-material has no explicit stream mapping, the agent traverses up to the parent material.

The decomposition chain — `Product → HAS_COMPONENT → PackagingComponent → MADE_OF → Material → DEFAULT_STREAM → WasteStream` — allows the agent to answer "How do I dispose of Nutella?" with per-component precision, without any LLM inference about packaging materials. The product itself also links to the core layer via `Product → IS_A → WasteItem`, connecting the product's *contents* (chocolate spread → Biotonne) to the disposal graph independently of its packaging.

This pattern drew on established ontology precedents: the CEON (Circular Economy Ontology Network) defines a similar `Product → ProductComponent → Material` hierarchy, and the Neo4j Bill of Materials pattern uses `HAS_PART` relationships with quantity properties for recursive composition trees. The novelty in our application is connecting the decomposition chain through to waste stream routing — no existing system we found combines barcode lookup, knowledge graph traversal, and per-component disposal guidance in a single pipeline.

## 4.5 The Final Schema

The schema evolved from three layers to five over the course of the semester, each extension motivated by a concrete problem encountered during implementation:

| Layer | Name | Node Types | Motivation |
|-------|------|------------|------------|
| 0 | Vocabulary | DictionaryEntry | Multilingual discovery, synonym matching, top-down graph seeding |
| 1 | Core Entities | WasteItem, WasteStream, AVVCode | Central disposal concepts and legal classification |
| 2 | Products | Product, PackagingComponent, Material | Per-component disposal routing for packaged products |
| 3 | Logic & Rules | DisposalRule, Condition, Instruction, Tip | Conditional disposal logic, handling instructions |
| 4 | Infrastructure | Container, Room, Building, Facility, Source | Physical locations and provenance tracking |

The complete schema, implemented in `src/db/schema.cql` (v5.0), defines uniqueness constraints and indexes for all node types and documents the full set of relationship patterns. Provenance tracking via `DERIVED_FROM` edges to `Source` nodes applies across all layers — every node imported by an ETL pipeline or extracted by an LLM records its origin, enabling both auditability and incremental re-extraction when source data changes.

The schema's most important property is that it supports a consistent reasoning pattern regardless of entry point. A query about any item — whether a raw waste category, a dictionary term, or a barcoded product — enters the graph at one of three points (WasteItem, DictionaryEntry, or Product) and follows the same traversal toward the answer: identify the item, resolve its disposal rule, evaluate conditions if any, arrive at a waste stream, and locate the nearest collection point. The five layers grew organically over the semester, but the traversal path they collectively support was present in the original three-layer design and survived every extension unchanged.
