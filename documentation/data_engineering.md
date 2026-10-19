<!-- Title Page -->
# Fessi: Knowledge Graph ETL Pipeline
## Data Engineering Documentation

**Project:** Fessi: Intelligent Waste Disposal Chatbot
**Module:** Data Engineering
**Institution:** Frankfurt University of Applied Sciences
**Semester:** Winter Semester 2024/25

<!-- TODO: Add student names, student numbers, supervisor, date -->

---

# Table of Contents

- [Management Summary](#management-summary)
- [1. Introduction](#1-introduction)
  - [1.1 Initial Situation](#11-initial-situation)
  - [1.2 The Case for Graph-Based Retrieval](#12-the-case-for-graph-based-retrieval)
  - [1.3 Project Scope](#13-project-scope)
- [2. Cleaning Up the Implementation](#2-cleaning-up-the-implementation)
  - [2.1 Usability Aspects of Agentic Chatbots](#21-usability-aspects-of-agentic-chatbots)
  - [2.2 Extending the Agent's Toolset](#22-extending-the-agents-toolset)
  - [2.3 The Impact of Unconstrained Reasoning](#23-the-impact-of-unconstrained-reasoning)
- [3. Designing the Graph Database Schema](#3-designing-the-graph-database-schema)
  - [3.1 Design Principles](#31-design-principles)
  - [3.2 The Three-Layer Ontology](#32-the-three-layer-ontology)
  - [3.3 Schema Evolution: The Vocabulary Layer](#33-schema-evolution-the-vocabulary-layer)
  - [3.4 Schema Evolution: The Product Decomposition Layer](#34-schema-evolution-the-product-decomposition-layer)
  - [3.5 The Final Schema](#35-the-final-schema)
- [4. Creating the Dictionary Baseline](#4-creating-the-dictionary-baseline)
  - [4.1 The Case for Top-Down Construction](#41-the-case-for-top-down-construction)
  - [4.2 The Two-Label Strategy](#42-the-two-label-strategy)
  - [4.3 Data Sources](#43-data-sources)
  - [4.4 The Import Pipeline](#44-the-import-pipeline)
  - [4.5 Results](#45-results)
  - [4.6 Integration with Existing ETL](#46-integration-with-existing-etl)
- [5. Infrastructure](#5-infrastructure)
  - [5.1 Why Python](#51-why-python)
  - [5.2 Neo4j Deployment](#52-neo4j-deployment)
  - [5.3 JupyterLab](#53-jupyterlab)
  - [5.4 Database Management](#54-database-management)
  - [5.5 The Chatbot Deployment](#55-the-chatbot-deployment)
  - [5.6 Reproducing the Setup](#56-reproducing-the-setup)
- [6. Results](#6-results)
  - [6.1 Data Engineering Perspective](#61-data-engineering-perspective)
- [7. Future Work](#7-future-work)
  - [7.1 Immediate Next Steps](#71-immediate-next-steps)
  - [7.2 Data Source Integration](#72-data-source-integration)
  - [7.3 Institutional Partnerships](#73-institutional-partnerships)
  - [7.4 Connecting Products to Disposal Knowledge](#74-connecting-products-to-disposal-knowledge)
  - [7.5 Graph Quality and Maintenance](#75-graph-quality-and-maintenance)

---

# Management Summary

Fessi is an intelligent waste disposal chatbot for the Frankfurt University of Applied Sciences. When we began this semester, the project's third iteration, the system contained 548 waste items and 38 facility entries, stored in a vector database that could not model the conditional logic central to waste disposal ("Is the paint can empty or still liquid?"). The chatbot took up to 30 seconds to respond with no visible feedback, and most user queries returned ungrounded answers because the knowledge base simply did not contain the relevant data.

This document describes the data engineering work that addressed both problems: designing and populating a Neo4j knowledge graph that replaces the vector store, and building the pipeline infrastructure to fill it at scale.

We designed a five-layer graph schema (vocabulary, core entities, products, logic and rules, and infrastructure) that models waste disposal as a traversable decision structure rather than a collection of text documents. A paint can connects to a disposal rule, which branches on a condition node; the true and false branches route to different waste streams and facilities. This structure enables the chatbot agent to ask clarifying questions and provide precise, unambiguous answers.

To populate the graph, we built two parallel pipeline architectures. Structured ETL pipelines handle sources with known schemas (CSV files, JSON APIs, Parquet dumps) through deterministic Python scripts using MERGE-based idempotent writes. An LLM-based extraction pipeline, described in the companion Data Mining documentation, handles unstructured German documents through schema-driven extraction with Pydantic models. Both pipeline types produce provenance-tracked, re-runnable imports.

The most consequential design decision was building a vocabulary foundation before importing domain-specific data. We parsed 355,660 German nouns from Wiktionary, clustered them into 329,231 concepts using WordNet synsets and synonym grouping, and used LLM classification to promote 67,912 entries to the WasteItem label, a 124-fold increase over the inherited 548 items. This broad anchor layer gave subsequent imports a merge target, preventing the entity duplication that would otherwise compound with every additional data source.

The largest single import was Open Food Facts: 4.29 million products and 555,227 packaging components with material links, enabling a full decomposition chain from barcode scan to per-component waste stream routing. Additional imports covered the legal AVV code hierarchy (1,640 codes), EU Ecolabel products (16,333), Open Beauty Facts (40,033), and a materials taxonomy for packaging classification. The graph now contains over 5.3 million nodes and approximately 5.95 million edges.

All services (Neo4j, JupyterLab, the chatbot application, and the ETL pipelines) run on a single shared workstation managed through rootless Podman containers and systemd Quadlet units. This co-location means the graph built by the pipelines is immediately available to the chatbot agent without synchronization. Every pipeline is idempotent, provenance-tracked, and reproducible from open-access data sources at negligible cost.

The graph is broad but unevenly deep. The product and vocabulary layers are well-populated; the disposal logic layer, meaning the conditional rules that make the chatbot's answers precise rather than generic, still contains only the original Abfall-ABC entries. Closing this gap is the primary objective for future work: completing the AVV classification across all waste items, integrating municipal disposal rules from the three SaaS platforms that consolidate most German municipal waste management, and establishing institutional partnerships for university-specific data such as procurement catalogs and campus infrastructure.

---

# 1. Introduction

The effective management of waste disposal on university campuses presents a deceptively complex challenge. The question "Where do I throw this away?" quickly reveals layers of conditional logic, regional regulations, and situational context that no simple lookup table can address. A paint can, for instance, may require fundamentally different disposal routes depending on whether it still contains liquid paint or is empty and dry. This single distinction determines whether the item constitutes hazardous waste or ordinary recyclable packaging. This kind of conditional reasoning, common across hundreds of waste categories, is precisely where traditional information retrieval systems fail.

"Fessi" is an intelligent waste disposal chatbot developed for the Frankfurt University of Applied Sciences campus. The project, now in its third semester of iterative development, aims to provide students, staff, and visitors with precise, context-aware disposal guidance through a conversational interface. The first semester produced "Müll-Meister," an initial prototype built on jQuery and Microsoft frameworks that demonstrated the concept but suffered from fundamental architectural limitations, including initialization times exceeding 58 seconds and a monolithic backend structure. The second semester saw a complete rewrite: our team rebuilt the frontend as a Progressive Web App using Angular, developed the API specifications, and delivered a fully functional chat interface with voice input, image upload, and offline capabilities.
Concurrently, the backend was migrated to a LangGraph-based agent architecture, but the underlying retrieval
mechanism remained a rigid, vector-based RAG pipeline that was not designed to handle the conditional logic
inherent in waste disposal queries. As a result, the chatbot's performance was severely hampered by long response
times and high token usage due to the need to retrieve and process large amounts of often irrelevant information,
rendering it effectively unusable.

## 1.1 Initial Situation

Upon beginning this semester's work, we inherited a system that was architecturally sound but operationally limited.
The previous backend/pipeline team had migrated the chatbot from a fixed processing pipeline to a LangGraph-based
agent, which was a significant step forward. However, the agent still operated under strict guardrails inherited from the original pipeline design: a rigid sequence of vector search, follow-up question evaluation, another vector search, and response formulation. The frontend had not been designed to accommodate the computational cost of this approach, resulting in response times of up to 30 seconds during which the user interface provided no feedback.

More critically, upon examining the underlying knowledge base, we found remarkably little to build upon. After
sorting and importing everything available, the system contained approximately 548 waste items sourced almost
entirely from the Abfall-ABC (a municipal waste directory), along with 38 disposal facility entries representing
campus collection points and drop-off locations. This limited coverage meant that the majority of user queries
simply could not be answered from the knowledge base. In practice, the agent would search the vector store, find
nothing relevant, and answer from its parametric knowledge, which often led to generic responses that were not grounded in the actual data.

## 1.2 The Case for Graph-Based Retrieval

The existing system relied on vector-based Retrieval-Augmented Generation (RAG), which retrieves document chunks by semantic similarity and passes them to a language model for answer generation. This approach works well for straightforward factual lookups such as "What are the opening hours of Wertstoffhof Nord?" where the answer exists verbatim in a document. However, it fundamentally fails on conditional disposal logic, which accounts for a significant portion of real waste disposal queries.

Consider the "Paint Can" problem: a user asks "How do I dispose of a paint can?" A vector search retrieves all document chunks mentioning paint cans, which simultaneously contain conflicting information: "dried paint residue goes in residual waste," "liquid paint must be taken to a hazardous waste facility," "empty metal cans belong in the packaging bin." The language model receives these contradictory passages and must somehow synthesize a coherent answer, typically resulting in a vague response that lists all options without determining which applies.

A knowledge graph models this problem as a decision tree rather than a collection of documents. The waste item "paint can" connects to a disposal rule, which branches on a condition ("Is it still liquid?"). The true branch routes to the hazardous waste facility; the false branch routes to the packaging bin. When the chatbot agent traverses this graph, it encounters the condition node and can ask the user a clarifying question before providing a precise, unambiguous answer. This is not a theoretical advantage. It represents the difference between "here are some things you could do with a paint can" and "bring it to the Schadstoffannahme on Campus Nibelungenplatz, open Tuesday and Thursday 10–14 Uhr."

Beyond conditional logic, graph-based retrieval offers structural advantages for an agentic chatbot. Rather than retrieving a fixed number of similar text chunks, the agent can traverse relationships, following links from a waste item to its disposal rules, from rules to facilities, from facilities to opening hours and addresses. This traversal mirrors the way a knowledgeable human would reason about waste disposal: identify the item, determine its category, check for special conditions, and locate the appropriate disposal point. A knowledge graph makes this reasoning path explicit and queryable.

## 1.3 Project Scope

This semester's work focuses on building the knowledge graph infrastructure that the chatbot needs to provide accurate, context-aware disposal guidance. This encompasses two complementary workstreams: constructing ETL pipelines that transform raw data sources into a structured Neo4j knowledge graph, and developing LLM-based extraction methods that can process unstructured German documents into graph-compatible entities and relationships.

The project also addresses the data scarcity problem identified in the initial situation. Through systematic
research, we identified roughly 400 viable data sources for waste disposal knowledge, ranging from chemical substance
databases with millions of entries to municipal PDF guides and legal statutes. Importing and classifying this data at scale, using both deterministic pipelines for structured sources and LLM-based extraction for unstructured documents, forms the core technical contribution of this work.

In parallel, we made targeted improvements to the chatbot agent itself. We replaced the rigid pipeline with a freely reasoning agent, removing the guardrails that had previously constrained it to a fixed sequence of tool calls. We implemented visible reasoning steps, inspired by the "thinking" displays in modern AI interfaces, so that the user could follow the agent's progress rather than staring at a blank screen. We added web search as a fallback knowledge source, and built a barcode scanning module that can decompose real-world products into their individual packaging components for per-material disposal routing.

This documentation is split across two companion documents reflecting the two modules under which this work was conducted. The present document covers the **Data Engineering** aspects: data sources and quality assessment, graph schema design, structured ETL pipelines, the dictionary baseline data pipeline, the EAN barcode product pipeline, and infrastructure.

# 2. Cleaning Up the Implementation

Given the initial situation described in Section 1.1 (an agent constrained by inherited guardrails, response times of up to 30 seconds with no user feedback, and a knowledge base too sparse to answer most queries), we concluded that incremental improvements to the existing system would not be sufficient. Instead, we decided to rebuild the chatbot's core interaction model from scratch. The guiding insight was that the very behavior which had rendered the chatbot "unusable," its tendency to make multiple tool calls and reason through a problem step by step, was not a defect to be suppressed, but a capability to be surfaced. The previous implementation had hidden this process behind a blank screen; our task was to make it visible.

## 2.1 Usability Aspects of Agentic Chatbots

### The Feedback Problem

The original chatbot suffered from what is, in retrospect, a straightforward usability failure rather than a performance problem. As described in Section 1.1, the agent's fixed pipeline of vector searches and evaluations could take 20 to 30 seconds per query. During this entire period, the user interface displayed nothing: no progress indicator, no status message, no evidence that the system was working at all. From the user's perspective, the chatbot appeared to have frozen.

Our initial assumption was that reducing response time would solve this. After working with the system, however, we came to a different conclusion: the wait itself was not the core issue. Users routinely wait 10 to 30 seconds for complex queries on modern AI interfaces. ChatGPT, Gemini, and Perplexity all exhibit similar latencies for multi-step reasoning tasks. What those systems provide, and what our chatbot lacked, was *visibility into the process*. A user who can see that the agent is "Searching the knowledge base for Batterien" and then "Exploring waste categories" perceives an active system working on their behalf. A user staring at a blank screen perceives a broken one.

### Visible Reasoning: The Thought Process Panel

Inspired by how Google's Gemini displays an expandable summary of its internal reasoning steps, we implemented a "Thought process" panel that renders the agent's actions in real time as collapsible accordion elements. Each action the agent takes (thinking, calling a tool, receiving a result) appears as a distinct step in a vertical timeline, color-coded by type:

- **Thought steps** (purple) represent the agent's internal reasoning: "Analyzing request..." These appear whenever the language model is invoked to decide what to do next.
- **Tool calls** (orange) show which tool the agent is invoking and with what parameters. For example, a knowledge base search displays the JSON input `{"waste_item": "Batterien"}`, making the agent's search strategy transparent.
- **Tool results** (green) display the outcome of each tool call, including the execution duration. Users can expand these to inspect the raw data the agent received.
- **Observations** (cyan) appear when the agent synthesizes intermediate findings before deciding on its next action.

The panel header shows a summary, "Thought process (20 steps)," along with a status indicator: a spinner while the agent is still working, a checkmark once it has finished. The entire panel is collapsible: users who want the quick answer can ignore it; users who want to understand *how* the agent arrived at that answer can expand individual steps and trace the full reasoning chain.

This design serves two purposes simultaneously. For the end user, it transforms an opaque wait into an observable process. For developers and domain experts, it provides a debugging and evaluation interface: one can see exactly which tools the agent called, what data it received, and where it may have gone wrong.

### Streaming Architecture

To deliver reasoning steps and response text incrementally rather than as a single payload after completion, we replaced the chatbot's request-response communication model with Server-Sent Events (SSE). The backend streams four event types to the frontend:

1. **`step`** events carry individual agent actions (thought, tool call, tool result) as they occur. Each step includes a type, title, content, timestamp, and, for tool results, execution duration and metadata.
2. **`token`** events carry individual text tokens of the final response, enabling the familiar character-by-character typing effect.
3. **`message_start`** provides metadata (message ID, conversation ID, timestamp) at the beginning of the stream.
4. **`done`** signals completion so the frontend can finalize the message.

On the backend, the FastAPI endpoint hooks into LangGraph's event system. Each LangGraph callback (`on_chat_model_start`, `on_tool_start`, `on_tool_end`, `on_chat_model_stream`) maps to an SSE event that is pushed to the client immediately. On the frontend, an Angular streaming service consumes these events using the native `fetch` API with an `AbortController` for cancellation support, parsing the SSE protocol and routing each event type to the appropriate UI component.

The result is that the first reasoning step appears within one to two seconds of the user submitting a query. Even when the full response takes 30 seconds or more, the user sees continuous activity from the moment they press send.

## 2.2 Extending the Agent's Toolset

With the usability foundation in place (users could now observe and tolerate multi-step reasoning), we turned to expanding what the agent could actually do. The inherited agent ran on GPT-4o and had access to a single tool: vector similarity search over the knowledge base. We changed the underlying model and added four new tools.

### Switching to a Reasoning Model

We replaced GPT-4o with a reasoning model (GPT-OSS-120B). The motivation was not raw capability but behavioral: reasoning models engage in explicit chain-of-thought before producing an output, which fundamentally changes how they interact with tools. Where GPT-4o required careful prompt engineering to follow a multi-step tool-use strategy, the reasoning model autonomously plans its approach, deciding which tools to call, in what order, and how to synthesize the results, with minimal instruction.

This had a direct consequence for the guardrail problem. The previous system needed a rigid pipeline (search, evaluate, search again, respond) precisely because the non-reasoning model could not reliably decide for itself what to do next. With a reasoning model, we could remove these guardrails entirely and let the agent determine its own strategy for each query. In practice, we observed the agent performing up to 30 tool calls on complex queries, searching the knowledge base, refining its search terms based on initial results, cross-referencing facilities, and only then formulating a response. This behavior would have been impossible under the old fixed-step pipeline and prohibitively slow without the streaming architecture described above.

The system supports configurable reasoning levels (low, medium, high) via environment variables, allowing the deployment to balance thoroughness against latency depending on the use case.

### Knowledge Graph Tools

We added tools that allow the agent to query and navigate the Neo4j knowledge graph directly. Rather than relying solely on vector similarity search, which returns text chunks ranked by embedding distance, these tools let the agent execute structured queries: looking up a waste item by name, retrieving its disposal rules and associated conditions, exploring connected facilities with their addresses and opening hours, and traversing the category hierarchy. This is the mechanism through which Graph-RAG delivers its advantage over vector-RAG: the agent does not receive a list of similar documents, but traverses a structured network of relationships.

### Web Search

We integrated web search via the Tavily API as a fallback knowledge source. When the knowledge base does not contain the information the agent needs, which, given the data scarcity described in Section 1.1, was frequently the case, the agent can issue targeted web queries and incorporate the results into its response.

This was initially intended as a stopgap to compensate for the sparse knowledge base. In practice, it proved more valuable than expected: it allowed us to test and refine the agent's reasoning capabilities without being blocked by missing data. We could observe how the agent narrowed its search queries progressively, starting broad, then targeting specific details it had identified as missing, which closely mirrors how it would navigate a well-populated knowledge graph. When asked about disposing of hydrochloric acid in Bremen, for instance, the agent issued three successive web searches: first for general chemical disposal regulations, then for the specific hazardous waste collection service, and finally for the address and opening hours of the nearest collection point. This iterative refinement pattern reinforced our confidence in the graph-based architecture, since it demonstrated that the agent could effectively navigate a structured information landscape to assemble a complete answer from multiple targeted queries.

### Vision Helper

The reasoning model we selected (GPT-OSS-120B) is not multimodal: it cannot process images directly. Since the chatbot's frontend supports image uploads (a capability built in the previous semester), we implemented a vision helper service that delegates image analysis to a separate multimodal model. When the agent receives a message containing an image, the vision helper sends it to a dedicated vision model (configurable, defaulting to GPT-4o-mini) which generates a text description. The agent then reasons about this description using its primary model.

This architecture decouples the choice of reasoning model from the requirement for multimodal input. It also enables deployment on heterogeneous hardware: in our development environment, we ran the vision model (Qwen2.5-VL 7B) on one GPU and the reasoning model on another, with audio transcription (via faster-whisper) on a third.

### EAN Barcode Scanner

Building on the vision helper, we developed a barcode scanning module that allows users to photograph product barcodes for disposal guidance. The module uses pyzbar (a Python wrapper around the ZBar library) for barcode detection and decoding, a deterministic computer vision step, not an LLM call. The extracted EAN number is then passed to a multi-API resolver that queries product databases in a cascading fallback sequence: Open Food Facts, Open Beauty Facts, UPCitemdb, and optionally OpenGTINDB. The first API to return a match provides the product data, including, critically, a breakdown of packaging components with their materials.

This capability transforms a single user action (photographing a product) into per-component disposal routing. A jar of Nutella, for instance, decomposes into a glass jar (Altglascontainer), a polypropylene lid (Verpackungstonne), and a paper label (Altpapiertonne). The barcode pipeline and the underlying product-to-material-to-waste-stream graph traversal are described in detail in the EAN barcode pipeline section later in this document. The integration into the live chatbot as an agent tool is planned but not yet deployed.

## 2.3 The Impact of Unconstrained Reasoning

The combination of visible reasoning, streaming, and an expanded toolset produced a qualitative shift in the chatbot's behavior. Under the old system, the agent followed a script: search, evaluate, search, respond. Under the new system, the agent adapts to the query. A simple factual question ("What goes in the Biotonne?") might require only a single knowledge base lookup. A complex conditional question ("How do I dispose of a half-full paint can?") triggers a multi-step investigation: searching for the item, identifying the relevant condition, checking both branches of the decision tree, and locating the appropriate facility. A question that falls outside the knowledge base's coverage might prompt the agent to fall back to web search, querying multiple sources and synthesizing the results into a structured answer.

This flexibility came not from more complex prompt engineering, but from *less*. The reasoning model's internal chain-of-thought replaced the external guardrails that had previously been necessary to guide a non-reasoning model through multi-step tasks. With fewer constraints, the agent could recover gracefully from dead ends (for instance, falling back to web search when a graph query returned no results) rather than failing silently at a predetermined step in a fixed pipeline.

The visible reasoning panel made this adaptive behavior legible to both users and developers. Where the old chatbot was a black box that either returned an answer or did not, the new system exposes its entire decision-making process. A user asking about battery disposal can watch the agent search for "Batterien," discover that batteries are classified as hazardous waste, look up the campus hazardous waste collection point, retrieve its address and opening hours, and compose a response that synthesizes all of this into actionable guidance. The answer takes longer to produce, but the user understands why, and trusts the result more because they witnessed the reasoning behind it.

# 3. Designing the Graph Database Schema

The chatbot's knowledge graph must answer a deceptively layered question: "How do I dispose of X?" The answer is rarely a single fact. It depends on what the item is made of, what state it is in, which regulations apply, and where the nearest appropriate collection point is located. A banana peel goes in the Biotonne: one lookup, one answer. A paint can requires knowing whether it still contains liquid paint (hazardous waste collection) or is empty and dry (packaging bin), and in either case the user needs the address and opening hours of the relevant facility. An electric toothbrush on campus might follow different rules than the same item at home, because the university operates its own e-waste collection point that overrides the city-level default.

These three cases impose distinct requirements on the schema. The banana peel requires only a direct lookup: item → stream. The paint can requires conditional branching: item → rule → condition → stream. The toothbrush requires scope-aware override logic: item → campus rule (which supersedes city rule) → stream → campus collection point. A flat data model, a table of items and their disposal routes, cannot express any of these patterns beyond the first. The schema we designed had to encode all three, and it had to do so in a way that a language model could traverse through natural-language tool calls. This section describes the resulting ontology and the two major extensions it underwent as our data strategy evolved.

## 3.1 Design Principles

We developed the schema collaboratively with GPT-4, using it as a domain modeling partner: we described the problem space, discussed the entity types and relationships we anticipated, and iteratively refined the structure over several sessions. The result was documented in a 16-page schema specification (v4.0) that served as the blueprint for all subsequent implementation. Three principles guided the design.

**Layer separation.** The schema separates *what* something is (core entities), *how* it should be disposed of (rules and conditions), and *where* disposal physically happens (infrastructure). This separation mirrors how a knowledgeable human reasons about waste disposal: first identify the item, then determine the applicable rule, then locate the collection point. It also means that changes to campus infrastructure (a new bin location, a relocated collection point) do not require modifying any disposal rules, and vice versa.

**LLM-friendliness.** Since the chatbot agent traverses the graph through natural-language tool calls, the schema had to be legible to a language model. This meant few, semantically unambiguous relationship types (`HAS_RULE`, `REQUIRES_CHECK`, `LEADS_TO`, `LOCATED_AT`) rather than many specialized ones, and short, descriptive node labels that a model can reference in its reasoning. The schema was designed so that a system prompt listing the node types and their key relationships would be sufficient for the agent to formulate useful Cypher queries, with no specialized query templates needed.

**Core versus professional layers.** Not every node type is needed for a basic waste disposal answer. The minimum viable graph requires only `WasteItem`, `WasteStream`, `DisposalRule`, and `Facility`. More advanced features (material composition, AVV legal codes, hazard classifications, per-room container inventories) were designated as "professional layer" extensions that could be added incrementally without disrupting the core traversal pattern.

## 3.2 The Three-Layer Ontology

The initial schema (v4.0) organized the graph into three layers, each answering a distinct class of question.

### Core Entities: What Is It?

The **WasteItem** is the central node in the graph and the entry point for every query. It represents a waste category as a user would name it: "Bananenschale," "Farbdose," "elektrische Zahnbürste." Each WasteItem carries a canonical German name (unique-constrained), an array of synonyms for fuzzy matching, and an optional description. The chatbot agent's first action on any query is to resolve the user's input to a WasteItem node.

**WasteStream** represents the logical disposal pathway, a bin type or waste fraction. Frankfurt operates five standard streams: Restabfalltonne (residual waste), Biotonne (organic), Verpackungstonne (packaging), Altpapiertonne (paper), and Altglascontainer (glass). Each stream carries metadata such as lid color, body color, and emptying frequency. Streams also distinguish between collection modes: "hol" (curbside pickup), "bring" (drop-off), and "service" (scheduled collection).

**AVVCode** encodes the legal classification under the Abfallverzeichnis-Verordnung (European Waste Catalogue). The AVV is a three-level hierarchy of 20 chapters, approximately 113 subgroups, and 1,505 specific waste types, where codes marked with an asterisk denote hazardous waste. Connecting WasteItems to AVV codes via `CLASSIFIED_AS` edges anchors the graph in the regulatory framework, a distinction that matters when the chatbot must explain *why* an item requires special handling, not just *where* to bring it.

### Logic and Rules: How Should It Be Disposed Of?

This is the layer that solves the "Paint Can" problem described in Section 1.2. Rather than storing conflicting disposal instructions as flat text, the schema models conditional logic as a traversable decision tree.

A **DisposalRule** is the entry point for disposal logic. Each WasteItem can have multiple rules, one for the city-level default, another for campus-specific overrides, connected via `HAS_RULE` edges. Rules carry a `scope` property ("city," "campus," "generic") and a numeric `priority`, enabling the agent to determine which rule applies in a given context. A campus-scoped rule can explicitly override its city-level counterpart via an `OVERRIDES` relationship.

For items with straightforward disposal, the rule connects directly to a WasteStream via `LEADS_TO` with no conditions required. For conditional items, the rule connects to a **Condition** node via `REQUIRES_CHECK`. The Condition carries a human-readable question ("Ist die Farbe noch flüssig?") and a machine-readable key ("aggregatzustand"). The branching is binary: `IF_TRUE` and `IF_FALSE` edges route to either a WasteStream (terminating the traversal) or another DisposalRule (which may carry its own Condition, enabling multi-level decision trees). The paint can, for instance, first branches on whether the contents are paint or lacquer, then on whether the substance is liquid or dry, two chained conditions, each resolved by a single yes-or-no clarification from the user.

**Instructions** attach imperative handling steps to rules: "Verpackung ausspülen," "Deckel abtrennen," "Nicht in den Ausguss kippen." These do not branch; they are practical guidance that the chatbot should communicate alongside the disposal route. **Tips** serve a similar role at a broader scope, attaching sorting hints, exceptions, or best practices to WasteStreams, WasteItems, or Containers (e.g., "Blue glass goes in the green container").

### Infrastructure: Where Does It Go?

The infrastructure layer maps the abstract disposal routes to physical locations. **Facility** nodes represent external collection points (Wertstoffhöfe, Schadstoffmobile, glass container sites) with addresses, opening hours, and contact information. On campus, the hierarchy is finer-grained: **Building** → **Room** → **Container**, where each Container is typed by its WasteStream (e.g., "Biotonne_240L") and placed at a specific Room.

The key relationship is `AVAILABLE_AT`, which connects a WasteStream to the Facilities where it can be deposited. Combined with the disposal rules, this creates the full traversal path the agent follows: identify the item, determine the applicable rule, evaluate any conditions, arrive at a WasteStream, and find the nearest Facility or Container where that stream is collected.

## 3.3 Schema Evolution: The Vocabulary Layer

The three-layer ontology was designed to model disposal knowledge. It did not, however, address the *discovery* problem: how does the chatbot agent find the right WasteItem node when a user asks about "Tafelschokolade" but the graph only contains "Schokolade"? The original 548 items from the Abfall-ABC covered only a fraction of the terms users might employ.

This gap became acute when we developed the dictionary baseline strategy described later in this document. The idea was to seed the graph with structured dictionary data, tens of thousands of German nouns with synonyms, translations, and taxonomic relationships, as anchor nodes for the LLM extraction pipeline. But these dictionary entries are not all waste items: "Verwandtschaft" and "Demokratie" are valid German nouns that should never receive disposal routes.

We solved this with a two-label strategy. Dictionary entries are imported as **DictionaryEntry** nodes, a new Layer 0 (Vocabulary) beneath the original three layers. After LLM classification determines whether an entry represents a physical, disposable object, qualifying entries receive an additional `:WasteItem` label. This gives the agent four distinct response states:

1. A node with both `:WasteItem` and disposal edges: full disposal guidance
2. A node with `:WasteItem` but no disposal edges: "I know this is waste but don't have routing yet"
3. A node with only `:DictionaryEntry`: "This isn't something you'd dispose of"
4. No match at all: "I don't recognize this term"

The vocabulary layer also introduced `SYNONYM_OF` edges (same-language synonyms), `TRANSLATION_OF` edges (cross-language links between German and English entries), and `IS_A` edges (hypernym–hyponym hierarchies from WordNet). These relationships dramatically expand the agent's recall without polluting the curated WasteItem namespace.

## 3.4 Schema Evolution: The Product Decomposition Layer

The second major extension arose when we began working with real product data from Open Food Facts. The original schema assumed a one-to-one mapping between things people throw away and waste categories: a chocolate bar is "Schokolade," which goes in the Biotonne. But this model breaks down for packaged products. A jar of Nutella is not one waste item but at least three: a glass jar (Altglascontainer), a polypropylene lid (Verpackungstonne), and a cardboard label (Altpapiertonne), each routed to a different bin. Under the original schema, the chatbot agent would have had to infer this decomposition from its parametric knowledge, with no structured data to support it.

The Open Food Facts API confirmed that this decomposition data was readily available. Its `packagings` array returns per-component breakdowns for products: material, shape, weight, and recycling hints for each physical part of the packaging. For Nutella (EAN 3017620422003), the API returns four distinct components: a clear glass pot (199.94 g), a polypropylene lid (6.8 g), a non-corrugated cardboard backing (~2.2 g), and a paper-aluminum seal (~0.4 g). Each component carries a material tag that maps deterministically to a waste stream.

We introduced three new node types as **Layer 2 (Products)**, sitting between the core entities and the rules layer:

- **Product** represents a real-world commercial product identified by its EAN barcode, carrying properties like name, brand, quantity, and category tags.
- **PackagingComponent** represents a physical part of the packaging (the jar, the lid, the label) with shape, weight, and recycling hint properties.
- **Material** represents a standardized material type from a taxonomy of approximately 90 entries derived from Open Food Facts tags (e.g., `en:clear-glass`, `en:pp-5-polypropylene`). Materials are organized in an `IS_A` hierarchy (polypropylene IS_A plastic) that enables fallback routing: if a specific sub-material has no explicit stream mapping, the agent traverses up to the parent material.

The decomposition chain `Product → HAS_COMPONENT → PackagingComponent → MADE_OF → Material → DEFAULT_STREAM → WasteStream` allows the agent to answer "How do I dispose of Nutella?" with per-component precision, without any LLM inference about packaging materials. The product itself also links to the core layer via `Product → IS_A → WasteItem`, connecting the product's *contents* (chocolate spread → Biotonne) to the disposal graph independently of its packaging.

This pattern drew on established ontology precedents: the CEON (Circular Economy Ontology Network) defines a similar `Product → ProductComponent → Material` hierarchy, and the Neo4j Bill of Materials pattern uses `HAS_PART` relationships with quantity properties for recursive composition trees. The novelty in our application is connecting the decomposition chain through to waste stream routing; no existing system we found combines barcode lookup, knowledge graph traversal, and per-component disposal guidance in a single pipeline.

## 3.5 The Final Schema

The schema evolved from three layers to five over the course of the semester, each extension motivated by a concrete problem encountered during implementation:

| Layer | Name | Node Types | Motivation |
|-------|------|------------|------------|
| 0 | Vocabulary | DictionaryEntry | Multilingual discovery, synonym matching, top-down graph seeding |
| 1 | Core Entities | WasteItem, WasteStream, AVVCode | Central disposal concepts and legal classification |
| 2 | Products | Product, PackagingComponent, Material | Per-component disposal routing for packaged products |
| 3 | Logic & Rules | DisposalRule, Condition, Instruction, Tip | Conditional disposal logic, handling instructions |
| 4 | Infrastructure | Container, Room, Building, Facility, Source | Physical locations and provenance tracking |

The complete schema, implemented in `src/db/schema.cql` (v5.0), defines uniqueness constraints and indexes for all node types and documents the full set of relationship patterns. Provenance tracking via `DERIVED_FROM` edges to `Source` nodes applies across all layers, so every node imported by an ETL pipeline or extracted by an LLM records its origin, enabling both auditability and incremental re-extraction when source data changes.

The schema's most important property is that it supports a consistent reasoning pattern regardless of entry point. A query about any item, whether a raw waste category, a dictionary term, or a barcoded product, enters the graph at one of three points (WasteItem, DictionaryEntry, or Product) and follows the same traversal toward the answer: identify the item, resolve its disposal rule, evaluate conditions if any, arrive at a waste stream, and locate the nearest collection point. The five layers grew organically over the semester, but the traversal path they collectively support was present in the original three-layer design and survived every extension unchanged.

# 4. Creating the Dictionary Baseline

Before building individual import pipelines, we conducted a systematic evaluation of available data sources, documented in full in the companion Data Mining documentation. Using the schema layers as an organizing framework, we defined 19 research topics across four priority tiers and investigated each systematically. The resulting catalog assessed 68 sources on a consistent rubric covering relevance, data quality, format, and licensing. Key findings included Open Food Facts as the primary product data source (4.29 million products with per-component packaging breakdowns), the AVV code hierarchy for legal classification, chemical substance databases (PubChem, GESTIS) for lab waste, and the discovery that three SaaS platforms consolidate most German municipal waste management, reducing what appeared to be 400 custom integrations to three API endpoints. The evaluation also identified the most significant data gap: no open equivalent to Open Food Facts exists for everyday office and stationery items.

With 68 evaluated sources spanning millions of potential entities, the natural next step would have been to start building ETL pipelines for the highest-priority sources (import Open Food Facts products, parse AVV codes from the legal text, extract disposal rules from municipal PDFs) and let the graph grow incrementally as each pipeline delivered its data.

We did not take this approach. Before importing any domain-specific data, we needed to address a structural problem that would have compounded with every subsequent import: the graph had no vocabulary foundation. The 548 Abfall-ABC items described in Section 1.1 covered a fraction of the terms people use when describing waste. Running the LLM extraction pipeline against hundreds of source documents without a broad anchor layer would mean that entity creation and entity resolution would happen simultaneously: the pipeline would need to both understand a document and decide whether each entity it encountered was new or already existed in the graph. These are fundamentally different tasks, and combining them degrades both.

## 4.1 The Case for Top-Down Construction

The conventional approach to building a knowledge graph from heterogeneous sources is bottom-up: process each document, extract entities and relationships, merge duplicates after the fact. This approach has five structural problems that become acute at scale.

First, duplicate explosion. Source A mentions "Schokolade," source B "Schokoriegel," source C "chocolate bar." Without pre-existing anchor nodes, the pipeline creates three separate nodes for what is semantically one item. Second, inconsistent granularity. One source discusses "Batterie," another "Knopfzelle," a third "AA-Batterie." Without a taxonomy, the pipeline cannot determine whether these are synonyms, siblings, or a parent-child hierarchy. Third, deduplication is destructive. Merging nodes after the fact requires fuzzy matching and risks losing relationship edges and breaking provenance links. Fourth, limited recall. A student searches "Tafelschokolade" but the graph only contains "Schokolade" because that term appeared in the source PDFs. Without upfront synonym seeding, the graph's recall is bounded by the vocabulary of whichever documents have been processed. Fifth, the LLM carries two competing tasks: understanding document content and resolving entities against the existing graph.

A 2025 study comparing ontology-grounded and extraction-first knowledge graph construction for RAG found that ontology-first graphs achieved 90% answer accuracy, compared to 60% for vector-RAG baselines. The key finding was that aligning a knowledge graph with an ontology from a structured source yields performance comparable to ontologies derived from dynamic text corpora, while substantially reducing computational cost. This validated our decision to build the vocabulary layer first.

The approach cleanly separates two phases. Vocabulary building, the dictionary import, is deterministic, structured, and requires no LLM involvement. It creates the noun inventory with synonyms, translations, and taxonomic relationships. Knowledge attachment, the subsequent LLM extraction pipeline, processes source documents and attaches disposal rules, conditions, and facility links to existing anchor nodes. The LLM focuses on what it does well (understanding unstructured text) without the burden of entity resolution.

## 4.2 The Two-Label Strategy

Section 3.3 described the two-label strategy at a conceptual level and its four resulting agent response states. The implementation relies on Neo4j's multi-label capability: a single node can carry both `:DictionaryEntry` and `:WasteItem` labels simultaneously, queryable via either. Dictionary entries are imported exclusively as `:DictionaryEntry` nodes. Promotion to `:WasteItem` happens only after LLM classification confirms that an entry represents a physical, disposable object, a separate phase that can be re-run as classification models improve or criteria change.

The practical consequence is that promotion is cheap and reversible. Adding a label is a single Cypher `SET` operation; removing one is equally trivial. This means the boundary between "recognized vocabulary" and "actionable waste item" can be adjusted without re-importing any data, and without affecting the synonym, translation, or taxonomy edges attached to the underlying DictionaryEntry node.

## 4.3 Data Sources

We selected three primary sources, prioritizing open licensing, structured bulk access, and complementary coverage.

**Kaikki.org / Wiktextract** provides the raw noun inventory. The project extracts structured data from Wiktionary using the `wiktextract` library and publishes it as JSONL files. We used the German Wiktionary extraction (`de-extract.jsonl.gz`, approximately 280 MB compressed), which contains 1.3 million entries across all languages and parts of speech defined in the German Wiktionary. Filtering for German-language nouns (`lang_code == "de"` and `pos == "noun"`) yielded 355,660 entries. Each entry carries the headword, definitions (glosses), same-language synonyms, cross-language translations, hypernyms, hyponyms, and semantic categories. The data is licensed under CC-BY-SA 3.0 and GFDL.

**OdeNet (Open German WordNet)** provides taxonomic structure. Developed at Hochschule Darmstadt, OdeNet organizes approximately 36,000 synsets (of which 25,767 are noun synsets) into hypernym-hyponym trees: "Schokoriegel" IS_A "Schokolade" IS_A "Suesswaren" IS_A "Lebensmittel." The `wn` Python package provides a unified query interface for both OdeNet and the English WordNet, and critically, OdeNet includes interlingual links (ILI) to the Open English WordNet that enable cross-language translation at the synset level. OdeNet was originally seeded from OpenThesaurus synonym clusters and Princeton WordNet, providing a bridge between colloquial German synonyms and formal taxonomic structure. Licensed under CC-BY-SA 4.0.

**Open English WordNet (2024 edition)** provides the English-side taxonomy, with approximately 120,000 synsets accessible through the same `wn` API. English DictionaryEntry nodes linked via `TRANSLATION_OF` edges to their German counterparts enable the chatbot agent to handle English queries natively through graph traversal rather than runtime translation.

Source selection was driven by practical constraints. We chose Kaikki.org over Duden or PONS because of its open license and bulk JSONL download; Duden is copyrighted and scraping would violate terms of service. We chose OdeNet over GermaNet because GermaNet, while considered the gold standard, requires a license application signed by a thesis advisor at Universität Tübingen, a bureaucratic dependency we could not afford in a semester project. OdeNet provides the same `wn` API, the same taxonomic structure, and a permissive CC-BY-SA license.

## 4.4 The Import Pipeline

The pipeline proceeds in four phases, each independently verifiable and re-runnable.

**Phase 0: Download and Cache.** The Kaikki.org JSONL file is downloaded via HTTP streaming and cached locally. OdeNet and the Open English WordNet are installed through the `wn` package, which manages its own SQLite database. All downloads are cached in `data/dictionaries/` and reprocessed as needed. The `kaikki-json` Python package was evaluated but its built-in download URL returned a 404; we replaced it with direct `requests` streaming.

**Phase 1: Parse and Filter.** The raw JSONL (1.3 million entries) is filtered to German nouns (355,660 entries). For each entry, the pipeline extracts the headword, glosses, synonyms, translations, hypernyms, and semantic categories. These are cross-referenced against OdeNet synsets using synset IDs as deduplication keys.

The central processing step is concept clustering, which groups multiple surface forms into a single canonical concept. This proceeds in two passes. First, synset-based grouping: entries that share an OdeNet synset ID are merged into a single cluster. This produced 16,001 clusters from 28,816 matched words, entries like "Schokoriegel" and "Tafelschokolade" that OdeNet already groups under the same synset. Second, union-find synonym clustering: for the remaining entries (those without OdeNet synset matches), a union-find data structure groups words that share synonyms transitively. If word A lists word B as a synonym, and word B lists word C, all three are merged into one cluster. This second pass produced 313,230 additional clusters from the remaining entries.

For each cluster, a canonical term is selected using a heuristic: prefer the term that appears as a hypernym of the others in OdeNet; failing that, prefer the shortest single-word lemma. This favors "Schokolade" over "Schokoriegel" and "Batterie" over "Knopfzelle." The total output is 329,231 concept clusters written to an intermediate JSONL file (177 MB), each containing the canonical term, its synonyms, English translations, hypernyms, hyponyms, semantic categories, and source provenance. The pipeline runs in approximately 29 seconds after the initial download.

**Phase 2: Neo4j Import.** Rather than routing through the LLM extraction pipeline's EntityLoader, which is designed for single-entity dispatch from document processing, we implemented a dedicated five-pass UNWIND batch importer. Pass 1 creates canonical DictionaryEntry nodes. Pass 2 creates synonym nodes and `SYNONYM_OF` edges. Pass 3 creates English translation nodes and `TRANSLATION_OF` edges. Pass 4 creates hypernym nodes and `IS_A` edges. Pass 5 creates the Source node and `DERIVED_FROM` provenance edges. All operations use `MERGE` for idempotent re-runs. The import completes in approximately 20 seconds for all 329,231 clusters, roughly 100 times faster than routing through the single-entity loader would have been.

**Phase 3: LLM Classification and Promotion.** The 329,231 concept clusters are classified by a small language model (Llama 3.1 8B via Groq) using structured output. For each entry, the model determines whether the noun refers to a physical object (`is_physical`), whether someone could plausibly need to dispose of it (`disposal_relevant`), and assigns a broad category (object, substance, food, organism, structure, person, place, event, or nonphysical).

A key optimization reduced the number of LLM calls by approximately 70%. Of the 329,231 clusters, roughly 237,000 are inflected forms (genitives, plurals, and datives) whose dictionary glosses follow patterns like "Nominativ Plural des Substantivs Schokolade." These forms inherit their classification from the base form without requiring an LLM call. The classifier detects inflections via regex, classifies the approximately 92,000 base forms through the LLM in batches of 25, then propagates results to all associated inflected forms.

An initial implementation bug caused approximately 104,000 inflected forms to fail base-form resolution: their glosses referenced a base form (such as "0,5-Liter-Flasche") that existed only as a synonym in another cluster, not as a canonical name. The fix was straightforward (extend the lookup table to map synonym names, not just canonical names, back to their cluster) but it affected a third of all entries. Because the classifier includes resume support (re-running skips already-classified entries), the fix required only an 11-minute incremental run rather than repeating the full 98-minute initial classification. The total cost for classifying all 329,231 clusters was approximately $0.30 via Groq.

After classification, promotion is a single Cypher query: `MATCH (d:DictionaryEntry {is_physical: true}) WHERE NOT d:WasteItem SET d:WasteItem`. This adds the `:WasteItem` label to all entries classified as physical and disposal-relevant, making them instantly visible to all existing queries and tools that operate on the WasteItem label.

## 4.5 Results

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

The 67,912 promoted WasteItem nodes represent a 124-fold increase over the previous graph's 548 items, transforming a sparse vocabulary into a broad foundation that covers the vast majority of physical objects a German-speaking user might ask about. The classification breakdown shows the distribution: approximately 55,000 objects, 9,000 substances, 3,700 food items, and smaller counts for organisms, structures, and other physical categories.

The taxonomy structure provides inheritance-based reasoning that the previous flat list could not support. A disposal rule attached to "Batterie" automatically applies to all hyponyms ("Knopfzelle," "AA-Batterie," "Lithium-Ionen-Akku") through IS_A traversal. The 122,960 synonym edges ensure that the chatbot agent can resolve colloquial, regional, and informal terms to their canonical forms. The 24,738 cross-language translation edges enable English-language queries to reach German disposal knowledge through graph traversal rather than runtime translation.

The entire pipeline, from raw download through classification and promotion, cost under $0.50 in LLM API fees and ran in under two hours of wall-clock time, most of which was the LLM classification phase. The deterministic steps (download, parsing, Neo4j import) complete in under a minute combined. This makes the dictionary baseline reproducible from scratch at negligible cost, an important property for a pipeline that may need to be re-run as dictionary sources update or classification criteria are refined.

## 4.6 Integration with Existing ETL

The dictionary baseline was designed as Phase 1 of the new graph, the first import after schema initialization. This required adapting the existing structured ETL modules to work against a dictionary-seeded graph rather than an empty one.

The facility import (`src/etl/facilities.py`) required no changes, since Facility nodes are independent of the vocabulary layer. The waste item import (`src/etl/waste_items.py`), however, posed a subtle problem. The existing pipeline uses `MERGE (w:WasteItem {name: ...})` to create or match waste items. Before the dictionary import, this worked correctly, since every MERGE created a new node. After the dictionary import, "Backofen" already exists in the graph as a `:DictionaryEntry` node, but without the `:WasteItem` label, so the MERGE does not find it and creates a duplicate.

The solution was a pre-promotion step: before the main import loop, all CSV item names that exist as `:DictionaryEntry` nodes (but not yet as `:WasteItem`) receive the `:WasteItem` label. Then the existing MERGE finds them via the WasteItem label index and fires the ON MATCH branch, which updates the existing node with disposal relationships rather than creating a duplicate. For items not present in the dictionary, the MERGE creates new standalone `:WasteItem` nodes as before. We also added `DERIVED_FROM` provenance edges linking every CSV-imported item to a Source node, bringing the waste item importer in line with the provenance tracking pattern established by the dictionary pipeline.

This approach preserved backward compatibility (the same import code handles both dictionary-seeded and empty graphs) while preventing the duplicate nodes that would have undermined the entire rationale for top-down construction.

# 5. Infrastructure

The ETL pipelines described in the preceding sections require two things to run: a Neo4j database to write into and a Python environment to execute from. Both of these, along with a JupyterLab instance for interactive development and the chatbot application itself, are deployed on a shared workstation (10.18.2.105) that serves as the project's combined data processing and application server. All services (Neo4j, JupyterLab, and the chatbot) run as rootless Podman containers managed by systemd, sharing the same machine and the same graph database. This section describes how these systems are deployed and how to reproduce or extend the setup.

## 5.1 Why Python

The entire data processing stack is implemented in Python. This was a deliberate choice driven by ecosystem fit rather than language preference. Python provides the tightest integration with the tools the project depends on: LangChain and LangGraph for the LLM extraction pipeline, the `neo4j` driver for database access, PyArrow for reading the multi-gigabyte Open Food Facts Parquet dump, Pydantic for schema-driven extraction, and the `wn` package for WordNet access in the dictionary baseline. Equally important, Python's dominance in data science means that JupyterLab, our primary environment for developing and testing extraction pipelines, treats Python as a first-class citizen. Every pipeline in this project was prototyped in a Jupyter notebook before being formalized into a module, and that workflow would have been significantly more friction-laden in any other language.

## 5.2 Neo4j Deployment

Neo4j 5 Community Edition runs as a rootless Podman container managed by systemd through Podman's Quadlet mechanism. Quadlet files are declarative container definitions, similar in spirit to Docker Compose services but translated directly into native systemd units. This means Neo4j starts on boot, restarts on failure, and integrates with standard Linux service management (`systemctl`, `journalctl`) without requiring a container orchestrator.

The deployment consists of four Quadlet files installed to `~/.config/containers/systemd/`: a container unit (`neo4j.container`) defining the image, ports (7474 for the HTTP browser, 7687 for the Bolt protocol), environment variables, and the APOC plugin configuration; and three companion volume units for persistent storage of graph data, logs, and the APOC import staging directory. An install script (`deployment/scripts/install.sh`) handles the complete setup: it verifies prerequisites (Podman installed, user lingering enabled), copies the Quadlet files, reloads the systemd daemon, enables the service, and polls the HTTP endpoint until Neo4j responds.

The choice of rootless Podman over Docker was motivated by the server environment. The workstation runs Fedora, where Podman is the native container runtime and rootless execution is the default, requiring no daemon, no root privileges, and no Docker socket to secure. Quadlet was chosen over a plain `podman-compose` setup (which also exists in the repository under `docker/podman-compose.yml` for local development) because it provides proper service supervision: automatic restarts, boot-time startup, and log aggregation through journald.

## 5.3 JupyterLab

A JupyterLab instance runs on the same workstation, providing an interactive environment for pipeline development, data exploration, and one-off extraction tasks. This co-location was necessary for practical reasons: the Open Food Facts Parquet dump is 4.4 GB of highly compressed columnar data representing 4.29 million products, the JSONL variant adds another 2.6 GB, and several intermediate processing files (the Kaikki dictionary JSONL at 280 MB, classification result caches, OFF subset extracts) add to the data footprint. Running notebooks on a laptop and connecting to a remote Neo4j instance would have meant either transferring these files repeatedly or maintaining duplicate copies. With JupyterLab on the same machine as Neo4j, both the data files and the database are local, and iteration cycles (load a Parquet partition, transform it, write to Neo4j, inspect the result) take seconds rather than minutes.

Every pipeline in this project has a corresponding notebook that served as its development environment: `notebooks/pipeline_example.ipynb` for the LLM extraction chain, `notebooks/avv_classification.ipynb` for the AVV code hierarchy, and additional notebooks for the dictionary baseline and Open Food Facts imports. These notebooks remain the most practical way to understand what each pipeline does, since they show the intermediate outputs that the command-line interfaces hide.

## 5.4 Database Management

Three operations cover the database lifecycle: initialization, dump import, and reset.

**Initialization** is handled by `src/scripts/init_db.py`, which verifies the Neo4j connection, applies schema constraints and indexes from `src/db/schema.cql`, and reports statistics (node counts by label, total relationships). The schema file defines uniqueness constraints on all node types, ensuring that `MERGE` operations in the ETL pipelines behave correctly, and indexes on frequently queried properties. Schema application is idempotent: the script parses `schema.cql` line by line and silently skips any constraint or index that already exists, so it can be re-run safely after a dump import, after manual schema changes, or as a post-deployment smoke test.

**Dump import** allows transferring a complete graph state between environments. The `deployment/scripts/import-dump.sh` script automates the process: it stops the Neo4j service (required by Community Edition for offline loading), mounts the dump file into a temporary container, runs `neo4j-admin database load`, restarts the service, and verifies the result by querying the node count.

**Reset** clears the entire database and is available via `python src/scripts/init_db.py --reset`. It requires interactive confirmation before executing, and is primarily used during development when a clean graph is needed for testing an import pipeline from scratch.

## 5.5 The Chatbot Deployment

The chatbot application (Advanced-LLM-Chat) is a separate repository containing the Angular frontend, FastAPI backend, and the LangGraph-based agent described in Section 2. It ships with three deployment options: a Docker Compose stack for local development (`docker/docker-compose.yml`), a production Compose variant that pulls pre-built images from the GitHub Container Registry (`docker/docker-compose.prod.yml`), and a Helm chart (`helm/fessi/`) for Kubernetes deployment with StatefulSets for both PostgreSQL and Neo4j, configurable ingress, and secrets management.

In practice, the chatbot is deployed on the same workstation as a Podman Quadlet, loosely based on the Docker Compose definition but adapted for rootless systemd management, following the same pattern as the Neo4j deployment. Because all services share one machine, the backend connects to Neo4j at `bolt://localhost:7687` and both the ETL pipelines and the chatbot agent operate on the same graph without any network transfer or synchronization step. The PostgreSQL database that the chatbot uses for session management and user data is a separate concern; it does not interact with the knowledge graph.

## 5.6 Reproducing the Setup

Because the current deployment co-locates all services on a single workstation, the graph built by the ETL pipelines is immediately available to the chatbot agent. For future deployments where the data processing environment and the chatbot run on separate machines, the dump import mechanism described in Section 5.4 provides the transfer path: export a dump from the source Neo4j, transfer the file, and run the import script on the target. The dump captures the complete graph state, so the target environment does not need access to any of the raw data files or pipeline code.

The Neo4j browser is accessible at `http://10.18.2.105:7474` and the Bolt endpoint at `bolt://10.18.2.105:7687`. The ETL pipeline's `.env` file requires three variables for database access (`NEO4J_URI`, `NEO4J_USER`, `NEO4J_PASSWORD`) and, for LLM-dependent pipelines, `LLM_API_KEY` and `LLM_BASE_URL` (described in the companion Data Mining documentation). An `.env.example` template documents all available variables. Dependencies are installed via `pip install -r requirements.txt`; no system-level packages are required beyond Python 3.12 and Podman.

# 6. Results

The semester began with 548 waste items, 38 facilities, and a chatbot that could not answer most queries. It ended with a knowledge graph of over 5.3 million nodes and approximately 5.95 million edges, populated by nine distinct import pipelines, served from a single workstation, and immediately available to the chatbot agent.

| Metric | Start of Semester | End of Semester |
|--------|-------------------|-----------------|
| WasteItem nodes | 548 | 67,912 (124x increase) |
| Total nodes | ~590 | 5,327,000+ |
| Total edges | ~1,100 | ~5,950,000 |
| Product coverage | 0 | 4,290,312 products, 555,227 packaging components |
| Legal classification | 0 | 1,640 AVV codes, 499 classified |
| Provenance-tracked sources | 0 | 9 distinct import pipelines |

The following table summarizes each import pipeline's contribution:

| Import | Nodes | Key Edges | Status |
|--------|-------|-----------|--------|
| Dictionary baseline | 423,717 | 589,124 (synonym, translation, taxonomy, provenance) | Complete |
| Open Food Facts | 4,290,312 Products + 555,227 Components | 5,305,389 (HAS_COMPONENT, MADE_OF, DERIVED_FROM) | Complete |
| EU Ecolabel | 16,333 Products | 16,333 DERIVED_FROM | Complete |
| Open Beauty Facts | 40,033 Products | 40,033 DERIVED_FROM | Complete |
| AVV codes | 1,640 AVVCode nodes | 2,265 HAS_PARENT | Complete |
| AVV classification | — | 499 CLASSIFIED_AS (first batch) | Proof of concept |
| Facilities | 38 Facility nodes | — | Complete |
| Waste items (Abfall-ABC) | 548 WasteItem + 5 WasteStream | 1,067 (DISPOSED_AT, DISPOSED_IN) | Complete |
| Materials taxonomy | ~90 Material nodes | IS_A + DEFAULT_STREAM | Complete |

All pipelines are reproducible from open-access data sources using either cloud inference APIs or local GPU hardware. No paid data sources, proprietary APIs, or dedicated compute infrastructure were required.

## 6.1 Data Engineering Perspective

**Schema as a living design.** The graph schema began as a three-layer ontology (core entities, logic and rules, infrastructure) and grew to five layers over the course of the semester. Each extension was motivated by a concrete problem encountered during implementation, not by upfront speculation. The vocabulary layer (Layer 0) was added when the dictionary baseline strategy required nodes that were not yet waste items, German nouns that might or might not represent disposable objects. The product decomposition layer (Layer 2) was added when Open Food Facts data revealed that a single product could decompose into multiple packaging components, each routed to a different waste stream. Both extensions preserved the core traversal pattern unchanged: identify the item, resolve its disposal rule, evaluate conditions, arrive at a waste stream, locate the nearest collection point. The fact that this pattern survived two major schema extensions without modification validated the original design principles: layer separation, LLM-friendliness, and core-versus-professional layering.

**The dictionary baseline as foundation.** The decision to build a vocabulary layer before importing any domain-specific data proved to be the most consequential architectural choice of the semester. The conventional approach of importing sources one by one and deduplicating after the fact would have produced a graph where entity resolution errors compounded with every additional import. By seeding the graph with 329,231 concept clusters first, we established a broad anchor layer that subsequent imports could merge against rather than duplicate. The two-label strategy kept the graph clean: 67,912 entries received the WasteItem label after LLM classification, while the remaining vocabulary nodes gave the agent a fourth response state ("this is a recognized term but not something you'd dispose of") that the previous system could not express at all. The synonym, translation, and taxonomy edges, over 260,000 relationships connecting colloquial terms to canonical forms, German entries to English equivalents, and specific items to broader categories, are what transform a flat vocabulary list into a navigable structure. The entire pipeline is reproducible from scratch at negligible cost, which matters because it means the vocabulary foundation can be rebuilt as classification criteria improve rather than being a one-time artifact.

**Product decomposition chain.** The full traversal path from barcode to per-component disposal route (`Product → HAS_COMPONENT → PackagingComponent → MADE_OF → Material → DEFAULT_STREAM → WasteStream`) is operational in the graph. The Open Food Facts import populated this chain with 4.29 million products and 555,227 packaging components in 9 minutes of processing time. The barcode scanner module (pyzbar for detection, multi-API resolver for product lookup) has been built and tested against real products, demonstrating the end-to-end flow from photographing a barcode to receiving per-component disposal guidance. Integration as a live chatbot agent tool is planned but not yet deployed; the data infrastructure is ready.

**Infrastructure.** All services (Neo4j, JupyterLab, the chatbot application, and the ETL pipelines) run on a single shared workstation managed through rootless Podman containers and systemd Quadlet units. This co-location means that the graph built by the ETL pipelines is immediately available to the chatbot agent without any synchronization or data transfer step. Every import pipeline is idempotent (MERGE-based, safe to re-run), provenance-tracked (DERIVED_FROM edges to Source nodes), and reproducible from documented data sources. The dump import mechanism provides the transfer path for future deployments where data processing and the chatbot run on separate machines.

**Chatbot improvements.** The visible reasoning panel, SSE streaming architecture, reasoning model switch, and expanded toolset described in Section 2 served a dual purpose. For end users, they transformed an opaque 30-second wait into a transparent, observable process. For the project itself, they made the knowledge graph evaluable, since a chatbot that cannot tolerate multi-step tool use cannot meaningfully traverse a knowledge graph. The web search fallback proved particularly valuable during development: it allowed us to validate the agent's reasoning architecture and identify which data categories users would need most, while the import pipelines were still being built. The agent's failed graph queries became the requirements list for the data sourcing strategy described in the companion Data Mining documentation.

# 7. Future Work

The pipeline infrastructure, graph schema, and data sourcing catalog produced this semester provide a foundation that subsequent project teams can build on without starting from scratch. The schema supports all anticipated data categories without modification. The ETL and LLM extraction pipelines are source-agnostic and extensible. What remains is primarily data work: running existing pipelines against assessed sources, importing data that requires institutional access, and connecting the product layer to disposal knowledge. This section organizes the remaining work by effort and dependency.

## 7.1 Immediate Next Steps

Three tasks require no new infrastructure, no institutional partnerships, and no additional research. The pipelines exist and have been validated at proof-of-concept scale.

**Completing the AVV classification.** The hierarchical classification pipeline described in this document classified 500 WasteItem nodes against the AVV code hierarchy at a 99.8% success rate. The remaining ~67,000 WasteItem nodes can be processed with the same pipeline without modification. Classification quality will improve as the WasteItem population shifts from dictionary-promoted entries (which bias toward generic catch-all codes) to curated items from domain-specific sources.

**Completing the dictionary baseline classification.** The dictionary import pipeline produced 329,231 concept clusters, of which approximately 92,000 base forms require LLM classification to determine physical and disposal-relevant status. The classification pipeline, including inflection detection and resume support, has been built and tested. Running it at full scale is a single batch job.

**Municipal vocabulary expansion.** Berlin's BSR Open Data provides over 2,000 waste items with synonyms, avoidance tips, and recycling instructions under a CC BY license. Because the waste items themselves are universal across German cities, the BSR vocabulary can serve as a high-quality enrichment layer on top of the dictionary baseline, adding curated disposal knowledge to items that currently have only dictionary-level metadata. The import follows the same MERGE-based pattern as existing structured ETL pipelines.

## 7.2 Data Source Integration

The source evaluation catalog documents 68 assessed sources, of which nine have been imported. The following categories represent the highest-priority remaining integrations, ordered by expected impact.

**Municipal SaaS APIs.** The discovery that three platforms (Abfall+ (abfall.io), MyMüll (JumoMind), and Müllmax) consolidate most German municipal waste management is the single most actionable finding from the source research. Rather than building custom scrapers for each of Germany's 400+ districts, three API integrations would provide access to waste item vocabularies, sorting rules, collection schedules, and facility data for the majority of German university towns. The open-source Home Assistant integration (`hacs_waste_collection_schedule`) has already reverse-engineered these APIs and provides pre-configured tenant keys and municipality IDs for hundreds of cities, substantially reducing the integration effort. Imported municipal data would populate the disposal rules layer, currently the shallowest part of the graph, with city-specific sorting instructions and conditional logic.

**Chemical substance databases.** For a university with chemistry and biology laboratories, hazardous waste classification is a critical use case. GESTIS, the German occupational safety database maintained by the DGUV, provides 8,800 substance entries with storage classes, handling instructions, and regulatory references directly relevant to lab waste segregation. PubChem offers GHS hazard classifications for 111 million compounds, though the university context requires aggressive filtering to perhaps 500–2,000 common lab chemicals. The BAM dangerous goods database, which became freely accessible in July 2025, provides UN transport numbers and ADR classification codes for 4,500 substances. All three sources have been assessed and documented with access methods and format specifications.

**LLM extraction from unstructured documents.** The orchestrator pipeline, the four-stage LangGraph state machine for processing multi-entity documents, has been tested on individual documents but not yet run against the full corpus of sources identified during the research phase. Municipal waste guides, LAGA technical guidelines, university-specific chemical waste procedures, and AVV legal commentary documents remain as extraction targets. The pipeline infrastructure exists; the remaining work is prompt engineering for each document category and validation of extraction quality through dry-run iterations.

## 7.3 Institutional Partnerships

Two data gaps identified during the source evaluation cannot be closed through publicly available sources alone.

**University procurement catalogs.** The most significant data gap is the absence of an open database for everyday office and stationery items. German universities procure these items through framework contracts with B2B distributors who deliver product catalogs in the BMEcat XML format, a structured standard containing EAN barcodes, product descriptions, eCl@ss classification codes, manufacturer information, and links to Safety Data Sheets. These catalogs contain exactly the items the university purchases and needs to provide disposal guidance for. However, BMEcat catalogs are confidential B2B exchange documents, and access requires an institutional procurement account. A partnership with the university's procurement department (Vergabestelle) is the most practical path to obtaining this data. A BMEcat parser would follow the same pattern as the existing structured ETL pipelines: parse XML, map fields to graph properties, MERGE into Neo4j with provenance tracking. The EAN-based deduplication already built into the product layer means that items appearing in both a BMEcat catalog and Open Food Facts would merge into a single enriched Product node rather than creating duplicates.

**University-specific infrastructure data.** Campus bin locations, per-room container assignments, building-specific waste rules, and Schadstoffmobil schedules are not published in any open dataset. Previous semesters attempted to collect this data through manual surveying and interviews with facilities management, with mixed results. The graph schema already supports this data through its infrastructure layer (Building → Room → Container nodes with WasteStream typing), and the disposal rule override mechanism (campus-scoped rules superseding city-level defaults) is designed for exactly this use case. The remaining challenge is establishing a reliable data collection process, ideally a collaboration with the university's facilities management department that provides periodic updates rather than a one-time manual survey.

## 7.4 Connecting Products to Disposal Knowledge

The product layer currently contains 4.29 million Product nodes and 555,227 PackagingComponent nodes with material links, but these products are not yet connected to the disposal knowledge in the core layer. The barcode scanner can decompose a product into its packaging components and route each component to a waste stream via the material taxonomy, but it cannot yet answer "How do I dispose of the product itself?" (as opposed to its packaging) without this connection.

Bridging this gap requires matching products to WasteItem categories, determining, for instance, that a jar of Nutella maps to the WasteItem "Schokoladencreme" (Biotonne) while its glass jar maps to "Altglas" (Altglascontainer) via the packaging decomposition chain. This matching can be approached in two ways: deterministic matching via Open Food Facts category tags (which map product categories like "Chocolates" to waste categories), or LLM-based matching where a language model infers the appropriate WasteItem from the product name and category. The deterministic approach is faster and cheaper; the LLM approach handles edge cases better. Both can run against the existing product and WasteItem nodes without schema changes.

Broader product database integration, incorporating UPCitemdb for non-food products, retail product data for electronics and household items, would extend the barcode scanner's coverage beyond food and cosmetics. The EAN-based deduplication mechanism ensures that products appearing in multiple sources merge correctly.

## 7.5 Graph Quality and Maintenance

As additional data sources are imported, two maintenance tasks will become necessary.

**Material taxonomy extension.** Of the 555,227 packaging components imported from Open Food Facts, 5,213 (1.1%) reference material tags not present in the current 90-entry taxonomy. These were flagged during import rather than silently dropped. Extending the taxonomy to cover these tags and mapping them to waste streams is a bounded task that improves the precision of the packaging decomposition chain.

**Classification refinement.** The AVV classification and dictionary baseline classification pipelines are both designed to be re-run as input data improves. The current classification results are biased by the dictionary-promoted WasteItem population, which contains many entries that are technically physical objects but not meaningful waste items. As curated waste items from the BSR vocabulary, municipal APIs, and domain-specific databases enter the graph, re-running the classification pipelines will produce more specific and useful results. The provenance tracking built into every pipeline ensures that re-classified entities can be traced back to their source, and the MERGE-based idempotency means re-runs are safe.

---

<!-- TODO: Add references/bibliography if needed -->
<!-- TODO: Add appendices (e.g., full schema.cql, sample Cypher queries, configuration templates) if needed -->
