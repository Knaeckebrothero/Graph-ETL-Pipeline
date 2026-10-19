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

---

> **Note:** The Data Mining variant of this final paragraph reads:
>
> This documentation is split across two companion documents reflecting the two modules under which this work was conducted. The present document covers the **Data Mining** aspects: LLM-based information extraction, schema-driven entity extraction, document analysis and chunking, the LangGraph orchestration pipeline, LLM classification for the dictionary baseline, and barcode-based product matching.
