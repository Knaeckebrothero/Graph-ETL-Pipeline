# 2. Cleaning Up the Implementation

Given the initial situation described in Section 1.1 — an agent constrained by inherited guardrails, response times of up to 30 seconds with no user feedback, and a knowledge base too sparse to answer most queries — we concluded that incremental improvements to the existing system would not be sufficient. Instead, we decided to rebuild the chatbot's core interaction model from scratch. The guiding insight was that the very behavior which had rendered the chatbot "unusable" — its tendency to make multiple tool calls and reason through a problem step by step — was not a defect to be suppressed, but a capability to be surfaced. The previous implementation had hidden this process behind a blank screen; our task was to make it visible.

## 2.1 Usability Aspects of Agentic Chatbots

### The Feedback Problem

The original chatbot suffered from what is, in retrospect, a straightforward usability failure rather than a performance problem. As described in Section 1.1, the agent's fixed pipeline of vector searches and evaluations could take 20 to 30 seconds per query. During this entire period, the user interface displayed nothing: no progress indicator, no status message, no evidence that the system was working at all. From the user's perspective, the chatbot appeared to have frozen.

Our initial assumption was that reducing response time would solve this. After working with the system, however, we came to a different conclusion: the wait itself was not the core issue. Users routinely wait 10 to 30 seconds for complex queries on modern AI interfaces — ChatGPT, Gemini, and Perplexity all exhibit similar latencies for multi-step reasoning tasks. What those systems provide, and what our chatbot lacked, was *visibility into the process*. A user who can see that the agent is "Searching the knowledge base for Batterien" and then "Exploring waste categories" perceives an active system working on their behalf. A user staring at a blank screen perceives a broken one.

### Visible Reasoning: The Thought Process Panel

Inspired by how Google's Gemini displays an expandable summary of its internal reasoning steps, we implemented a "Thought process" panel that renders the agent's actions in real time as collapsible accordion elements. Each action the agent takes — thinking, calling a tool, receiving a result — appears as a distinct step in a vertical timeline, color-coded by type:

- **Thought steps** (purple) represent the agent's internal reasoning: "Analyzing request..." These appear whenever the language model is invoked to decide what to do next.
- **Tool calls** (orange) show which tool the agent is invoking and with what parameters. For example, a knowledge base search displays the JSON input `{"waste_item": "Batterien"}`, making the agent's search strategy transparent.
- **Tool results** (green) display the outcome of each tool call, including the execution duration. Users can expand these to inspect the raw data the agent received.
- **Observations** (cyan) appear when the agent synthesizes intermediate findings before deciding on its next action.

The panel header shows a summary — "Thought process (20 steps)" — along with a status indicator: a spinner while the agent is still working, a checkmark once it has finished. The entire panel is collapsible: users who want the quick answer can ignore it; users who want to understand *how* the agent arrived at that answer can expand individual steps and trace the full reasoning chain.

This design serves two purposes simultaneously. For the end user, it transforms an opaque wait into an observable process. For developers and domain experts, it provides a debugging and evaluation interface — one can see exactly which tools the agent called, what data it received, and where it may have gone wrong.

### Streaming Architecture

To deliver reasoning steps and response text incrementally rather than as a single payload after completion, we replaced the chatbot's request-response communication model with Server-Sent Events (SSE). The backend streams four event types to the frontend:

1. **`step`** events carry individual agent actions (thought, tool call, tool result) as they occur. Each step includes a type, title, content, timestamp, and — for tool results — execution duration and metadata.
2. **`token`** events carry individual text tokens of the final response, enabling the familiar character-by-character typing effect.
3. **`message_start`** provides metadata (message ID, conversation ID, timestamp) at the beginning of the stream.
4. **`done`** signals completion so the frontend can finalize the message.

On the backend, the FastAPI endpoint hooks into LangGraph's event system. Each LangGraph callback — `on_chat_model_start`, `on_tool_start`, `on_tool_end`, `on_chat_model_stream` — maps to an SSE event that is pushed to the client immediately. On the frontend, an Angular streaming service consumes these events using the native `fetch` API with an `AbortController` for cancellation support, parsing the SSE protocol and routing each event type to the appropriate UI component.

The result is that the first reasoning step appears within one to two seconds of the user submitting a query. Even when the full response takes 30 seconds or more, the user sees continuous activity from the moment they press send.

## 2.2 Extending the Agent's Toolset

With the usability foundation in place — users could now observe and tolerate multi-step reasoning — we turned to expanding what the agent could actually do. The inherited agent ran on GPT-4o and had access to a single tool: vector similarity search over the knowledge base. We changed the underlying model and added four new tools.

### Switching to a Reasoning Model

We replaced GPT-4o with a reasoning model (GPT-OSS-120B). The motivation was not raw capability but behavioral: reasoning models engage in explicit chain-of-thought before producing an output, which fundamentally changes how they interact with tools. Where GPT-4o required careful prompt engineering to follow a multi-step tool-use strategy, the reasoning model autonomously plans its approach — deciding which tools to call, in what order, and how to synthesize the results — with minimal instruction.

This had a direct consequence for the guardrail problem. The previous system needed a rigid pipeline (search, evaluate, search again, respond) precisely because the non-reasoning model could not reliably decide for itself what to do next. With a reasoning model, we could remove these guardrails entirely and let the agent determine its own strategy for each query. In practice, we observed the agent performing up to 30 tool calls on complex queries — searching the knowledge base, refining its search terms based on initial results, cross-referencing facilities, and only then formulating a response. This behavior would have been impossible under the old fixed-step pipeline and prohibitively slow without the streaming architecture described above.

The system supports configurable reasoning levels (low, medium, high) via environment variables, allowing the deployment to balance thoroughness against latency depending on the use case.

### Knowledge Graph Tools

We added tools that allow the agent to query and navigate the Neo4j knowledge graph directly. Rather than relying solely on vector similarity search — which returns text chunks ranked by embedding distance — these tools let the agent execute structured queries: looking up a waste item by name, retrieving its disposal rules and associated conditions, exploring connected facilities with their addresses and opening hours, and traversing the category hierarchy. This is the mechanism through which Graph-RAG delivers its advantage over vector-RAG: the agent does not receive a list of similar documents, but traverses a structured network of relationships.

### Web Search

We integrated web search via the Tavily API as a fallback knowledge source. When the knowledge base does not contain the information the agent needs — which, given the data scarcity described in Section 1.1, was frequently the case — the agent can issue targeted web queries and incorporate the results into its response.

This was initially intended as a stopgap to compensate for the sparse knowledge base. In practice, it proved more valuable than expected: it allowed us to test and refine the agent's reasoning capabilities without being blocked by missing data. We could observe how the agent narrowed its search queries progressively — starting broad, then targeting specific details it had identified as missing — which closely mirrors how it would navigate a well-populated knowledge graph. When asked about disposing of hydrochloric acid in Bremen, for instance, the agent issued three successive web searches: first for general chemical disposal regulations, then for the specific hazardous waste collection service, and finally for the address and opening hours of the nearest collection point. This iterative refinement pattern reinforced our confidence in the graph-based architecture, since it demonstrated that the agent could effectively navigate a structured information landscape to assemble a complete answer from multiple targeted queries.

### Vision Helper

The reasoning model we selected (GPT-OSS-120B) is not multimodal — it cannot process images directly. Since the chatbot's frontend supports image uploads (a capability built in the previous semester), we implemented a vision helper service that delegates image analysis to a separate multimodal model. When the agent receives a message containing an image, the vision helper sends it to a dedicated vision model (configurable, defaulting to GPT-4o-mini) which generates a text description. The agent then reasons about this description using its primary model.

This architecture decouples the choice of reasoning model from the requirement for multimodal input. It also enables deployment on heterogeneous hardware: in our development environment, we ran the vision model (Qwen2.5-VL 7B) on one GPU and the reasoning model on another, with audio transcription (via faster-whisper) on a third.

### EAN Barcode Scanner

Building on the vision helper, we developed a barcode scanning module that allows users to photograph product barcodes for disposal guidance. The module uses pyzbar (a Python wrapper around the ZBar library) for barcode detection and decoding — a deterministic computer vision step, not an LLM call. The extracted EAN number is then passed to a multi-API resolver that queries product databases in a cascading fallback sequence: Open Food Facts, Open Beauty Facts, UPCitemdb, and optionally OpenGTINDB. The first API to return a match provides the product data, including — critically — a breakdown of packaging components with their materials.

This capability transforms a single user action (photographing a product) into per-component disposal routing. A jar of Nutella, for instance, decomposes into a glass jar (Altglascontainer), a polypropylene lid (Verpackungstonne), and a paper label (Altpapiertonne). The barcode pipeline and the underlying product-to-material-to-waste-stream graph traversal are described in detail in the EAN barcode pipeline section later in this document. The integration into the live chatbot as an agent tool is planned but not yet deployed.

## 2.3 The Impact of Unconstrained Reasoning

The combination of visible reasoning, streaming, and an expanded toolset produced a qualitative shift in the chatbot's behavior. Under the old system, the agent followed a script: search, evaluate, search, respond. Under the new system, the agent adapts to the query. A simple factual question ("What goes in the Biotonne?") might require only a single knowledge base lookup. A complex conditional question ("How do I dispose of a half-full paint can?") triggers a multi-step investigation: searching for the item, identifying the relevant condition, checking both branches of the decision tree, and locating the appropriate facility. A question that falls outside the knowledge base's coverage might prompt the agent to fall back to web search, querying multiple sources and synthesizing the results into a structured answer.

This flexibility came not from more complex prompt engineering, but from *less*. The reasoning model's internal chain-of-thought replaced the external guardrails that had previously been necessary to guide a non-reasoning model through multi-step tasks. With fewer constraints, the agent could recover gracefully from dead ends — for instance, falling back to web search when a graph query returned no results — rather than failing silently at a predetermined step in a fixed pipeline.

The visible reasoning panel made this adaptive behavior legible to both users and developers. Where the old chatbot was a black box that either returned an answer or did not, the new system exposes its entire decision-making process. A user asking about battery disposal can watch the agent search for "Batterien," discover that batteries are classified as hazardous waste, look up the campus hazardous waste collection point, retrieve its address and opening hours, and compose a response that synthesizes all of this into actionable guidance. The answer takes longer to produce, but the user understands why — and trusts the result more because they witnessed the reasoning behind it.
