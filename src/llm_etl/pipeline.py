"""
Unified Pipeline: text + prompt + settings → entities → optional DB insert.

Two modes:
- chain: Single LLM structured-output call. Fast, deterministic.
- agent: LangGraph ReAct loop exploring the graph before extracting. Thorough.

The notebook user handles source-specific logic (parsing PDFs, iterating CSVs).
The Pipeline handles everything that's the same across LLM ETL jobs.

Usage:
    from src.llm_etl import Pipeline, WasteItemExtract, SourceExtract

    pipeline = Pipeline(
        schema=WasteItemExtract,
        prompt="Extract waste items with disposal routes",
        mode="chain",
        load_to_db=True,
        source=SourceExtract(name="doc.pdf", file_type="pdf"),
    )

    for page_text in pages:
        result = pipeline.run(page_text)
        print(result.entities)
"""

import logging
from dataclasses import dataclass, field
from typing import Annotated, Any, TypedDict, TypeVar

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage
from langchain_core.tools import tool
from langgraph.graph import END, StateGraph
from langgraph.graph.message import add_messages
from langgraph.prebuilt import ToolNode
from pydantic import BaseModel

from src.llm_etl.config import get_llm
from src.llm_etl.extraction_chain import ExtractionChain
from src.llm_etl.generic_loader import GenericEntityLoader
from src.llm_etl.loader import EntityLoader, LoadResult
from src.llm_etl.schemas import SCHEMA_REGISTRY, SourceExtract, schema_to_prompt_hint
from src.llm_etl.tools.db_tools import (
    get_database_stats,
    get_entity_by_name,
    get_existing_entities,
)

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)


# =============================================================================
# Result Type
# =============================================================================


@dataclass
class PipelineResult:
    """Uniform return type from Pipeline.run()."""

    success: bool
    entities: list  # list[BaseModel] — extracted Pydantic instances
    load_result: LoadResult | None  # None if load_to_db=False
    messages: list[str]
    errors: list[str] = field(default_factory=list)


# =============================================================================
# Agent Tools (read-only graph exploration)
# =============================================================================


@tool
def search_entities(entity_type: str, limit: int = 20) -> str:
    """Search for existing entities of a given type in the knowledge graph.

    Args:
        entity_type: Node label (WasteItem, Facility, WasteStream, Product, Material, AVVCode)
        limit: Maximum results to return
    """
    names = get_existing_entities(entity_type, limit=limit)
    if not names:
        return f"No {entity_type} entities found in database."
    return f"Found {len(names)} {entity_type} entities: {', '.join(names[:limit])}"


@tool
def get_entity_details(entity_type: str, name: str) -> str:
    """Get detailed properties of a specific entity from the knowledge graph.

    Args:
        entity_type: Node label (WasteItem, Facility, WasteStream, etc.)
        name: Exact name of the entity
    """
    entity = get_entity_by_name(entity_type, name)
    if not entity:
        return f"{entity_type} '{name}' not found in database."
    props = "\n".join(f"  {k}: {v}" for k, v in entity.items())
    return f"{entity_type} '{name}':\n{props}"


@tool
def get_graph_stats() -> str:
    """Get current knowledge graph statistics (node counts by type, total relationships)."""
    stats = get_database_stats()
    lines = [
        f"  {label}: {count}"
        for label, count in stats.get("node_counts", {}).items()
    ]
    lines.append(f"  Relationships: {stats.get('relationship_count', 0)}")
    return "Graph statistics:\n" + "\n".join(lines)


@tool
def list_waste_streams() -> str:
    """List all waste streams (bin types) currently in the knowledge graph."""
    streams = get_existing_entities("WasteStream", limit=100)
    if not streams:
        return "No WasteStream entities found in database."
    return f"Waste streams: {', '.join(streams)}"


@tool
def list_facilities() -> str:
    """List all disposal facilities currently in the knowledge graph."""
    facilities = get_existing_entities("Facility", limit=100)
    if not facilities:
        return "No Facility entities found in database."
    return f"Facilities ({len(facilities)}): {', '.join(facilities)}"


AGENT_TOOLS = [
    search_entities,
    get_entity_details,
    get_graph_stats,
    list_waste_streams,
    list_facilities,
]


# =============================================================================
# Agent Graph
# =============================================================================

AGENT_SYSTEM_PROMPT = """You are a knowledge graph extraction agent for a German waste disposal chatbot.

Your task: Explore the existing knowledge graph to gather context, then the system will perform structured extraction from the provided text.

Available schema for extraction:
{schema_hint}

Extraction instructions:
{prompt}

Workflow:
1. Use your tools to explore what already exists in the graph (waste streams, facilities, existing entities).
2. Use this context to inform better extraction decisions (e.g., matching disposal_targets to existing entity names exactly).
3. When you have enough context, respond with a summary of your findings — what exists, what's relevant, what names to use.
4. The system will then perform the structured extraction using your gathered context.

IMPORTANT: Do NOT output the structured entities yourself. Gather context and summarize what you found. The extraction step handles structured output.

{context}"""


class AgentState(TypedDict):
    """State for the extraction agent graph."""

    messages: Annotated[list, add_messages]
    text: str
    schema_hint: str
    prompt: str
    context: str
    extracted_entities: list[dict]
    iteration_count: int
    max_iterations: int


def _summarize_agent_findings(messages: list) -> str:
    """Extract tool results from agent message history into a context string."""
    findings = []
    for msg in messages:
        if isinstance(msg, ToolMessage):
            findings.append(msg.content)
        elif isinstance(msg, AIMessage) and not getattr(msg, "tool_calls", None):
            # Agent's final summary message
            findings.append(msg.content)
    return "\n\n".join(findings) if findings else ""


def _build_agent_graph(
    schema: type[BaseModel],
    prompt: str,
    temperature: float,
    max_iterations: int,
) -> Any:
    """Build and compile the LangGraph ReAct agent for extraction."""
    llm = get_llm(temperature=temperature)
    llm_with_tools = llm.bind_tools(AGENT_TOOLS)
    chain = ExtractionChain(temperature=temperature)
    schema_hint = schema_to_prompt_hint(schema)

    def agent_node(state: dict) -> dict:
        """Call LLM with tools bound."""
        response = llm_with_tools.invoke(state["messages"])
        return {
            "messages": [response],
            "iteration_count": state.get("iteration_count", 0) + 1,
        }

    def should_continue(state: dict) -> str:
        """Route: tools if tool_calls present and under limit, else extract."""
        last_message = state["messages"][-1]
        has_tool_calls = (
            hasattr(last_message, "tool_calls") and last_message.tool_calls
        )
        at_limit = state.get("iteration_count", 0) >= state.get(
            "max_iterations", max_iterations
        )

        if has_tool_calls and not at_limit:
            return "tools"
        return "extract"

    tool_node = ToolNode(AGENT_TOOLS)

    def extract_node(state: dict) -> dict:
        """Perform structured extraction using agent's gathered context."""
        agent_context = _summarize_agent_findings(state["messages"])
        base_context = state.get("context", "")
        merged_context = f"{base_context}\n\nAgent findings:\n{agent_context}".strip()

        entities = chain.extract(
            text=state["text"],
            output_schema=schema,
            extraction_prompt=prompt,
            context=merged_context,
        )
        return {
            "extracted_entities": [e.model_dump() for e in entities],
        }

    # Build graph
    graph = StateGraph(AgentState)
    graph.add_node("agent", agent_node)
    graph.add_node("tools", tool_node)
    graph.add_node("extract", extract_node)

    graph.set_entry_point("agent")
    graph.add_conditional_edges(
        "agent",
        should_continue,
        {"tools": "tools", "extract": "extract"},
    )
    graph.add_edge("tools", "agent")
    graph.add_edge("extract", END)

    return graph.compile()


# =============================================================================
# Pipeline Class
# =============================================================================


class Pipeline:
    """
    Unified entry point: text + prompt + schema → entities → optional DB load.

    Args:
        schema: Pydantic model class defining what to extract.
        prompt: Extraction instructions for the LLM.
        mode: "chain" (single LLM call) or "agent" (ReAct loop + extraction).
        max_retries: Retry count for structured output (chain mode).
        temperature: LLM sampling temperature.
        max_iterations: Max agent tool-calling rounds (agent mode only).
        load_to_db: Whether to persist entities to Neo4j.
        dry_run: Simulate DB writes without executing them.
        source: Provenance metadata (required if load_to_db=True).
        context: Static context string passed to every run() call.
    """

    def __init__(
        self,
        schema: type[BaseModel],
        prompt: str = "",
        mode: str = "chain",
        max_retries: int = 2,
        temperature: float = 0.0,
        max_iterations: int = 10,
        load_to_db: bool = False,
        dry_run: bool = False,
        source: SourceExtract | None = None,
        context: str = "",
    ):
        if mode not in ("chain", "agent"):
            raise ValueError(f"Invalid mode: '{mode}'. Must be 'chain' or 'agent'.")

        self.schema = schema
        self.prompt = prompt
        self.mode = mode
        self.max_retries = max_retries
        self.temperature = temperature
        self.max_iterations = max_iterations
        self.load_to_db = load_to_db
        self.dry_run = dry_run
        self.context = context

        # Source handling
        if load_to_db and source is None:
            logger.warning("load_to_db=True but no source provided. Using default.")
            self.source = SourceExtract(
                name="pipeline", file_type="unknown", file_path=""
            )
        else:
            self.source = source

        # Determine loader type at init time
        self._use_generic_loader = schema not in SCHEMA_REGISTRY.values()

        # Lazy-init backing fields
        self.__chain: ExtractionChain | None = None
        self.__loader: EntityLoader | None = None
        self.__generic_loader: GenericEntityLoader | None = None
        self.__agent_graph = None

    # -- Lazy properties -------------------------------------------------------

    @property
    def _chain(self) -> ExtractionChain:
        if self.__chain is None:
            self.__chain = ExtractionChain(
                max_retries=self.max_retries,
                temperature=self.temperature,
            )
        return self.__chain

    @property
    def _loader(self) -> EntityLoader:
        if self.__loader is None:
            self.__loader = EntityLoader()
        return self.__loader

    @property
    def _generic_loader(self) -> GenericEntityLoader:
        if self.__generic_loader is None:
            self.__generic_loader = GenericEntityLoader()
        return self.__generic_loader

    @property
    def _agent_graph(self):
        if self.__agent_graph is None:
            self.__agent_graph = _build_agent_graph(
                schema=self.schema,
                prompt=self.prompt,
                temperature=self.temperature,
                max_iterations=self.max_iterations,
            )
        return self.__agent_graph

    # -- Public API ------------------------------------------------------------

    def run(self, text: str, context: str = "") -> PipelineResult:
        """
        Run the pipeline on a single text passage.

        Args:
            text: The text to extract entities from.
            context: Per-call context (merged with constructor context).

        Returns:
            PipelineResult with entities and optional load result.
        """
        if not text or not text.strip():
            return PipelineResult(
                success=True,
                entities=[],
                load_result=None,
                messages=["Empty text, skipping extraction."],
            )

        merged_context = self._merge_context(context)

        try:
            if self.mode == "chain":
                return self._run_chain(text, merged_context)
            else:
                return self._run_agent(text, merged_context)
        except Exception as e:
            logger.error(f"Pipeline run failed: {e}")
            return PipelineResult(
                success=False,
                entities=[],
                load_result=None,
                messages=[f"Pipeline error: {e}"],
                errors=[str(e)],
            )

    # -- Chain mode ------------------------------------------------------------

    def _run_chain(self, text: str, context: str) -> PipelineResult:
        """Direct extraction via ExtractionChain."""
        messages = []

        entities = self._chain.extract(
            text=text,
            output_schema=self.schema,
            extraction_prompt=self.prompt,
            context=context,
        )
        messages.append(f"Extracted {len(entities)} {self.schema.__name__} entities.")

        load_result = None
        if self.load_to_db and self.source:
            load_result = self._load_entities(entities)
            messages.append(
                f"Loaded: {load_result.entities_created} created, "
                f"{load_result.entities_updated} updated, "
                f"{load_result.entities_skipped} skipped."
            )

        return PipelineResult(
            success=True,
            entities=entities,
            load_result=load_result,
            messages=messages,
        )

    # -- Agent mode ------------------------------------------------------------

    def _run_agent(self, text: str, context: str) -> PipelineResult:
        """Agent explores graph, then ExtractionChain extracts."""
        messages = []

        schema_hint = schema_to_prompt_hint(self.schema)
        system_prompt = AGENT_SYSTEM_PROMPT.format(
            schema_hint=schema_hint,
            prompt=self.prompt or "Extract all relevant entities.",
            context=f"Additional context:\n{context}" if context else "",
        )

        initial_state = {
            "messages": [
                SystemMessage(content=system_prompt),
                HumanMessage(content=f"Text to extract from:\n\n{text}"),
            ],
            "text": text,
            "schema_hint": schema_hint,
            "prompt": self.prompt,
            "context": context,
            "extracted_entities": [],
            "iteration_count": 0,
            "max_iterations": self.max_iterations,
        }

        # Run agent graph
        final_state = self._agent_graph.invoke(
            initial_state,
            config={"recursion_limit": self.max_iterations * 2 + 5},
        )

        # Reconstruct entities from dicts
        entities = []
        for entity_dict in final_state.get("extracted_entities", []):
            try:
                entity = self.schema(**entity_dict)
                entities.append(entity)
            except Exception as e:
                logger.warning(f"Failed to reconstruct entity: {e}")

        # Count agent iterations
        iteration_count = final_state.get("iteration_count", 0)
        messages.append(
            f"Agent completed in {iteration_count} iterations, "
            f"extracted {len(entities)} {self.schema.__name__} entities."
        )

        load_result = None
        if self.load_to_db and self.source:
            load_result = self._load_entities(entities)
            messages.append(
                f"Loaded: {load_result.entities_created} created, "
                f"{load_result.entities_updated} updated, "
                f"{load_result.entities_skipped} skipped."
            )

        return PipelineResult(
            success=True,
            entities=entities,
            load_result=load_result,
            messages=messages,
        )

    # -- Loading ---------------------------------------------------------------

    def _load_entities(self, entities: list[BaseModel]) -> LoadResult:
        """Load entities via the appropriate loader."""
        if self._use_generic_loader:
            return self._generic_loader.load(entities, self.source, self.dry_run)
        return self._loader.load(entities, self.source, self.dry_run)

    # -- Helpers ---------------------------------------------------------------

    def _merge_context(self, per_call_context: str) -> str:
        """Merge static constructor context with per-call context."""
        parts = [p for p in (self.context, per_call_context) if p and p.strip()]
        return "\n\n".join(parts)
