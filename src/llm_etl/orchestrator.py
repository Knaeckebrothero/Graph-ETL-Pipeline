"""
LangGraph-based orchestrator for document processing.

The orchestrator is the intelligence layer that:
1. Analyzes documents to determine entity types
2. Creates extraction plans
3. Delegates to extraction chains
4. Validates against the database
5. Optionally enriches with web search

Usage:
    orchestrator = Orchestrator()
    result = orchestrator.process(
        document="...",
        file_path="data/document.txt",
    )
"""

import logging
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Annotated, Any, TypedDict

from langgraph.graph import END, StateGraph
from pydantic import BaseModel

from src.llm_etl.config import get_llm
from src.llm_etl.extraction_chain import ExtractionChain
from src.llm_etl.schemas import SCHEMA_REGISTRY, get_schema
from src.llm_etl.tools.db_tools import (
    find_gaps,
    get_database_stats,
    get_existing_entities,
    validate_extraction,
)
from src.llm_etl.tools.doc_tools import (
    Chunk,
    ChunkStrategy,
    chunk_document,
    detect_document_type,
    estimate_entity_types,
    get_document_stats,
)

logger = logging.getLogger(__name__)


# =============================================================================
# State Definition
# =============================================================================


class ProcessingStatus(str, Enum):
    """Current processing status."""

    PENDING = "pending"
    ANALYZING = "analyzing"
    PLANNING = "planning"
    EXTRACTING = "extracting"
    VALIDATING = "validating"
    COMPLETE = "complete"
    ERROR = "error"


@dataclass
class ExtractionPlanStep:
    """A step in the extraction plan."""

    schema_name: str
    chunk_indices: list[int]
    prompt: str = ""
    context: str = ""
    priority: int = 0


@dataclass
class ExtractedEntity:
    """An extracted entity with metadata."""

    schema_name: str
    entity: BaseModel
    chunk_index: int
    confidence: float = 1.0
    validation_result: dict = field(default_factory=dict)


class OrchestratorState(TypedDict):
    """State for the orchestrator graph."""

    # Input
    document: str
    file_path: str
    file_type: str

    # Analysis
    document_stats: dict
    detected_entity_types: list[str]
    chunks: list[dict]  # Serialized Chunk objects

    # Planning
    extraction_plan: list[dict]  # Serialized ExtractionPlanStep objects
    current_step_index: int

    # Extraction
    extracted_entities: list[dict]  # Serialized ExtractedEntity objects

    # Validation
    validation_summary: dict
    db_gaps: dict

    # Control
    status: str
    error: str
    messages: list[str]


# =============================================================================
# Graph Nodes
# =============================================================================


def analyze_document(state: OrchestratorState) -> OrchestratorState:
    """
    Analyze the document to determine its structure and likely content.

    This node:
    - Computes document statistics
    - Estimates entity types present
    - Prepares chunks for extraction
    """
    logger.info("Analyzing document...")
    messages = list(state.get("messages", []))
    document = state["document"]

    try:
        # Get document statistics
        stats = get_document_stats(document)
        messages.append(f"Document: {stats['words']} words, {stats['paragraphs']} paragraphs")

        # Detect entity types
        entity_types = estimate_entity_types(document)
        messages.append(f"Detected entity types: {', '.join(entity_types)}")

        # Chunk document
        chunks = chunk_document(
            document,
            strategy=ChunkStrategy.PARAGRAPH,
            max_chunk_size=2000,
        )
        messages.append(f"Created {len(chunks)} chunks")

        # Serialize chunks for state
        serialized_chunks = [
            {
                "content": c.content,
                "index": c.index,
                "start_char": c.start_char,
                "end_char": c.end_char,
                "metadata": c.metadata,
            }
            for c in chunks
        ]

        return {
            **state,
            "document_stats": stats,
            "detected_entity_types": entity_types,
            "chunks": serialized_chunks,
            "status": ProcessingStatus.PLANNING.value,
            "messages": messages,
        }

    except Exception as e:
        logger.error(f"Document analysis failed: {e}")
        return {
            **state,
            "status": ProcessingStatus.ERROR.value,
            "error": str(e),
            "messages": messages + [f"Analysis error: {e}"],
        }


def create_extraction_plan(state: OrchestratorState) -> OrchestratorState:
    """
    Create an extraction plan based on document analysis.

    This node creates a sequence of extraction steps, each specifying:
    - Which schema to use
    - Which chunks to process
    - Additional context/prompts
    """
    logger.info("Creating extraction plan...")
    messages = list(state.get("messages", []))

    try:
        entity_types = state.get("detected_entity_types", ["WasteItem"])
        chunks = state.get("chunks", [])

        # Get existing entities for context
        db_context = {}
        for entity_type in entity_types:
            if entity_type in SCHEMA_REGISTRY:
                existing = get_existing_entities(entity_type, limit=100)
                if existing:
                    db_context[entity_type] = existing

        # Create extraction plan
        plan = []
        all_chunk_indices = list(range(len(chunks)))

        for entity_type in entity_types:
            if entity_type not in SCHEMA_REGISTRY:
                messages.append(f"Skipping unknown entity type: {entity_type}")
                continue

            # Build context from existing entities
            context = ""
            if entity_type in db_context:
                existing_names = db_context[entity_type][:20]
                context = f"Known {entity_type}s in database: {', '.join(existing_names)}"

            step = ExtractionPlanStep(
                schema_name=entity_type,
                chunk_indices=all_chunk_indices,
                prompt=f"Extract all {entity_type} entities from the text.",
                context=context,
                priority=0 if entity_type == "WasteItem" else 1,
            )

            plan.append(
                {
                    "schema_name": step.schema_name,
                    "chunk_indices": step.chunk_indices,
                    "prompt": step.prompt,
                    "context": step.context,
                    "priority": step.priority,
                }
            )

        # Sort by priority
        plan.sort(key=lambda x: x["priority"])

        messages.append(f"Created extraction plan with {len(plan)} steps")
        for step in plan:
            messages.append(f"  - {step['schema_name']}: {len(step['chunk_indices'])} chunks")

        return {
            **state,
            "extraction_plan": plan,
            "current_step_index": 0,
            "status": ProcessingStatus.EXTRACTING.value,
            "messages": messages,
        }

    except Exception as e:
        logger.error(f"Plan creation failed: {e}")
        return {
            **state,
            "status": ProcessingStatus.ERROR.value,
            "error": str(e),
            "messages": messages + [f"Planning error: {e}"],
        }


def run_extraction(state: OrchestratorState) -> OrchestratorState:
    """
    Execute the extraction plan.

    Runs extraction chain for each step in the plan.
    """
    logger.info("Running extraction...")
    messages = list(state.get("messages", []))

    try:
        plan = state.get("extraction_plan", [])
        chunks = state.get("chunks", [])
        current_index = state.get("current_step_index", 0)
        all_extracted = list(state.get("extracted_entities", []))

        if current_index >= len(plan):
            messages.append("Extraction complete - no more steps")
            return {
                **state,
                "status": ProcessingStatus.VALIDATING.value,
                "messages": messages,
            }

        step = plan[current_index]
        schema_name = step["schema_name"]
        chunk_indices = step["chunk_indices"]

        messages.append(f"Executing step {current_index + 1}/{len(plan)}: {schema_name}")

        # Get schema class
        schema_class = get_schema(schema_name)
        if not schema_class:
            messages.append(f"Schema not found: {schema_name}")
            return {
                **state,
                "current_step_index": current_index + 1,
                "messages": messages,
            }

        # Initialize extraction chain
        chain = ExtractionChain()

        # Process each chunk
        step_entities = []
        for chunk_idx in chunk_indices:
            if chunk_idx >= len(chunks):
                continue

            chunk = chunks[chunk_idx]
            chunk_text = chunk["content"]

            try:
                entities = chain.extract(
                    text=chunk_text,
                    output_schema=schema_class,
                    extraction_prompt=step.get("prompt", ""),
                    context=step.get("context", ""),
                )

                for entity in entities:
                    extracted = {
                        "schema_name": schema_name,
                        "entity": entity.model_dump(),
                        "chunk_index": chunk_idx,
                        "confidence": 1.0,
                        "validation_result": {},
                    }
                    step_entities.append(extracted)

            except Exception as e:
                logger.warning(f"Extraction failed for chunk {chunk_idx}: {e}")
                messages.append(f"Chunk {chunk_idx} extraction error: {e}")

        all_extracted.extend(step_entities)
        messages.append(f"Extracted {len(step_entities)} {schema_name} entities")

        # Check if more steps
        next_index = current_index + 1
        next_status = (
            ProcessingStatus.EXTRACTING.value
            if next_index < len(plan)
            else ProcessingStatus.VALIDATING.value
        )

        return {
            **state,
            "extracted_entities": all_extracted,
            "current_step_index": next_index,
            "status": next_status,
            "messages": messages,
        }

    except Exception as e:
        logger.error(f"Extraction failed: {e}")
        return {
            **state,
            "status": ProcessingStatus.ERROR.value,
            "error": str(e),
            "messages": messages + [f"Extraction error: {e}"],
        }


def validate_results(state: OrchestratorState) -> OrchestratorState:
    """
    Validate extracted entities against database.

    Checks for:
    - Required fields
    - Referenced entities existence
    - Duplicates
    """
    logger.info("Validating results...")
    messages = list(state.get("messages", []))

    try:
        extracted = state.get("extracted_entities", [])

        if not extracted:
            messages.append("No entities to validate")
            return {
                **state,
                "validation_summary": {"total": 0, "valid": 0, "warnings": 0, "errors": 0},
                "status": ProcessingStatus.COMPLETE.value,
                "messages": messages,
            }

        # Validate each entity
        valid_count = 0
        warning_count = 0
        error_count = 0
        validated_entities = []

        for item in extracted:
            schema_name = item["schema_name"]
            entity_data = item["entity"]

            # Reconstruct entity for validation
            schema_class = get_schema(schema_name)
            if not schema_class:
                continue

            try:
                entity = schema_class(**entity_data)
                result = validate_extraction(entity, schema_name)

                item["validation_result"] = result
                validated_entities.append(item)

                if result["valid"]:
                    valid_count += 1
                if result["warnings"]:
                    warning_count += len(result["warnings"])
                if result["errors"]:
                    error_count += len(result["errors"])

            except Exception as e:
                logger.warning(f"Validation failed for {schema_name}: {e}")
                error_count += 1

        # Find gaps in database
        db_gaps = {}
        for schema_name in set(item["schema_name"] for item in extracted):
            names = [
                item["entity"].get("name", "")
                for item in extracted
                if item["schema_name"] == schema_name
            ]
            names = [n for n in names if n]
            if names:
                gaps = find_gaps(schema_name, names)
                db_gaps[schema_name] = gaps

        summary = {
            "total": len(extracted),
            "valid": valid_count,
            "warnings": warning_count,
            "errors": error_count,
        }

        messages.append(
            f"Validation: {valid_count}/{len(extracted)} valid, "
            f"{warning_count} warnings, {error_count} errors"
        )

        # Log gap analysis
        for schema, gaps in db_gaps.items():
            new_count = len(gaps.get("new", []))
            existing_count = len(gaps.get("existing", []))
            messages.append(f"{schema}: {new_count} new, {existing_count} existing in DB")

        return {
            **state,
            "extracted_entities": validated_entities,
            "validation_summary": summary,
            "db_gaps": db_gaps,
            "status": ProcessingStatus.COMPLETE.value,
            "messages": messages,
        }

    except Exception as e:
        logger.error(f"Validation failed: {e}")
        return {
            **state,
            "status": ProcessingStatus.ERROR.value,
            "error": str(e),
            "messages": messages + [f"Validation error: {e}"],
        }


# =============================================================================
# Graph Construction
# =============================================================================


def should_continue_extraction(state: OrchestratorState) -> str:
    """Determine if extraction should continue or move to validation."""
    status = state.get("status", "")

    if status == ProcessingStatus.ERROR.value:
        return "end"
    elif status == ProcessingStatus.EXTRACTING.value:
        return "extract"
    elif status == ProcessingStatus.VALIDATING.value:
        return "validate"
    else:
        return "end"


def build_orchestrator_graph() -> StateGraph:
    """Build the LangGraph state machine for orchestration."""

    # Create graph
    graph = StateGraph(OrchestratorState)

    # Add nodes
    graph.add_node("analyze", analyze_document)
    graph.add_node("plan", create_extraction_plan)
    graph.add_node("extract", run_extraction)
    graph.add_node("validate", validate_results)

    # Add edges
    graph.set_entry_point("analyze")
    graph.add_edge("analyze", "plan")
    graph.add_edge("plan", "extract")

    # Conditional edge for extraction loop
    graph.add_conditional_edges(
        "extract",
        should_continue_extraction,
        {
            "extract": "extract",
            "validate": "validate",
            "end": END,
        },
    )

    graph.add_edge("validate", END)

    return graph


# =============================================================================
# Orchestrator Class
# =============================================================================


@dataclass
class OrchestratorResult:
    """Result of orchestrator processing."""

    success: bool
    extracted_entities: list[dict]
    validation_summary: dict
    db_gaps: dict
    messages: list[str]
    error: str = ""


class Orchestrator:
    """
    LangGraph-based document processing orchestrator.

    Usage:
        orchestrator = Orchestrator()
        result = orchestrator.process(
            document="...",
            file_path="data/doc.txt",
        )
    """

    def __init__(self):
        """Initialize the orchestrator."""
        self._graph = None

    @property
    def graph(self):
        """Lazy-build the graph."""
        if self._graph is None:
            builder = build_orchestrator_graph()
            self._graph = builder.compile()
        return self._graph

    def process(
        self,
        document: str,
        file_path: str = "",
        file_type: str = "",
    ) -> OrchestratorResult:
        """
        Process a document through the extraction pipeline.

        Args:
            document: Document text content
            file_path: Optional path to source file
            file_type: Optional file type override

        Returns:
            OrchestratorResult with extracted entities and metadata
        """
        logger.info(f"Processing document: {file_path or '(inline text)'}")

        # Detect file type if not provided
        if not file_type and file_path:
            file_type = detect_document_type(file_path).value

        # Initial state
        initial_state: OrchestratorState = {
            "document": document,
            "file_path": file_path,
            "file_type": file_type,
            "document_stats": {},
            "detected_entity_types": [],
            "chunks": [],
            "extraction_plan": [],
            "current_step_index": 0,
            "extracted_entities": [],
            "validation_summary": {},
            "db_gaps": {},
            "status": ProcessingStatus.ANALYZING.value,
            "error": "",
            "messages": [f"Starting processing: {file_path or 'inline document'}"],
        }

        # Run the graph
        try:
            final_state = self.graph.invoke(initial_state)

            success = final_state.get("status") == ProcessingStatus.COMPLETE.value

            return OrchestratorResult(
                success=success,
                extracted_entities=final_state.get("extracted_entities", []),
                validation_summary=final_state.get("validation_summary", {}),
                db_gaps=final_state.get("db_gaps", {}),
                messages=final_state.get("messages", []),
                error=final_state.get("error", ""),
            )

        except Exception as e:
            logger.error(f"Orchestrator failed: {e}")
            return OrchestratorResult(
                success=False,
                extracted_entities=[],
                validation_summary={},
                db_gaps={},
                messages=[f"Orchestrator error: {e}"],
                error=str(e),
            )

    def process_file(self, file_path: str | Path) -> OrchestratorResult:
        """
        Process a file through the extraction pipeline.

        Args:
            file_path: Path to the file to process

        Returns:
            OrchestratorResult with extracted entities and metadata
        """
        path = Path(file_path)

        if not path.exists():
            return OrchestratorResult(
                success=False,
                extracted_entities=[],
                validation_summary={},
                db_gaps={},
                messages=[f"File not found: {file_path}"],
                error=f"File not found: {file_path}",
            )

        # Read file
        try:
            document = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            document = path.read_text(encoding="latin-1")

        return self.process(
            document=document,
            file_path=str(path),
        )
