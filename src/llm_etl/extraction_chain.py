"""
Generic extraction chain with dynamic schema support.

The extraction chain is a stateless, reusable component that extracts
structured entities from unstructured text. The schema IS the prompt -
swap the Pydantic model to extract different entity types.

Usage:
    chain = ExtractionChain()
    items = chain.extract(
        text="Altöl muss bei der Schadstoffannahme abgegeben werden...",
        output_schema=WasteItemExtract,
        extraction_prompt="Extract all waste items with their disposal routes",
    )
"""

import logging
from typing import TypeVar

from langchain_core.prompts import ChatPromptTemplate
from pydantic import BaseModel

from src.llm_etl.config import get_llm

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)


class ExtractionResult(BaseModel):
    """Wrapper for extraction results to handle lists."""

    items: list[BaseModel] = []


class ExtractionChain:
    """Generic extraction chain with dynamic schema support."""

    DEFAULT_SYSTEM_PROMPT = """You are an expert data extraction system for German waste disposal information.
Your task is to extract structured entities from the provided text and return them as JSON.

Guidelines:
- Extract ALL relevant entities matching the requested type
- Preserve German names and terminology exactly as written
- If information is missing or unclear, omit that field rather than guessing
- Return an empty list if no relevant entities are found
- Be precise: only extract information explicitly stated in the text

{context}"""

    DEFAULT_EXTRACTION_PROMPT = """Extract all entities matching the schema from this text:

{text}

{instructions}"""

    def __init__(
        self,
        max_retries: int = 2,
        temperature: float = 0.0,
        structured_output_method: str = "json_mode",
    ):
        """
        Initialize the extraction chain.

        Args:
            max_retries: Number of retries for malformed outputs
            temperature: LLM sampling temperature (0.0 for deterministic)
            structured_output_method: Method for structured output extraction.
                "json_mode" (default) — works with all providers (Groq, OpenAI, local).
                "function_calling" — uses tool/function calling (OpenAI-native, may fail on Groq).
        """
        self.max_retries = max_retries
        self.temperature = temperature
        self.structured_output_method = structured_output_method
        self._llm = None

    @property
    def llm(self):
        """Lazy-load LLM to allow configuration after instantiation."""
        if self._llm is None:
            self._llm = get_llm(temperature=self.temperature)
        return self._llm

    def extract(
        self,
        text: str,
        output_schema: type[T],
        extraction_prompt: str = "",
        context: str = "",
    ) -> list[T]:
        """
        Extract entities matching the provided schema from text.

        Args:
            text: The text to extract entities from
            output_schema: Pydantic model defining the entity structure
            extraction_prompt: Additional instructions for extraction
            context: Related entities or hints (e.g., known facility names)

        Returns:
            List of extracted entities matching the schema
        """
        if not text.strip():
            logger.warning("Empty text provided for extraction")
            return []

        # Build the wrapper schema for list output
        wrapper_schema = self._create_wrapper_schema(output_schema)

        # Build the prompt
        prompt = self._build_prompt(text, extraction_prompt, context)

        # Get structured output with retries
        for attempt in range(self.max_retries + 1):
            try:
                structured_llm = self.llm.with_structured_output(
                    wrapper_schema, method=self.structured_output_method
                )
                result = structured_llm.invoke(prompt)

                if result is None:
                    logger.warning(f"Attempt {attempt + 1}: LLM returned None")
                    continue

                entities = result.items if hasattr(result, "items") else []
                logger.info(
                    f"Extracted {len(entities)} {output_schema.__name__} entities"
                )
                return entities

            except Exception as e:
                logger.warning(f"Attempt {attempt + 1} failed: {e}")
                if attempt == self.max_retries:
                    logger.error(f"All {self.max_retries + 1} attempts failed")
                    raise

        return []

    def extract_single(
        self,
        text: str,
        output_schema: type[T],
        extraction_prompt: str = "",
        context: str = "",
    ) -> T | None:
        """
        Extract a single entity from text.

        Convenience method when expecting exactly one entity.

        Args:
            text: The text to extract from
            output_schema: Pydantic model defining the entity structure
            extraction_prompt: Additional instructions for extraction
            context: Related entities or hints

        Returns:
            Single extracted entity or None if not found
        """
        if not text.strip():
            return None

        prompt = self._build_prompt(text, extraction_prompt, context)

        for attempt in range(self.max_retries + 1):
            try:
                structured_llm = self.llm.with_structured_output(
                    output_schema, method=self.structured_output_method
                )
                result = structured_llm.invoke(prompt)

                if result is not None:
                    logger.info(f"Extracted single {output_schema.__name__}")
                    return result

            except Exception as e:
                logger.warning(f"Attempt {attempt + 1} failed: {e}")
                if attempt == self.max_retries:
                    raise

        return None

    def _create_wrapper_schema(self, item_schema: type[T]) -> type[BaseModel]:
        """Create a wrapper schema for list extraction."""

        class ExtractionWrapper(BaseModel):
            items: list[item_schema] = []  # type: ignore

        ExtractionWrapper.__name__ = f"{item_schema.__name__}List"
        return ExtractionWrapper

    def _build_prompt(
        self, text: str, extraction_prompt: str, context: str
    ) -> list[tuple[str, str]]:
        """Build the prompt messages for LLM."""
        context_section = ""
        if context:
            context_section = f"\nContext (known entities, hints):\n{context}"

        instructions = extraction_prompt or "Extract all relevant entities."

        system_msg = self.DEFAULT_SYSTEM_PROMPT.format(context=context_section)
        user_msg = self.DEFAULT_EXTRACTION_PROMPT.format(
            text=text, instructions=instructions
        )

        return [("system", system_msg), ("human", user_msg)]
