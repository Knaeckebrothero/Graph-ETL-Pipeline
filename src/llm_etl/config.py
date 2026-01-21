"""
LLM configuration for the extraction pipeline.

Configures OpenAI-compatible endpoint via environment variables.
When LLM_BASE_URL is set, it overrides the default OpenAI endpoint
to use a local llama.cpp server or other compatible API.
"""

import os
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI

load_dotenv()

# LLM Configuration
LLM_API_KEY = os.getenv("LLM_API_KEY", "")
LLM_BASE_URL = os.getenv("LLM_BASE_URL", "")
LLM_MODEL = os.getenv("LLM_MODEL", "gpt-4o")


def get_llm(temperature: float = 0.0) -> ChatOpenAI:
    """
    Get configured LLM instance.

    Uses OpenAI-compatible endpoint when LLM_BASE_URL is set,
    otherwise falls back to OpenAI API.

    Args:
        temperature: Sampling temperature (0.0 for deterministic extraction)

    Returns:
        Configured ChatOpenAI instance
    """
    kwargs = {
        "model": LLM_MODEL,
        "temperature": temperature,
        "api_key": LLM_API_KEY or "not-needed",
    }

    # Override base URL for local llama.cpp server or other compatible APIs
    if LLM_BASE_URL:
        kwargs["base_url"] = LLM_BASE_URL

    return ChatOpenAI(**kwargs)
