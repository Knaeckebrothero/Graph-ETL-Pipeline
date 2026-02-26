"""
LLM configuration for the extraction pipeline.

Supports multiple providers via LLM_PROVIDER env var:
  - "openai" (default): Uses ChatOpenAI. Set LLM_BASE_URL to override
    the endpoint for local llama.cpp or other OpenAI-compatible APIs.
  - "groq": Uses ChatGroq for fast inference. Set GROQ_API_KEY
    (or LLM_API_KEY as fallback) and pick a model like "llama-3.3-70b-versatile".
"""

import os
from dotenv import load_dotenv
from langchain_core.language_models.chat_models import BaseChatModel

load_dotenv()

# LLM Configuration
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "openai").lower()
LLM_API_KEY = os.getenv("LLM_API_KEY", "")
LLM_BASE_URL = os.getenv("LLM_BASE_URL", "")
LLM_MODEL = os.getenv("LLM_MODEL", "gpt-4o")


def get_llm(temperature: float = 0.0) -> BaseChatModel:
    """
    Get configured LLM instance based on LLM_PROVIDER.

    Providers:
      - "openai": ChatOpenAI (default). Honors LLM_BASE_URL for
        local llama.cpp or other OpenAI-compatible endpoints.
      - "groq": ChatGroq. Uses GROQ_API_KEY env var (falls back
        to LLM_API_KEY).

    Args:
        temperature: Sampling temperature (0.0 for deterministic extraction)

    Returns:
        Configured chat model instance
    """
    if LLM_PROVIDER == "groq":
        from langchain_groq import ChatGroq

        groq_api_key = os.getenv("GROQ_API_KEY", "") or LLM_API_KEY
        return ChatGroq(
            model=LLM_MODEL,
            temperature=temperature,
            api_key=groq_api_key,
        )

    # Default: OpenAI-compatible
    from langchain_openai import ChatOpenAI

    kwargs = {
        "model": LLM_MODEL,
        "temperature": temperature,
        "api_key": LLM_API_KEY or "not-needed",
    }

    if LLM_BASE_URL:
        kwargs["base_url"] = LLM_BASE_URL

    return ChatOpenAI(**kwargs)
