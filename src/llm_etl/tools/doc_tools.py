"""
Document processing tools for the orchestrator agent.

Provides tools to analyze, chunk, and parse documents.
"""

import logging
import re
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

logger = logging.getLogger(__name__)


class DocumentType(str, Enum):
    """Supported document types."""

    TEXT = "text"
    CSV = "csv"
    JSON = "json"
    PDF = "pdf"
    DOCX = "docx"
    UNKNOWN = "unknown"


class ChunkStrategy(str, Enum):
    """Document chunking strategies."""

    FIXED_SIZE = "fixed_size"
    PARAGRAPH = "paragraph"
    SECTION = "section"
    SENTENCE = "sentence"


@dataclass
class Chunk:
    """A document chunk with metadata."""

    content: str
    index: int
    start_char: int
    end_char: int
    metadata: dict


def detect_document_type(file_path: str | Path) -> DocumentType:
    """
    Detect document type from file extension.

    Args:
        file_path: Path to the document

    Returns:
        DocumentType enum value
    """
    path = Path(file_path)
    extension = path.suffix.lower()

    type_map = {
        ".txt": DocumentType.TEXT,
        ".csv": DocumentType.CSV,
        ".json": DocumentType.JSON,
        ".pdf": DocumentType.PDF,
        ".docx": DocumentType.DOCX,
    }

    return type_map.get(extension, DocumentType.UNKNOWN)


def estimate_entity_types(text: str) -> list[str]:
    """
    Estimate which entity types are likely present in a text.

    Uses keyword heuristics to suggest schemas for extraction.

    Args:
        text: Document text to analyze

    Returns:
        List of likely entity type names (e.g., ['WasteItem', 'Facility'])
    """
    text_lower = text.lower()
    detected = []

    # WasteItem indicators
    waste_keywords = [
        "abfall",
        "müll",
        "entsorgung",
        "wertstoff",
        "recycling",
        "restmüll",
        "biomüll",
        "altpapier",
        "verpackung",
        "schadstoff",
    ]
    if any(kw in text_lower for kw in waste_keywords):
        detected.append("WasteItem")

    # Facility indicators
    facility_keywords = [
        "wertstoffhof",
        "annahmestelle",
        "sammelstelle",
        "öffnungszeiten",
        "adresse",
        "straße",
        "str.",
        "frankfurt",
    ]
    if any(kw in text_lower for kw in facility_keywords):
        detected.append("Facility")

    # WasteStream indicators (bin types)
    stream_keywords = [
        "tonne",
        "biotonne",
        "restabfall",
        "gelbe tonne",
        "altpapiertonne",
        "container",
        "deckelfarbe",
        "leerung",
    ]
    if any(kw in text_lower for kw in stream_keywords):
        detected.append("WasteStream")

    # DisposalRule indicators (conditional logic)
    rule_keywords = [
        "wenn",
        "falls",
        "flüssig",
        "trocken",
        "leer",
        "voll",
        "→",
        "->",
        "dann",
    ]
    if any(kw in text_lower for kw in rule_keywords):
        detected.append("DisposalRule")

    # Default to WasteItem if nothing specific detected
    if not detected:
        detected.append("WasteItem")

    return detected


def chunk_document(
    text: str,
    strategy: ChunkStrategy = ChunkStrategy.PARAGRAPH,
    max_chunk_size: int = 2000,
    overlap: int = 100,
) -> list[Chunk]:
    """
    Split a document into chunks for processing.

    Args:
        text: Document text to chunk
        strategy: Chunking strategy to use
        max_chunk_size: Maximum characters per chunk
        overlap: Character overlap between chunks (for context)

    Returns:
        List of Chunk objects
    """
    if strategy == ChunkStrategy.FIXED_SIZE:
        return _chunk_fixed_size(text, max_chunk_size, overlap)
    elif strategy == ChunkStrategy.PARAGRAPH:
        return _chunk_by_paragraph(text, max_chunk_size)
    elif strategy == ChunkStrategy.SECTION:
        return _chunk_by_section(text, max_chunk_size)
    elif strategy == ChunkStrategy.SENTENCE:
        return _chunk_by_sentence(text, max_chunk_size)
    else:
        return _chunk_fixed_size(text, max_chunk_size, overlap)


def _chunk_fixed_size(text: str, max_size: int, overlap: int) -> list[Chunk]:
    """Chunk by fixed character size with overlap."""
    chunks = []
    start = 0
    index = 0

    while start < len(text):
        end = min(start + max_size, len(text))

        # Try to break at word boundary
        if end < len(text):
            last_space = text.rfind(" ", start, end)
            if last_space > start:
                end = last_space

        chunk_text = text[start:end].strip()
        if chunk_text:
            chunks.append(
                Chunk(
                    content=chunk_text,
                    index=index,
                    start_char=start,
                    end_char=end,
                    metadata={"strategy": "fixed_size"},
                )
            )
            index += 1

        start = end - overlap if end < len(text) else len(text)

    return chunks


def _chunk_by_paragraph(text: str, max_size: int) -> list[Chunk]:
    """Chunk by paragraph breaks, combining small paragraphs."""
    # Split on double newlines or multiple newlines
    paragraphs = re.split(r"\n\s*\n", text)

    chunks = []
    current_chunk = []
    current_size = 0
    current_start = 0
    index = 0
    char_pos = 0

    for para in paragraphs:
        para = para.strip()
        if not para:
            char_pos += 2  # Account for split characters
            continue

        para_size = len(para)

        # If single paragraph exceeds max, split it
        if para_size > max_size:
            # Flush current buffer
            if current_chunk:
                chunk_text = "\n\n".join(current_chunk)
                chunks.append(
                    Chunk(
                        content=chunk_text,
                        index=index,
                        start_char=current_start,
                        end_char=char_pos,
                        metadata={"strategy": "paragraph"},
                    )
                )
                index += 1
                current_chunk = []
                current_size = 0

            # Split large paragraph by sentences
            sentences = re.split(r"(?<=[.!?])\s+", para)
            for sent in sentences:
                if current_size + len(sent) > max_size and current_chunk:
                    chunk_text = " ".join(current_chunk)
                    chunks.append(
                        Chunk(
                            content=chunk_text,
                            index=index,
                            start_char=current_start,
                            end_char=char_pos,
                            metadata={"strategy": "paragraph"},
                        )
                    )
                    index += 1
                    current_chunk = []
                    current_size = 0
                    current_start = char_pos

                current_chunk.append(sent)
                current_size += len(sent) + 1

        elif current_size + para_size > max_size:
            # Flush current buffer and start new
            chunk_text = "\n\n".join(current_chunk)
            chunks.append(
                Chunk(
                    content=chunk_text,
                    index=index,
                    start_char=current_start,
                    end_char=char_pos,
                    metadata={"strategy": "paragraph"},
                )
            )
            index += 1
            current_chunk = [para]
            current_size = para_size
            current_start = char_pos
        else:
            current_chunk.append(para)
            current_size += para_size + 2

        char_pos += para_size + 2  # Account for \n\n

    # Flush remaining
    if current_chunk:
        chunk_text = "\n\n".join(current_chunk)
        chunks.append(
            Chunk(
                content=chunk_text,
                index=index,
                start_char=current_start,
                end_char=len(text),
                metadata={"strategy": "paragraph"},
            )
        )

    return chunks


def _chunk_by_section(text: str, max_size: int) -> list[Chunk]:
    """Chunk by section headers (lines starting with # or uppercase)."""
    # Pattern for section headers: markdown headers or all-caps lines
    section_pattern = r"^(?:#{1,6}\s+.+|[A-ZÄÖÜ][A-ZÄÖÜ\s]{3,}:?)$"

    lines = text.split("\n")
    sections = []
    current_section = []
    current_start = 0
    char_pos = 0

    for i, line in enumerate(lines):
        if re.match(section_pattern, line.strip()) and current_section:
            # New section found, save current
            sections.append(
                {
                    "content": "\n".join(current_section),
                    "start": current_start,
                    "end": char_pos,
                }
            )
            current_section = [line]
            current_start = char_pos
        else:
            current_section.append(line)

        char_pos += len(line) + 1

    # Add final section
    if current_section:
        sections.append(
            {
                "content": "\n".join(current_section),
                "start": current_start,
                "end": char_pos,
            }
        )

    # Convert sections to chunks, splitting if too large
    chunks = []
    index = 0

    for section in sections:
        content = section["content"]
        if len(content) <= max_size:
            chunks.append(
                Chunk(
                    content=content,
                    index=index,
                    start_char=section["start"],
                    end_char=section["end"],
                    metadata={"strategy": "section"},
                )
            )
            index += 1
        else:
            # Split large section by paragraphs
            sub_chunks = _chunk_by_paragraph(content, max_size)
            for sub in sub_chunks:
                sub.index = index
                sub.start_char += section["start"]
                sub.end_char += section["start"]
                sub.metadata["parent_section"] = True
                chunks.append(sub)
                index += 1

    return chunks


def _chunk_by_sentence(text: str, max_size: int) -> list[Chunk]:
    """Chunk by sentences, combining into max_size groups."""
    # Split on sentence boundaries
    sentences = re.split(r"(?<=[.!?])\s+", text)

    chunks = []
    current_chunk = []
    current_size = 0
    current_start = 0
    index = 0
    char_pos = 0

    for sent in sentences:
        sent = sent.strip()
        if not sent:
            continue

        sent_size = len(sent)

        if current_size + sent_size > max_size and current_chunk:
            chunk_text = " ".join(current_chunk)
            chunks.append(
                Chunk(
                    content=chunk_text,
                    index=index,
                    start_char=current_start,
                    end_char=char_pos,
                    metadata={"strategy": "sentence"},
                )
            )
            index += 1
            current_chunk = []
            current_size = 0
            current_start = char_pos

        current_chunk.append(sent)
        current_size += sent_size + 1
        char_pos += sent_size + 1

    # Flush remaining
    if current_chunk:
        chunk_text = " ".join(current_chunk)
        chunks.append(
            Chunk(
                content=chunk_text,
                index=index,
                start_char=current_start,
                end_char=len(text),
                metadata={"strategy": "sentence"},
            )
        )

    return chunks


def read_text_file(file_path: str | Path) -> str:
    """
    Read a text file with encoding detection.

    Args:
        file_path: Path to the text file

    Returns:
        File contents as string
    """
    path = Path(file_path)

    # Try common encodings
    encodings = ["utf-8", "latin-1", "cp1252"]

    for encoding in encodings:
        try:
            return path.read_text(encoding=encoding)
        except UnicodeDecodeError:
            continue

    # Fallback with error handling
    return path.read_text(encoding="utf-8", errors="replace")


def get_document_stats(text: str) -> dict:
    """
    Get statistics about a document.

    Args:
        text: Document text

    Returns:
        Dictionary with document statistics
    """
    lines = text.split("\n")
    words = text.split()
    paragraphs = [p for p in re.split(r"\n\s*\n", text) if p.strip()]

    return {
        "characters": len(text),
        "words": len(words),
        "lines": len(lines),
        "paragraphs": len(paragraphs),
        "estimated_tokens": len(text) // 4,  # Rough estimate
    }
