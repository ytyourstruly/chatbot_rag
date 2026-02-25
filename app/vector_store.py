"""
app/vector_store.py — FAISS-backed RAG retrieval using LangChain.

Flow:
  1. Load FAISS index from disk (built by scripts/ingest.py).
  2. Embed the user query.
  3. Similarity search (top_k=3).
  4. Return documents + max similarity score.
"""
import logging
import os
from typing import Optional

from langchain_community.vectorstores import FAISS
from langchain_openai import OpenAIEmbeddings
from langchain_core.documents import Document

from app.config import settings

logger = logging.getLogger(__name__)

# Module-level singleton
_vector_store: Optional[FAISS] = None


def load_vector_store() -> None:
    """Load FAISS index from disk into memory. Call once at startup."""
    global _vector_store
    index_path = settings.faiss_index_path
    if not os.path.exists(index_path):
        logger.warning(
            "FAISS index not found at '%s'. Run scripts/ingest.py first.", index_path
        )
        return
    embeddings = OpenAIEmbeddings(
        model=settings.embedding_model,
        openai_api_key=settings.openai_api_key,
    )
    _vector_store = FAISS.load_local(
        index_path,
        embeddings,
        allow_dangerous_deserialization=True,   # safe: we wrote the index ourselves
    )
    logger.info("FAISS index loaded from '%s'.", index_path)


def get_vector_store() -> Optional[FAISS]:
    return _vector_store


async def retrieve_context(question: str) -> tuple[str, float]:
    """
    Search FAISS for relevant documentation chunks.

    Returns:
        context  — concatenated text of top-k documents (empty string if none)
        score    — best (lowest) L2 distance, normalised to 0-1 similarity
    """
    store = get_vector_store()
    if store is None:
        return "", 0.0

    # similarity_search_with_score returns (doc, L2_distance) pairs
    results: list[tuple[Document, float]] = store.similarity_search_with_score(
        question, k=settings.top_k_results
    )

    if not results:
        return "", 0.0

    # Convert L2 distance to a rough 0-1 similarity (lower distance = higher similarity)
    # FAISS uses squared L2; a distance of 0 means identical embeddings.
    best_distance = results[0][1]
    # Normalise: treat distance ≤ 0.3 as high confidence, ≥ 1.5 as low confidence
    similarity = max(0.0, 1.0 - best_distance / 1.5)

    context = "\n\n---\n\n".join(doc.page_content for doc, _ in results)
    return context, similarity
