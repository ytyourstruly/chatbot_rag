"""
app/chatbot.py — Orchestrates the full RAG + analytics pipeline.

Decision flow:
  1. Search FAISS vector store for relevant documentation.
  2. If similarity score is above threshold → stream LLM answer from context.
  3. If below threshold → check for analytics intent (keyword-based).
  4. If analytics intent found → query PostgreSQL and return result.
  5. If unsupported analytics → return helpful refusal.
  6. Otherwise → stream LLM answer without context (general knowledge).
"""
import logging
from typing import AsyncIterator, Callable, Awaitable

from app.analytics import detect_analytics_intent, resolve_analytics, AnalyticsIntent
from app.vector_store import retrieve_context
from app.llm import stream_rag_response, stream_general_response
from app.config import settings

logger = logging.getLogger(__name__)

# Type alias for the step-update callback used by Chainlit
StepCallback = Callable[[str], Awaitable[None]]


async def process_question(
    question: str,
    on_step: StepCallback | None = None,
) -> AsyncIterator[str]:
    """
    Main entry point. Yields response tokens (strings) for streaming.

    `on_step` is an optional async callback called with status messages like
    "Searching documentation…" so the UI can display progress indicators.
    """

    async def _notify(msg: str) -> None:
        if on_step:
            await on_step(msg)



    # ── Step 1: Analytics intent detection ───────────────────────────────────
    await _notify("Анализ запроса…")
    intent, parameters = await detect_analytics_intent(question)
    logger.info("Analytics intent: %s, parameters: %s", intent, parameters)

    if intent != AnalyticsIntent.NONE:
        await _notify("Выполняется запрос аналитики…")
        result = await resolve_analytics(intent, parameters)
        # Yield the full analytics result as a single chunk
        # (it's already formatted markdown, no streaming needed from DB)
        yield result
        return
    
    # ── Step 2: Vector search ────────────────────────────────────────────────
    # TODO: remove if needed from Dias the boss
    # await _notify("Поиск из документации…")
    # context, similarity = await retrieve_context(question)
    # logger.info("RAG similarity score: %.3f (threshold: %.3f)", similarity, settings.min_similarity_score)

    # # ── Step 3: High-confidence RAG answer ───────────────────────────────────
    # if similarity >= settings.min_similarity_score and context:
    #     logger.info("Answering from documentation context.")
    #     async for token in stream_rag_response(question, context):
    #         yield token
    #     return


    # # ── Step 4: General LLM answer (no DB, low RAG confidence) ───────────────
    # if context:
    #     # We have some context but below threshold — still use it, just less confident
    #     logger.info("Low-confidence RAG; streaming with partial context.")
    #     async for token in stream_rag_response(question, context):
    #         yield token
    # else:
    logger.info("No context found; falling back to general LLM.")
    async for token in stream_general_response(question):
        yield token
