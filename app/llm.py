"""
app/llm.py — Streaming OpenAI chat via LangChain.

Builds a simple RAG prompt when context is available,
or a general Q&A prompt when falling back gracefully.
"""
from typing import AsyncIterator
from langchain_openai import ChatOpenAI
from langchain.messages import HumanMessage, SystemMessage
from app.config import settings
from app.prompts.llm_prompts import SYSTEM_RAG, SYSTEM_GENERAL
from langchain_core.globals import set_llm_cache
from langchain_community.cache import InMemoryCache

set_llm_cache(InMemoryCache())


def _build_llm(streaming: bool = True) -> ChatOpenAI:
    return ChatOpenAI(
        model=settings.openai_model,
        openai_api_key=settings.openai_api_key,
        streaming=streaming,
        temperature=0.2,
    )


async def stream_rag_response(question: str, context: str) -> AsyncIterator[str]:
    """
    Stream an LLM response grounded in retrieved documentation context.
    Yields string tokens as they arrive.
    """
    llm = _build_llm(streaming=True)
    messages = [
        SystemMessage(content=SYSTEM_RAG),
        HumanMessage(
            content=f"Documentation context:\n\n{context}\n\n---\n\nQuestion: {question}"
        ),
    ]
    async for chunk in llm.astream(messages):
        token = chunk.content
        if token:
            yield token


async def stream_general_response(question: str) -> AsyncIterator[str]:
    """
    Stream a general LLM response when no documentation context is available.
    """
    llm = _build_llm(streaming=True)
    messages = [
        SystemMessage(content=SYSTEM_GENERAL),
        HumanMessage(content=question),
    ]
    async for chunk in llm.astream(messages):
        token = chunk.content
        if token:
            yield token
