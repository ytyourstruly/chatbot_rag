"""
app/main.py — FastAPI app with streaming chat endpoint.

Endpoints:
  POST /chat          — Server-Sent Events stream of response tokens
  GET  /health        — Liveness check
  GET  /cache/clear   — Dev utility to flush analytics cache
"""
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from app.config import settings
from app.database import create_pool, close_pool
from app.vector_store import load_vector_store
from app.chatbot import process_question
from app.cache import cache_clear

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(name)s | %(message)s")
logger = logging.getLogger(__name__)


# ── Lifespan (startup / shutdown) ───────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Starting up…")
    await create_pool()
    load_vector_store()
    yield
    logger.info("Shutting down…")
    await close_pool()


# ── App instance ─────────────────────────────────────────────────────────────

app = FastAPI(
    title="Website Chatbot API",
    description="RAG + analytics chatbot — MVP",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],      # tighten for production
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Request / response schemas ───────────────────────────────────────────────

class ChatRequest(BaseModel):
    question: str = Field(..., min_length=1, max_length=1000)


# ── Routes ───────────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/chat", summary="Stream a chatbot response")
async def chat(req: ChatRequest):
    """
    Returns a Server-Sent Events stream.
    Each event is `data: <token>\\n\\n`.
    The stream ends with `data: [DONE]\\n\\n`.
    """
    question = req.question.strip()
    if not question:
        raise HTTPException(status_code=400, detail="Question must not be empty.")

    async def event_stream():
        try:
            async for token in process_question(question):
                # SSE format
                yield f"data: {token}\n\n"
        except Exception as exc:
            logger.exception("Error during chat processing: %s", exc)
            yield "data: ⚠️ An error occurred. Please try again.\n\n"
        finally:
            yield "data: [DONE]\n\n"

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",     # disable nginx buffering
        },
    )


@app.delete("/cache", summary="Clear analytics cache (dev utility)")
async def clear_cache():
    cache_clear()
    return {"message": "Cache cleared."}
