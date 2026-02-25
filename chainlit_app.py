"""
chainlit_app.py — Chainlit frontend for the chatbot.

Run with:
    chainlit run chainlit_app.py --port 8001

Features:
  • Streaming tokens displayed in real time
  • Status messages: "Searching documentation…" / "Fetching analytics…"
  • Clean markdown rendering
"""
import sys
from pathlib import Path

# Ensure project root is on the path
sys.path.insert(0, str(Path(__file__).parent))

from dotenv import load_dotenv
load_dotenv()

import chainlit as cl
from app.database import create_pool, close_pool
from app.vector_store import load_vector_store
from app.chatbot import process_question
from app.prompts.ui_prompts import WELCOME_MESSAGE


# ── App lifecycle ─────────────────────────────────────────────────────────────

@cl.on_chat_start
async def on_chat_start():
    """Called once when a user opens a new chat session."""
    # Initialise DB pool and vector store for this process
    await create_pool()
    load_vector_store()

    await cl.Message(
        content=WELCOME_MESSAGE
    ).send()


@cl.on_chat_end
async def on_chat_end():
    await close_pool()


# ── Message handler ───────────────────────────────────────────────────────────

@cl.on_message
async def on_message(message: cl.Message):
    question = message.content.strip()
    if not question:
        return

    # Step indicator — updated dynamically as pipeline progresses
    step_msg = cl.Message(content="")
    await step_msg.send()

    # Collect status labels shown briefly before streaming starts
    current_step_text = ""

    async def on_step(step_label: str):
        nonlocal current_step_text
        current_step_text = step_label
        await step_msg.stream_token(f"_{step_label}_\n\n")

    # Stream response tokens
    response_msg = cl.Message(content="")
    full_response = ""
    first_token = True

    async for token in process_question(question, on_step=on_step):
        if first_token:
            # Replace the step indicator message with actual response
            await step_msg.remove()
            await response_msg.send()
            first_token = False
        full_response += token
        await response_msg.stream_token(token)

    await response_msg.update()
