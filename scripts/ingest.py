#!/usr/bin/env python3
"""
scripts/ingest.py — Load documentation files and build the FAISS index.

Supported formats:  .md  .html  .txt  .pdf
Run once (or whenever docs change):
    python scripts/ingest.py

The index is saved to the path configured in FAISS_INDEX_PATH (.env).
"""
import os
import sys
import logging
from pathlib import Path

# Make the project root importable
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
load_dotenv()

from langchain_community.document_loaders import (
    DirectoryLoader,
    UnstructuredMarkdownLoader,
    UnstructuredHTMLLoader,
    TextLoader,
    PyPDFDirectoryLoader,
)
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_community.vectorstores import FAISS
from langchain_openai import OpenAIEmbeddings

from app.config import settings

logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
logger = logging.getLogger(__name__)


def load_documents(docs_path: str):
    """
    Load all supported document types from the docs directory.
    Supported: .md  .html  .txt  .pdf
    """
    docs_dir = Path(docs_path)
    if not docs_dir.exists():
        raise FileNotFoundError(f"Docs directory not found: {docs_dir}")

    documents = []

    # ── Markdown, HTML, plain text ───────────────────────────────────────────
    text_loaders = [
        ("**/*.md",   UnstructuredMarkdownLoader),
        ("**/*.html", UnstructuredHTMLLoader),
        ("**/*.txt",  TextLoader),
    ]
    for glob_pattern, loader_cls in text_loaders:
        try:
            loader = DirectoryLoader(
                str(docs_dir),
                glob=glob_pattern,
                loader_cls=loader_cls,
                silent_errors=True,
            )
            docs = loader.load()
            if docs:
                logger.info("Loaded %d document(s) matching '%s'.", len(docs), glob_pattern)
            documents.extend(docs)
        except Exception as e:
            logger.warning("Loader error for '%s' (skipping): %s", glob_pattern, e)

    # ── PDF — PyPDFDirectoryLoader splits each page into its own Document ────
    pdf_files = list(docs_dir.rglob("*.pdf"))
    if pdf_files:
        logger.info("Found %d PDF file(s): %s", len(pdf_files), [f.name for f in pdf_files])
        try:
            pdf_loader = PyPDFDirectoryLoader(
                str(docs_dir),
                recursive=True,
                extract_images=False,   # keep lightweight for MVP
            )
            pdf_docs = pdf_loader.load()
            # Tag each page with its source filename for traceability
            for doc in pdf_docs:
                src = doc.metadata.get("source", "")
                doc.metadata["file_type"] = "pdf"
                doc.metadata["filename"] = Path(src).name if src else "unknown.pdf"
            logger.info(
                "Loaded %d page(s) from %d PDF file(s).", len(pdf_docs), len(pdf_files)
            )
            documents.extend(pdf_docs)
        except Exception as e:
            logger.warning("PDF loader error (skipping PDFs): %s", e)
    else:
        logger.info("No PDF files found in '%s' — skipping PDF loader.", docs_path)

    if not documents:
        raise ValueError(
            f"No documents found in '{docs_path}'. "
            "Add .md / .html / .txt / .pdf files and re-run."
        )

    logger.info("Total documents loaded: %d", len(documents))
    return documents


def build_index(documents) -> FAISS:
    """Split documents into chunks and build FAISS index."""
    splitter = RecursiveCharacterTextSplitter(
        chunk_size=800,       # ~500-1000 tokens as specified
        chunk_overlap=100,
        separators=["\n\n", "\n", ". ", " ", ""],
    )
    chunks = splitter.split_documents(documents)
    logger.info("Split into %d chunks.", len(chunks))

    embeddings = OpenAIEmbeddings(
        model=settings.embedding_model,
        openai_api_key=settings.openai_api_key,
    )

    logger.info("Generating embeddings (this may take a moment)…")
    vector_store = FAISS.from_documents(chunks, embeddings)
    return vector_store


def main():
    logger.info("=== Ingestion started ===")
    logger.info("Docs path       : %s", settings.docs_path)
    logger.info("FAISS index path: %s", settings.faiss_index_path)

    documents = load_documents(settings.docs_path)

    logger.info("Building FAISS index…")
    vector_store = build_index(documents)

    index_path = settings.faiss_index_path
    os.makedirs(index_path, exist_ok=True)
    vector_store.save_local(index_path)
    logger.info("✅ FAISS index saved to: %s", index_path)
    logger.info("=== Ingestion complete ===")


if __name__ == "__main__":
    main()