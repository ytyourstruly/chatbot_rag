"""
app/config.py — Centralised settings loaded from .env
"""
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    openai_api_key: str
    database_url: str
    docs_path: str = "./docs"
    faiss_index_path: str = "./faiss_index"
    cache_ttl_seconds: int = 300
    openai_model: str = "gpt-5.2-2025-12-11"
    embedding_model: str = "text-embedding-3-large"
    top_k_results: int = 3
    min_similarity_score: float = 0.75    # threshold for RAG confidence

    class Config:
        env_file = ".env"
        extra = "ignore"


settings = Settings()
