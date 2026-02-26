"""
app/config.py — Centralised settings loaded from .env with validation.

Database Configuration:
  - DATABASE_URL must be a valid PostgreSQL connection string
  - Format: postgresql://user:password@host:port/database
  - Credentials are stored in .env only (never hardcoded)
"""
from pydantic import Field, validator
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings loaded from .env file."""
    
    openai_api_key: str = Field(..., description="OpenAI API key for LLM")
    database_url: str = Field(..., description="PostgreSQL connection string")
    
    docs_path: str = Field("./docs", description="Path to documentation files")
    faiss_index_path: str = Field("./faiss_index", description="Path to FAISS vector index")
    cache_ttl_seconds: int = Field(300, description="Cache time-to-live in seconds")
    
    openai_model: str = Field("gpt-5.2-2025-12-11", description="OpenAI model ID")
    embedding_model: str = Field("text-embedding-3-large", description="Embedding model ID")
    
    top_k_results: int = Field(3, description="Number of context chunks to retrieve")
    min_similarity_score: float = Field(0.75, description="RAG confidence threshold")
    
    db_pool_min_size: int = Field(1, description="Minimum pool connections")
    db_pool_max_size: int = Field(10, description="Maximum pool connections")
    db_connection_timeout: int = Field(10, description="Connection timeout (seconds)")
    db_statement_timeout: int = Field(30, description="Statement timeout (seconds)")
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"
        case_sensitive = False
    
    @validator('database_url')
    def validate_database_url(cls, v: str) -> str:
        if not v:
            raise ValueError("DATABASE_URL cannot be empty")
        if not v.startswith('postgresql://'):
            raise ValueError(f"DATABASE_URL must start with 'postgresql://', got: {v[:20]}...")
        if '@' not in v or ':' not in v:
            raise ValueError("DATABASE_URL format: postgresql://user:pass@host:port/database")
        return v
    
    @validator('openai_api_key')
    def validate_openai_key(cls, v: str) -> str:
        if not v:
            raise ValueError("OPENAI_API_KEY cannot be empty")
        if not v.startswith('sk-'):
            raise ValueError(f"OPENAI_API_KEY should start with 'sk-', got: {v[:10]}...")
        return v


try:
    settings = Settings()
except Exception as e:
    raise RuntimeError(
        f"Failed to load settings from .env: {e}\n"
        "Make sure all required environment variables are set."
    ) from e
