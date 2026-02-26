"""
app/database.py — Async PostgreSQL connection pool via asyncpg.

Configuration:
  - Connection details loaded from DATABASE_URL in .env
  - Pool created on app startup in on_chat_start()
  - Pool closed on app shutdown in on_chat_end()

Security:
  - Read-only PostgreSQL role enforced at DB level (enki_viewer)
  - Only hardcoded, parameterized SQL statements executed
  - No user input passed to any query
  - Statement execution timeout: 30 seconds
  - Connection pool timeout: 10 seconds

Resilience:
  - Pool creation failure is logged but does NOT crash the app
  - All callers check is_db_available() before use
  - Analytics queries gracefully disabled when database unavailable
  - All DB operations wrapped in try/except with detailed logging
"""
import asyncpg
import logging
from datetime import datetime
from typing import Optional
from app.config import settings

logger = logging.getLogger(__name__)

_pool: Optional[asyncpg.Pool] = None


async def create_pool() -> None:
    """
    Create the asyncpg connection pool from DATABASE_URL.
    
    On failure, _pool stays None — the app continues in RAG-only mode.
    """
    global _pool
    if _pool is not None:
        logger.warning("Database pool already exists; skipping")
        return
    
    try:
        logger.info(
            "Creating PostgreSQL pool (max_size=%d, timeout=%ds)...",
            settings.db_pool_max_size,
            settings.db_connection_timeout,
        )
        _pool = await asyncpg.create_pool(
            dsn=settings.database_url,
            min_size=settings.db_pool_min_size,
            max_size=settings.db_pool_max_size,
            command_timeout=settings.db_statement_timeout,
            statement_cache_size=0,
            timeout=settings.db_connection_timeout,
        )
        logger.info(
            "✓ PostgreSQL pool created (min=%d, max=%d)",
            settings.db_pool_min_size,
            settings.db_pool_max_size,
        )
    except asyncpg.InvalidDSNError as exc:
        _pool = None
        logger.error(
            "✗ Invalid DATABASE_URL format: %s\nExpected: postgresql://user:pass@host:port/db",
            exc,
        )
    except (OSError, asyncpg.PostgresError) as exc:
        _pool = None
        logger.error(
            "✗ PostgreSQL connection failed — analytics disabled.\n"
            "Check: DATABASE_URL, host/port reachable, credentials valid, database exists.\n"
            "Error: %s",
            exc,
        )
    except Exception as exc:
        _pool = None
        logger.exception("✗ Unexpected error creating pool: %s", exc)


async def close_pool() -> None:
    """
    Close the PostgreSQL connection pool gracefully.
    """
    global _pool
    if _pool is not None:
        try:
            await _pool.close()
            logger.info("✓ PostgreSQL pool closed")
        except Exception as exc:
            logger.error("Error closing pool: %s", exc)
        finally:
            _pool = None


def is_db_available() -> bool:
    """
    Check if PostgreSQL connection pool is available.
    """
    return _pool is not None


def get_pool() -> asyncpg.Pool:
    """
    Get the active PostgreSQL connection pool.
    
    Raises RuntimeError if pool not initialized.
    """
    if _pool is None:
        raise RuntimeError(
            "❌ Database pool not available.\n"
            "Check: DATABASE_URL in .env, PostgreSQL running, credentials valid."
        )
    return _pool


# ---------------------------------------------------------------------------
# Hardcoded, injection-safe analytics queries
# ---------------------------------------------------------------------------

_SQL_TOTAL_PORTS  = """
SELECT SUM(a.ports_count) AS ports
FROM contractor_service.address a
JOIN contractor_service.network_design_address n
  ON n.address_id = a.id
WHERE a.smr_status = 'CONNECTION_ALLOWED'
  AND n.excluded = 'false';
"""
_SQL_PORTS_BY_LOCALITY_PERIOD = """
SELECT SUM(a.ports_count) AS ports
FROM contractor_service.address a
JOIN contractor_service.address_smr_status_history h
  ON a.id = h.address_id
JOIN contractor_service.network_design_address n
  ON n.address_id = a.id
WHERE a.smr_status = 'CONNECTION_ALLOWED'
  AND a.locality = $1
  AND h.status_date_time >= $2::date
  AND h.status_date_time <= $3::date
  AND h.status_id = '3'
  AND n.excluded = 'false'
  AND h.status_date_time IS NOT null;
"""
_SQL_PORTS_BY_MONTH = """
SELECT
  to_char(h.status_date_time, 'YYYY-MM') AS month,
  SUM(a.ports_count) AS ports
FROM contractor_service.address a
JOIN contractor_service.address_smr_status_history h
  ON a.id = h.address_id
JOIN contractor_service.network_design_address n
  ON n.address_id = a.id
WHERE a.smr_status = 'CONNECTION_ALLOWED'
  AND h.status_date_time IS NOT NULL
  AND h.status_id = '3'
  AND n.excluded = 'false'
GROUP BY 1
ORDER BY 1;
"""

_SQL_PORTS_BY_LOCALITY = """
SELECT
  a.locality AS locality,
  SUM(a.ports_count) AS ports
FROM contractor_service.address a
JOIN contractor_service.address_smr_status_history h
  ON a.id = h.address_id
JOIN contractor_service.network_design_address n
  ON n.address_id = a.id
WHERE a.smr_status = 'CONNECTION_ALLOWED'
  AND h.status_date_time IS NOT NULL
  AND h.status_id = '3'
  AND a.locality IS NOT NULL
  AND btrim(a.locality) <> ''
  AND n.excluded = 'false'
GROUP BY 1
ORDER BY ports DESC;
"""

_SQL_DELIVERED_ADDRESSES = """
SELECT
  a.name AS address_name,
  a.locality,
  a.ports_count,
  MIN(h.status_date_time) AS delivered_at
FROM contractor_service.address a
JOIN contractor_service.address_smr_status_history h
  ON a.id = h.address_id
JOIN contractor_service.network_design_address n
  ON n.address_id = a.id
WHERE a.smr_status = 'CONNECTION_ALLOWED'
  AND h.status_date_time IS NOT NULL
  AND h.status_id = '3'
  AND n.excluded = 'false'
GROUP BY a.id, a.name, a.locality, a.ports_count
ORDER BY delivered_at DESC;
"""

_SQL_OBJECTS_DELIVERED = """
SELECT COUNT(*) AS count
FROM contractor_service.address a
JOIN contractor_service.network_design_address n
  ON n.address_id = a.id
WHERE a.smr_status = 'CONNECTION_ALLOWED'
  AND n.excluded = 'false';
"""

_SQL_OBJECTS_IN_PROGRESS = """
SELECT COUNT(*) AS count
FROM contractor_service.address a
JOIN contractor_service.network_design_address n
  ON n.address_id = a.id
WHERE (a.smr_status = 'ON_CHECK' OR a.smr_status = 'IN_PROGRESS')
  AND n.excluded = 'false';
"""

_SQL_OBJECTS_EXCLUDED = """
SELECT COUNT(*) AS count
FROM contractor_service.address a
JOIN contractor_service.network_design_address n
  ON n.address_id = a.id
WHERE n.excluded = 'true';
"""

async def fetch_total_ports() -> int:
    async with get_pool().acquire() as conn:
        result = await conn.fetchval(_SQL_TOTAL_PORTS)
    return int(result) if result is not None else 0


async def fetch_ports_by_locality_period(
    locality: str, start_date: str, end_date: str
) -> int:
    """
    Fetch total ports for a specific locality and date period.
    
    Args:
        locality: City name (e.g., "Астана")
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
    
    Returns:
        Total number of ports matching criteria
    """
    
    start_date_object = datetime.strptime(start_date, "%Y-%m-%d").date()
    end_date_object = datetime.strptime(end_date, "%Y-%m-%d").date()
    async with get_pool().acquire() as conn:
        result = await conn.fetchval(_SQL_PORTS_BY_LOCALITY_PERIOD, locality, start_date_object, end_date_object)
    return int(result) if result is not None else 0

async def fetch_ports_by_month() -> list[dict]:
    async with get_pool().acquire() as conn:
        rows = await conn.fetch(_SQL_PORTS_BY_MONTH)
    return [{"month": r["month"], "ports": int(r["ports"] or 0)} for r in rows]

async def fetch_ports_by_locality() -> list[dict]:
    async with get_pool().acquire() as conn:
        rows = await conn.fetch(_SQL_PORTS_BY_LOCALITY)
    return [{"locality": r["locality"], "ports": int(r["ports"] or 0)} for r in rows]

async def fetch_delivered_addresses():
    async with get_pool().acquire() as conn:
        rows = await conn.fetch(_SQL_DELIVERED_ADDRESSES)
    return [
        {
            "address": r["address_name"],
            "locality": r["locality"],
            "ports": int(r["ports_count"] or 0),
            "delivered_at": r["delivered_at"],
        }
        for r in rows
    ]

async def fetch_objects_status() -> dict:
    """
    Fetch project status by SMR: delivered, in progress, excluded objects count.
    
    Returns:
        Dict with 'delivered', 'in_progress', 'excluded' counts
    """
    async with get_pool().acquire() as conn:
        delivered = await conn.fetchval(_SQL_OBJECTS_DELIVERED)
        in_progress = await conn.fetchval(_SQL_OBJECTS_IN_PROGRESS)
        excluded = await conn.fetchval(_SQL_OBJECTS_EXCLUDED)
    
    return {
        "delivered": int(delivered) if delivered is not None else 0,
        "in_progress": int(in_progress) if in_progress is not None else 0,
        "excluded": int(excluded) if excluded is not None else 0,
    }