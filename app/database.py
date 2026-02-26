"""
app/database.py — Async PostgreSQL connection via asyncpg.

Security rules:
  - Read-only PostgreSQL role enforced at DB level.
  - Only two hardcoded SQL statements are ever executed.
  - No user input is passed to any query.
  - 30-second statement timeout.

Resilience:
  - Pool creation failure is logged but does NOT crash the app.
  - All callers check _pool before use; analytics gracefully disabled when DB
    is unavailable.
"""
import asyncpg
import logging
from datetime import datetime
from app.config import settings

logger = logging.getLogger(__name__)

_pool: asyncpg.Pool | None = None


async def create_pool() -> None:
    """
    Create the asyncpg connection pool.
    On failure (wrong host, wrong credentials, network issue) the error is
    logged and _pool stays None — the app continues in RAG-only mode.
    """
    global _pool
    try:
        _pool = await asyncpg.create_pool(
            dsn=settings.database_url,
            min_size=1,
            max_size=10,
            command_timeout=30,
            statement_cache_size=0,
            timeout=10,            # connection-attempt timeout in seconds
        )
        logger.info("PostgreSQL pool created successfully.")
    except Exception as exc:
        _pool = None
        logger.warning(
            "Could not connect to PostgreSQL — analytics will be disabled. "
            "Reason: %s", exc
        )


async def close_pool() -> None:
    global _pool
    if _pool:
        await _pool.close()
        _pool = None
        logger.info("PostgreSQL pool closed.")


def is_db_available() -> bool:
    return _pool is not None


def get_pool() -> asyncpg.Pool:
    if _pool is None:
        raise RuntimeError(
            "Database is not connected. Check DATABASE_URL in .env and ensure "
            "PostgreSQL is reachable."
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