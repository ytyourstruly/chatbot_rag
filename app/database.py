"""
app/database.py — Async PostgreSQL connection via asyncpg.

Security rules:
  - Read-only PostgreSQL role enforced at DB level.
  - SQL structure is code-controlled; only scalar values reach parameterised
    placeholders — no user input is ever interpolated into the query string.
  - 30-second statement timeout.

Resilience:
  - Pool creation failure is logged but does NOT crash the app.
  - All callers check _pool before use; analytics gracefully disabled when DB
    is unavailable.

Modularity — ports:
  - _build_ports_query() assembles a single parameterised SQL statement from
    optional filters (locality, months) and grouping flags.
  - fetch_ports() is the single entry-point for all port-count queries.
  - fetch_ports_by_month() and fetch_ports_by_locality() are thin wrappers
    that call fetch_ports() with the appropriate grouping flags.
  - fetch_ports_by_locality_period() has been removed; pass locality + months
    to fetch_ports() directly instead.

Modularity — addresses:
  - _build_addresses_query() builds a parameterised SELECT for delivered
    addresses; accepts optional locality, months, and address_search filters.
    When include_all_statuses=True it omits the smr_status filter and exposes
    the raw smr_status column (used for the "not found / wrong status" path).
  - fetch_addresses() orchestrates a two-step lookup:
      Step 1 — query with smr_status = 'CONNECTION_ALLOWED'.
               If rows are found, return them as delivered addresses.
      Step 2 — only when address_search is given and step 1 returned nothing:
               re-query without the status filter to surface the actual
               smr_status value, so the caller can show a human-readable
               "this address is IN_PROGRESS" message instead of a blank result.
"""
import asyncpg
import logging
from app.config import settings

logger = logging.getLogger(__name__)

_pool: asyncpg.Pool | None = None


# ---------------------------------------------------------------------------
# Pool lifecycle
# ---------------------------------------------------------------------------

async def create_pool() -> None:
    """
    Create the asyncpg connection pool.
    On failure the error is logged and _pool stays None — the app continues
    in RAG-only mode.
    """
    global _pool
    try:
        _pool = await asyncpg.create_pool(
            dsn=settings.database_url,
            min_size=1,
            max_size=10,
            command_timeout=30,
            statement_cache_size=0,
            timeout=10,
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
# Modular query builder
# ---------------------------------------------------------------------------

def _build_ports_query(
    locality: str | None = None,
    months: list[str] | None = None,   # e.g. ["2026-01", "2026-02"]
    group_by_locality: bool = False,
    group_by_month: bool = False,
) -> tuple[str, list]:
    """
    Build a fully-parameterised ports-count SQL query.

    The query structure (column list, GROUP BY, ORDER BY) is determined
    entirely by Python flags — never by user input.  User-supplied values
    (locality name, month strings) are passed as asyncpg positional
    parameters ($1, $2, …) so SQL injection is impossible.

    Returns:
        (sql_string, params_list) ready for conn.fetch() / conn.fetchval()
    """
    params: list = []
    idx = 1  # asyncpg uses $1, $2, …

    # ── SELECT columns ──────────────────────────────────────────────────────
    select_cols: list[str] = []
    if group_by_locality:
        select_cols.append("a.locality AS locality")
    if group_by_month:
        select_cols.append("to_char(h.status_date_time, 'YYYY-MM') AS month")
    select_cols.append("SUM(a.ports_count) AS ports")

    # ── Static WHERE clauses ─────────────────────────────────────────────────
    where_clauses: list[str] = [
        "a.smr_status = 'CONNECTION_ALLOWED'",
        "h.status_date_time IS NOT NULL",
        "h.status_id = '3'",
        "n.excluded = 'false'",
    ]
    if group_by_locality:
        # exclude rows where locality is blank so they don't pollute grouping
        where_clauses.append("a.locality IS NOT NULL")
        where_clauses.append("btrim(a.locality) <> ''")

    # ── Dynamic WHERE clauses (parameterised) ────────────────────────────────
    if locality:
        where_clauses.append(f"a.locality = ${idx}")
        params.append(locality)
        idx += 1

    if months:
        # Pass the list as a native PostgreSQL text[] array; asyncpg handles
        # the conversion automatically, so no string interpolation is needed.
        where_clauses.append(
            f"to_char(h.status_date_time, 'YYYY-MM') = ANY(${idx}::text[])"
        )
        params.append(months)
        idx += 1

    # ── GROUP BY / ORDER BY ──────────────────────────────────────────────────
    group_cols: list[str] = []
    if group_by_locality:
        group_cols.append("a.locality")
    if group_by_month:
        group_cols.append("to_char(h.status_date_time, 'YYYY-MM')")

    where_block = "\n  AND ".join(where_clauses)
    sql = (
        f"SELECT {', '.join(select_cols)}\n"
        f"FROM contractor_service.address a\n"
        f"JOIN contractor_service.address_smr_status_history h\n"
        f"  ON a.id = h.address_id\n"
        f"JOIN contractor_service.network_design_address n\n"
        f"  ON n.address_id = a.id\n"
        f"WHERE {where_block}\n"
    )

    if group_cols:
        sql += f"GROUP BY {', '.join(group_cols)}\n"
        # Month-grouped results are chronological; locality results are by volume.
        if group_by_month and not group_by_locality:
            sql += "ORDER BY to_char(h.status_date_time, 'YYYY-MM') ASC\n"
        elif group_by_locality and not group_by_month:
            sql += "ORDER BY ports DESC\n"
        else:
            # both dimensions: month outer, locality inner
            sql += (
                "ORDER BY to_char(h.status_date_time, 'YYYY-MM') ASC, "
                "ports DESC\n"
            )

    return sql, params


# ---------------------------------------------------------------------------
# Unified port-count entry-point
# ---------------------------------------------------------------------------

async def fetch_ports(
    locality: str | None = None,
    months: list[str] | None = None,
    group_by_locality: bool = False,
    group_by_month: bool = False,
) -> "int | list[dict]":
    """
    Unified, composable port-count query.

    Behaviour matrix
    ────────────────────────────────────────────────────────
    group_by_locality  group_by_month  return type
    False              False           int   (scalar total)
    False              True            list[{month, ports}]
    True               False           list[{locality, ports}]
    True               True            list[{month, locality, ports}]
    ────────────────────────────────────────────────────────

    Optional filters
    ────────────────
    locality  — restrict to a single city name
    months    — restrict to a list of "YYYY-MM" strings
                (both filters compose with any grouping)

    Composition examples
    ────────────────────
    "ports in Astana in February"
        → fetch_ports(locality="Астана", months=["2026-02"])
          returns int

    "ports by city in February"
        → fetch_ports(months=["2026-02"], group_by_locality=True)
          returns list[{locality, ports}]

    "ports by month in Astana"
        → fetch_ports(locality="Астана", group_by_month=True)
          returns list[{month, ports}]

    "ports by city in January and February"
        → fetch_ports(months=["2026-01","2026-02"], group_by_locality=True)
          returns list[{locality, ports}]
    """
    sql, params = _build_ports_query(locality, months, group_by_locality, group_by_month)

    async with get_pool().acquire() as conn:
        if group_by_locality or group_by_month:
            rows = await conn.fetch(sql, *params)
            result: list[dict] = []
            for r in rows:
                row: dict = {"ports": int(r["ports"] or 0)}
                if group_by_locality:
                    row["locality"] = r["locality"]
                if group_by_month:
                    row["month"] = r["month"]
                result.append(row)
            return result
        else:
            val = await conn.fetchval(sql, *params)
            return int(val) if val is not None else 0


# ---------------------------------------------------------------------------
# Named wrappers (backwards-compatible & self-documenting)
# ---------------------------------------------------------------------------

async def fetch_total_ports() -> int:
    """Total ports across all localities, all time."""
    return await fetch_total_ports_raw()


async def fetch_total_ports_raw() -> int:
    """
    Uses the original, simpler query (no history join) for the grand total.
    Kept separate because it does not require address_smr_status_history.
    """
    _SQL = """
SELECT SUM(a.ports_count) AS ports
FROM contractor_service.address a
JOIN contractor_service.network_design_address n
  ON n.address_id = a.id
WHERE a.smr_status = 'CONNECTION_ALLOWED'
  AND n.excluded = 'false';
"""
    async with get_pool().acquire() as conn:
        result = await conn.fetchval(_SQL)
    return int(result) if result is not None else 0


async def fetch_ports_by_month(
    locality: str | None = None,
    months: list[str] | None = None,
) -> list[dict]:
    """
    Ports grouped by month.

    Optionally filtered by:
      locality — restrict to one city
      months   — restrict to specific months (e.g. ["2026-01", "2026-02"])
    """
    return await fetch_ports(
        locality=locality,
        months=months,
        group_by_month=True,
    )


async def fetch_ports_by_locality(
    locality: str | None = None,
    months: list[str] | None = None,
) -> list[dict]:
    """
    Ports grouped by locality.

    Optionally filtered by:
      locality — narrow to a single city (returns one-row list)
      months   — restrict to specific months
    """
    return await fetch_ports(
        locality=locality,
        months=months,
        group_by_locality=True,
    )


# ---------------------------------------------------------------------------
# Addresses — modular query builder
# ---------------------------------------------------------------------------

# Human-readable labels for every known smr_status value.
# Used by the application layer to format "not delivered" messages.
SMR_STATUS_LABELS: dict[str, str] = {
    "CONNECTION_ALLOWED": "сдан",
    "SMR_COMPLETED":      "СМР завершён, ведутся работы по вводу в эксплуатацию",
    "IN_PROGRESS":        "в работе (ведутся СМР)",
    "NOT_STARTED":        "строительные работы не начаты",
    "ON_CHECK":           "на проверке для подключения абонентов",
}


def _build_addresses_query(
    locality: str | None = None,
    months: list[str] | None = None,
    address_search: str | None = None,
    include_all_statuses: bool = False,
) -> tuple[str, list]:
    """
    Build a parameterised SQL query for delivered (or all-status) addresses.

    Parameters
    ──────────
    locality            — filter to a specific city name
    months              — filter to a list of "YYYY-MM" strings
    address_search      — partial address name; tokenised and matched with one
                          ILIKE '%token%' clause per whitespace-separated token,
                          all AND-ed together.
                          This handles the DB format "улица Сарайшык, 4" when
                          the user types "Сарайшык 4": each token ("Сарайшык",
                          "4") is checked independently, so the comma in the
                          stored name is irrelevant.
    include_all_statuses — when False (default) only rows with
                           smr_status = 'CONNECTION_ALLOWED' are returned;
                           when True the filter is omitted and smr_status is
                           included in SELECT (used for the fallback path).

    Returns (sql, params) ready for conn.fetch().
    """
    params: list = []
    idx = 1

    # ── SELECT ───────────────────────────────────────────────────────────────
    if include_all_statuses:
        select_block = (
            "  a.name       AS address_name,\n"
            "  a.locality,\n"
            "  a.ports_count,\n"
            "  a.smr_status,\n"
            "  MIN(h.status_date_time) AS delivered_at"
        )
    else:
        select_block = (
            "  a.name       AS address_name,\n"
            "  a.locality,\n"
            "  a.ports_count,\n"
            "  MIN(h.status_date_time) AS delivered_at"
        )

    # ── Static WHERE ─────────────────────────────────────────────────────────
    where_clauses: list[str] = ["n.excluded = 'false'"]

    if not include_all_statuses:
        where_clauses += [
            "a.smr_status = 'CONNECTION_ALLOWED'",
            "h.status_date_time IS NOT NULL",
            "h.status_id = '3'",
        ]

    # ── Dynamic WHERE (parameterised) ────────────────────────────────────────
    if locality:
        where_clauses.append(f"a.locality = ${idx}")
        params.append(locality)
        idx += 1

    if months:
        where_clauses.append(
            f"to_char(h.status_date_time, 'YYYY-MM') = ANY(${idx}::text[])"
        )
        params.append(months)
        idx += 1

    if address_search:
        # Split the search string into tokens and add one ILIKE clause per token,
        # all AND-ed together.  This handles the mismatch between user input like
        # "Сарайшык 4" and the DB value "улица Сарайшык, 4": each token is matched
        # independently so punctuation in the stored name is irrelevant.
        # Every token is a separate $N parameter — no string interpolation.
        tokens = [t for t in address_search.split() if t]
        for token in tokens:
            where_clauses.append(f"a.name ILIKE ${idx}")
            params.append(f"%{token}%")
            idx += 1

    where_block = "\n  AND ".join(where_clauses)

    # ── GROUP BY / ORDER BY ──────────────────────────────────────────────────
    if include_all_statuses:
        group_block   = "GROUP BY a.id, a.name, a.locality, a.ports_count, a.smr_status"
    else:
        group_block   = "GROUP BY a.id, a.name, a.locality, a.ports_count"

    # Join strategy: for include_all_statuses we LEFT JOIN history so addresses
    # that have never had any history entry still appear.
    if include_all_statuses:
        join_block = (
            "LEFT JOIN contractor_service.address_smr_status_history h\n"
            "  ON a.id = h.address_id\n"
            "JOIN contractor_service.network_design_address n\n"
            "  ON n.address_id = a.id"
        )
    else:
        join_block = (
            "JOIN contractor_service.address_smr_status_history h\n"
            "  ON a.id = h.address_id\n"
            "JOIN contractor_service.network_design_address n\n"
            "  ON n.address_id = a.id"
        )

    sql = (
        f"SELECT\n{select_block}\n"
        f"FROM contractor_service.address a\n"
        f"{join_block}\n"
        f"WHERE {where_block}\n"
        f"{group_block}\n"
        f"ORDER BY delivered_at DESC NULLS LAST;\n"
    )

    return sql, params


async def fetch_addresses(
    locality: str | None = None,
    months: list[str] | None = None,
    address_search: str | None = None,
) -> dict:
    """
    Two-step composable address lookup.

    Step 1 — query with smr_status = 'CONNECTION_ALLOWED' (+ optional filters).
             If rows are found, return them under key "rows".

    Step 2 — only executed when address_search is provided AND step 1 found
             nothing: re-queries without the status filter to surface the
             actual smr_status, returned under key "not_found_rows".
             Each not_found_row contains {address_name, locality, ports_count,
             smr_status}.

    Returns:
        {
            "rows":           list[dict],        # delivered addresses (may be empty)
            "not_found_rows": list[dict] | None  # status-only rows when step 2 ran
        }
    """
    # ── Step 1: delivered addresses ──────────────────────────────────────────
    sql1, params1 = _build_addresses_query(
        locality=locality,
        months=months,
        address_search=address_search,
        include_all_statuses=False,
    )
    async with get_pool().acquire() as conn:
        raw1 = await conn.fetch(sql1, *params1)

    rows = [
        {
            "address":      r["address_name"],
            "locality":     r["locality"],
            "ports":        int(r["ports_count"] or 0),
            "delivered_at": r["delivered_at"],
        }
        for r in raw1
    ]

    # ── Step 2: fallback status lookup (only for specific address searches) ──
    not_found_rows = None
    if address_search and not rows:
        sql2, params2 = _build_addresses_query(
            locality=locality,
            months=None,          # months irrelevant for a status lookup
            address_search=address_search,
            include_all_statuses=True,
        )
        async with get_pool().acquire() as conn:
            raw2 = await conn.fetch(sql2, *params2)

        if raw2:
            not_found_rows = [
                {
                    "address":      r["address_name"],
                    "locality":     r["locality"],
                    "ports":        int(r["ports_count"] or 0),
                    "smr_status":   r["smr_status"],
                    "delivered_at": r["delivered_at"],  # may be None
                }
                for r in raw2
            ]

    return {"rows": rows, "not_found_rows": not_found_rows}


# Thin backwards-compatible wrapper kept for any legacy call sites.
async def fetch_delivered_addresses(
    locality: str | None = None,
    months: list[str] | None = None,
    address_search: str | None = None,
) -> list[dict]:
    result = await fetch_addresses(locality=locality, months=months, address_search=address_search)
    return result["rows"]


# ---------------------------------------------------------------------------
# Objects-status queries (unchanged)
# ---------------------------------------------------------------------------

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


async def fetch_objects_status() -> dict:
    """
    Fetch project status by SMR: delivered, in progress, excluded objects count.
    """
    async with get_pool().acquire() as conn:
        delivered  = await conn.fetchval(_SQL_OBJECTS_DELIVERED)
        in_progress = await conn.fetchval(_SQL_OBJECTS_IN_PROGRESS)
        excluded   = await conn.fetchval(_SQL_OBJECTS_EXCLUDED)

    return {
        "delivered":   int(delivered)   if delivered   is not None else 0,
        "in_progress": int(in_progress) if in_progress is not None else 0,
        "excluded":    int(excluded)    if excluded    is not None else 0,
    }