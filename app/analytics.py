"""
app/analytics.py — LLM-based intent detection + safe DB calls.

Analytics queries supported:
  1. Total deployed ports         → fetch_total_ports_raw()
  2. Ports (filtered / grouped)   → fetch_ports()  [unified modular query]
       • scalar total, optionally filtered by locality and/or months
       • grouped by month
       • grouped by locality
       • grouped by month AND locality
  3. Delivered addresses          → fetch_addresses()
       • full list, optionally filtered by locality and/or months
       • specific address lookup: step 1 with CONNECTION_ALLOWED,
         step 2 fallback surfaces actual smr_status if not delivered
  4. Objects status (SMR)         → fetch_objects_status()

Intent schema (v2)
──────────────────
  "total_ports"          — grand total, no parameters
  "ports"                — unified ports query:
       locality  str | None
       months    list[str] | None   (["YYYY-MM", …])
       group_by  "none" | "locality" | "month" | "both"
  "delivered_addresses"  — no parameters
  "objects_status"       — no parameters
  "unsupported"          — analytics question outside scope
  "none"                 — not an analytics question
"""
import logging
import json
from enum import Enum
from typing import Optional
from app.cache import cache_get, cache_set
from app.config import settings
from app.prompts.analytics_prompts import (
    INTENT_DETECTION,
    format_total_ports_prompt,
    format_ports_scalar_prompt,
    smr_status_label,
)

logger = logging.getLogger(__name__)


class AnalyticsIntent(str, Enum):
    TOTAL_PORTS        = "total_ports"
    PORTS              = "ports"            # unified modular intent
    DELIVERED_ADDRESSES = "delivered_addresses"
    OBJECTS_STATUS     = "objects_status"
    UNSUPPORTED        = "unsupported"
    NONE               = "none"


# ---------------------------------------------------------------------------
# Intent detection
# ---------------------------------------------------------------------------

async def detect_analytics_intent(
    question: str,
) -> tuple[AnalyticsIntent, Optional[dict]]:
    """
    Use the LLM to detect analytics intent from the question.

    Returns:
        (intent, parameters)  — parameters is a dict or None.
        For the "ports" intent, parameters contains:
            locality  str | None
            months    list[str] | None
            group_by  "none" | "locality" | "month" | "both"
    """
    from app.llm import _build_llm

    llm = _build_llm(streaming=False)
    prompt = INTENT_DETECTION.format(question=question)
    messages = [
        {"role": "system", "content": prompt},
        {"role": "user",   "content": question},
    ]

    try:
        response = await llm.ainvoke(messages)
        parsed = json.loads(response.content.strip())
        intent_str = parsed.get("intent", "none").lower()
        parameters = parsed.get("parameters", {})

        try:
            intent = AnalyticsIntent(intent_str)
        except ValueError:
            intent = AnalyticsIntent.NONE

        if intent == AnalyticsIntent.PORTS:
            return intent, parameters or {}

        if intent == AnalyticsIntent.DELIVERED_ADDRESSES:
            return intent, parameters or {}

        return intent, {}

    except json.JSONDecodeError:
        logger.error("LLM returned invalid JSON for intent detection")
        return AnalyticsIntent.NONE, None
    except Exception as exc:
        logger.error("Intent detection failed: %s", exc)
        return AnalyticsIntent.NONE, None


# ---------------------------------------------------------------------------
# Data-fetch helpers (with caching)
# ---------------------------------------------------------------------------

async def get_total_ports() -> int:
    cached = cache_get("total_ports")
    if cached is not None:
        logger.info("Cache hit: total_ports")
        return cached
    from app.database import fetch_total_ports_raw
    value = await fetch_total_ports_raw()
    cache_set("total_ports", value, settings.cache_ttl_seconds)
    return value


async def get_ports(
    locality: Optional[str] = None,
    months: Optional[list[str]] = None,
    group_by_locality: bool = False,
    group_by_month: bool = False,
):
    """
    Cached wrapper around database.fetch_ports().

    Cache key encodes all four axes so every distinct combination is cached
    independently.
    """
    key_months = ",".join(sorted(months)) if months else "all"
    key_loc    = locality or "all"
    key_gb     = f"gl{int(group_by_locality)}gm{int(group_by_month)}"
    cache_key  = f"ports_{key_loc}_{key_months}_{key_gb}"

    cached = cache_get(cache_key)
    if cached is not None:
        logger.info("Cache hit: %s", cache_key)
        return cached

    from app.database import fetch_ports
    value = await fetch_ports(
        locality=locality,
        months=months,
        group_by_locality=group_by_locality,
        group_by_month=group_by_month,
    )
    cache_set(cache_key, value, settings.cache_ttl_seconds)
    return value


async def get_delivered_addresses(
    locality: Optional[str] = None,
    months: Optional[list[str]] = None,
    address_search: Optional[str] = None,
) -> dict:
    """
    Cached wrapper around database.fetch_addresses().

    Returns the full dict:  {"rows": [...], "not_found_rows": [...] | None}

    Cache key encodes all three filter axes.  When address_search is present
    we skip caching (the result is already specific and likely not repeated).
    """
    if address_search:
        # Skip cache for specific address lookups — not worth the key complexity.
        from app.database import fetch_addresses
        return await fetch_addresses(
            locality=locality, months=months, address_search=address_search
        )

    key_loc    = locality or "all"
    key_months = ",".join(sorted(months)) if months else "all"
    cache_key  = f"delivered_addresses_{key_loc}_{key_months}"

    cached = cache_get(cache_key)
    if cached is not None:
        logger.info("Cache hit: %s", cache_key)
        return cached

    from app.database import fetch_addresses
    value = await fetch_addresses(locality=locality, months=months)
    cache_set(cache_key, value, settings.cache_ttl_seconds)
    return value


async def get_objects_status() -> dict:
    cache_key = "objects_status"
    cached = cache_get(cache_key)
    if cached is not None:
        logger.info("Cache hit: %s", cache_key)
        return cached
    from app.database import fetch_objects_status
    value = await fetch_objects_status()
    cache_set(cache_key, value, settings.cache_ttl_seconds)
    return value


# ---------------------------------------------------------------------------
# Markdown formatters
# ---------------------------------------------------------------------------

_MONTH_NAMES = {
    "01": "Январь",  "02": "Февраль", "03": "Март",
    "04": "Апрель",  "05": "Май",     "06": "Июнь",
    "07": "Июль",    "08": "Август",  "09": "Сентябрь",
    "10": "Октябрь", "11": "Ноябрь",  "12": "Декабрь",
}


def _month_label(ym: str) -> str:
    """Convert "2026-02" → "Февраль 2026"."""
    if ym and len(ym) == 7 and ym[4] == "-":
        return f"{_MONTH_NAMES.get(ym[5:7], ym[5:7])} {ym[:4]}"
    return ym


def _format_ports_by_month_markdown(
    rows: list[dict],
    locality: Optional[str] = None,
) -> str:
    if not rows:
        return "**Данных нет.**"

    header = (
        f"**Сданные порты по месяцам — {locality}:**"
        if locality else
        "**Сданные порты по месяцам:**"
    )
    lines = [header, "", "| Месяц | Порты |", "|---|---:|"]
    total = 0
    for r in rows:
        p = int(r.get("ports") or 0)
        total += p
        lines.append(f"| {_month_label(r['month'])} | {p:,} |")
    lines += ["", f"**Итого:** {total:,}"]
    return "\n".join(lines)


def _format_ports_by_locality_markdown(
    rows: list[dict],
    locality: Optional[str] = None,
    months: Optional[list[str]] = None,
    limit: int = 50,
) -> str:
    if not rows:
        return "**Данных нет.**"

    # Build context subtitle
    parts = []
    if months:
        parts.append(", ".join(_month_label(m) for m in sorted(months)))
    subtitle = f" ({', '.join(parts)})" if parts else ""

    # Single-city, single-row view
    if locality and len(rows) == 1:
        r = rows[0]
        return (
            f"**Сданные порты — {r['locality']}{subtitle}:**\n\n"
            f"| Населённый пункт | Порты |\n"
            f"|---|---:|\n"
            f"| {r['locality']} | {int(r.get('ports') or 0):,} |\n"
        )

    # Multi-row table
    header = f"**Сданные порты по городам{subtitle}:**"
    lines  = [header, "", "| Населённый пункт | Порты |", "|---|---:|"]
    total  = 0
    for r in rows[:limit]:
        p = int(r.get("ports") or 0)
        total += p
        lines.append(f"| {r['locality']} | {p:,} |")

    if len(rows) > limit:
        lines.append(f"\n_Показаны топ-{limit} по количеству портов._")

    lines += ["", f"**Итого (по показанным):** {total:,}"]
    return "\n".join(lines)


def _format_ports_both_markdown(
    rows: list[dict],
    months: Optional[list[str]] = None,
    limit: int = 100,
) -> str:
    """Ports grouped by month AND locality."""
    if not rows:
        return "**Данных нет.**"

    lines = ["**Сданные порты по месяцам и городам:**", "",
             "| Месяц | Населённый пункт | Порты |", "|---|---|---:|"]
    total = 0
    for r in rows[:limit]:
        p = int(r.get("ports") or 0)
        total += p
        lines.append(
            f"| {_month_label(r.get('month', ''))} | {r.get('locality', '')} | {p:,} |"
        )
    if len(rows) > limit:
        lines.append(f"\n_Показаны первые {limit} записей._")
    lines += ["", f"**Итого (по показанным):** {total:,}"]
    return "\n".join(lines)


def _format_delivered_addresses(
    rows: list[dict],
    locality: Optional[str] = None,
    months: Optional[list[str]] = None,
    limit: int = 50,
) -> str:
    """Format a list of delivered addresses as a Markdown table."""
    if not rows:
        return "**Сданных адресов не найдено.**"

    # Build a context subtitle for the header
    parts: list[str] = []
    if locality:
        parts.append(locality)
    if months:
        parts.append(", ".join(_month_label(m) for m in sorted(months)))
    subtitle = f" — {', '.join(parts)}" if parts else ""

    lines = [
        f"**Сданные адреса{subtitle}:**", "",
        "| Адрес | Населённый пункт | Порты | Дата сдачи |",
        "|---|---|---:|---|",
    ]
    for r in rows[:limit]:
        date_str = r["delivered_at"].date() if r.get("delivered_at") else "—"
        lines.append(
            f"| {r['address']} | {r['locality']} | {r['ports']} | {date_str} |"
        )
    if len(rows) > limit:
        lines.append(f"\n_Показаны первые {limit} записей._")
    return "\n".join(lines)


def _format_address_status(not_found_rows: list[dict]) -> str:
    """
    Format one or more addresses that were found in the DB but are NOT yet
    delivered (smr_status != CONNECTION_ALLOWED).
    """
    if not not_found_rows:
        return "**Адрес не найден в базе данных.**"

    lines: list[str] = []
    for r in not_found_rows:
        status_raw   = r.get("smr_status", "")
        status_label = smr_status_label(status_raw)
        delivered_at = r.get("delivered_at")
        date_str     = delivered_at.date() if delivered_at else None

        block = [
            f"**{r['address']}**",
            f"- Населённый пункт: {r['locality']}",
            f"- Количество портов: {r['ports']}",
            f"- Статус: **{status_label}**",
        ]
        if date_str:
            block.append(f"- Дата последнего статуса: {date_str}")
        lines.append("\n".join(block))

    return "\n\n---\n\n".join(lines)


def _format_objects_status_markdown(status: dict) -> str:
    delivered  = status.get("delivered", 0)
    in_progress = status.get("in_progress", 0)
    excluded   = status.get("excluded", 0)
    total      = delivered + in_progress + excluded

    return "\n".join([
        "**Статус проекта по СМР:**", "",
        "| Статус | Объекты |", "|---|---:|",
        f"| Сдано     | {delivered}   |",
        f"| В работе  | {in_progress} |",
        f"| Исключено | {excluded}    |",
        f"| **Всего** | **{total}**   |",
    ])


# ---------------------------------------------------------------------------
# LLM response formatter
# ---------------------------------------------------------------------------

async def _format_analytics_response(
    intent: AnalyticsIntent,
    result,
    parameters: Optional[dict] = None,
) -> str:
    from app.llm import _build_llm

    llm = _build_llm(streaming=False)

    if intent == AnalyticsIntent.TOTAL_PORTS:
        prompt = format_total_ports_prompt(result)
    elif intent == AnalyticsIntent.PORTS:
        locality = (parameters or {}).get("locality")
        months   = (parameters or {}).get("months")
        prompt   = format_ports_scalar_prompt(result, locality, months)
    else:
        return ""

    try:
        response = await llm.ainvoke([{"role": "user", "content": prompt}])
        return response.content.strip()
    except Exception as exc:
        logger.error("Failed to format analytics response with LLM: %s", exc)
        return f"**Итого:** {result:,}"


# ---------------------------------------------------------------------------
# Main resolver
# ---------------------------------------------------------------------------

async def resolve_analytics(
    intent: AnalyticsIntent,
    parameters: Optional[dict] = None,
) -> str:
    """
    Execute the analytics query and return a Markdown-formatted response.
    Handles DB unavailability gracefully.
    """
    from app.database import is_db_available

    if intent == AnalyticsIntent.UNSUPPORTED:
        return (
            "**Этот MVP поддерживает следующие аналитические запросы:**\n\n"
            "- Всего развёрнутых портов\n"
            "- Количество портов (с фильтрами по городу и/или месяцам,\n"
            "  с группировкой по городам, по месяцам или по обоим)\n"
            "- Сданные адреса\n"
            "- Статус проекта по СМР (сдано, в работе, исключено)\n\n"
            "Пожалуйста, задайте один из этих вопросов."
        )

    if not is_db_available():
        return (
            "**Аналитика в настоящее время недоступна.**\n\n"
            "Не удалось подключиться к базе данных. "
            "Пожалуйста, проверьте `DATABASE_URL` в вашем файле `.env`."
        )

    params = parameters or {}

    try:
        # ── total_ports ───────────────────────────────────────────────────
        if intent == AnalyticsIntent.TOTAL_PORTS:
            try:
                ports = await get_total_ports()
                if ports == 0:
                    return (
                        "**Данные не найдены.**\n\n"
                        "Не найдено развёрнутых портов. Проверьте:\n"
                        "- Адреса со статусом 'CONNECTION_ALLOWED'\n"
                        "- Включены ли они в сетевой дизайн (not excluded)"
                    )
                return await _format_analytics_response(intent, ports)
            except Exception as exc:
                logger.error("Failed to fetch total ports: %s", exc)
                return (
                    "**Не удалось получить данные по портам.**\n\n"
                    "Попробуйте уточнить: по месяцам, по городам, или за конкретный период?"
                )

        # ── ports (unified modular intent) ───────────────────────────────
        if intent == AnalyticsIntent.PORTS:
            locality  = params.get("locality")       or None
            months    = params.get("months")         or None
            group_by  = params.get("group_by", "none")

            group_by_locality = group_by in ("locality", "both")
            group_by_month    = group_by in ("month",    "both")

            try:
                result = await get_ports(
                    locality=locality,
                    months=months,
                    group_by_locality=group_by_locality,
                    group_by_month=group_by_month,
                )
            except Exception as exc:
                logger.error("Failed to fetch ports: %s", exc)
                return (
                    "**Не удалось получить данные по портам.**\n\n"
                    "Проверьте правильность названия города и формат дат."
                )

            # ── scalar result (group_by = "none") ────────────────────────
            if not group_by_locality and not group_by_month:
                if result == 0:
                    ctx = []
                    if locality:
                        ctx.append(f"город: **{locality}**")
                    if months:
                        ctx.append(
                            f"месяцы: **{', '.join(_month_label(m) for m in months)}**"
                        )
                    ctx_str = "; ".join(ctx) if ctx else "заданным фильтрам"
                    return (
                        f"**Данные не найдены.**\n\n"
                        f"Для {ctx_str} портов не обнаружено.\n\n"
                        "Проверьте правильность названия города и период."
                    )
                return await _format_analytics_response(intent, result, params)

            # ── grouped result ────────────────────────────────────────────
            if not result:
                return "**Данных нет.**\n\nПо заданным фильтрам записей не найдено."

            if group_by_locality and group_by_month:
                return _format_ports_both_markdown(result, months=months)

            if group_by_month:
                return _format_ports_by_month_markdown(result, locality=locality)

            if group_by_locality:
                return _format_ports_by_locality_markdown(
                    result, locality=locality, months=months
                )

        # ── delivered_addresses ───────────────────────────────────────────
        if intent == AnalyticsIntent.DELIVERED_ADDRESSES:
            locality       = params.get("locality")       or None
            months         = params.get("months")         or None
            address_search = params.get("address_search") or None

            try:
                result = await get_delivered_addresses(
                    locality=locality,
                    months=months,
                    address_search=address_search,
                )
            except Exception as exc:
                logger.error("Failed to fetch delivered addresses: %s", exc)
                return "**Не удалось получить список сданных адресов.**"

            rows           = result.get("rows", [])
            not_found_rows = result.get("not_found_rows")

            # ── Case 1: delivered rows found ──────────────────────────────
            if rows:
                return _format_delivered_addresses(
                    rows, locality=locality, months=months
                )

            # ── Case 2: specific address search, address exists but not
            #            delivered → show actual smr_status ─────────────────
            if address_search and not_found_rows:
                return _format_address_status(not_found_rows)

            # ── Case 3: nothing at all ────────────────────────────────────
            if address_search:
                return (
                    f"**Адрес не найден.**\n\n"
                    f"Поиск по запросу «{address_search}» не дал результатов.\n\n"
                    "Проверьте правильность написания названия улицы и номера дома."
                )

            # No address_search — filtered list was simply empty
            ctx_parts: list[str] = []
            if locality:
                ctx_parts.append(f"город **{locality}**")
            if months:
                ctx_parts.append(
                    f"месяцы **{', '.join(_month_label(m) for m in months)}**"
                )
            ctx_str = " и ".join(ctx_parts) if ctx_parts else "заданным фильтрам"
            return (
                f"**Сданные адреса не найдены.**\n\n"
                f"По фильтрам: {ctx_str} — нет адресов со статусом «сдан».\n\n"
                "Проверьте правильность названия города и диапазона дат."
            )

        # ── objects_status ────────────────────────────────────────────────
        if intent == AnalyticsIntent.OBJECTS_STATUS:
            try:
                status = await get_objects_status()
                total  = sum(status.values())
                if total == 0:
                    return (
                        "**Статус объектов не определён.**\n\n"
                        "Объектов в базе данных не найдено."
                    )
                return _format_objects_status_markdown(status)
            except Exception as exc:
                logger.error("Failed to fetch objects status: %s", exc)
                return "**Не удалось получить статус проекта.**"

    except Exception as exc:
        logger.error("Analytics query failed: %s", exc)
        return (
            "**Неожиданная ошибка при выполнении аналитического запроса.**\n\n"
            f"Ошибка: `{exc}`\n\n"
            "Пожалуйста, повторите вопрос или попробуйте другой запрос."
        )

    return ""