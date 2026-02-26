"""
app/analytics.py — LLM-based intent detection + safe DB calls.

Analytics queries supported:
  1. Total contract value  → SUM(amount)
  2. Total deployed ports  → SUM(total_ports_count)
  3. Ports by locality and period  → SUM(total_ports_count) WHERE localities = ? AND (start_date, end_date)
"""
import logging
import json
from enum import Enum
from typing import Optional
from app.cache import cache_get, cache_set
from app.config import settings
from app.prompts.analytics_prompts import (
    INTENT_DETECTION,
    format_total_amount_prompt,
    format_total_ports_prompt,
    format_ports_by_locality_period_prompt,
)

logger = logging.getLogger(__name__)


class AnalyticsIntent(str, Enum):
    TOTAL_AMOUNT = "total_amount"
    TOTAL_PORTS  = "total_ports"
    PORTS_BY_LOCALITY_PERIOD = "ports_by_locality_period"
    PORTS_BY_MONTH = "ports_by_month"
    PORTS_BY_LOCALITY = "ports_by_locality"
    UNSUPPORTED  = "unsupported"
    NONE         = "none"


async def detect_analytics_intent(question: str) -> tuple[AnalyticsIntent, Optional[dict]]:
    """
    Use LLM to detect analytics intent from the question.
    
    Returns:
        tuple of (intent, parameters)
        - intent: the detected AnalyticsIntent
        - parameters: dict with extracted parameters (e.g., locality, start_date, end_date) or None
    """
    from app.llm import _build_llm
    
    llm = _build_llm(streaming=False)
    
    intent_detection_prompt = INTENT_DETECTION.format(question=question)

    messages = [
        {"role": "system", "content": intent_detection_prompt},
        {"role": "user", "content": question},
    ]
    
    try:
        response = await llm.ainvoke(messages)
        response_text = response.content.strip()
        
        # Parse JSON response
        parsed = json.loads(response_text)
        intent_str = parsed.get("intent", "none").lower()
        parameters = parsed.get("parameters", {})
        
        # Map to AnalyticsIntent enum
        try:
            intent = AnalyticsIntent(intent_str)
        except ValueError:
            intent = AnalyticsIntent.NONE
        
        # Return parameters only for supported intents
        if intent in (AnalyticsIntent.PORTS_BY_LOCALITY_PERIOD, AnalyticsIntent.PORTS_BY_MONTH):
            return intent, parameters or {}
        
        return intent, {}

    except json.JSONDecodeError:
        logger.error("LLM returned invalid JSON for intent detection: %s", response_text)
        return AnalyticsIntent.NONE, None
    except Exception as exc:
        logger.error("Intent detection failed: %s", exc)
        return AnalyticsIntent.NONE, None


async def get_total_amount() -> float:
    cached = cache_get("total_amount")
    if cached is not None:
        logger.info("Cache hit: total_amount")
        return cached
    from app.database import fetch_total_contract_amount
    value = await fetch_total_contract_amount()
    cache_set("total_amount", value, settings.cache_ttl_seconds)
    return value


async def get_total_ports() -> int:
    cached = cache_get("total_ports")
    if cached is not None:
        logger.info("Cache hit: total_ports")
        return cached
    from app.database import fetch_total_ports
    value = await fetch_total_ports()
    cache_set("total_ports", value, settings.cache_ttl_seconds)
    return value


async def get_ports_by_locality_period(
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
    cache_key = f"ports_{locality}_{start_date}_{end_date}"
    cached = cache_get(cache_key)
    if cached is not None:
        logger.info("Cache hit: %s", cache_key)
        return cached
    
    from app.database import fetch_ports_by_locality_period
    value = await fetch_ports_by_locality_period(locality, start_date, end_date)
    cache_set(cache_key, value, settings.cache_ttl_seconds)
    return value

async def get_ports_by_month() -> list[dict]:
    cache_key = "ports_by_month"
    cached = cache_get(cache_key)
    if cached is not None:
        logger.info("Cache hit: %s", cache_key)
        return cached

    from app.database import fetch_ports_by_month
    value = await fetch_ports_by_month()
    cache_set(cache_key, value, settings.cache_ttl_seconds)
    return value

def _format_ports_by_month_markdown(rows: list[dict]) -> str:
    if not rows:
        return "**Данных нет.**"

    lines = [
        "**Сданные порты по месяцам:**",
        "",
        "| Месяц | Порты |",
        "|---|---:|",
    ]
    total = 0
    for r in rows:
        m = r["month"]
        p = int(r["ports"] or 0)
        total += p
        lines.append(f"| {m} | {p} |")

    lines += ["", f"**Итого:** {total}"]
    return "\n".join(lines)

def _format_ports_by_locality_markdown(rows: list[dict], limit: int = 50) -> str:
    if not rows:
        return "**Данных нет.**"

    lines = [
        "**Сданные порты в разрезе городов и сел:**",
        "",
        "| Населённый пункт | Порты |",
        "|---|---:|",
    ]
    total = 0
    for r in rows[:limit]:
        loc = str(r["locality"])
        p = int(r["ports"] or 0)
        total += p
        lines.append(f"| {loc} | {p} |")

    if len(rows) > limit:
        lines.append(f"\n_Показаны топ-{limit} по количеству портов._")

    lines += ["", f"**Итого (по показанным):** {total}"]
    return "\n".join(lines)

async def get_ports_by_locality() -> list[dict]:
    cache_key = "ports_by_locality"
    cached = cache_get(cache_key)
    if cached is not None:
        logger.info("Cache hit: %s", cache_key)
        return cached

    from app.database import fetch_ports_by_locality
    value = await fetch_ports_by_locality()
    cache_set(cache_key, value, settings.cache_ttl_seconds)
    return value

async def _format_analytics_response(intent: AnalyticsIntent, result: any, parameters: Optional[dict] = None) -> str:
    """
    Use LLM to format the analytics result into a unique, well-written response.
    
    Args:
        intent: The analytics intent
        result: The query result (amount, ports count, etc.)
        parameters: Optional query parameters
    
    Returns:
        LLM-formatted markdown response
    """
    from app.llm import _build_llm
    
    llm = _build_llm(streaming=False)
    
    if intent == AnalyticsIntent.TOTAL_AMOUNT:
        prompt = format_total_amount_prompt(result)
    elif intent == AnalyticsIntent.TOTAL_PORTS:
        prompt = format_total_ports_prompt(result)
    elif intent == AnalyticsIntent.PORTS_BY_LOCALITY_PERIOD:
        locality = parameters.get("locality") if parameters else None
        start_date = parameters.get("start_date") if parameters else None
        end_date = parameters.get("end_date") if parameters else None
        prompt = format_ports_by_locality_period_prompt(locality, start_date, end_date, result)
    else:
        return ""
    
    try:
        messages = [{"role": "user", "content": prompt}]
        response = await llm.ainvoke(messages)
        return response.content.strip()
    except Exception as exc:
        logger.error("Failed to format analytics response with LLM: %s", exc)
        return f"Total result: {result}"


async def resolve_analytics(intent: AnalyticsIntent, parameters: Optional[dict] = None) -> str:
    """
    Execute the analytics query and return an LLM-formatted response.
    Handles DB unavailability gracefully.
    
    Args:
        intent: The detected analytics intent
        parameters: Optional dict with query parameters (e.g., locality, start_date, end_date)
    """
    from app.database import is_db_available

    if intent == AnalyticsIntent.UNSUPPORTED:
        return (
            "**Этот MVP поддерживает только три аналитических запроса:**\n\n"
            "- Общая стоимость контракта\n"
            "- Всего развернутых портов\n"
            "- Количество портов по городу и периоду\n\n"
            "- Количество портов по месяцам\n\n"
            "- Количество портов по городам\n\n"
            "Пожалуйста, задайте один из этих вопросов или спросите о документации."
        )

    if not is_db_available():
        return (
            "**Аналитика в настоящее время недоступна.**\n\n"
            "Не удалось подключиться к базе данных. "
            "Пожалуйста, проверьте `DATABASE_URL` в вашем файле `.env` и убедитесь, "
            "что PostgreSQL запущен и доступен."
        )

    try:
        if intent == AnalyticsIntent.TOTAL_AMOUNT:
            amount = await get_total_amount()
            return await _format_analytics_response(intent, amount)
        
        if intent == AnalyticsIntent.TOTAL_PORTS:
            ports = await get_total_ports()
            return await _format_analytics_response(intent, ports)

        if intent == AnalyticsIntent.PORTS_BY_MONTH:
            rows = await get_ports_by_month()
            return _format_ports_by_month_markdown(rows)
        
        if intent == AnalyticsIntent.PORTS_BY_LOCALITY:
            rows = await get_ports_by_locality()
            return _format_ports_by_locality_markdown(rows)
        
        if intent == AnalyticsIntent.PORTS_BY_LOCALITY_PERIOD:
            if not parameters:
                return "**Ошибка:** Не удалось извлечь параметры локальности и периода из вопроса."
            
            locality = parameters.get("locality")
            start_date = parameters.get("start_date")
            end_date = parameters.get("end_date")
            
            if not all([locality, start_date, end_date]):
                return (
                    "**Ошибка:** Не удалось определить город и период. "
                    "Пожалуйста, укажите явно город и диапазон дат (например, '2026-02-21' до '2026-08-20')."
                )
            
            ports = await get_ports_by_locality_period(locality, start_date, end_date)
            return await _format_analytics_response(intent, ports, parameters)
    
    except Exception as exc:
        logger.error("Analytics DB query failed: %s", exc)
        return (
            "**Не удалось получить данные аналитики.**\n\n"
            f"Ошибка базы данных: `{exc}`\n\n"
            "Пожалуйста, проверьте параметры подключения PostgreSQL."
        )

    return ""