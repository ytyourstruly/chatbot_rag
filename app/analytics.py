"""
app/analytics.py — LLM-based intent detection + safe DB calls.

Analytics queries supported:
  1. Total deployed ports  → SUM(contractor_service.address.ports_count)
  2. Ports by locality and period  → SUM(contractor_service.address.ports_count) WHERE locality = ? AND status_date_time range
  3. Ports by month  → grouped by month
  4. Ports by locality  → grouped by locality
  5. Delivered addresses  → list with delivery dates
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
    format_ports_by_locality_period_prompt,
)

logger = logging.getLogger(__name__)


class AnalyticsIntent(str, Enum):
    TOTAL_PORTS  = "total_ports"
    PORTS_BY_LOCALITY_PERIOD = "ports_by_locality_period"
    PORTS_BY_MONTH = "ports_by_month"
    PORTS_BY_LOCALITY = "ports_by_locality"
    DELIVERED_ADDRESSES = "delivered_addresses"
    OBJECTS_STATUS = "objects_status"
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

async def get_delivered_addresses():
    cache_key = "delivered_addresses"
    cached = cache_get(cache_key)
    if cached is not None:
        logger.info("Cache hit: %s", cache_key)
        return cached

    from app.database import fetch_delivered_addresses
    value = await fetch_delivered_addresses()
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

def _format_delivered_addresses(rows: list[dict], limit: int = 50) -> str:
    if not rows:
        return "**Сданных адресов не найдено.**"

    lines = [
        "**Сданные адреса:**",
        "",
        "| Адрес | Населённый пункт | Порты | Дата сдачи |",
        "|---|---|---:|---|",
    ]

    for r in rows[:limit]:
        lines.append(
            f"| {r['address']} | {r['locality']} | {r['ports']} | {r['delivered_at'].date()} |"
        )

    if len(rows) > limit:
        lines.append(f"\n_Показаны первые {limit} записей._")

    return "\n".join(lines)

def _format_objects_status_markdown(status: dict) -> str:
    """Format project status by SMR as markdown table."""
    delivered = status.get("delivered", 0)
    in_progress = status.get("in_progress", 0)
    excluded = status.get("excluded", 0)
    total = delivered + in_progress + excluded
    
    lines = [
        "**Статус проекта по СМР:**",
        "",
        "| Статус | Объекты |",
        "|---|---:|",
        f"| Сдано | {delivered} |",
        f"| В работе | {in_progress} |",
        f"| Исключено | {excluded} |",
        f"| **Всего** | **{total}** |",
    ]
    
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
    
    if intent == AnalyticsIntent.TOTAL_PORTS:
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
    Handles DB unavailability gracefully with clarifying questions.
    
    Args:
        intent: The detected analytics intent
        parameters: Optional dict with query parameters (e.g., locality, start_date, end_date)
    """
    from app.database import is_db_available

    if intent == AnalyticsIntent.UNSUPPORTED:
        return (
            "**Этот MVP поддерживает следующие аналитические запросы:**\n\n"
            "- Всего развернутых портов\n"
            "- Количество портов по городу и периоду\n"
            "- Количество портов по месяцам\n"
            "- Количество портов по городам\n"
            "- Сданные адреса\n"
            "- Статус проекта по СМР (сдано, в работе, исключено)\n\n"
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
        if intent == AnalyticsIntent.TOTAL_PORTS:
            try:
                ports = await get_total_ports()
                if ports == 0:
                    return (
                        "**Данные не найдены.**\n\n"
                        "Не найдено развёрнутых портов в базе данных. "
                        "Проверьте, пожалуйста:\n"
                        "- Есть ли адреса со статусом 'CONNECTION_ALLOWED'?\n"
                        "- Включены ли они в сетевой дизайн (not excluded)?"
                    )
                return await _format_analytics_response(intent, ports)
            except Exception as exc:
                logger.error("Failed to fetch total ports: %s", exc)
                return (
                    "**Не удалось получить данные по портам.**\n\n"
                    "Возможно вопрос неточен. Вы имели в виду:\n"
                    "- Сколько портов в каждом месяце?\n"
                    "- Сколько портов в каждом городе?\n"
                    "- Сколько портов сдано в конкретном городе и периоде?"
                )

        if intent == AnalyticsIntent.PORTS_BY_MONTH:
            try:
                rows = await get_ports_by_month()
                if not rows:
                    return (
                        "**Данные по месяцам не найдены.**\n\n"
                        "В базе данных нет информации о портах по месяцам. "
                        "Проверьте, пожалуйста:\n"
                        "- Есть ли история статусов (status_date_time)?\n"
                        "- Все ли адреса имеют статус 'CONNECTION_ALLOWED' и включены в дизайн?"
                    )
                return _format_ports_by_month_markdown(rows)
            except Exception as exc:
                logger.error("Failed to fetch ports by month: %s", exc)
                return (
                    "**Не удалось получить данные по месяцам.**\n\n"
                    "Возможно вопрос неточен. Вы имели в виду:\n"
                    "- Портов в определённом городе за месяц?\n"
                    "- Общее количество портов?\n"
                    "- Сданные адреса в конкретный период?"
                )
        
        if intent == AnalyticsIntent.PORTS_BY_LOCALITY:
            try:
                rows = await get_ports_by_locality()
                if not rows:
                    return (
                        "**Данные по городам не найдены.**\n\n"
                        "В базе данных нет информации о портах по городам. "
                        "Проверьте, пожалуйста:\n"
                        "- Заполнены ли названия городов (locality) в адресах?\n"
                        "- Есть ли адреса со статусом 'CONNECTION_ALLOWED'?"
                    )
                return _format_ports_by_locality_markdown(rows)
            except Exception as exc:
                logger.error("Failed to fetch ports by locality: %s", exc)
                return (
                    "**Не удалось получить данные по городам.**\n\n"
                    "Возможно вопрос неточен. Вы имели в виду:\n"
                    "- Портов в конкретном городе за период?\n"
                    "- Статус проекта (сдано, в работе, исключено)?\n"
                    "- Сданные адреса в определённом городе?"
                )
        
        if intent == AnalyticsIntent.DELIVERED_ADDRESSES:
            try:
                rows = await get_delivered_addresses()
                if not rows:
                    return (
                        "**Сданные адреса не найдены.**\n\n"
                        "Нет информации о доставленных адресах. "
                        "Проверьте, пожалуйста:\n"
                        "- Есть ли адреса со статусом 'CONNECTION_ALLOWED'?\n"
                        "- Заполнены ли даты статусов (status_date_time)?"
                    )
                return _format_delivered_addresses(rows)
            except Exception as exc:
                logger.error("Failed to fetch delivered addresses: %s", exc)
                return (
                    "**Не удалось получить список сданных адресов.**\n\n"
                    "Возможно вопрос неточен. Вы имели в виду:\n"
                    "- Портов в конкретном городе?\n"
                    "- Портов в определённый период времени?\n"
                    "- Статус проекта по СМР?"
                )
        
        if intent == AnalyticsIntent.OBJECTS_STATUS:
            try:
                status = await get_objects_status()
                total = status.get("delivered", 0) + status.get("in_progress", 0) + status.get("excluded", 0)
                if total == 0:
                    return (
                        "**Статус объектов не определён.**\n\n"
                        "В базе данных не найдено объектов. "
                        "Проверьте, пожалуйста:\n"
                        "- Есть ли адреса в сетевом дизайне (network_design_address)?\n"
                        "- Корректны ли статусы адресов (smr_status)?"
                    )
                return _format_objects_status_markdown(status)
            except Exception as exc:
                logger.error("Failed to fetch objects status: %s", exc)
                return (
                    "**Не удалось получить статус проекта.**\n\n"
                    "Возможно вопрос неточен. Вы имели в виду:\n"
                    "- Сколько портов в целом?\n"
                    "- Портов по городам?\n"
                    "- Сданные адреса в определённый период?"
                )
                
        if intent == AnalyticsIntent.PORTS_BY_LOCALITY_PERIOD:
            if not parameters:
                return (
                    "**Ошибка:** Не удалось извлечь параметры из вопроса.\n\n"
                    "Пожалуйста, уточните:\n"
                    "- Какой город (например, 'Астана')?\n"
                    "- Какой период? (например, 'с 1 января по 28 февраля 2026')"
                )
            
            locality = parameters.get("locality")
            start_date = parameters.get("start_date")
            end_date = parameters.get("end_date")
            
            if not all([locality, start_date, end_date]):
                missing = []
                if not locality:
                    missing.append("город (locality)")
                if not start_date:
                    missing.append("дата начала (start_date)")
                if not end_date:
                    missing.append("дата окончания (end_date)")
                
                return (
                    f"**Неполные параметры.**\n\n"
                    f"Отсутствуют: {', '.join(missing)}.\n\n"
                    "Пожалуйста, укажите явно:\n"
                    f"- Город: {locality or '?'}\n"
                    f"- Начало периода: {start_date or 'YYYY-MM-DD'}\n"
                    f"- Окончание периода: {end_date or 'YYYY-MM-DD'}\n\n"
                    "Пример: 'Порты в Астане с 1 января до 31 декабря 2026'"
                )
            
            try:
                ports = await get_ports_by_locality_period(locality, start_date, end_date)
                if ports == 0:
                    return (
                        f"**Данные не найдены.**\n\n"
                        f"Для города '{locality}' в период {start_date} - {end_date} "
                        f"не найдено портов.\n\n"
                        "Проверьте, пожалуйста:\n"
                        f"- Верно ли написано название города?\n"
                        f"- Есть ли адреса в этом городе?\n"
                        f"- Попадают ли они в указанный период?"
                    )
                return await _format_analytics_response(intent, ports, parameters)
            except Exception as exc:
                logger.error("Failed to fetch ports by locality/period: %s", exc)
                return (
                    f"**Не удалось получить данные для города '{locality}'.**\n\n"
                    "Возможные причины:\n"
                    "- Город не найден в базе данных\n"
                    "- Неверный формат даты\n"
                    "- Нет данных за этот период\n\n"
                    "Пожалуйста, уточните:\n"
                    "- Правильное название города\n"
                    "- Точный диапазон дат"
                )
    
    except Exception as exc:
        logger.error("Analytics query failed: %s", exc)
        return (
            "**Неожиданная ошибка при выполнении аналитического запроса.**\n\n"
            f"Ошибка: `{exc}`\n\n"
            "Пожалуйста, повторите вопрос или попробуйте другой запрос. "
            "Если проблема сохранится, проверьте подключение к базе данных."
        )

    return ""