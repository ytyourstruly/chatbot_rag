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
        
        # Return parameters only for intents that need them
        if intent in (AnalyticsIntent.PORTS_BY_LOCALITY_PERIOD, AnalyticsIntent.PORTS_BY_LOCALITY):
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
    
    This is the most granular query (most specific parameters).
    
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

async def get_ports_by_month(locality: Optional[str] = None) -> list[dict]:
    """
    Fetch ports grouped by month across all localities (or filtered by locality).
    
    If locality is provided, uses PORTS_BY_LOCALITY_PERIOD to fetch
    ports for each month in that locality.
    
    Args:
        locality: Optional city name to filter by (filters the monthly breakdown)
    
    Returns:
        List of dicts with 'month' and 'ports' keys
    """
    # Use cache key based on whether we're filtering by locality
    cache_key = f"ports_by_month_{locality}" if locality else "ports_by_month"
    cached = cache_get(cache_key)
    if cached is not None:
        logger.info("Cache hit: %s", cache_key)
        return cached

    from app.database import fetch_ports_by_month
    
    if locality is None:
        # Get all months across all localities
        value = await fetch_ports_by_month()
    else:
        # Filter by locality: fetch monthly data for specific city
        # We need to query each month for this locality
        all_months_data = await fetch_ports_by_month()
        
        # Build a mapping of months we have data for
        months_to_query = []
        for r in all_months_data:
            month = r.get("month")
            if month:
                months_to_query.append(month)
        
        if not months_to_query:
            value = []
        else:
            # Query each month/year for this specific locality
            value = []
            for month_str in months_to_query:  # Format: "2026-02"
                try:
                    year, month_num = month_str.split("-")
                    start_date = f"{year}-{month_num}-01"
                    # Calculate end date (last day of month)
                    from calendar import monthrange
                    last_day = monthrange(int(year), int(month_num))[1]
                    end_date = f"{year}-{month_num}-{last_day:02d}"
                    
                    ports = await get_ports_by_locality_period(locality, start_date, end_date)
                    value.append({"month": month_str, "ports": ports})
                except Exception as e:
                    logger.error(f"Failed to query month {month_str} for locality {locality}: {e}")
    
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
    
    # Month names in Russian
    month_names = {
        "01": "Январь",
        "02": "Февраль",
        "03": "Март",
        "04": "Апрель",
        "05": "Май",
        "06": "Июнь",
        "07": "Июль",
        "08": "Август",
        "09": "Сентябрь",
        "10": "Октябрь",
        "11": "Ноябрь",
        "12": "Декабрь",
    }
    
    for r in rows:
        m = r["month"]  # Format: "2026-02"
        p = int(r["ports"] or 0)
        total += p
        
        # Convert "2026-02" to "Февраль 2026"
        if m and len(m) == 7 and m[4] == "-":
            year = m[:4]
            month_num = m[5:7]
            month_name = month_names.get(month_num, m)
            formatted_month = f"{month_name} {year}"
        else:
            formatted_month = m
        
        lines.append(f"| {formatted_month} | {p} |")

    lines += ["", f"**Итого:** {total}"]
    return "\n".join(lines)

def _format_ports_by_locality_markdown(rows: list[dict], limit: int = 50, locality: Optional[str] = None) -> str:
    if not rows:
        return "**Данных нет.**"

    # Single locality view (one row)
    if locality and len(rows) == 1:
        r = rows[0]
        p = int(r["ports"] or 0)
        loc = str(r["locality"])
        return (
            f"**Сданные порты в городе {loc}:**\n\n"
            f"| Населённый пункт | Порты |\n"
            f"|---|---:|\n"
            f"| {loc} | {p} |\n"
        )
    
    # Multiple localities view (table)
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

async def get_ports_by_locality(locality: Optional[str] = None, start_date: Optional[str] = None, end_date: Optional[str] = None) -> list[dict]:
    """
    Fetch ports by locality. Can return all localities or filter by specific locality.
    
    If start_date and end_date are provided, uses PORTS_BY_LOCALITY_PERIOD
    to get accurate data for the time period. Otherwise, uses the default
    all-time data from PORTS_BY_LOCALITY.
    
    Args:
        locality: Optional city name to filter by single locality, or None for all localities
        start_date: Optional start date in YYYY-MM-DD format (enables period filtering)
        end_date: Optional end date in YYYY-MM-DD format (enables period filtering)
    
    Returns:
        List of dicts with locality and ports count
    """
    # If date range is provided, use period-based query
    if start_date and end_date:
        if locality:
            # Single locality + period: use PORTS_BY_LOCALITY_PERIOD
            ports_count = await get_ports_by_locality_period(locality, start_date, end_date)
            return [{"locality": locality, "ports": ports_count}]
        else:
            # All localities + period: need to get all localities then query each
            all_localities_data = await get_ports_by_locality()
            localities = [r.get("locality") for r in all_localities_data if r.get("locality")]
            
            value = []
            for loc in localities:
                try:
                    ports = await get_ports_by_locality_period(loc, start_date, end_date)
                    if ports > 0:
                        value.append({"locality": loc, "ports": ports})
                except Exception as e:
                    logger.error(f"Failed to query locality {loc} for period {start_date}-{end_date}: {e}")
            
            # Sort by ports descending
            value.sort(key=lambda x: x["ports"], reverse=True)
            return value
    
    # No date range: use all-time locality data
    cache_key = f"ports_by_locality_{locality}" if locality else "ports_by_locality"
    cached = cache_get(cache_key)
    if cached is not None:
        logger.info("Cache hit: %s", cache_key)
        return cached

    from app.database import fetch_ports_by_locality
    all_rows = await fetch_ports_by_locality()
    
    # Filter by locality if specified
    if locality:
        value = [r for r in all_rows if r.get("locality") == locality]
    else:
        value = all_rows
    
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
    Uses a modular, hierarchical approach where functions call each other
    to fill in missing information.
    
    Hierarchy:
    1. PORTS_BY_LOCALITY_PERIOD (most specific: locality + dates)
    2. PORTS_BY_LOCALITY (middle: locality only, or all localities + dates)
    3. PORTS_BY_MONTH (high-level: months across all/specific locality)
    
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

        if intent == AnalyticsIntent.PORTS_BY_LOCALITY_PERIOD:
            # Most specific query: requires both locality and date range
            if not parameters or not parameters.get("locality") or not parameters.get("start_date") or not parameters.get("end_date"):
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
                # Call the most specific function
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
                    f"**Не удалось получить данные для города {locality}.**\n\n"
                    "Возможные причины:\n"
                    "- Город не найден в базе данных\n"
                    "- Неверный формат даты\n"
                    "- Нет данных за этот период\n\n"
                    "Пожалуйста, уточните:\n"
                    "- Правильное название города\n"
                    "- Точный диапазон дат"
                )

        if intent == AnalyticsIntent.PORTS_BY_MONTH:
            try:
                # If locality is provided in parameters, filter by that locality
                locality = parameters.get("locality") if parameters else None
                rows = await get_ports_by_month(locality=locality)
                
                if not rows:
                    if locality:
                        return (
                            f"**Данные по месяцам для города '{locality}' не найдены.**\n\n"
                            "В базе данных нет информации о портах по месяцам в этом городе. "
                            "Проверьте, пожалуйста:\n"
                            f"- Верно ли написано название города '{locality}'?\n"
                            "- Есть ли адреса в этом городе со статусом 'CONNECTION_ALLOWED'?"
                        )
                    else:
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
                # Extract optional parameters
                locality = parameters.get("locality") if parameters else None
                start_date = parameters.get("start_date") if parameters else None
                end_date = parameters.get("end_date") if parameters else None
                
                # Call with modular approach: if dates are provided, use them
                rows = await get_ports_by_locality(locality=locality, start_date=start_date, end_date=end_date)
                
                if not rows:
                    if locality:
                        return (
                            f"**Данные для города '{locality}' не найдены.**\n\n"
                            "В базе данных нет информации о портах в этом городе. "
                            "Проверьте, пожалуйста:\n"
                            f"- Верно ли написано название города '{locality}'?\n"
                            "- Есть ли адреса в этом городе со статусом 'CONNECTION_ALLOWED'?"
                        )
                    else:
                        return (
                            "**Данные по городам не найдены.**\n\n"
                            "В базе данных нет информации о портах по городам. "
                            "Проверьте, пожалуйста:\n"
                            "- Заполнены ли названия городов (locality) в адресах?\n"
                            "- Есть ли адреса со статусом 'CONNECTION_ALLOWED'?"
                        )
                return _format_ports_by_locality_markdown(rows, locality=locality)
            except Exception as exc:
                logger.error("Failed to fetch ports by locality: %s", exc)
                return (
                    "**Не удалось получить данные по городам.**\n\n"
                    "Возможно вопрос неточен. Вы имели в виду:\n"
                    "- Портов в конкретном городе за период (укажите даты)?\n"
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
    
    except Exception as exc:
        logger.error("Analytics query failed: %s", exc)
        return (
            "**Неожиданная ошибка при выполнении аналитического запроса.**\n\n"
            f"Ошибка: `{exc}`\n\n"
            "Пожалуйста, повторите вопрос или попробуйте другой запрос. "
            "Если проблема сохранится, проверьте подключение к базе данных."
        )

    return ""