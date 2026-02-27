"""
app/prompts/analytics_prompts.py — Prompts for analytics intent detection and response formatting.

Intent schema (v2)
──────────────────
The three old intents ports_by_locality_period / ports_by_month / ports_by_locality
have been merged into a single "ports" intent.  The LLM now returns:

  {
    "intent": "ports",
    "parameters": {
      "locality": "<city in Russian>  | null",
      "months":   ["YYYY-MM", …]      | null,
      "group_by": "none" | "locality" | "month" | "both"
    }
  }

"delivered_addresses" now accepts optional filters:
  {
    "intent": "delivered_addresses",
    "parameters": {
      "locality":       "<city in Russian> | null",
      "months":         ["YYYY-MM", …]     | null,
      "address_search": "<partial street + number> | null"
    }
  }

group_by semantics
  "none"     -> scalar total (optionally filtered by locality / months)
  "locality" -> list grouped by city      (optionally filtered)
  "month"    -> list grouped by month     (optionally filtered)
  "both"     -> list grouped by city AND month

Mapping from user questions -- ports
  "всего портов"                             -> total_ports  (unchanged)
  "портов в феврале в Астане"                -> ports  locality=Астана months=[2026-02] group_by=none
  "портов в феврале"                         -> ports  months=[2026-02]                group_by=none
  "портов в феврале по городам"              -> ports  months=[2026-02]                group_by=locality
  "портов по городам за январь и февраль"   -> ports  months=[2026-01,2026-02]        group_by=locality
  "портов по месяцам"                        -> ports  group_by=month
  "портов по месяцам в Астане"               -> ports  locality=Астана                 group_by=month

Mapping from user questions -- delivered_addresses
  "сданные адреса по городу Астана"          -> delivered_addresses  locality=Астана
  "сданные адреса за февраль"                -> delivered_addresses  months=[2026-02]
  "дай статус по Сарайшык 4"                -> delivered_addresses  address_search="Сарайшык 4"
  "адреса в Астане за январь"               -> delivered_addresses  locality=Астана months=[2026-01]
"""

INTENT_DETECTION = """You are an analytics intent detector for a Kazakhstan telecom contractor platform.
Analyse the user question and return a single JSON object describing what data they need.

==========================================================
AVAILABLE INTENTS
==========================================================

1. "total_ports"
   Grand total of deployed ports across all addresses and all time.
   No parameters needed.
   Trigger: "всего", "итого", "общее количество портов", "сколько всего портов"

2. "ports"
   Flexible port-count query with optional filters and grouping.
   Parameters:
     locality  - Russian city name, or null
     months    - list of "YYYY-MM" strings, or null
     group_by  - one of: "none" | "locality" | "month" | "both"

   group_by rules:
     "none"     -> return a single number (filtered totals)
     "locality" -> return a table broken down by city
     "month"    -> return a table broken down by month
     "both"     -> return a table broken down by city AND month

3. "delivered_addresses"
   List of delivered addresses, optionally filtered; or a status lookup for a
   specific address (which may or may not be delivered yet).
   Parameters:
     locality       - Russian city name, or null
     months         - list of "YYYY-MM" strings, or null
     address_search - partial street name + optional number extracted from the
                      question (e.g. "Сарайшык 4", "Степан Разин"), or null

   Trigger: "сданные адреса", "список адресов", "адреса по городу X",
            "адреса за [месяц]", "дай статус по [адресу]",
            "какой статус у [адреса]", "статус адреса [название]"

4. "objects_status"
   Project status by SMR: counts of delivered / in-progress / excluded objects.
   No parameters needed.
   Trigger: "статус проекта", "статус по СМР", "сдано / в работе / исключено"

5. "unsupported"
   The question is analytics-related but cannot be answered by the above.

6. "none"
   Not an analytics question at all.

==========================================================
DECISION RULES  (apply in order)
==========================================================

R1. "всего" / "итого" / "общее" AND no city/period AND no address -> "total_ports"

R2. Question mentions a specific street name, building, or address lookup
    ("статус по", "дай статус", "какой статус у") -> "delivered_addresses"
    Extract address_search from the street + optional number.
    Also extract locality and months if mentioned.

R3. Question mentions "сданные адреса" / "список адресов" / "адреса по" /
    "адреса за" -> "delivered_addresses"
    Extract locality and months if mentioned.

R4. Question contains port-count keywords AND a date / month / period?
    YES -> intent = "ports"
    Extract months list and locality (if mentioned).
    Set group_by:
      - city mentioned + no "по городам"  -> "none"
      - "по городам" or "по городу"       -> "locality"
      - "по месяцам"                       -> "month"
      - "по городам" AND "по месяцам"     -> "both"

R5. No date/month, but locality mentioned WITHOUT "по месяцам" AND no address
    -> intent = "ports", locality = <city>, months = null, group_by = "none"

R6. "по городам" / "по городу" with no date -> intent = "ports", group_by = "locality"

R7. "по месяцам" with no date -> intent = "ports", group_by = "month"

R8. "статус проекта" / "статус по СМР" -> "objects_status"

==========================================================
PARAMETER EXTRACTION
==========================================================

MONTHS LIST - convert every mentioned month to "YYYY-MM":
  - Single month with no year -> assume 2026
  - Range "с X по Y месяц"   -> expand to every month in range
  - "за январь и февраль"    -> ["2026-01", "2026-02"]
  - No month mentioned       -> null

MONTH MAPPING (2026):
  Январь   -> 2026-01    Февраль  -> 2026-02    Март     -> 2026-03
  Апрель   -> 2026-04    Май      -> 2026-05    Июнь     -> 2026-06
  Июль     -> 2026-07    Август   -> 2026-08    Сентябрь -> 2026-09
  Октябрь  -> 2026-10    Ноябрь   -> 2026-11    Декабрь  -> 2026-12

LOCALITY - CITY NAME ONLY. NEVER put a street name, avenue, building, or district here.
  These are city names: Астана, Алматы, Шымкент, Актобе, Тараз, Павлодар...
  These are NOT city names (they are street/address names): Сарайшык, Степан Разин, Бекарыс,
  Шакарим, проспект, улица, переулок — these belong in address_search, never in locality.

  ALWAYS return the city name in its Russian NOMINATIVE form (именительный падеж).
  City names are declined in speech; restore the base form before returning:

  Declined form in question        ->  Nominative form to return
  ────────────────────────────────────────────────────────────────
  "в Астане"  / "Астане"           ->  "Астана"
  "в Алмате"  / "Алматы" / "в Алматы" -> "Алматы"
  "в Шымкенте"                     ->  "Шымкент"
  "в Актобе"                       ->  "Актобе"
  "в Таразе"                       ->  "Тараз"
  "в Павлодаре"                    ->  "Павлодар"
  "в Усть-Каменогорске"            ->  "Усть-Каменогорск"
  "в Семее"                        ->  "Семей"
  "в Костанае"                     ->  "Костанай"
  "в Кызылорде"                    ->  "Кызылорда"
  "в Атырау"                       ->  "Атырау"
  "в Актау"                        ->  "Актау"
  "в Петропавловске"               ->  "Петропавловск"
  "в Туркестане"                   ->  "Туркестан"
  "в Кокшетау"                     ->  "Кокшетау"
  "в Талдыкоргане"                 ->  "Талдыкорган"
  "в Темиртау"                     ->  "Темиртау"

  For any other city not listed, still return the nominative form yourself.
  If no city is mentioned -> null.

ADDRESS_SEARCH - extract ONLY a street / building name + optional number.
  This is NEVER a city name. Strip navigation words like "по", "статус по",
  "адрес", "улица", "проспект" only when they are NOT part of the actual name.
  Keep the building number if present.
  Examples:
    "дай статус по Сарайшык 4"         -> "Сарайшык 4"
    "статус адреса Степан Разин 14/1"  -> "Степан Разин 14/1"
    "что с Бекарыс 5/1"                -> "Бекарыс 5/1"
    "адреса в Алмате"  (city, not street) -> address_search=null, locality="Алматы"
  If no specific street/building is mentioned  -> null.

==========================================================
EXAMPLES
==========================================================

  "сколько всего портов?"
  -> {{"intent":"total_ports","parameters":{{}}}}

  "сколько портов в феврале в Астане?"
  -> {{"intent":"ports","parameters":{{"locality":"Астана","months":["2026-02"],"group_by":"none"}}}}

  "сколько портов в феврале?"
  -> {{"intent":"ports","parameters":{{"locality":null,"months":["2026-02"],"group_by":"none"}}}}

  "сколько портов в феврале по городам?"
  -> {{"intent":"ports","parameters":{{"locality":null,"months":["2026-02"],"group_by":"locality"}}}}

  "сколько портов по городам за январь и февраль?"
  -> {{"intent":"ports","parameters":{{"locality":null,"months":["2026-01","2026-02"],"group_by":"locality"}}}}

  "сколько портов по месяцам?"
  -> {{"intent":"ports","parameters":{{"locality":null,"months":null,"group_by":"month"}}}}

  "сколько портов по месяцам в Астане?"
  -> {{"intent":"ports","parameters":{{"locality":"Астана","months":null,"group_by":"month"}}}}

  "порты в Астане"
  -> {{"intent":"ports","parameters":{{"locality":"Астана","months":null,"group_by":"none"}}}}

  "порты по городам"
  -> {{"intent":"ports","parameters":{{"locality":null,"months":null,"group_by":"locality"}}}}

  "сданные адреса"
  -> {{"intent":"delivered_addresses","parameters":{{"locality":null,"months":null,"address_search":null}}}}

  "сданные адреса по городу Астана"
  -> {{"intent":"delivered_addresses","parameters":{{"locality":"Астана","months":null,"address_search":null}}}}

  "сданные адреса за февраль"
  -> {{"intent":"delivered_addresses","parameters":{{"locality":null,"months":["2026-02"],"address_search":null}}}}

  "адреса в Астане за январь"
  -> {{"intent":"delivered_addresses","parameters":{{"locality":"Астана","months":["2026-01"],"address_search":null}}}}

  "сданные адреса в Алмате"
  -> {{"intent":"delivered_addresses","parameters":{{"locality":"Алматы","months":null,"address_search":null}}}}

  "порты в Алмате"
  -> {{"intent":"ports","parameters":{{"locality":"Алматы","months":null,"group_by":"none"}}}}

  "дай статус по Сарайшык 4"
  -> {{"intent":"delivered_addresses","parameters":{{"locality":null,"months":null,"address_search":"Сарайшык 4"}}}}

  "статус адреса Степан Разин 14/1"
  -> {{"intent":"delivered_addresses","parameters":{{"locality":null,"months":null,"address_search":"Степан Разин 14/1"}}}}

  "что с Бекарыс 5/1"
  -> {{"intent":"delivered_addresses","parameters":{{"locality":null,"months":null,"address_search":"Бекарыс 5/1"}}}}

  "статус проекта по СМР"
  -> {{"intent":"objects_status","parameters":{{}}}}

==========================================================

User question: "{question}"

Return ONLY valid JSON matching this exact schema - no extra text:

For "ports":
{{
  "intent": "ports",
  "parameters": {{
    "locality": "<city in Russian or null>",
    "months":   ["YYYY-MM", ...] or null,
    "group_by": "none" | "locality" | "month" | "both"
  }}
}}

For "delivered_addresses":
{{
  "intent": "delivered_addresses",
  "parameters": {{
    "locality":       "<city in Russian or null>",
    "months":         ["YYYY-MM", ...] or null,
    "address_search": "<partial address or null>"
  }}
}}

For all other intents, parameters may be an empty object {{}}.
"""


# ---------------------------------------------------------------------------
# SMR status -> human-readable Russian label
# (mirrors SMR_STATUS_LABELS in database.py -- keep in sync)
# ---------------------------------------------------------------------------

SMR_STATUS_LABELS: dict[str, str] = {
    "CONNECTION_ALLOWED": "сдан",
    "SMR_COMPLETED":      "СМР завершён, ведутся работы по вводу в эксплуатацию",
    "IN_PROGRESS":        "в работе (ведутся СМР)",
    "NOT_STARTED":        "строительные работы не начаты",
    "ON_CHECK":           "на проверке для подключения абонентов",
}


def smr_status_label(status: str) -> str:
    """Return a Russian label for an smr_status value, falling back to the raw value."""
    return SMR_STATUS_LABELS.get(status, status)


# ---------------------------------------------------------------------------
# City-name declension normaliser
# ---------------------------------------------------------------------------

# Maps every common declined (non-nominative) form of Kazakh city names back
# to the nominative form stored in the database.  The LLM is instructed to do
# this itself, but this dict acts as a deterministic safety net in case the LLM
# returns a declined form anyway.
_CITY_NOMINATIVE: dict[str, str] = {
    # Астана
    "астане": "Астана",
    # Алматы — multiple common variants
    "алмате": "Алматы",
    "алматы": "Алматы",
    "алма-ате": "Алматы",
    "алма-аты": "Алматы",
    # Шымкент
    "шымкенте": "Шымкент",
    "шимкенте": "Шымкент",
    "шимкент": "Шымкент",
    # Актобе
    "актобе": "Актобе",
    "актюбинске": "Актобе",
    # Тараз
    "таразе": "Тараз",
    # Павлодар
    "павлодаре": "Павлодар",
    # Усть-Каменогорск
    "усть-каменогорске": "Усть-Каменогорск",
    "оскемене": "Усть-Каменогорск",
    # Семей
    "семее": "Семей",
    "семипалатинске": "Семей",
    # Костанай
    "костанае": "Костанай",
    "кустанае": "Костанай",
    # Кызылорда
    "кызылорде": "Кызылорда",
    # Атырау
    "атырау": "Атырау",
    # Актау
    "актау": "Актау",
    # Петропавловск
    "петропавловске": "Петропавловск",
    # Туркестан
    "туркестане": "Туркестан",
    # Кокшетау
    "кокшетау": "Кокшетау",
    "кокшетауе": "Кокшетау",
    # Талдыкорган
    "талдыкоргане": "Талдыкорган",
    # Темиртау
    "темиртау": "Темиртау",
    "темиртауе": "Темиртау",
    # Рудный
    "рудном": "Рудный",
    # Жезказган
    "жезказгане": "Жезказган",
    # Экибастуз
    "экибастузе": "Экибастуз",
}


def normalize_locality(city: str | None) -> str | None:
    """
    Normalise a city name to its database-stored nominative form.

    The LLM is prompted to return nominative forms, but this function acts as
    a deterministic safety net for any slippage.  Lookup is case-insensitive;
    the original capitalisation of known cities is restored.

    Unknown values are returned unchanged (with leading/trailing whitespace
    stripped) so they can still be tried against the database.
    """
    if not city:
        return None
    normalised = _CITY_NOMINATIVE.get(city.strip().lower())
    return normalised if normalised else city.strip()


# ---------------------------------------------------------------------------
# Response-formatting prompts
# ---------------------------------------------------------------------------

def format_total_ports_prompt(ports: int) -> str:
    return f"""You are a helpful analytics assistant for a Kazakhstan telecom contractor platform.
Format the following total deployed ports result into a brief, informative response in Russian (Markdown format).
Include the number and a short explanation of what it represents.
Only answer with provided information.
Always refer to ports as "порты" in Russian. Say "сдано" instead of "установлено" or "развернуто". Be grammatically correct.
Total deployed ports: {ports:,}

Provide a concise, engaging response in Russian with proper Markdown formatting. Be unique and vary the phrasing."""


def format_ports_scalar_prompt(
    ports: int,
    locality: str | None,
    months: list[str] | None,
) -> str:
    """Prompt for a single filtered port-count result (group_by='none')."""
    parts = []
    if locality:
        parts.append(f"Город: {locality}")
    if months:
        parts.append(f"Месяцы: {', '.join(months)}")
    context = "\n".join(parts) if parts else "Фильтры: не применены"
    return f"""You are a helpful analytics assistant for a Kazakhstan telecom contractor platform.
Format the following port-count result into a brief, informative response in Russian (Markdown format).
Only answer with provided information.
Always refer to ports as "порты". Say "сдано" instead of "установлено". Be grammatically correct.

{context}
Итого портов сдано: {ports:,}

Provide a concise, engaging response in Russian with proper Markdown formatting. Vary the phrasing."""