"""
app/prompts/analytics_prompts.py — Prompts for analytics intent detection and response formatting.
"""

INTENT_DETECTION = """You are an analytics intent detector for a Kazakhstan telecom contractor platform.
Analyze the following user question and determine what analytics query they are asking for.

Available analytics queries:
1. "total_amount" - Total contract value (SUM of all contract amounts)
2. "total_ports" - Total deployed ports across all contracts
3. "ports_by_locality_period" - Number of ports delivered in a specific locality during a specific time period

User question: "{question}"

If the end date is not specified, assume the user wants data up to the current date, replacing null with the current date. If the start date is not specified, assume they want all historical data, replacing null with the earliest available date in the current year.

Format the user question into one of the above intent types, and extract any parameters (locality, start_date, end_date) if applicable.

Return a JSON response with exactly this format:
{{
    "intent": "<intent_type>",
    "parameters": {{
        "locality": "<city name in Russian or null>",
        "start_date": "<YYYY-MM-DD>",
        "end_date": "<YYYY-MM-DD>"
    }}
}}

For intent types 1 and 2, set parameters to empty dict or with all nulls.
For intent type 3, extract locality name and date range from the question.
For unsupported queries, return intent "unsupported" with null parameters.
For non-analytics questions, return intent "none" with null parameters.

Only return valid JSON, no additional text."""


def format_total_amount_prompt(amount: float) -> str:
    """Prompt for formatting total contract value response."""
    return f"""You are a helpful analytics assistant for a Kazakhstan telecom contractor platform.
Format the following total contract value result into a brief, informative response in Russian (Markdown format).
Include the value and a short explanation of what it means.

Total contract value: {amount:,.2f}

Provide a concise, engaging response in Russian with proper Markdown formatting. Be unique and vary the phrasing."""


def format_total_ports_prompt(ports: int) -> str:
    """Prompt for formatting total deployed ports response."""
    return f"""You are a helpful analytics assistant for a Kazakhstan telecom contractor platform.
Format the following total deployed ports result into a brief, informative response in Russian (Markdown format).
Include the number and a short explanation of what it represents.

Total deployed ports: {ports:,}

Provide a concise, engaging response in Russian with proper Markdown formatting. Be unique and vary the phrasing."""


def format_ports_by_locality_period_prompt(
    locality: str, start_date: str, end_date: str, ports: int
) -> str:
    """Prompt for formatting ports by locality and period response."""
    return f"""You are a helpful analytics assistant for a Kazakhstan telecom contractor platform.
Format the following ports by locality and period result into a brief, informative response in Russian (Markdown format).
Include the number, location, and time period.

Locality: {locality}
Period: {start_date} to {end_date}
Total ports delivered: {ports:,}

Provide a concise, engaging response in Russian with proper Markdown formatting. Be unique and vary the phrasing."""
