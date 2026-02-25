"""
app/cache.py — Minimal in-memory TTL cache for analytics results.
"""
import time
from typing import Any

_store: dict[str, tuple[Any, float]] = {}   # key → (value, expiry_timestamp)


def cache_get(key: str) -> Any | None:
    entry = _store.get(key)
    if entry is None:
        return None
    value, expiry = entry
    if time.time() > expiry:
        del _store[key]
        return None
    return value


def cache_set(key: str, value: Any, ttl_seconds: int) -> None:
    _store[key] = (value, time.time() + ttl_seconds)


def cache_clear() -> None:
    _store.clear()
