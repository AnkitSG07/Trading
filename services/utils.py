from __future__ import annotations

import os
from typing import Any, Dict


def get_trade_events_maxlen(default: int = 10000) -> int:
    """Return the configured ``trade_events`` retention size.

    The value is read from the ``TRADE_EVENTS_MAXLEN`` environment variable. If
    the variable is unset, empty or invalid, ``default`` is returned instead so
    callers always receive a positive integer.
    """

    raw_value = os.getenv("TRADE_EVENTS_MAXLEN")
    if raw_value is None or raw_value == "":
        return default

    try:
        parsed = int(raw_value)
        return parsed if parsed > 0 else default
    except ValueError:
        return default
        
def get_redis_url() -> str:
    """Return the Redis URL for Celery.

    The function checks the ``CELERY_BROKER_URL`` and ``REDIS_URL``
    environment variables and returns the first one found.  If neither
    variable is defined, a :class:`RuntimeError` is raised so the caller is
    forced to explicitly configure the connection.
    """

    url = os.environ.get("CELERY_BROKER_URL") or os.environ.get("REDIS_URL")
    if not url:
        raise RuntimeError("CELERY_BROKER_URL or REDIS_URL must be set")
    return url
    

def _decode_event(raw: Dict[Any, Any]) -> Dict[str, Any]:
    """Decode Redis bytes into a plain ``dict``.

    The Redis client returns byte strings for stream values. This helper
    normalises the payload by decoding UTF-8 strings and converting numeric
    fields to ``int`` where possible.
    """
    NO_INT_FIELDS = {"client_id", "master_id"}
    event: Dict[str, Any] = {}
    for k, v in raw.items():
        key = k.decode() if isinstance(k, bytes) else k
        if isinstance(v, bytes):
            decoded = v.decode()
            if key not in NO_INT_FIELDS:
                try:
                    v = int(decoded)
                except ValueError:
                    v = decoded
            else:
                v = decoded
        event[key] = v
    return event
