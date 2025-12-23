import logging
import os
import json
import time
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Optional
from uuid import UUID

import redis

_TRUE_STRINGS = {"true", "1", "yes", "y", "on"}


class CacheBackendUnavailable(RuntimeError):
    """Raised when the cache backend is unavailable and fallback is disabled."""

logger = logging.getLogger(__name__)

_redis_client = None
_local_store: dict[str, tuple[Any, Optional[float]]] = {}
_ALLOW_LOCAL_FALLBACK = (
    os.environ.get("ALLOW_LOCAL_CACHE_FALLBACK", "false").strip().lower()
    in _TRUE_STRINGS
)
_metrics = {
    "hits": 0,
    "misses": 0,
    "fallback_hits": 0,
    "fallback_misses": 0,
    "unavailable_sets": 0,
    "unavailable_gets": 0,
    "unavailable_deletes": 0,
}
_last_evicted_keys: int | None = None


def _evict_expired_local() -> None:
    """Remove expired entries from the local fallback store."""

    now = time.time()
    expired_keys = [k for k, (_, expiry) in _local_store.items() if expiry and now > expiry]
    for key in expired_keys:
        _local_store.pop(key, None)


def _configure_redis_limits(client) -> None:
    """Configure Redis memory caps and eviction strategy."""

    maxmemory = os.environ.get("REDIS_MAXMEMORY", "256mb")
    policy = os.environ.get("REDIS_MAXMEMORY_POLICY", "allkeys-lfu")
    try:
        client.config_set("maxmemory", maxmemory)
        client.config_set("maxmemory-policy", policy)
        logger.info(
            "Redis memory limit configured (maxmemory=%s, policy=%s)",
            maxmemory,
            policy,
        )
    except Exception as exc:  # pragma: no cover - configuration edge
        logger.warning("Unable to configure Redis memory policy: %s", exc)


def _record_redis_evictions(client) -> None:
    """Log when Redis evicts keys so we can keep memory bounded."""

    global _last_evicted_keys
    try:
        stats = client.info("stats")
        evicted_keys = int(stats.get("evicted_keys", 0))
    except Exception:  # pragma: no cover - defensive
        return

    if _last_evicted_keys is None:
        _last_evicted_keys = evicted_keys
        return

    if evicted_keys > _last_evicted_keys:
        delta = evicted_keys - _last_evicted_keys
        logger.warning("Redis evicted %s keys since last check", delta)
    _last_evicted_keys = evicted_keys
    
def _json_default(value: Any) -> Any:
    """Serialize non-JSON primitives for :func:`json.dumps`."""

    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, UUID):
        return str(value)
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")

def _get_client():
    global _redis_client
    if _redis_client is None:
        url = os.environ.get("REDIS_URL") or "redis://localhost:6379/0"
        try:
            _redis_client = redis.from_url(url, decode_responses=True)
            _redis_client.ping()
            _configure_redis_limits(_redis_client)
            _record_redis_evictions(_redis_client)
            logger.debug("Connected to Redis at %s", url)
        except Exception as exc:  # pragma: no cover - connection errors
            logger.error(
                "Redis unavailable; caching will degrade%s: %s",
                " with local fallback" if _ALLOW_LOCAL_FALLBACK else " and operations will raise",
                exc,
            )
            _redis_client = False  # type: ignore
    return _redis_client if _redis_client not in (None, False) else None


def cache_set(key: str, value: Any, ttl: int | None = None) -> None:
    client = _get_client()
    serialized = json.dumps(value, default=_json_default)
    if client:
        client.set(name=key, value=serialized, ex=ttl)
        _record_redis_evictions(client)
        return
    _metrics["unavailable_sets"] += 1
    if not _ALLOW_LOCAL_FALLBACK:
        raise CacheBackendUnavailable("Cache backend unavailable for cache_set")

    _evict_expired_local()
    expiry = time.time() + ttl if ttl else None
    _local_store[key] = (serialized, expiry)
    logger.warning(
        "cache_set using local fallback for key %s; consistency is degraded", key
    )


def cache_get(key: str) -> Optional[Any]:
    client = _get_client()
    if client:
        data = client.get(key)
        if data is None:
            _metrics["misses"] += 1
            logger.debug("cache_miss redis key=%s", key)
            return None
        try:
            value = json.loads(data)
        except json.JSONDecodeError:
            value = data
        _metrics["hits"] += 1
        logger.debug("cache_hit redis key=%s", key)
        _record_redis_evictions(client)
        return value
    _metrics["unavailable_gets"] += 1
    if not _ALLOW_LOCAL_FALLBACK:
        raise CacheBackendUnavailable("Cache backend unavailable for cache_get")

    _evict_expired_local()
    entry = _local_store.get(key)
    if not entry:
        _metrics["fallback_misses"] += 1
        logger.debug("cache_miss local key=%s", key)
        return None
    value, expiry = entry
    if expiry and time.time() > expiry:
        del _local_store[key]
        _metrics["fallback_misses"] += 1
        return None
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return value
    _metrics["fallback_hits"] += 1
    logger.debug("cache_hit local key=%s", key)
    return value


def cache_delete(key: str) -> None:
    client = _get_client()
    if client:
        client.delete(key)
        return
    _metrics["unavailable_deletes"] += 1
    if not _ALLOW_LOCAL_FALLBACK:
        raise CacheBackendUnavailable("Cache backend unavailable for cache_delete")
    _local_store.pop(key, None)


def cache_clear(prefix: str = "") -> None:
    client = _get_client()
    if client:
        if prefix:
            for k in client.scan_iter(f"{prefix}*"):
                client.delete(k)
        else:
            client.flushdb()
    else:
        return
    if not _ALLOW_LOCAL_FALLBACK:
        raise CacheBackendUnavailable("Cache backend unavailable for cache_clear")
    if prefix:
        for k in list(_local_store.keys()):
            if k.startswith(prefix):
                del _local_store[k]
    else:
        _local_store.clear()
