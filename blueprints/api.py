import base64
import gzip
import json
import logging
import os
import secrets
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from copy import deepcopy
from threading import Lock

from flask import Blueprint, jsonify, session, request
from kombu.exceptions import OperationalError
from redis.exceptions import RedisError
from helpers import (
    current_user,
    get_primary_account,
    order_mappings_for_user,
    normalize_position,
)
from models import Account, db, Strategy, StrategyLog, StrategySubscription, Trade
from functools import wraps
from datetime import datetime, timedelta

from cache import cache_delete, cache_get, cache_set, _json_default
from celery.exceptions import CeleryError

api_bp = Blueprint("api", __name__, url_prefix="/api")

logger = logging.getLogger(__name__)

def _add_strategy_log(
    strategy: Strategy, message: str, *, level: str = "INFO", performance=None
) -> None:
    """Persist a user-facing log entry for the strategy."""

    log = StrategyLog(
        strategy_id=strategy.id,
        level=level,
        message=message,
        performance=performance,
    )
    db.session.add(log)

_HOLDINGS_LOCK = Lock()

_SNAPSHOT_INTERVAL = timedelta(
    seconds=int(os.environ.get("DASHBOARD_SNAPSHOT_INTERVAL", "15"))
)
_HOLDINGS_INTERVAL = timedelta(
    seconds=int(os.environ.get("DASHBOARD_HOLDINGS_INTERVAL", "30"))
)
_BROKER_TIMEOUT_SECONDS = float(os.environ.get("BROKER_TIMEOUT_SECONDS", "4"))
_EXECUTOR = ThreadPoolExecutor(max_workers=4)

_SNAPSHOT_MAX_SHARD_BYTES = int(
    os.environ.get("SNAPSHOT_CACHE_MAX_SHARD_BYTES", 524288)
)
_SNAPSHOT_COMPRESS_THRESHOLD_BYTES = int(
    os.environ.get("SNAPSHOT_CACHE_COMPRESS_THRESHOLD_BYTES", 65536)
)
_SNAPSHOT_MAX_TTL_SECONDS = int(
    os.environ.get("SNAPSHOT_CACHE_MAX_TTL_SECONDS", 1800)
)

_SNAPSHOT_CACHE_METRICS = {
    "summary_hits": 0,
    "summary_misses": 0,
    "summary_bytes": 0,
    "charts_hits": 0,
    "charts_misses": 0,
    "charts_bytes": 0,
    "skipped_shards": 0,
}

_TRUE_STRINGS = {"true", "1", "yes", "y", "on"}
_FALSE_STRINGS = {"false", "0", "no", "n", "off"}


def _parse_bool(value):
    """Best-effort parsing of truthy and falsy values from common inputs."""

    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        text = value.strip().lower()
        if not text:
            return None
        if text in _TRUE_STRINGS:
            return True
        if text in _FALSE_STRINGS:
            return False
    return None


def _bool_or_default(value, default: bool) -> bool:
    parsed = _parse_bool(value)
    if parsed is None:
        return default
    return parsed


_BOOL_FIELDS = {
    "allow_auto_submit",
    "allow_live_trading",
    "allow_any_ticker",
    "notify_failures_only",
    "is_active",
    "track_performance",
    "is_public",
}

def snapshot_cache_key_for(user_id: int, client_id: str | None) -> str:
    client_key = client_id or "primary"
    return f"snapshot:{user_id}:{client_key}"


def login_required_api(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not session.get("user"):
            return jsonify({"error": "Unauthorized"}), 401
        return fn(*args, **kwargs)


    return wrapper


def _snapshot_cache_key(account: Account) -> str:
    return snapshot_cache_key_for(account.user_id, account.client_id)


def _snapshot_shard_key(base_key: str, shard: str) -> str:
    """Return a namespaced key for a snapshot shard (summary, charts, etc.)."""

    prefix, _, remainder = base_key.partition(":")
    if prefix == "snapshot" and remainder:
        return f"{prefix}:{shard}:{remainder}"
    return f"{base_key}:{shard}"


def _holdings_cache_key(account: Account) -> str:
    client_id = account.client_id or "primary"
    return f"holdings:{account.user_id}:{client_id}"


def _holdings_ttl_seconds(
    *, ttl_override: int | None = None, interval_seconds: int | None = None
) -> int:
    """Return a holdings TTL that keeps entries alive between refreshes."""

    if ttl_override is None:
        env_ttl = os.environ.get("HOLDINGS_CACHE_TTL_SECONDS")
        ttl_override = int(env_ttl) if env_ttl else None

    interval_seconds = interval_seconds or int(_HOLDINGS_INTERVAL.total_seconds())
    interval_seconds = max(interval_seconds, 1)
    minimum_sla_seconds = int(os.environ.get("HOLDINGS_CACHE_MIN_SLA_SECONDS", 300))

    ttl = max(interval_seconds * 6, minimum_sla_seconds)
    if ttl_override is not None:
        return max(ttl_override, minimum_sla_seconds)
    return ttl


def _holdings_payload_limit(default: int = 524288) -> int:
    """Return the maximum payload size (bytes) allowed for holdings entries."""

    env_cap = os.environ.get("HOLDINGS_CACHE_MAX_PAYLOAD_BYTES")
    try:
        cap = int(env_cap) if env_cap else default
    except (TypeError, ValueError):  # pragma: no cover - defensive
        cap = default
    return max(cap, 65536)  # enforce a reasonable floor (~64KiB)


def _load_holdings_entry(key: str) -> dict | None:
    entry = cache_get(key)
    if not isinstance(entry, dict):
        return None

    timestamp = entry.get("timestamp")
    if isinstance(timestamp, str):
        cleaned = timestamp.rstrip("Z")
        try:
            timestamp = datetime.fromisoformat(cleaned)
        except ValueError:
            timestamp = None
    if not isinstance(timestamp, datetime):
        return None
    entry["timestamp"] = timestamp

    if entry.get("compressed"):
        compressed_data = entry.get("data")
        try:
            compressed_bytes = base64.b64decode(compressed_data)
            raw_bytes = gzip.decompress(compressed_bytes)
            entry["data"] = json.loads(raw_bytes.decode("utf-8"))
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("Failed to decompress holdings cache entry %s: %s", key, exc)
            return None
    elif entry.get("truncated"):
        # Truncated payloads are stored as-is but intentionally lightweight.
        entry.setdefault("footprint_bytes", entry.get("raw_size_bytes"))
    return entry


def _summarize_holdings(data: list) -> list:
    """Return a lightweight version of the holdings payload for navigation."""

    keep_keys = {
        "symbol",
        "tradingsymbol",
        "instrument_token",
        "token",
        "quantity",
        "qty",
        "average_price",
        "avg_price",
        "ltp",
        "last_price",
        "pnl",
        "day_change",
        "day_change_percentage",
    }
    summarized: list = []
    for item in data:
        if not isinstance(item, dict):
            continue
        summarized.append({k: v for k, v in item.items() if k in keep_keys})
    return summarized


def _save_holdings_entry(
    key: str,
    data: list,
    stale: bool = False,
    *,
    ttl_override: int | None = None,
    max_payload_bytes: int | None = None,
) -> dict:
    """Persist holdings to cache with compression and size guards.

    The expected per-entry footprint stays under ``max_payload_bytes`` by
    compressing large payloads (gzip+base64) or falling back to a summarized
    view containing only navigation-friendly fields when compression is not
    sufficient.
    """

    timestamp = datetime.utcnow()
    ttl = _holdings_ttl_seconds(ttl_override=ttl_override)
    cap = max_payload_bytes or _holdings_payload_limit()

    raw_json = json.dumps(data, default=str).encode("utf-8")
    raw_size = len(raw_json)
    entry: dict = {
        "timestamp": timestamp.isoformat() + "Z",
        "stale": stale,
        "raw_size_bytes": raw_size,
    }
    response_payload = data

    if raw_size > cap:
        compressed = gzip.compress(raw_json)
        encoded = base64.b64encode(compressed).decode("ascii")
        compressed_size = len(encoded)
        compression_ratio = compressed_size / raw_size
        if compressed_size <= cap:
            entry.update(
                {
                    "data": encoded,
                    "compressed": True,
                    "compressed_size_bytes": compressed_size,
                    "compression_ratio": round(compression_ratio, 3),
                }
            )
            logger.info(
                "holdings cache entry compressed for key %s (raw=%sB, compressed=%sB, ratio=%.3f)",
                key,
                raw_size,
                compressed_size,
                compression_ratio,
            )
        else:
            summarized = _summarize_holdings(data)
            summary_size = len(json.dumps(summarized, default=str).encode("utf-8"))
            entry.update(
                {
                    "data": summarized,
                    "truncated": True,
                    "footprint_bytes": summary_size,
                }
            )
            response_payload = summarized
            logger.warning(
                "holdings cache entry truncated for key %s (raw=%sB > cap=%sB, summary=%sB)",
                key,
                raw_size,
                cap,
                summary_size,
            )
    else:
        entry["data"] = data

    cache_set(key, entry, ttl=ttl)
    return {"timestamp": timestamp, "data": response_payload, "stale": stale}


def _default_order_summary() -> dict:
    return {"total_trades": 0, "total_quantity": 0, "last_status": "-"}


def _safe_float(val, default: float = 0.0) -> float:
    try:
        if val is None:
            return default
        return float(val)
    except (TypeError, ValueError):
        return default


def _call_with_timeout(func, timeout: float):
    future = _EXECUTOR.submit(func)
    try:
        return future.result(timeout=timeout)
    except FuturesTimeoutError as exc:  # pragma: no cover - concurrency edge
        future.cancel()
        raise TimeoutError(str(exc))


def _prepare_snapshot_for_response(entry: dict) -> dict:
    now = datetime.utcnow()
    data = deepcopy(entry.get("data", {}))
    data.setdefault("errors", {})
    data.setdefault("stale", False)
    placeholder_flag = data.get("placeholder")
    if isinstance(placeholder_flag, bool):
        data["placeholder"] = placeholder_flag
    else:
        data["placeholder"] = False
    cached_at = entry.get("timestamp", now)
    if isinstance(cached_at, datetime):
        data["cached_at"] = cached_at.isoformat() + "Z"
        data["age"] = max((now - cached_at).total_seconds(), 0.0)
    else:  # pragma: no cover - defensive
        data["cached_at"] = None
        data["age"] = None
    return data


def _snapshot_ttl_seconds() -> int:
    ttl_seconds = max(int(_SNAPSHOT_INTERVAL.total_seconds() * 6), 1)
    return min(ttl_seconds, _SNAPSHOT_MAX_TTL_SECONDS)


def _snapshot_payload_for_shard(
    shard: str, timestamp: datetime, data: dict
) -> dict | None:
    """Serialize a shard payload, compressing and enforcing size limits."""

    serialized = json.dumps(data, default=_json_default).encode("utf-8")
    raw_size = len(serialized)

    if raw_size > _SNAPSHOT_MAX_SHARD_BYTES:
        _SNAPSHOT_CACHE_METRICS["skipped_shards"] += 1
        logger.info(
            "Skipping cache for snapshot shard %s (size=%s bytes exceeds cap=%s)",
            shard,
            raw_size,
            _SNAPSHOT_MAX_SHARD_BYTES,
        )
        return None

    payload: dict[str, object] = {
        "timestamp": timestamp.isoformat() + "Z",
        "data": data,
        "raw_size_bytes": raw_size,
        "compressed": False,
        "footprint_bytes": raw_size,
        "shard": shard,
    }

    if raw_size >= _SNAPSHOT_COMPRESS_THRESHOLD_BYTES:
        compressed_bytes = gzip.compress(serialized)
        payload["data"] = base64.b64encode(compressed_bytes).decode("ascii")
        payload["compressed"] = True
        payload["footprint_bytes"] = len(compressed_bytes)

    return payload


def _decode_snapshot_shard(key: str, entry: dict, shard: str) -> dict | None:
    """Decode cached shard data, handling compression and timestamp parsing."""

    if not isinstance(entry, dict):
        return None

    timestamp = entry.get("timestamp")
    if isinstance(timestamp, str):
        cleaned = timestamp.rstrip("Z")
        try:
            timestamp = datetime.fromisoformat(cleaned)
        except ValueError:
            timestamp = None

    if not isinstance(timestamp, datetime):
        logger.debug("Discarding snapshot shard %s due to missing timestamp", key)
        return None

    data = entry.get("data")
    if entry.get("compressed"):
        try:
            compressed_bytes = base64.b64decode(data)
            raw_json = gzip.decompress(compressed_bytes).decode("utf-8")
            data = json.loads(raw_json)
        except Exception as exc:  # pragma: no cover - defensive
            logger.warning("Failed to decompress snapshot shard %s: %s", key, exc)
            return None

    footprint = entry.get("footprint_bytes") or entry.get("raw_size_bytes")
    return {
        "timestamp": timestamp,
        "data": data if isinstance(data, dict) else {},
        "footprint_bytes": footprint if isinstance(footprint, int) else 0,
    }


def _merge_snapshot_shards(summary: dict | None, charts: dict | None) -> dict | None:
    if not summary:
        return None

    merged = deepcopy(summary)
    data = merged.get("data", {}) if isinstance(merged, dict) else {}
    charts_data = charts.get("data") if isinstance(charts, dict) else None
    if isinstance(data, dict) and isinstance(charts_data, dict) and charts_data:
        for key, value in charts_data.items():
            data[key] = value
        merged["missing_charts"] = False
    else:
        merged["missing_charts"] = True
    merged["data"] = data
    return merged


def snapshot_cache_metrics() -> dict:
    """Expose snapshot cache metrics for external monitoring."""

    return deepcopy(_SNAPSHOT_CACHE_METRICS)


def _refreshing_cache_key(key: str) -> str:
    return f"{key}:refreshing"


def _load_snapshot_entry(key: str) -> dict | None:
    summary_key = _snapshot_shard_key(key, "summary")
    charts_key = _snapshot_shard_key(key, "charts")

    raw_summary = cache_get(summary_key)
    summary_entry = _decode_snapshot_shard(summary_key, raw_summary, "summary")
    if summary_entry:
        _SNAPSHOT_CACHE_METRICS["summary_hits"] += 1
        _SNAPSHOT_CACHE_METRICS["summary_bytes"] = summary_entry.get(
            "footprint_bytes", 0
        )
    else:
        _SNAPSHOT_CACHE_METRICS["summary_misses"] += 1
        legacy_entry = cache_get(key)
        if isinstance(legacy_entry, dict):
            summary_entry = _decode_snapshot_shard(key, legacy_entry, "summary")

    raw_charts = cache_get(charts_key)
    charts_entry = _decode_snapshot_shard(charts_key, raw_charts, "charts")
    if charts_entry:
        _SNAPSHOT_CACHE_METRICS["charts_hits"] += 1
        _SNAPSHOT_CACHE_METRICS["charts_bytes"] = charts_entry.get(
            "footprint_bytes", 0
        )
    else:
        _SNAPSHOT_CACHE_METRICS["charts_misses"] += 1

    return _merge_snapshot_shards(summary_entry, charts_entry)


def _save_snapshot_entry(key: str, snapshot: dict) -> dict:
    timestamp = datetime.utcnow()
    ttl_seconds = _snapshot_ttl_seconds()

    portfolio = snapshot.get("portfolio") if isinstance(snapshot, dict) else []
    if not isinstance(portfolio, list):
        portfolio = []
    orders = snapshot.get("orders") if isinstance(snapshot, dict) else {}
    orders = orders if isinstance(orders, dict) else {}
    order_items = orders.get("items") if isinstance(orders.get("items"), list) else []
    order_summary = orders.get("summary") if isinstance(orders.get("summary"), dict) else _default_order_summary()

    account_data = snapshot.get("account") if isinstance(snapshot, dict) else {}
    account_data = account_data if isinstance(account_data, dict) else {}
    errors = snapshot.get("errors") if isinstance(snapshot, dict) else {}
    errors = deepcopy(errors) if isinstance(errors, dict) else {}
    stale_flag = bool(snapshot.get("stale")) if isinstance(snapshot, dict) else False

    summary_snapshot = {
        "portfolio": _summarize_holdings(portfolio),
        "orders": {"summary": order_summary, "items": []},
        "account": account_data,
        "errors": errors,
        "stale": stale_flag,
    }
    if snapshot.get("placeholder"):
        summary_snapshot["placeholder"] = True

    charts_snapshot = {
        "portfolio": portfolio,
        "orders": {"items": order_items, "summary": order_summary},
        "account": account_data,
        "errors": errors,
        "stale": stale_flag,
    }
    charts_data = snapshot.get("charts") if isinstance(snapshot, dict) else None
    if isinstance(charts_data, dict) and charts_data:
        charts_snapshot["charts"] = charts_data

    summary_payload = _snapshot_payload_for_shard("summary", timestamp, summary_snapshot)
    charts_payload = _snapshot_payload_for_shard("charts", timestamp, charts_snapshot)

    if summary_payload:
        cache_set(_snapshot_shard_key(key, "summary"), summary_payload, ttl=ttl_seconds)
        _SNAPSHOT_CACHE_METRICS["summary_bytes"] = summary_payload.get(
            "footprint_bytes", 0
        )
    else:
        logger.warning(
            "Unable to persist snapshot summary shard for %s; forcing refresh on next request",
            key,
        )

    if charts_payload:
        cache_set(_snapshot_shard_key(key, "charts"), charts_payload, ttl=ttl_seconds)
        _SNAPSHOT_CACHE_METRICS["charts_bytes"] = charts_payload.get(
            "footprint_bytes", 0
        )
    else:
        cache_delete(_snapshot_shard_key(key, "charts"))

    cache_delete(key)  # clear legacy combined payloads
    cache_delete(_refreshing_cache_key(key))
    stored_snapshot = deepcopy(summary_snapshot)
    if isinstance(charts_snapshot, dict):
        for key, value in charts_snapshot.items():
            stored_snapshot[key] = value
    return {"timestamp": timestamp, "data": stored_snapshot}


def _refresh_snapshot_now(
    account: Account, *, key: str | None = None, entry: dict | None = None
) -> dict:
    key = key or _snapshot_cache_key(account)
    refresh_key = _refreshing_cache_key(key)
    existing_data: dict = {}
    if entry and isinstance(entry.get("data"), dict):
        existing_data = deepcopy(entry["data"])

    try:
        snapshot = _collect_snapshot(account, existing_data)
    except Exception:
        cache_delete(refresh_key)
        raise

    return _save_snapshot_entry(key, snapshot)


def _mark_refresh_scheduled(key: str) -> bool:
    refresh_key = _refreshing_cache_key(key)
    if cache_get(refresh_key):
        return False
    min_ttl = int((_SNAPSHOT_INTERVAL * 2).total_seconds())
    ttl_seconds = max(min_ttl, 60)
    cache_set(
        refresh_key,
        {"timestamp": datetime.utcnow().isoformat() + "Z"},
        ttl=ttl_seconds,
    )
    return True


def _enqueue_snapshot_refresh(account: Account, key: str) -> bool:
    if not _mark_refresh_scheduled(key):
        refresh_key = _refreshing_cache_key(key)
        marker = cache_get(refresh_key)
        if isinstance(marker, dict):
            timestamp = marker.get("timestamp")
            if isinstance(timestamp, str):
                cleaned = timestamp.rstrip("Z")
                try:
                    timestamp = datetime.fromisoformat(cleaned)
                except ValueError:
                    timestamp = None
            if not isinstance(timestamp, datetime):
                cache_delete(refresh_key)
                return False
            age = datetime.utcnow() - timestamp
            if age >= _SNAPSHOT_INTERVAL * 2:
                cache_delete(refresh_key)
                return False
            return True
        cache_delete(refresh_key)
        return False

    try:
        from task import celery

        # Summary requests must fail fast when the broker is offline so cached data
        # continues to render instantly.
        celery.send_task(
            "services.tasks.refresh_dashboard_snapshot",
            args=[account.user_id, account.client_id],
            ignore_result=True,
            retry=False,
            throw=False,
        )
    except (OperationalError, CeleryError, RedisError) as exc:
        logger.debug(
            "Unable to enqueue snapshot refresh due to broker issue: %s", exc,
        )
        cache_delete(_refreshing_cache_key(key))
        return False
    except Exception:
        cache_delete(_refreshing_cache_key(key))
        raise

    return True


def update_dashboard_snapshot_cache(account: Account) -> dict:
    entry = _refresh_snapshot_now(account)
    return _prepare_snapshot_for_response(entry)


def get_cached_dashboard_snapshot(
    account: Account, *, prefer_cache: bool = False
) -> dict:
    key = _snapshot_cache_key(account)
    entry = _load_snapshot_entry(key)
    now = datetime.utcnow()
    missing_charts = bool(entry and entry.get("missing_charts"))
    entry_age = None
    if entry and isinstance(entry.get("timestamp"), datetime):
        entry_age = now - entry["timestamp"]
    if missing_charts:
        entry_age = None  # force refresh path when heavy shards are absent

    def _schedule_refresh(response: dict, *, log_missing_charts: bool = False) -> dict:
        response["stale"] = response.get("stale") or True
        scheduled = _enqueue_snapshot_refresh(account, key)
        if scheduled is False:
            errors = response.setdefault("errors", {})
            message = "Snapshot refresh deferred: background queue unavailable"
            if errors.get("snapshot"):
                errors["snapshot"] = f"{errors['snapshot']}; {message}"
            else:
                errors["snapshot"] = message
            log_message = (
                "Unable to schedule snapshot refresh for user %s client %s due to missing shards"
                if log_missing_charts
                else "Skipping synchronous snapshot refresh for user %s client %s; background queue unavailable"
            )
            logger.warning(
                log_message,
                account.user_id,
                account.client_id or "primary",
            )
        return response

    if entry:
        response = _prepare_snapshot_for_response(entry)
        if missing_charts:
            return _schedule_refresh(response, log_missing_charts=True)
        if entry_age is None or entry_age >= _SNAPSHOT_INTERVAL:
            return _schedule_refresh(response)
        if prefer_cache is False:
            return response

    if prefer_cache:
        if entry:
            return response

        placeholder_entry = {
            "timestamp": now,
            "data": {
                "portfolio": [],
                "orders": {"items": [], "summary": _default_order_summary()},
                "account": {},
                "errors": {},
                "stale": True,
                "placeholder": True,
            },
        }
        placeholder = _prepare_snapshot_for_response(placeholder_entry)
        placeholder["stale"] = True
        placeholder["placeholder"] = True

        scheduled = _enqueue_snapshot_refresh(account, key)
        if scheduled is False:
            errors = placeholder.setdefault("errors", {})
            message = "Snapshot refresh deferred: background queue unavailable"
            errors["snapshot"] = message
            logger.warning(
                "Unable to schedule snapshot refresh for user %s client %s; background queue unavailable",
                account.user_id,
                account.client_id or "primary",
            )

        return placeholder

    if entry:
        return response

    try:
        refreshed = _refresh_snapshot_now(account, key=key, entry=entry)
        return _prepare_snapshot_for_response(refreshed)
    except Exception as exc:
        if entry:
            cached = _prepare_snapshot_for_response(entry)
            cached.setdefault("errors", {})["snapshot"] = str(exc)
            cached["stale"] = True
            return cached
        raise


def _normalize_portfolio_data(resp, account: Account) -> list:
    data = []
    if isinstance(resp, dict):
        if resp.get("status") == "success" and isinstance(resp.get("data"), list):
            data = resp.get("data")
        else:
            data = (
                resp.get("data")
                or resp.get("positions")
                or resp.get("net")
                or resp.get("netPositions")
                or resp.get("net_positions")
                or []
            )
    elif isinstance(resp, list):
        data = resp
    else:
        data = resp or []

    if isinstance(data, dict):
        data = (
            data.get("data")
            or data.get("positions")
            or data.get("net")
            or data.get("netPositions")
            or data.get("net_positions")
            or []
        )

    if not isinstance(data, list):
        data = []

    standardized = [normalize_position(p, account.broker) for p in data]
    return [p for p in standardized if p]


def _order_summary_from_items(items: list) -> dict:
    summary = _default_order_summary()
    total_qty = 0
    last_status = summary["last_status"]
    for order in items:
        qty = order.get("quantity") or order.get("qty") or order.get("filledQty")
        total_qty += _safe_float(qty, 0)
        status = (
            order.get("order_status")
            or order.get("status")
            or order.get("statusMessage")
            or last_status
        )
        last_status = status
    summary["total_trades"] = len(items)
    summary["total_quantity"] = int(total_qty)
    summary["last_status"] = last_status or "-"
    return summary


def _normalize_orders(resp) -> dict:
    items = []
    summary = _default_order_summary()

    if isinstance(resp, dict):
        candidate = resp.get("data") or resp.get("orders") or []
        items = candidate if isinstance(candidate, list) else []
        raw_summary = resp.get("summary")
        if isinstance(raw_summary, dict):
            summary.update({k: raw_summary.get(k, summary[k]) for k in summary})
    elif isinstance(resp, list):
        items = resp
    else:
        items = []

    if not isinstance(items, list):
        items = []

    computed = _order_summary_from_items(items)
    summary = {
        "total_trades": summary.get("total_trades") or computed["total_trades"],
        "total_quantity": summary.get("total_quantity") or computed["total_quantity"],
        "last_status": summary.get("last_status") or computed["last_status"],
    }

    return {"items": items, "summary": summary}


def _normalize_account_stats(resp) -> dict:
    data = resp
    if isinstance(resp, dict):
        data = resp.get("data") or resp

    def first_float(keys):
        if not isinstance(data, dict):
            return None
        for key in keys:
            if key in data:
                val = _safe_float(data.get(key), default=None)
                if val is not None:
                    return val
        return None

    stats: dict[str, float] = {}
    if isinstance(data, dict):
        stats = {
            "total_funds": first_float(
                [
                    "availabelBalance",
                    "availableBalance",
                    "availableAmount",
                    "netCashAvailable",
                    "netCash",
                ]
            ),
            "available_margin": first_float(
                ["withdrawableBalance", "availableBalance", "availableAmount"]
            ),
            "used_margin": first_float(
                ["utilizedAmount", "usedMargin", "blockedMargin"]
            ),
        }

    def _search_numeric(value, keywords: tuple[str, ...]):
        if isinstance(value, dict):
            for key, candidate in value.items():
                if isinstance(key, str):
                    lowered = key.lower()
                    if any(word in lowered for word in keywords):
                        result = _safe_float(candidate, default=None)
                        if result is not None:
                            return result
                result = _search_numeric(candidate, keywords)
                if result is not None:
                    return result
        elif isinstance(value, list):
            for item in value:
                result = _search_numeric(item, keywords)
                if result is not None:
                    return result
        return None

    fallback: dict[str, float] = {}
    search_source = data if isinstance(data, (dict, list)) else resp
    try:
        from app import extract_balance
    except Exception:  # pragma: no cover - defensive
        extract_balance = None

    if extract_balance:
        balance = extract_balance(search_source)
        if balance is None and search_source is not resp:
            balance = extract_balance(resp)
        if balance is not None:
            fallback["total_funds"] = balance

    available = _search_numeric(
        search_source,
        (
            "availablemargin",
            "marginavailable",
            "availmargin",
            "withdrawablebalance",
            "availablebalance",
        ),
    )
    if available is not None:
        fallback["available_margin"] = available

    used = _search_numeric(
        search_source,
        ("usedmargin", "utilizedmargin", "utilizedamount", "blockedmargin", "marginused"),
    )
    if used is not None:
        fallback["used_margin"] = used

    if "total_funds" in fallback and "available_margin" not in fallback:
        fallback["available_margin"] = fallback["total_funds"]

    keys = ("total_funds", "available_margin", "used_margin")
    result: dict[str, float] = {}
    has_value = False
    for key in keys:
        value = None
        if stats:
            value = stats.get(key)
        if value is None and fallback:
            value = fallback.get(key)
        if value is not None:
            result[key] = value
            has_value = True
        else:
            result[key] = 0.0

    if has_value:
        return result

    return {}


def _collect_snapshot(account: Account, existing: dict | None = None) -> dict:
    existing = existing or {}
    from app import broker_api, _account_to_dict

    api = broker_api(_account_to_dict(account))
    if hasattr(api, "timeout"):
        try:
            api.timeout = min(getattr(api, "timeout", _BROKER_TIMEOUT_SECONDS), _BROKER_TIMEOUT_SECONDS)
        except Exception:  # pragma: no cover - defensive
            api.timeout = _BROKER_TIMEOUT_SECONDS

    existing_portfolio = deepcopy(existing.get("portfolio", []))
    existing_account = deepcopy(existing.get("account", {}))

    snapshot = {
        "portfolio": existing_portfolio,
        "orders": existing.get(
            "orders", {"items": [], "summary": _default_order_summary()}
        ),
        "account": existing_account,
        "errors": {},
        "stale": False,
    }

    positions_failed = False
    orders_future = None
    positions_future = None
    account_future = None

    def _future_result(future):
        try:
            return future.result(timeout=_BROKER_TIMEOUT_SECONDS)
        except FuturesTimeoutError as exc:  # pragma: no cover - concurrency edge
            future.cancel()
            raise TimeoutError(str(exc))

    if hasattr(api, "get_positions"):
        positions_future = _EXECUTOR.submit(api.get_positions)
    else:
        snapshot["errors"]["portfolio"] = "Broker does not expose positions"
        positions_failed = True

    if hasattr(api, "get_order_list"):
        orders_future = _EXECUTOR.submit(api.get_order_list)
    else:
        snapshot["errors"]["orders"] = "Broker does not expose order list"

    account_failed = False
    profile_method = None
    for candidate in ("get_profile", "get_fund_limits", "get_account_details"):
        if hasattr(api, candidate):
            profile_method = getattr(api, candidate)
            break

    if profile_method is not None:
        account_future = _EXECUTOR.submit(profile_method)
    elif not snapshot["account"]:
        snapshot["errors"]["account"] = "Broker does not expose account stats"

    if positions_future is not None:
        try:
            resp = _future_result(positions_future)
            snapshot["portfolio"] = _normalize_portfolio_data(resp, account)
        except Exception as exc:
            snapshot["errors"]["portfolio"] = str(exc)
            snapshot["portfolio"] = existing_portfolio
            positions_failed = True

    if orders_future is not None:
        try:
            resp = _future_result(orders_future)
            snapshot["orders"] = _normalize_orders(resp)
        except Exception as exc:
            snapshot["errors"]["orders"] = str(exc)

    if account_future is not None:
        try:
            resp = _future_result(account_future)
            stats = _normalize_account_stats(resp)
            if stats:
                snapshot["account"] = stats
            else:
                snapshot["errors"].setdefault(
                    "account", "Account response missing expected fields"
                )
                snapshot["account"] = existing_account
                account_failed = True
        except Exception as exc:
            snapshot["errors"]["account"] = str(exc)
            snapshot["account"] = existing_account
            account_failed = True

    snapshot["stale"] = positions_failed or account_failed
    return snapshot


def _get_dashboard_snapshot(account: Account) -> dict:
    return get_cached_dashboard_snapshot(account, prefer_cache=False)


def _resolve_account(user, client_id: str | None) -> Account | None:
    if client_id:
        return Account.query.filter_by(user_id=user.id, client_id=client_id).first()
    return get_primary_account(user)


def _get_holdings_payload(account: Account) -> tuple[list, bool]:
    key = _holdings_cache_key(account)
    now = datetime.utcnow()
    ttl_seconds = _holdings_ttl_seconds()
    
    with _HOLDINGS_LOCK:
        entry = _load_holdings_entry(key)
        if entry and now - entry["timestamp"] < _HOLDINGS_INTERVAL:
            logger.info(
                "holdings cache hit for user %s client %s", account.user_id, account.client_id
            )
            return deepcopy(entry["data"]), entry.get("stale", False)

    from app import broker_api, _account_to_dict

    api = broker_api(_account_to_dict(account))
    if hasattr(api, "timeout"):
        try:
            api.timeout = min(getattr(api, "timeout", _BROKER_TIMEOUT_SECONDS), _BROKER_TIMEOUT_SECONDS)
        except Exception:  # pragma: no cover - defensive
            api.timeout = _BROKER_TIMEOUT_SECONDS

    if not hasattr(api, "get_holdings"):
        raise AttributeError("Broker does not expose holdings")

    cached_data = deepcopy(entry["data"]) if entry else []
    try:
        resp = _call_with_timeout(api.get_holdings, _BROKER_TIMEOUT_SECONDS)
        if isinstance(resp, dict):
            data = resp.get("data") or resp.get("holdings") or []
        else:
            data = resp or []
        if not isinstance(data, list):
            data = []
        with _HOLDINGS_LOCK:
            entry = _save_holdings_entry(
                key,
                data,
                stale=False,
                ttl_override=ttl_seconds,
                max_payload_bytes=_holdings_payload_limit(),
            )
        logger.info(
            "holdings cache refreshed for user %s client %s (ttl=%ss)",
            account.user_id,
            account.client_id,
            ttl_seconds,
        )
        return deepcopy(entry["data"]), False
    except Exception as exc:
        if cached_data:
            logger.warning(
                "Broker holdings fetch failed for user %s client %s; serving stale cache: %s",
                account.user_id,
                account.client_id,
                exc,
            )
            return cached_data, True
        raise exc
        
@api_bp.route("/dashboard/snapshot", defaults={"client_id": None})
@api_bp.route("/dashboard/snapshot/<client_id>")
@login_required_api
def dashboard_snapshot(client_id=None):
    user = current_user()
    account = None
    account_id_param = request.args.get("account_id")
    if account_id_param:
        try:
            account_id_value = int(account_id_param)
        except (TypeError, ValueError):
            account_id_value = None
        if account_id_value is not None:
            account = Account.query.filter_by(
                user_id=user.id, id=account_id_value
            ).first()
    if account is None:
        account = _resolve_account(user, client_id)
    if not account or not account.credentials:
        return jsonify({"error": "Account not configured"}), 400

    prefer_cache = _bool_or_default(request.args.get("cache"), True)
    snapshot = get_cached_dashboard_snapshot(account, prefer_cache=prefer_cache)
    return jsonify(snapshot)


@api_bp.route("/portfolio", defaults={"client_id": None})
@api_bp.route("/portfolio/<client_id>")
@login_required_api
def portfolio(client_id=None):
    """Return positions for the requested account."""
    user = current_user()
    account = _resolve_account(user, client_id)
    if not account or not account.credentials:
        return jsonify({"error": "Account not configured"}), 400

    snapshot = _get_dashboard_snapshot(account)
    return jsonify(snapshot.get("portfolio", []))


@api_bp.route("/orders", defaults={"client_id": None})
@api_bp.route("/orders/<client_id>")
@login_required_api
def orders(client_id=None):
    """Return order list for the requested account."""
    user = current_user()
    account = _resolve_account(user, client_id)
    if not account or not account.credentials:
        return jsonify({"error": "Account not configured"}), 400

    snapshot = _get_dashboard_snapshot(account)
    orders = snapshot.get("orders", {})
    if isinstance(orders, dict):
        payload = {
            "orders": orders.get("items", []),
            "summary": orders.get("summary", _default_order_summary()),
            "stale": bool(snapshot.get("errors", {}).get("orders")),
            "cached_at": snapshot.get("cached_at"),
            "age": snapshot.get("age"),
        }
        error = snapshot.get("errors", {}).get("orders")
        if error:
            payload["error"] = error
        return jsonify(payload)
    return jsonify(orders)


@api_bp.route("/holdings", defaults={"client_id": None})
@api_bp.route("/holdings/<client_id>")
@login_required_api
def holdings(client_id=None):
    """Return holdings for the requested account if supported."""
    user = current_user()
    account = _resolve_account(user, client_id)
    if not account or not account.credentials:
        return jsonify({"error": "Account not configured"}), 400

    try:
        data, stale = _get_holdings_payload(account)
    except AttributeError:
        return jsonify({"error": "Holdings not supported for this broker"}), 400
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    return jsonify({"data": data, "stale": stale})


@api_bp.route("/account")
@login_required_api
def account_stats():
    user = current_user()
    account = get_primary_account(user)
    if not account or not account.credentials:
        return jsonify({"error": "Account not configured"}), 400

    snapshot = _get_dashboard_snapshot(account)
    stats = snapshot.get("account", {})
    payload = dict(stats)
    payload.update(
        {
            "stale": bool(snapshot.get("errors", {}).get("account")),
            "cached_at": snapshot.get("cached_at"),
            "age": snapshot.get("age"),
        }
    )
    error = snapshot.get("errors", {}).get("account")
    if error:
        payload["error"] = error
    if not stats and error:
        return jsonify(payload), 502
    return jsonify(payload)


@api_bp.route("/order-mappings")
@login_required_api
def order_mappings():
    user = current_user()
    mappings = [
        {
            "master_order_id": m.master_order_id,
            "child_order_id": m.child_order_id,
            "master_client_id": m.master_client_id,
            "master_broker": m.master_broker,
            "child_client_id": m.child_client_id,
            "child_broker": m.child_broker,
            "symbol": m.symbol,
            "status": m.status,
            "timestamp": m.timestamp,
            "child_timestamp": m.child_timestamp,
            "remarks": m.remarks,
            "multiplier": m.multiplier,
        }
        for m in order_mappings_for_user(user).all()
    ]
    return jsonify(mappings)


@api_bp.route("/child-orders")
@login_required_api
def child_orders():
    user = current_user()
    master_order_id = request.args.get("master_order_id")
    mappings = order_mappings_for_user(user)
    if master_order_id:
        mappings = mappings.filter_by(master_order_id=master_order_id)
    data = [
        {
            "master_order_id": m.master_order_id,
            "child_order_id": m.child_order_id,
            "master_client_id": m.master_client_id,
            "master_broker": m.master_broker,
            "child_client_id": m.child_client_id,
            "child_broker": m.child_broker,
            "symbol": m.symbol,
            "status": m.status,
            "timestamp": m.timestamp,
            "child_timestamp": m.child_timestamp,
            "remarks": m.remarks,
            "multiplier": m.multiplier,
        }
        for m in mappings.all()
    ]
    return jsonify(data)


@api_bp.route("/strategies", methods=["GET"])
@login_required_api
def list_strategies():
    """Return all strategies for the logged-in user."""
    user = current_user()
    strategies = (
        Strategy.query.filter_by(user_id=user.id).order_by(Strategy.id.desc()).all()
    )
    data = [
        {
            "id": s.id,
            "name": s.name,
            "description": s.description,
            "asset_class": s.asset_class,
            "style": s.style,
            "exchange": s.exchange,
            "transaction_type": s.transaction_type,
            "order_type": s.order_type,
            "order_validity": s.order_validity,
            "product_type": s.product_type,
            "order_qty": s.order_qty,
            "order_value": s.order_value,
            "allow_auto_submit": s.allow_auto_submit,
            "allow_live_trading": s.allow_live_trading,
            "allow_any_ticker": s.allow_any_ticker,
            "allowed_tickers": s.allowed_tickers,
            "notification_emails": s.notification_emails,
            "notify_failures_only": s.notify_failures_only,
            "account_id": s.account_id,
            "is_active": s.is_active,
            "last_run_at": s.last_run_at,
            "signal_source": s.signal_source,
            "risk_max_positions": s.risk_max_positions,
            "risk_max_allocation": s.risk_max_allocation,
            "schedule": s.schedule,
            "webhook_secret": s.webhook_secret,
            "track_performance": s.track_performance,
            "log_retention_days": s.log_retention_days,
            "is_public": s.is_public,
            "icon": s.icon,
            "brokers": s.brokers,
            "master_accounts": s.master_accounts,
        }
        for s in strategies
    ]
    return jsonify(data)


@api_bp.route("/strategies", methods=["POST"])
@login_required_api
def create_strategy():
    """Create a new trading strategy."""
    data = request.get_json(silent=True) or {}
    user = current_user()
    
    required = ["name", "asset_class", "style"]
    for field in required:
        if not data.get(field):
            from app import record_system_log

            record_system_log(
                user,
                f"Strategy creation failed: missing {field}",
                level="ERROR",
                module="strategy",
                details={"type": "strategy", "action": "create", "reason": f"missing {field}"},
            )
            return jsonify({"error": f"{field} is required"}), 400

    account_id = data.get("account_id")
    if account_id:
        acc = Account.query.filter_by(id=account_id, user_id=user.id).first()
        if not acc:
            from app import record_system_log

            record_system_log(
                user,
                "Strategy creation failed: invalid account",
                level="ERROR",
                module="strategy",
                details={"type": "strategy", "action": "create", "account_id": account_id},
            )
            return jsonify({"error": "Invalid account_id"}), 400 
    def _to_int(val):
        try:
            return int(val)
        except (TypeError, ValueError):
            return None

    def _to_float(val):
        try:
            return float(val)
        except (TypeError, ValueError):
            return None
    brokers = data.get("brokers")
    if isinstance(brokers, list):
        brokers = ",".join(brokers)
    master_accounts = data.get("master_accounts")
    if isinstance(master_accounts, list):
        master_accounts = ",".join(str(a) for a in master_accounts)
    if not data.get("webhook_secret"):
        data["webhook_secret"] = secrets.token_hex(16)

    strategy = Strategy(
        user_id=user.id,
        account_id=_to_int(account_id) if account_id else None,
        name=data.get("name"),
        description=data.get("description"),
        asset_class=data.get("asset_class"),
        style=data.get("style"),
        exchange=data.get("exchange"),
        transaction_type=data.get("transaction_type"),
        order_type=data.get("order_type"),
        order_validity=data.get("order_validity"),
        product_type=data.get("product_type"),
        order_qty=_to_float(data.get("order_qty")),
        order_value=_to_float(data.get("order_value")),
        allow_auto_submit=_bool_or_default(data.get("allow_auto_submit"), True),
        allow_live_trading=_bool_or_default(data.get("allow_live_trading"), True),
        allow_any_ticker=_bool_or_default(data.get("allow_any_ticker"), True),
        allowed_tickers=data.get("allowed_tickers"),
        notification_emails=data.get("notification_emails"),
        notify_failures_only=_bool_or_default(data.get("notify_failures_only"), False),
        is_active=_bool_or_default(data.get("is_active"), False),
        signal_source=data.get("signal_source"),
        risk_max_positions=_to_int(data.get("risk_max_positions")),
        risk_max_allocation=_to_float(data.get("risk_max_allocation")),
        schedule=data.get("schedule"),
        webhook_secret=data.get("webhook_secret"),
        track_performance=_bool_or_default(data.get("track_performance"), False),
        log_retention_days=_to_int(data.get("log_retention_days")),
        is_public=_bool_or_default(data.get("is_public"), False),
        icon=data.get("icon"),
        brokers=brokers,
        master_accounts=master_accounts,
    )

    db.session.add(strategy)
    db.session.flush()
    _add_strategy_log(strategy, "Strategy created successfully")
    db.session.commit()

    from app import record_system_log

    record_system_log(
        user,
        f"Created strategy '{strategy.name}'",
        module="strategy",
        details={"type": "strategy", "action": "create", "strategy_id": strategy.id},
    )

    return jsonify({"message": "Strategy created", "id": strategy.id}), 201

@api_bp.route("/strategies/<int:strategy_id>", methods=["GET", "PUT", "DELETE"])
@login_required_api
def strategy_detail(strategy_id):
    """Retrieve, update or delete a strategy."""
    user = current_user()
    strategy = Strategy.query.filter_by(id=strategy_id, user_id=user.id).first_or_404()

    if request.method == "GET":
        return jsonify(
            {
                "id": strategy.id,
                "name": strategy.name,
                "description": strategy.description,
                "asset_class": strategy.asset_class,
                "style": strategy.style,
                "exchange": strategy.exchange,
                "transaction_type": strategy.transaction_type,
                "order_type": strategy.order_type,
                "order_validity": strategy.order_validity,
                "product_type": strategy.product_type,
                "order_qty": strategy.order_qty,
                "order_value": strategy.order_value,    
                "allow_auto_submit": strategy.allow_auto_submit,
                "allow_live_trading": strategy.allow_live_trading,
                "allow_any_ticker": strategy.allow_any_ticker,
                "allowed_tickers": strategy.allowed_tickers,
                "notification_emails": strategy.notification_emails,
                "notify_failures_only": strategy.notify_failures_only,
                "account_id": strategy.account_id,
                "is_active": strategy.is_active,
                "last_run_at": strategy.last_run_at,
                "signal_source": strategy.signal_source,
                "risk_max_positions": strategy.risk_max_positions,
                "risk_max_allocation": strategy.risk_max_allocation,
                "schedule": strategy.schedule,
                "webhook_secret": strategy.webhook_secret,
                "track_performance": strategy.track_performance,
                "log_retention_days": strategy.log_retention_days,
                "is_public": strategy.is_public,
                "icon": strategy.icon,
                "brokers": strategy.brokers,
                "master_accounts": strategy.master_accounts,
            }
        )

    if request.method == "PUT":
        data = request.get_json() or {}
        from app import record_system_log

        def _to_int(val):
            try:
                return int(val)
            except (TypeError, ValueError):
                return None

        def _to_float(val):
            try:
                return float(val)
            except (TypeError, ValueError):
                return None

        for field in [
            "name",
            "description",
            "asset_class",
            "style",
            "exchange",
            "transaction_type",
            "order_type",
            "order_validity",
            "product_type",
            "order_qty",
            "order_value",
            "allow_auto_submit",
            "allow_live_trading",
            "allow_any_ticker",
            "allowed_tickers",
            "notification_emails",
            "notify_failures_only",
            "is_active",
            "account_id",
            "signal_source",
            "risk_max_positions",
            "risk_max_allocation",
            "schedule",
            "webhook_secret",
            "track_performance",
            "log_retention_days",
            "is_public",
            "icon",
            "brokers",
            "master_accounts",
        ]:
            if field in data:
                if field == "account_id" and data[field] is not None:
                    acc = Account.query.filter_by(
                        id=data[field], user_id=user.id
                    ).first()
                    if not acc:
                        record_system_log(
                            user,
                            "Strategy update failed: invalid account",
                            level="ERROR",
                            module="strategy",
                            details={
                                "type": "strategy",
                                "action": "update",
                                "strategy_id": strategy.id,
                                "account_id": data[field],
                            },
                        )    
                        return jsonify({"error": "Invalid account_id"}), 400
                    setattr(strategy, field, _to_int(data[field]))
                elif field in ["risk_max_positions", "log_retention_days"]:
                    setattr(strategy, field, _to_int(data[field]))
                elif field == "risk_max_allocation":
                    setattr(strategy, field, _to_float(data[field]))
                elif field in ["order_qty", "order_value"]:
                    setattr(strategy, field, _to_float(data[field]))
                elif field in _BOOL_FIELDS:
                    value = data[field]
                    if value is None:
                        setattr(strategy, field, None)
                    else:
                        parsed = _parse_bool(value)
                        if parsed is None:
                            return jsonify({"error": f"Invalid value for {field}"}), 400
                        setattr(strategy, field, parsed)
                else:
                    if field in ["brokers", "master_accounts"] and isinstance(
                        data[field], list
                    ):
                        setattr(strategy, field, ",".join(str(v) for v in data[field]))
                    else:
                        setattr(strategy, field, data[field])
        _add_strategy_log(strategy, "Strategy updated successfully")
        db.session.commit()
        record_system_log(
            user,
            f"Updated strategy '{strategy.name}'",
            module="strategy",
            details={"type": "strategy", "action": "update", "strategy_id": strategy.id},
        )
        return jsonify({"message": "Strategy updated"})

    if request.method == "DELETE":
        db.session.delete(strategy)
        db.session.commit()
        from app import record_system_log

        record_system_log(
            user,
            f"Deleted strategy '{strategy.name}'",
            module="strategy",
            details={"type": "strategy", "action": "delete", "strategy_id": strategy.id},
        )
        return jsonify({"message": "Strategy deleted"})


@api_bp.route("/strategies/<int:strategy_id>/activate", methods=["POST"])
@login_required_api
def activate_strategy(strategy_id):
    """Activate a strategy"""
    user = current_user()
    strategy = Strategy.query.filter_by(id=strategy_id, user_id=user.id).first_or_404()
    strategy.is_active = True
    _add_strategy_log(strategy, "Strategy activated successfully")
    db.session.commit()
    from app import record_system_log

    record_system_log(
        user,
        f"Activated strategy '{strategy.name}'",
        module="strategy",
        details={"type": "strategy", "action": "activate", "strategy_id": strategy.id},
    )
    return jsonify({"message": "Strategy activated"})

@api_bp.route("/strategies/<int:strategy_id>/deactivate", methods=["POST"])
@login_required_api
def deactivate_strategy(strategy_id):
    """Deactivate a strategy"""
    user = current_user()
    strategy = Strategy.query.filter_by(id=strategy_id, user_id=user.id).first_or_404()
    strategy.is_active = False
    _add_strategy_log(strategy, "Strategy deactivated successfully")
    db.session.commit()
    from app import record_system_log

    record_system_log(
        user,
        f"Deactivated strategy '{strategy.name}'",
        module="strategy",
        details={"type": "strategy", "action": "deactivate", "strategy_id": strategy.id},
    )
    return jsonify({"message": "Strategy deactivated"})


@api_bp.route("/strategies/<int:strategy_id>/ping", methods=["POST"])
@login_required_api
def ping_strategy(strategy_id):
    """Update last run timestamp"""
    user = current_user()
    strategy = Strategy.query.filter_by(id=strategy_id, user_id=user.id).first_or_404()
    strategy.last_run_at = datetime.utcnow()
    db.session.commit()
    return jsonify({"message": "Strategy pinged"})

@api_bp.route("/strategies/<int:strategy_id>/regenerate-secret", methods=["POST"])
@login_required_api
def regenerate_webhook_secret(strategy_id):
    """Generate a new webhook secret for the strategy."""
    user = current_user()
    strategy = Strategy.query.filter_by(id=strategy_id, user_id=user.id).first_or_404()
    import secrets
    strategy.webhook_secret = secrets.token_hex(16)
    _add_strategy_log(strategy, "Webhook URL updated")
    db.session.commit()
    return jsonify({"webhook_secret": strategy.webhook_secret})

@api_bp.route("/strategies/<int:strategy_id>/test-webhook", methods=["POST"])
@login_required_api
def test_webhook(strategy_id):
    """Send a test payload to the webhook endpoint."""
    user = current_user()
    strategy = Strategy.query.filter_by(id=strategy_id, user_id=user.id).first_or_404()
    token = user.webhook_token
    if not token:
        import secrets
        token = secrets.token_hex(16)
        user.webhook_token = token
        db.session.commit()

    if not strategy.webhook_secret:
        import secrets
        strategy.webhook_secret = secrets.token_hex(16)
        db.session.commit()

    master_ids: list[int] = []
    if strategy.master_accounts:
        try:
            master_ids = [int(s) for s in str(strategy.master_accounts).split(",") if s.strip()]
        except ValueError:
            master_ids = []
    if not master_ids and strategy.account_id:
        master_ids = [strategy.account_id]

    masters = []
    if master_ids:
        masters = (
            Account.query.filter(Account.user_id == user.id, Account.id.in_(master_ids))
            .filter(db.func.lower(Account.role) == "master")
            .all()
        )

    active_masters = [m for m in masters if str(m.copy_status or "").lower() == "on"]
    if not active_masters:
        message = (
            "No active master accounts are configured for this strategy. "
            "Add a master account with copy trading enabled before testing the webhook."
        )
        _add_strategy_log(strategy, f"Test webhook failed: {message}", level="ERROR")
        db.session.commit()
        return jsonify({"error": message}), 400
        
    _add_strategy_log(
        strategy,
        "Test webhook dry-run completed with status 200",
        level="INFO",
    )
    db.session.commit()
    return jsonify({"status": 200, "response": "Strategy test successful"})


@api_bp.route("/strategies/<int:strategy_id>/subscribe", methods=["POST"])
@login_required_api
def subscribe_strategy(strategy_id):
    """Subscribe the logged in user to a strategy."""
    user = current_user()
    strategy = Strategy.query.get_or_404(strategy_id)
    if strategy.user_id == user.id:
        return jsonify({"error": "Cannot subscribe to your own strategy"}), 400
    data = request.get_json(silent=True) or {}
    
    sub = StrategySubscription.query.filter_by(
        strategy_id=strategy_id, subscriber_id=user.id
    ).first()
    if not sub:
        sub = StrategySubscription(strategy_id=strategy_id, subscriber_id=user.id)
        if strategy.is_public:
            sub.approved = True
        db.session.add(sub)
        db.session.commit()

    account_id = data.get("account_id")
    if account_id:
        acc = Account.query.filter_by(id=account_id, user_id=user.id).first()
        if not acc:
            return jsonify({"error": "Invalid account_id"}), 400
        sub.account_id = account_id

    sub.auto_submit = bool(data.get("auto_submit", True))
    sub.order_type = data.get("order_type") or sub.order_type
    sub.qty_mode = data.get("qty_mode") or sub.qty_mode
    sub.fixed_qty = (
        data.get("fixed_qty") if data.get("fixed_qty") is not None else sub.fixed_qty
    )

    db.session.commit()

    return jsonify(
        {"message": "Subscription saved", "id": sub.id, "approved": sub.approved}
    )


@api_bp.route("/strategies/<int:strategy_id>/subscribers", methods=["GET"])
@login_required_api
def list_subscribers(strategy_id):
    user = current_user()
    strategy = Strategy.query.filter_by(id=strategy_id, user_id=user.id).first_or_404()
    subs = [
        {
            "id": s.id,
            "subscriber_id": s.subscriber_id,
            "approved": s.approved,
            "created_at": s.created_at,
        }
        for s in StrategySubscription.query.filter_by(strategy_id=strategy_id).all()
    ]
    return jsonify(subs)


@api_bp.route("/strategies/<int:strategy_id>/clone", methods=["POST"])
@login_required_api
def clone_strategy(strategy_id):
    """Clone an existing strategy."""
    user = current_user()
    src = Strategy.query.filter_by(id=strategy_id, user_id=user.id).first_or_404()
    clone = Strategy(
        user_id=user.id,
        account_id=src.account_id,
        name=f"{src.name} (Copy)",
        description=src.description,
        asset_class=src.asset_class,
        style=src.style,
        exchange=src.exchange,
        transaction_type=src.transaction_type,
        order_type=src.order_type,
        order_validity=src.order_validity,
        product_type=src.product_type,
        order_qty=src.order_qty,
        order_value=src.order_value,
        allow_auto_submit=src.allow_auto_submit,
        allow_live_trading=src.allow_live_trading,
        allow_any_ticker=src.allow_any_ticker,
        allowed_tickers=src.allowed_tickers,
        notification_emails=src.notification_emails,
        notify_failures_only=src.notify_failures_only,
        signal_source=src.signal_source,
        risk_max_positions=src.risk_max_positions,
        risk_max_allocation=src.risk_max_allocation,
        schedule=src.schedule,
        webhook_secret=secrets.token_hex(16),
        track_performance=src.track_performance,
        log_retention_days=src.log_retention_days,
        is_public=src.is_public,
        icon=src.icon,
        brokers=src.brokers,
        master_accounts=src.master_accounts,
    )
    db.session.add(clone)
    # Flush to assign an ID before creating the log entry to satisfy the
    # NOT NULL constraint on StrategyLog.strategy_id.
    db.session.flush()
    _add_strategy_log(clone, f"Strategy cloned from '{src.name}'")
    db.session.commit()
    return jsonify({"message": "Strategy cloned", "id": clone.id})

@api_bp.route("/strategies/<int:strategy_id>/logs", methods=["GET", "POST"])
@login_required_api
def strategy_logs(strategy_id):
    """Retrieve or add log entries for a strategy"""
    user = current_user()
    strategy = Strategy.query.filter_by(id=strategy_id, user_id=user.id).first_or_404()

    if request.method == "POST":
        data = request.get_json() or {}
        log = StrategyLog(
            strategy_id=strategy.id,
            level=data.get("level", "INFO"),
            message=data.get("message"),
            performance=data.get("performance"),
        )
        db.session.add(log)
        db.session.commit()
        return jsonify({"message": "Log added", "id": log.id})

    logs = [
        {
            "id": l.id,
            "timestamp": l.timestamp,
            "level": l.level,
            "message": l.message,
            "performance": l.performance,
        }
        for l in StrategyLog.query.filter_by(strategy_id=strategy.id)
        .order_by(StrategyLog.timestamp.desc())
        .all()
    ]
    return jsonify(logs)


@api_bp.route("/strategies/<int:strategy_id>/orders", methods=["GET"])
@login_required_api
def strategy_orders(strategy_id):
    """Return trade logs grouped by broker for a strategy."""
    user = current_user()
    strategy = Strategy.query.filter_by(id=strategy_id, user_id=user.id).first_or_404()

    ids = []
    if strategy.master_accounts:
        try:
            ids = [int(s) for s in str(strategy.master_accounts).split(",") if s.strip()]
        except ValueError:
            ids = []
    if not ids and strategy.account_id:
        ids = [strategy.account_id]
    if not ids:
        return jsonify({})

    accounts = Account.query.filter(
        Account.user_id == user.id, Account.id.in_(ids)
    ).all()
    result = {}
    for acc in accounts:
        trades = (
            Trade.query.filter_by(user_id=user.id, client_id=acc.client_id)
            .order_by(Trade.timestamp.desc())
            .all()
        )
        if not trades:
            continue
        broker = acc.broker or "Unknown"
        result[broker] = [
            {
                "id": t.id,
                "timestamp": t.timestamp.isoformat() if t.timestamp else None,
                "symbol": t.symbol,
                "action": t.action,
                "qty": t.qty,
                "price": t.price,
                "status": t.status,
                "order_id": t.order_id,
                "client_id": t.client_id,
            }
            for t in trades
        ]

    return jsonify(result)


@api_bp.route("/subscriptions", methods=["GET"])
@login_required_api
def list_subscriptions():
    """Return subscriptions for the logged in user."""
    user = current_user()
    subs = StrategySubscription.query.filter_by(subscriber_id=user.id).all()
    out = []
    for s in subs:
        out.append(
            {
                "id": s.id,
                "strategy_id": s.strategy_id,
                "strategy_name": s.strategy.name if s.strategy else None,
                "account_id": s.account_id,
                "account_client_id": s.account.client_id if s.account else None,
                "order_type": s.order_type,
                "qty_mode": s.qty_mode,
                "fixed_qty": s.fixed_qty,
                "auto_submit": s.auto_submit,
                "approved": s.approved,
            }
        )
    return jsonify(out)


@api_bp.route("/subscriptions/<int:sub_id>", methods=["PUT", "DELETE"])
@login_required_api
def update_subscription(sub_id):
    """Update or delete a subscription."""
    user = current_user()
    sub = StrategySubscription.query.filter_by(
        id=sub_id, subscriber_id=user.id
    ).first_or_404()

    if request.method == "DELETE":
        db.session.delete(sub)
        db.session.commit()
        return jsonify({"message": "Subscription deleted"})

    data = request.get_json() or {}
    if "account_id" in data:
        acc = Account.query.filter_by(id=data["account_id"], user_id=user.id).first()
        if not acc:
            return jsonify({"error": "Invalid account_id"}), 400
        sub.account_id = data["account_id"]
    if "auto_submit" in data:
        sub.auto_submit = bool(data.get("auto_submit"))
    if "order_type" in data:
        sub.order_type = data.get("order_type") or sub.order_type
    if "qty_mode" in data:
        sub.qty_mode = data.get("qty_mode") or sub.qty_mode
    if "fixed_qty" in data:
        sub.fixed_qty = data.get("fixed_qty")

    db.session.commit()
    return jsonify({"message": "Subscription updated"})
