from __future__ import annotations

"""Monitor master broker accounts for manually placed trades.

This service periodically queries each master account's broker API for new
executed orders and publishes matching events to the ``trade_events`` Redis
stream so that the trade copier can replicate them to child accounts.
"""

import asyncio
import logging
import os
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, Set

import redis

# Ensure the repository root is on sys.path so imports like ``services`` and
# ``brokers`` resolve even when Celery workers start from a different working
# directory than the project root. Mirrors the safeguard used in
# ``services.tasks`` and ``task.py``.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from brokers.factory import get_broker_client
from models import Account
from sqlalchemy.orm import Session
from helpers import extract_exchange_from_order

from .webhook_receiver import get_redis_client
from .utils import get_trade_events_maxlen
from .db import get_session

log = logging.getLogger(__name__)

# Order statuses that indicate a completed or filled order.  Orders in any
# other state (e.g. ``REJECTED`` or ``PENDING``) are ignored.  Include
# broker-specific values such as Dhan's ``TRADED`` status.
COMPLETED_STATUSES = {
    "COMPLETE",
    "COMPLETED", 
    "FILLED",
    "EXECUTED",
    "TRADED",
    "FULL_EXECUTED",
    "FULLY_EXECUTED",
    "PARTIAL_FILLED",
    "PARTIALLY_FILLED",
    "PARTIAL",
    "2",
    "CONFIRMED",
    "SUCCESS",
    "OK",
    "FULLY_FILLED",
}

# Keep track of processed orders to avoid duplicates across restarts
PROCESSED_ORDERS_KEY = "processed_manual_orders:{master_id}"
PROCESSED_ORDERS_TTL = 86400 * 7  # 7 days

def monitor_master_trades(
    db_session: Session,
    *,
    redis_client=None,
    poll_interval: float | None = None,
    max_iterations: int | None = None,
) -> None:
    """Poll master accounts for new orders and publish to ``trade_events``.

    Parameters
    ----------
    db_session:
        SQLAlchemy session used to look up master accounts.
    redis_client:
        Redis client instance used for publishing trade events. If ``None`` the
        client is created from :func:`services.webhook_receiver.get_redis_client`.
    poll_interval:
        Seconds to wait between polling iterations. If ``None`` the value of
        the ``ORDER_MONITOR_INTERVAL`` environment variable is used (default
        ``3`` seconds).
    max_iterations:
        Optional number of polling cycles to run.  ``None`` means run
        indefinitely.  Primarily intended for tests.
    """

    if poll_interval is None:
        poll_interval = float(os.getenv("ORDER_MONITOR_INTERVAL", "3"))

    if redis_client is None:
        redis_client = get_redis_client()

    async def _monitor() -> None:
        iterations = 0
        last_heartbeat: float = time.monotonic()
        heartbeat_interval = float(os.getenv("MONITOR_HEARTBEAT_INTERVAL", "300"))
        # Track order IDs we've already published for each master to avoid
        # emitting duplicate trade events when polling repeatedly.
        seen_order_ids: Dict[str, Set[str]] = defaultdict(set)
        # Remember masters that have invalid/expired credentials so we don't
        # spam the logs on every iteration.  The value is the last access token
        # seen for that master.  If the token changes we will attempt to fetch
        # orders again.
        invalid_creds: Dict[str, str] = {}
        # Track masters that should skip broker calls after receiving a rate
        # limit response.  Values are epoch timestamps after which requests may
        # resume.
        rate_limit_until: Dict[str, float] = {}

        RATE_LIMIT_COOLDOWN = 60

        def is_rate_limit_error(exc: Exception) -> bool:
            """Detect HTTP 429 or similar rate limit responses."""

            status_code = getattr(exc, "status_code", None)
            if status_code == 429:
                return True

            response = getattr(exc, "response", None)
            response_status = getattr(response, "status_code", None)
            if response_status == 429:
                return True

            message = str(exc).lower()
            return "429" in message or "too many requests" in message

        def apply_rate_limit(master_id: str, exc: Exception) -> None:
            """Record a cooldown after a rate limit error and log appropriately."""

            now = time.time()
            cooldown_until = now + RATE_LIMIT_COOLDOWN
            in_cooldown = rate_limit_until.get(master_id, 0) > now
            rate_limit_until[master_id] = cooldown_until

            if not in_cooldown:
                log.warning(
                    "Rate limit encountered while fetching orders for %s: %s. "
                    "Pausing requests for %s seconds.",
                    master_id,
                    exc,
                    RATE_LIMIT_COOLDOWN,
                )
            else:
                log.debug(
                    "Rate limit persists for %s: %s (cooldown until %s)",
                    master_id,
                    exc,
                    cooldown_until,
                )

        def clear_rate_limit(master_id: str) -> None:
            """Reset cooldown tracking once a successful response is received."""

            rate_limit_until.pop(master_id, None)
        
        # Load previously processed orders from Redis to handle service restarts
        def load_processed_orders(master_id: str) -> Set[str]:
            try:
                key = PROCESSED_ORDERS_KEY.format(master_id=master_id)
                processed = redis_client.smembers(key)
                return {p.decode() if isinstance(p, bytes) else str(p) for p in processed}
            except Exception:
                return set()
        
        def mark_order_processed(master_id: str, order_id: str):
            try:
                key = PROCESSED_ORDERS_KEY.format(master_id=master_id)
                redis_client.sadd(key, order_id)
                redis_client.expire(key, PROCESSED_ORDERS_TTL)
            except Exception:
                pass

        while max_iterations is None or iterations < max_iterations:
            # Ensure we see any credential updates made by other processes.
            # Without ending the previous transaction the session may return
            # stale Account objects, causing the monitor to keep using an
            # expired access token even after it has been refreshed in the
            # database.
            db_session.rollback()
            db_session.expire_all()
            masters: Iterable[Account] = (
                db_session.query(Account).filter_by(role="master").all()
            )
            
            for master in masters:
                # Skip masters that are explicitly disabled for copy trading.  Treat
                # missing/empty values as active so masters without a stored
                # ``copy_status`` are monitored by default.
                raw_status = getattr(master, "copy_status", None)
                normalized_status = ""
                if raw_status is not None:
                    normalized_status = str(raw_status).strip().lower()

                if normalized_status in {"off", "disabled", "false", "0", "inactive", "stopped"}:
                    continue

                cooldown_until = rate_limit_until.get(master.client_id)
                if cooldown_until and time.time() < cooldown_until:
                    log.debug(
                        "Skipping %s (%s) due to active rate limit cooldown until %.0f",
                        master.client_id,
                        master.broker,
                        cooldown_until,
                    )
                    continue

                credentials: Dict[str, Any] = dict(master.credentials or {})
                access_token = credentials.pop("access_token", "")
                # Some accounts may persist ``client_id`` within the credentials
                # blob.  Passing it alongside the explicit ``master.client_id``
                # argument would result in ``TypeError: multiple values for
                # client_id`` when instantiating the broker client, so drop it
                # here.
                credentials.pop("client_id", None)
                
                # Skip masters whose credentials are known to be invalid unless
                # the access token has changed since the last failure.
                cached_token = invalid_creds.get(master.client_id)
                if cached_token and cached_token == access_token:
                    continue

                client_cls = get_broker_client(master.broker)
                # Pass the access token as a keyword argument so that broker
                # clients which expect other positional parameters (like
                # ``api_key`` for AliceBlue) don't receive the token as a
                # second positional value.  Any additional credentials are
                # expanded as keyword arguments as well.
                try:
                    client = client_cls(
                        master.client_id, access_token=access_token, **credentials
                    )
                    
                    # Try multiple methods to get orders for better detection
                    orders = []

                    # Method 1: Try list_orders first (normalized format)
                    if hasattr(client, 'list_orders'):
                        try:
                            orders = client.list_orders()
                            clear_rate_limit(master.client_id)
                            log.debug(f"Got {len(orders)} orders from list_orders() for {master.client_id}")
                        except Exception as e:
                            if is_rate_limit_error(e):
                                apply_rate_limit(master.client_id, e)
                                continue
                            log.warning(f"list_orders() failed for {master.client_id}: {e}")

                    # Method 2: Try get_order_list if list_orders didn't work
                    if not orders and hasattr(client, 'get_order_list'):
                        try:
                            order_resp = client.get_order_list()
                            clear_rate_limit(master.client_id)
                            if isinstance(order_resp, dict) and order_resp.get("status") == "success":
                                orders = order_resp.get("data", []) or order_resp.get("orders", [])
                            elif isinstance(order_resp, list):
                                orders = order_resp
                            log.debug(f"Got {len(orders)} orders from get_order_list() for {master.client_id}")
                        except Exception as e:
                            if is_rate_limit_error(e):
                                apply_rate_limit(master.client_id, e)
                                continue
                            log.warning(f"get_order_list() failed for {master.client_id}: {e}")

                    # Method 3: Also try to get trade book for executed trades
                    if hasattr(client, 'get_trade_book'):
                        try:
                            trade_resp = client.get_trade_book()
                            clear_rate_limit(master.client_id)
                            if isinstance(trade_resp, dict) and trade_resp.get("status") == "success":
                                trades = trade_resp.get("trades", [])
                                log.debug(f"Got {len(trades)} trades from trade book for {master.client_id}")
                                # Convert trades to order format and add to orders
                                for trade in trades:
                                    trade_order = {
                                        "order_id": trade.get("NOrdNo") or trade.get("nestOrderNumber") or trade.get("order_id"),
                                        "symbol": trade.get("trading_symbol") or trade.get("symbol"),
                                        "action": trade.get("transtype") or trade.get("action"),
                                        "qty": trade.get("qty") or trade.get("quantity"),
                                        "exchange": trade.get("exch") or trade.get("exchange"),
                                        "order_type": trade.get("prctyp") or trade.get("order_type"),
                                        "status": "EXECUTED",  # Trades are always executed
                                        "order_time": trade.get("order_time") or trade.get("trade_time"),
                                        "price": trade.get("avg_price") or trade.get("price"),
                                        "product_type": trade.get("product_type") or trade.get("pCode"),
                                    }
                                    orders.append(trade_order)
                        except Exception as e:
                            if is_rate_limit_error(e):
                                apply_rate_limit(master.client_id, e)
                                continue
                            log.debug(f"Trade book fetch failed for {master.client_id}: {e}")
                    
                    log.debug(
                        "Master %s (%s): Total %d orders/trades found",
                        master.client_id,
                        master.broker,
                        len(orders),
                    )
                    
                    # If we successfully fetched orders, clear any cached
                    # invalid credential flag for this master.
                    invalid_creds.pop(master.client_id, None)
                except Exception as exc:  # pragma: no cover - defensive
                    msg = str(exc)
                    if any(keyword in msg.lower() for keyword in ["invalid", "token", "expired", "unauthorized"]):
                        # Log once per token and remember the failing token so
                        # future iterations skip this master until the token
                        # changes.
                        if cached_token != access_token:
                            log.error(
                                "failed to fetch manual orders for %s: %s",
                                master.client_id,
                                msg,
                            )
                        invalid_creds[master.client_id] = access_token
                    else:
                        log.exception(
                            "failed to fetch manual orders",
                            extra={"master": master.client_id},
                        )
                    continue

                # Load previously processed orders for this master. Refresh the
                # in-memory cache on every iteration so that orders recorded by
                # other services (e.g. the webhook consumer) are taken into
                # account immediately.  Previously we only loaded this data the
                # first time a master was encountered which meant subsequent
                # iterations missed freshly processed order IDs and resulted in
                # duplicate trade events being published.
                latest_processed = load_processed_orders(master.client_id)
                seen = seen_order_ids.setdefault(master.client_id, set())
                if latest_processed:
                    seen.update(latest_processed)
                new_orders_found = 0
                
                for order in orders:
                    # More flexible status checking
                    status = str(order.get("status") or "").upper()
                    if status and status not in COMPLETED_STATUSES:
                        # Only forward non-completed statuses if there is a positive fill
                        filled_qty = (
                            order.get("filled_qty")
                            or order.get("filledQty")
                            or order.get("Fillshares")
                            or 0
                        )
                        try:
                            if float(filled_qty) <= 0:
                                continue
                        except (ValueError, TypeError):
                            continue

                    candidate_id_fields = [
                        "id",
                        "order_id",
                        "orderId",
                        "orderNumber",
                        "norenordno",  # Finvasia
                        "NOrdNo",  # AliceBlue
                        "nestOrderNumber",  # AliceBlue
                        "orderid",  # Flattrade
                        "trade_id",
                        "exchangeTradeId",
                        "order_time",
                        "orderTime",
                        "trade_time",
                    ]
                    candidate_ids = []
                    for field in candidate_id_fields:
                        value = order.get(field)
                        if value is None:
                            continue
                        value_str = str(value).strip()
                        if not value_str:
                            continue
                        candidate_ids.append(value_str)
                    # Preserve order while ensuring uniqueness
                    candidate_ids = list(dict.fromkeys(candidate_ids))
                    order_id = candidate_ids[0] if candidate_ids else None

                    # Skip orders that were already published
                    if candidate_ids and any(candidate in seen for candidate in candidate_ids):
                        for candidate in candidate_ids:
                            if candidate not in seen:
                                seen.add(candidate)
                                mark_order_processed(master.client_id, candidate)
                        continue

                    # Extract order details with multiple field name attempts
                    symbol = (
                        order.get("symbol") 
                        or order.get("tradingSymbol") 
                        or order.get("tradingsymbol")
                        or order.get("trading_symbol")
                        or order.get("tsym")
                    )
                    
                    action = (
                        order.get("action") 
                        or order.get("transactionType")
                        or order.get("transaction_type") 
                        or order.get("trantype")
                        or order.get("transtype")
                        or order.get("side")
                    )
                    
                    qty = (
                        order.get("qty") 
                        or order.get("orderQty")
                        or order.get("quantity")
                        or order.get("filled_qty")  # Use filled quantity for partial fills
                        or order.get("filledQty")
                        or order.get("Fillshares")
                    )
                    
                    exchange = extract_exchange_from_order(order)
                    if not exchange:
                        exchange = (
                            order.get("exchange")
                            or order.get("exchangeSegment")
                            or order.get("exch")
                        )
                    
                    order_type = (
                        order.get("order_type") 
                        or order.get("orderType")
                        or order.get("prctyp")
                        or order.get("type")
                    )
                    
                    instrument_type = (
                        order.get("instrument_type")
                        or order.get("instrumentType")
                        or order.get("instrument")
                        or order.get("instType")
                        or order.get("iType")
                    )

                    product_type = None
                    for field in (
                        "product_type",
                        "productType",
                        "product",
                        "productcode",
                        "productCode",
                        "pCode",
                        "prodType",
                    ):
                        value = order.get(field)
                        if value is None:
                            continue
                        if isinstance(value, str) and not value.strip():
                            continue
                        product_type = value
                        break
                    
                    # Additional fields for derivatives
                    expiry = order.get("expiry") or order.get("expiryDate")
                    strike = order.get("strike") or order.get("strikePrice")
                    option_type = (
                        order.get("option_type") 
                        or order.get("optionType") 
                        or order.get("right")
                    )
                    lot_size = order.get("lot_size") or order.get("lotSize")
                    
                    # Price information
                    price = (
                        order.get("price")
                        or order.get("avg_price")
                        or order.get("average_price")
                        or order.get("avgPrice")
                        or order.get("last_price")
                        or order.get("ltp")
                    )

                    # Skip orders missing essential information
                    if not symbol or not action or not qty:
                        log.warning(
                            "Skipping manual order with missing essential data: symbol=%s, action=%s, qty=%s, order_id=%s",
                            symbol, action, qty, order_id
                        )
                        continue

                    # Normalize action to standard format
                    action_upper = str(action).upper()
                    if action_upper in ["B", "BUY", "1"]:
                        action = "BUY"
                    elif action_upper in ["S", "SELL", "-1", "2"]:
                        action = "SELL"
                    else:
                        action = action_upper

                    # Normalize symbol format for consistency
                    try:
                        from services.fo_symbol_utils import normalize_symbol_to_dhan_format
                        normalized_symbol = normalize_symbol_to_dhan_format(symbol)
                        if normalized_symbol != symbol:
                            log.debug(
                                "Normalized manual trade symbol from %s to %s",
                                symbol,
                                normalized_symbol,
                            )
                            symbol = normalized_symbol
                    except Exception as e:
                        log.warning(f"Could not normalize symbol {symbol}: {e}")

                    # Determine instrument type from symbol if not provided
                    if not instrument_type and symbol:
                        sym_upper = symbol.upper()
                        if sym_upper.endswith('FUT') or '-FUT' in sym_upper:
                            instrument_type = 'FUTIDX' if 'NIFTY' in sym_upper else 'FUTSTK'
                        elif sym_upper.endswith(('CE', 'PE')) or any(x in sym_upper for x in ['-CE', '-PE']):
                            instrument_type = 'OPTIDX' if 'NIFTY' in sym_upper else 'OPTSTK'
                        elif sym_upper.endswith('-EQ') or (not any(x in sym_upper for x in ['FUT', 'CE', 'PE'])):
                            instrument_type = 'EQ'

                    # Set default exchange based on instrument type if not provided
                    if not exchange:
                        if instrument_type in ['FUTIDX', 'FUTSTK', 'OPTIDX', 'OPTSTK']:
                            exchange = 'NFO'
                        else:
                            exchange = 'NSE'

                    if exchange:
                        normalized_exchange = extract_exchange_from_order({"exchange": exchange})
                        if normalized_exchange:
                            exchange = normalized_exchange
                        else:
                            exchange = str(exchange).strip().upper()

                    raw_event = {
                        "master_id": master.client_id,
                        "symbol": symbol,
                        "action": action,
                        "qty": qty,
                        "exchange": exchange,
                        "order_type": order_type,
                        "instrument_type": instrument_type,
                        "expiry": expiry,
                        "strike": strike,
                        "option_type": option_type,
                        "lot_size": lot_size,
                        "price": price,
                        "product_type": product_type,
                        "order_id": order_id,
                        "order_time": order.get("order_time") or order.get("orderTime") or order.get("trade_time"),
                        "source": "manual_trade_monitor",  # Mark as manual trade
                        "broker": master.broker,  # Add broker info for debugging
                    }
                    
                    # Filter out ``None`` values and convert the rest to strings so
                    # the Redis client never receives ``None``.
                    event = {k: str(v) for k, v in raw_event.items() if v is not None}
                    
                    try:
                        # Publish to trade_events stream for copying to children
                        redis_client.xadd(
                            "trade_events",
                            event,
                            maxlen=get_trade_events_maxlen(),
                            approximate=True,
                        )
                        log.debug(
                            "📤 Published manual trade event for master %s (%s): %s %s %s at %s [Order ID: %s]",
                            master.client_id,
                            master.broker,
                            action,
                            qty,
                            symbol,
                            exchange,
                            order_id,
                        )
                        new_orders_found += 1
                    except redis.exceptions.RedisError:
                        log.exception("failed to publish manual trade event")
                        continue

                    # Mark order as processed
                    for candidate in candidate_ids:
                        if candidate not in seen:
                            seen.add(candidate)
                        mark_order_processed(master.client_id, candidate)

                if new_orders_found > 0:
                    log.info(
                        "Found %d new manual orders for master %s (%s)",
                        new_orders_found,
                        master.client_id,
                        master.broker,
                    )
                    log.info(
                        "Master %s (%s): Total %d orders/trades found",
                        master.client_id,
                        master.broker,
                        len(orders),
                    )
                elif iterations % 20 == 0:  # Log every 20 iterations to show it's working
                    log.debug(
                        "No new manual orders for master %s (%s)",
                        master.client_id,
                        master.broker,
                    )

            iterations += 1
            now = time.monotonic()
            if now - last_heartbeat >= heartbeat_interval:
                log.info(
                    "Heartbeat: master trade monitor alive (iteration %d)",
                    iterations,
                )
                last_heartbeat = now
            if max_iterations is not None and iterations >= max_iterations:
                break
            await asyncio.sleep(poll_interval)

    asyncio.run(_monitor())


def main() -> None:
    """Run the master trade monitor service."""
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO"),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    log.info("🚀 Starting master trade monitor service...")
    log.info(f"📊 Polling interval: {os.getenv('ORDER_MONITOR_INTERVAL', '3')} seconds")
    
    session = get_session()
    try:
        monitor_master_trades(session)
    finally:
        session.close()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:  # pragma: no cover - interactive use
        log.info("👋 Master trade monitor service stopped by user")
        pass


__all__ = ["monitor_master_trades", "main"]
