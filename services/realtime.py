"""WebSocket event handlers and helpers for real-time updates."""
from __future__ import annotations

import logging
import os
import time
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Any, Deque, Dict, List, Optional, Tuple

from flask import request, session
from flask_socketio import SocketIO, emit, join_room, leave_room


logger = logging.getLogger(__name__)
_socketio: Optional[SocketIO] = None


@dataclass(frozen=True)
class RecordedEvent:
    """Normalized payload stored for replay/backfill."""

    event: str
    payload: Dict[str, Any]
    timestamp: float


class BackfillStore:
    """Keeps a bounded buffer of recent events per room for replay."""

    def __init__(self, limit: int = 200):
        self.limit = limit
        self._events: Dict[str, Deque[RecordedEvent]] = defaultdict(deque)

    def record(self, room: str, event: str, payload: Dict[str, Any]) -> None:
        stream = self._events[room]
        stream.append(RecordedEvent(event=event, payload=payload, timestamp=time.time()))
        while len(stream) > self.limit:
            stream.popleft()

    def recent(self, room: str, limit: int = 50) -> List[RecordedEvent]:
        stream = self._events.get(room)
        if not stream:
            return []
        return list(stream)[-limit:]


class RateLimiter:
    """Simple in-memory limiter to prevent noisy clients."""

    def __init__(self, max_events: int, window_seconds: float):
        self.max_events = max_events
        self.window_seconds = window_seconds
        self._events: Dict[Tuple[str, str], Deque[float]] = defaultdict(deque)

    def allow(self, key: Tuple[str, str]) -> bool:
        now = time.time()
        window_start = now - self.window_seconds
        events = self._events[key]
        while events and events[0] < window_start:
            events.popleft()
        if len(events) >= self.max_events:
            return False
        events.append(now)
        return True


_backfill_store = BackfillStore(limit=int(os.environ.get("REALTIME_BACKFILL_LIMIT", "200")))
_subscription_counts: Dict[str, int] = defaultdict(int)
_room_rate_limiter = RateLimiter(max_events=20, window_seconds=5.0)


def _is_authenticated() -> bool:
    """Best-effort auth check for Socket.IO connect events."""

    try:
        return bool(session.get("user_id"))
    except Exception:  # pragma: no cover - defensive fallback
        return False


def _enrich(event_name: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    """Attach delivery metadata to outbound events."""

    envelope: Dict[str, Any]
    if isinstance(payload, dict):
        envelope = dict(payload)
    else:
        envelope = {"data": payload}
    envelope.setdefault("message_id", str(uuid.uuid4()))
    envelope.setdefault("sent_at", time.time())
    envelope.setdefault("event", event_name)
    return envelope


def _emit(event_name: str, payload: Dict[str, Any], room: Optional[str] = None) -> None:
    enriched = _enrich(event_name, payload)
    target_room = room or "broadcast"
    _backfill_store.record(target_room, event_name, enriched)
    if not _socketio:
        return
    _socketio.emit(event_name, enriched, room=room)


def init_realtime(socketio: SocketIO) -> None:
    """Register Socket.IO event handlers.

    This binds core connection and subscription events while keeping the
    handlers isolated from the rest of the application logic.
    """

    global _socketio
    _socketio = socketio
    require_auth = os.environ.get("SOCKETIO_REQUIRE_AUTH", "0") == "1"

    @socketio.on("connect")
    def handle_connect():
        if require_auth and not _is_authenticated():
            logger.warning("Socket.IO connection rejected: unauthenticated")
            return False
        emit("server_ready", {"message": "Connected"})

    @socketio.on("subscribe_room")
    def handle_subscribe(data: Dict[str, Any] | None):
        room = (data or {}).get("room")
        if not room:
            emit("subscription_error", {"error": "room is required"})
            return
        rate_key = (request.sid, "subscribe_room")
        if not _room_rate_limiter.allow(rate_key):
            emit("subscription_error", {"error": "rate_limited"})
            return
        join_room(room)
        _subscription_counts[room] += 1
        emit("subscribed", {"room": room, "connections": _subscription_counts[room]}, room=room)

    @socketio.on("unsubscribe_room")
    def handle_unsubscribe(data: Dict[str, Any] | None):
        room = (data or {}).get("room")
        if not room:
            emit("unsubscription_error", {"error": "room is required"})
            return
        leave_room(room)
        _subscription_counts[room] = max(0, _subscription_counts[room] - 1)
        emit("unsubscribed", {"room": room, "connections": _subscription_counts[room]})

    @socketio.on("disconnect")
    def handle_disconnect():
        emit("disconnected", {"message": "Disconnected"})

    @socketio.on("heartbeat")
    def handle_heartbeat(data: Dict[str, Any] | None):
        emit("heartbeat_ack", {"echo": data or {}, "status": "ok"})

    @socketio.on("fetch_recent")
    def handle_fetch_recent(data: Dict[str, Any] | None):
        room = (data or {}).get("room")
        if not room:
            emit("recent_error", {"error": "room is required"})
            return
        limit = int((data or {}).get("limit", 50))
        events = [e.__dict__ for e in _backfill_store.recent(room, limit=limit)]
        emit("recent_events", {"room": room, "events": events})


def get_recent_events(room: str, limit: int = 50) -> List[Dict[str, Any]]:
    """Expose recorded events for HTTP backfill endpoints."""

    return [e.__dict__ for e in _backfill_store.recent(room, limit=limit)]


def push_trade_update(account_id: str, payload: Dict[str, Any]) -> None:
    """Send a trade update to a user-specific room."""

    room = f"acct:{account_id}"
    _emit("trade_update", payload, room=room)


def push_portfolio_ticker(account_id: str, payload: Dict[str, Any]) -> None:
    """Push portfolio/PNL snapshots to a user-specific room."""

    room = f"acct:{account_id}"
    _emit("portfolio_ticker", payload, room=room)


def push_order_update(account_id: str, payload: Dict[str, Any]) -> None:
    """Broadcast order lifecycle changes to the requesting account."""

    room = f"acct:{account_id}"
    _emit("order_update", payload, room=room)


def push_price_alert(account_id: str, payload: Dict[str, Any]) -> None:
    """Send price/margin/risk alerts to the account room."""

    room = f"acct:{account_id}"
    _emit("price_alert", payload, room=room)


def push_notification(account_id: str, payload: Dict[str, Any]) -> None:
    """Send general notifications to the account room."""

    room = f"acct:{account_id}"
    _emit("notification", payload, room=room)


def push_market_data(symbol: str, payload: Dict[str, Any]) -> None:
    """Stream market data or order book deltas to symbol-specific rooms."""

    room = f"symbol:{symbol}"
    _emit("market_data", payload, room=room)


def push_background_job_progress(job_id: str, payload: Dict[str, Any]) -> None:
    """Emit progress updates for long-running background jobs."""

    room = f"job:{job_id}"
    _emit("job_progress", payload, room=room)


def push_collaboration_activity(dashboard_id: str, payload: Dict[str, Any]) -> None:
    """Share collaborative events (presence, cursors, notes) per dashboard room."""

    room = f"dashboard:{dashboard_id}"
    _emit("collaboration", payload, room=room)


def push_system_notice(payload: Dict[str, Any]) -> None:
    """Emit system health notices globally or to specific rooms if provided."""

    room = payload.get("room") if isinstance(payload, dict) else None
    _emit("system_notice", payload, room=room)


def push_latency_alert(payload: Dict[str, Any]) -> None:
    """Emit latency or degradation alerts for operators."""

    room = payload.get("room") if isinstance(payload, dict) else None
    _emit("latency_alert", payload, room=room)


def push_feature_flag_update(payload: Dict[str, Any]) -> None:
    """Stream feature flag or config rollouts to clients."""

    _emit("feature_flag", payload)


def push_audit_event(payload: Dict[str, Any]) -> None:
    """Mirror critical audit events to admin listeners."""

    room = payload.get("room") if isinstance(payload, dict) else "audit"
    _emit("audit_event", payload, room=room)


def push_chat_message(room: str, payload: Dict[str, Any]) -> None:
    """Send chat/support/copilot messages to a shared room."""

    target_room = room if room.startswith("chat:") else f"chat:{room}"
    _emit("chat_message", payload, room=target_room)
