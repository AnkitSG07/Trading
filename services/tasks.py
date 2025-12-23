"""Celery tasks for background trade polling."""
from __future__ import annotations

import logging
import os
import sys
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

# Ensure the repository root is on sys.path so imports like ``services`` and
# ``brokers`` resolve even when Celery workers start from a different working
# directory than the project root.  Mirrors the safeguard used in ``task.py``.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

if not os.environ.get("PYTHONPATH"):
    log.warning(
        "PYTHONPATH is not set; defaulting to project root %s for Celery imports",
        PROJECT_ROOT,
    )

from task import celery


@celery.task(name="services.tasks.poll_trades")
def poll_trades(max_iterations: int | None = 1, poll_interval: float | None = None) -> dict[str, Any]:
    """Poll master accounts for new trades and publish events.

    The task runs the :func:`services.master_trade_monitor.monitor_master_trades`
    loop for a bounded number of iterations so that it can be dispatched by
    Celery beat without running indefinitely inside a worker process.
    """

    from services.db import get_session
    from services.master_trade_monitor import monitor_master_trades

    session = get_session()
    try:
        monitor_master_trades(
            session,
            poll_interval=poll_interval,
            max_iterations=max_iterations,
        )
        return {"status": "ok"}
    except Exception as exc:  # pragma: no cover - defensive logging
        log.exception("poll_trades task failed: %s", exc)
        return {"status": "error", "error": str(exc)}
    finally:
        session.close()

@celery.task(name="services.tasks.refresh_dashboard_snapshot")
def refresh_dashboard_snapshot(user_id: int, client_id: str | None = None) -> dict[str, Any]:
    """Refresh the cached dashboard snapshot for the requested account.

    Celery workers invoke this task when :func:`blueprints.api._enqueue_snapshot_refresh`
    schedules a background refresh.  The task resolves the account using a
    standalone SQLAlchemy session so it can run outside a Flask application
    context, then delegates to :func:`blueprints.api.update_dashboard_snapshot_cache`
    to rebuild the cached payload.
    """

    from app import app
    from blueprints.api import update_dashboard_snapshot_cache
    from models import Account
    from services.db import get_session

    result: dict[str, Any] = {"status": "unknown"}
    with app.app_context():
        session = get_session()
        try:
            account = None
            if client_id:
                account = (
                    session.query(Account)
                    .filter_by(user_id=user_id, client_id=client_id)
                    .first()
                )
            else:
                account = (
                    session.query(Account)
                    .filter_by(user_id=user_id)
                    .order_by(Account.id)
                    .first()
                )

            if account is None:
                log.warning(
                    "No account found for user %s and client %s",
                    user_id,
                    client_id or "primary",
                )
                result = {"status": "missing", "error": "account_not_found"}
            else:
                snapshot = update_dashboard_snapshot_cache(account)
                result = {"status": "ok", "snapshot": snapshot}
        except Exception as exc:  # pragma: no cover - defensive logging
            log.exception(
                "refresh_dashboard_snapshot task failed for user %s client %s: %s",
                user_id,
                client_id or "primary",
                exc,
            )
            result = {"status": "error", "error": str(exc)}
        finally:
            session.close()
            log.info(
                "refresh_dashboard_snapshot completed for user %s client %s with status %s",
                user_id,
                client_id or "primary",
                result.get("status"),
            )

    return result
