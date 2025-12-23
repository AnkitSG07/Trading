"""
Celery Background Tasks for Production Dashboard
Handles heavy operations asynchronously to keep API responses fast.
"""
from celery import Celery
from celery.schedules import crontab
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

# Ensure the repository root is on sys.path so `services` and `brokers`
# modules can be imported reliably when Celery bootstraps, even if the
# worker process is started without PYTHONPATH being set.
PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Warn deployers if PYTHONPATH is not explicitly pointing at the repo root
if not os.environ.get("PYTHONPATH"):
    logger.warning(
        "PYTHONPATH is not set; defaulting to project root %s for Celery imports",
        PROJECT_ROOT,
    )
    
# Initialize Celery
celery_broker = os.environ.get('CELERY_BROKER_URL', 'redis://localhost:6379/0')
celery_backend = os.environ.get('CELERY_RESULT_BACKEND', 'redis://localhost:6379/0')

celery = Celery(
    'dhanbot_tasks',
    broker=celery_broker,
    backend=celery_backend
)

# Celery configuration
celery.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='Asia/Kolkata',
    enable_utc=True,
    task_track_started=True,
    task_time_limit=300,  # 5 minutes max
    worker_prefetch_multiplier=1,
    worker_concurrency=int(os.getenv("CELERY_WORKER_CONCURRENCY", "1")),
    worker_max_tasks_per_child=int(
        os.getenv("CELERY_WORKER_MAX_TASKS_PER_CHILD", "100")
    ),
)

# Import task modules so Celery workers register all tasks when starting up.
import services.tasks  # noqa: E402  pylint: disable=wrong-import-position

@celery.task(name='tasks.fetch_portfolio_snapshot')
def fetch_portfolio_snapshot(account_id, user_id):
    """
    Fetch portfolio snapshot from broker API in background.
    Heavy operation that can take 5-10 seconds per account.
    """
    try:
        from app import db, Account, cache
        from brokers.factory import get_broker_client
        
        logger.info(f"Fetching portfolio snapshot for account {account_id}")
        
        account = Account.query.get(account_id)
        if not account:
            logger.error(f"Account {account_id} not found")
            return {'error': 'Account not found'}
        
        # Get broker client
        broker_class = get_broker_client(account.broker)
        if not broker_class:
            return {'error': f'Unsupported broker: {account.broker}'}
        
        # Initialize broker with credentials
        creds = account.credentials or {}
        broker = broker_class(
            account.client_id,
            creds.get('access_token'),
            **{k: v for k, v in creds.items() if k != 'access_token'}
        )
        
        # Fetch holdings and positions
        holdings = broker.get_holdings() if hasattr(broker, 'get_holdings') else []
        positions = broker.get_positions() if hasattr(broker, 'get_positions') else []
        
        snapshot = {
            'account_id': account_id,
            'holdings': holdings,
            'positions': positions,
            'timestamp': datetime.utcnow().isoformat()
        }
        
        # Cache the result for 5 minutes
        cache_key = f'portfolio_snapshot:{account_id}'
        cache.set(cache_key, snapshot, timeout=300)
        
        logger.info(f"Portfolio snapshot cached for account {account_id}")
        return snapshot
        
    except Exception as e:
        logger.error(f"Error fetching portfolio for account {account_id}: {str(e)}")
        return {'error': str(e)}


@celery.task(name='tasks.refresh_account_balances')
def refresh_account_balances(user_id):
    """
    Refresh balances for all user accounts in background.
    Runs periodically to keep dashboard data fresh.
    """
    try:
        from app import db, Account, cache
        from brokers.factory import get_broker_client
        
        logger.info(f"Refreshing balances for user {user_id}")
        
        accounts = Account.query.filter_by(user_id=user_id, status='Connected').all()
        results = []
        
        for account in accounts:
            try:
                broker_class = get_broker_client(account.broker)
                if not broker_class:
                    continue
                
                creds = account.credentials or {}
                broker = broker_class(
                    account.client_id,
                    creds.get('access_token'),
                    **{k: v for k, v in creds.items() if k != 'access_token'}
                )
                
                # Get balance
                if hasattr(broker, 'get_opening_balance'):
                    balance = broker.get_opening_balance()
                    
                    # Cache balance
                    cache_key = f'opening_balance:{account.broker}:{account.client_id}'
                    cache.set(cache_key, balance, timeout=300)
                    
                    results.append({
                        'account_id': account.id,
                        'client_id': account.client_id,
                        'balance': balance
                    })
                    
            except Exception as e:
                logger.error(f"Error refreshing balance for {account.client_id}: {str(e)}")
                continue
        
        logger.info(f"Refreshed {len(results)} account balances for user {user_id}")
        return results
        
    except Exception as e:
        logger.error(f"Error refreshing balances for user {user_id}: {str(e)}")
        return {'error': str(e)}


@celery.task(name='tasks.cleanup_old_logs')
def cleanup_old_logs(days=30):
    """
    Clean up old system logs to prevent database bloat.
    Runs daily to remove logs older than specified days.
    """
    try:
        from app import db, SystemLog
        
        cutoff_date = datetime.utcnow() - timedelta(days=days)
        
        deleted = SystemLog.query.filter(
            SystemLog.timestamp < cutoff_date
        ).delete()
        
        db.session.commit()
        
        logger.info(f"Cleaned up {deleted} old log entries")
        return {'deleted': deleted, 'cutoff_date': cutoff_date.isoformat()}
        
    except Exception as e:
        logger.error(f"Error cleaning up logs: {str(e)}")
        db.session.rollback()
        return {'error': str(e)}


# Periodic tasks configuration
@celery.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    """Configure periodic background tasks."""
    
    # Clean up old logs daily at 2 AM
    sender.add_periodic_task(
        crontab(hour=2, minute=0),
        cleanup_old_logs.s(days=30),
        name='cleanup-old-logs-daily'
    )


if __name__ == '__main__':
    celery.start()
