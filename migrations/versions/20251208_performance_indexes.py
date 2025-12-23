"""Add performance indexes

Revision ID: 20251208_performance_indexes
Revises: 20251002_add_strategy_order_fields
Create Date: 2025-12-08 20:26:00.000000

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '20251208_performance_indexes'
down_revision = '20251002_add_strategy_order_fields'
branch_labels = None
depends_on = None


def upgrade():
    # Add index on system_log for faster error log queries
    op.create_index(
        'idx_systemlog_user_level_module',
        'system_log',
        ['user_id', 'level', 'module'],
        unique=False
    )
    
    # Add index on system_log timestamp for faster sorting
    op.create_index(
        'idx_systemlog_timestamp',
        'system_log',
        ['timestamp'],
        unique=False
    )
    
    # Add index on account for faster user account queries
    op.create_index(
        'idx_account_user_broker',
        'account',
        ['user_id', 'broker'],
        unique=False
    )
    
    # Add index on account status for filtering
    op.create_index(
        'idx_account_user_status',
        'account',
        ['user_id', 'status'],
        unique=False
    )
    
    # Add index on account role for master/child queries
    op.create_index(
        'idx_account_role_master',
        'account',
        ['user_id', 'role', 'linked_master_id'],
        unique=False
    )
    
    # Add index on account client_id for faster lookups
    op.create_index(
        'idx_account_client_id',
        'account',
        ['client_id'],
        unique=False
    )


def downgrade():
    # Drop indexes in reverse order
    op.drop_index('idx_account_client_id', table_name='account')
    op.drop_index('idx_account_role_master', table_name='account')
    op.drop_index('idx_account_user_status', table_name='account')
    op.drop_index('idx_account_user_broker', table_name='account')
    op.drop_index('idx_systemlog_timestamp', table_name='system_log')
    op.drop_index('idx_systemlog_user_level_module', table_name='system_log')
