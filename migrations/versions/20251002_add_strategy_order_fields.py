"""Add order configuration fields to strategy"""

from alembic import op
import sqlalchemy as sa


revision = "20251002_add_strategy_order_fields"
down_revision = "20251001_add_user_address_fields"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("strategy", sa.Column("exchange", sa.String(length=20), nullable=True))
    op.add_column("strategy", sa.Column("transaction_type", sa.String(length=20), nullable=True))
    op.add_column("strategy", sa.Column("order_type", sa.String(length=20), nullable=True))
    op.add_column("strategy", sa.Column("order_validity", sa.String(length=20), nullable=True))
    op.add_column("strategy", sa.Column("product_type", sa.String(length=20), nullable=True))
    op.add_column("strategy", sa.Column("order_qty", sa.Float(), nullable=True))
    op.add_column("strategy", sa.Column("order_value", sa.Float(), nullable=True))


def downgrade():
    op.drop_column("strategy", "order_value")
    op.drop_column("strategy", "order_qty")
    op.drop_column("strategy", "product_type")
    op.drop_column("strategy", "order_validity")
    op.drop_column("strategy", "order_type")
    op.drop_column("strategy", "transaction_type")
    op.drop_column("strategy", "exchange")
