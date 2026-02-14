"""fix analyze threshold

Revision ID: a5202da9c3ad
Revises: faa65a97e02e
Create Date: 2026-02-14 13:44:09.383378

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'a5202da9c3ad'
down_revision = 'faa65a97e02e'
branch_labels = None
depends_on = None


def set_storage_params(table, **kwargs):
    storage_params = ', '.join(
        f"{param} = {str(value).lower()}" for param, value in kwargs.items()
    )
    op.execute(f"ALTER TABLE {table} SET ({storage_params})")


def reset_storage_params(table, param_names):
    op.execute(f"ALTER TABLE {table} RESET ({', '.join(param_names)})")


def upgrade():
    reset_storage_params(
        'registered_balance_change',
        [
            'autovacuum_analyze_threshold',
        ]
    )

    # Buffer tables:
    reset_storage_params(
        'pending_balance_change',
        [
            'autovacuum_analyze_threshold',
        ]
    )
    reset_storage_params(
        'finalization_request',
        [
            'autovacuum_analyze_threshold',
        ]
    )
    reset_storage_params(
        'transfer_request',
        [
            'autovacuum_analyze_threshold',
        ]
    )

    # Signals:
    reset_storage_params(
        'rejected_transfer_signal',
        [
            'autovacuum_analyze_threshold',
        ]
    )
    reset_storage_params(
        'prepared_transfer_signal',
        [
            'autovacuum_analyze_threshold',
        ]
    )
    reset_storage_params(
        'finalized_transfer_signal',
        [
            'autovacuum_analyze_threshold',
        ]
    )
    reset_storage_params(
        'account_transfer_signal',
        [
            'autovacuum_analyze_threshold',
        ]
    )
    reset_storage_params(
        'account_update_signal',
        [
            'autovacuum_analyze_threshold',
        ]
    )
    reset_storage_params(
        'account_purge_signal',
        [
            'autovacuum_analyze_threshold',
        ]
    )
    reset_storage_params(
        'rejected_config_signal',
        [
            'autovacuum_analyze_threshold',
        ]
    )
    reset_storage_params(
        'pending_balance_change_signal',
        [
            'autovacuum_analyze_threshold',
        ]
    )


def downgrade():
    set_storage_params(
        'registered_balance_change',
        autovacuum_analyze_threshold=2000000000,
    )

    # Buffer tables:
    set_storage_params(
        'pending_balance_change',
        autovacuum_analyze_threshold=2000000000,
    )
    set_storage_params(
        'finalization_request',
        autovacuum_analyze_threshold=2000000000,
    )
    set_storage_params(
        'transfer_request',
        autovacuum_analyze_threshold=2000000000,
    )

    # Signals:
    set_storage_params(
        'rejected_transfer_signal',
        autovacuum_analyze_threshold=2000000000,
    )
    set_storage_params(
        'prepared_transfer_signal',
        autovacuum_analyze_threshold=2000000000,
    )
    set_storage_params(
        'finalized_transfer_signal',
        autovacuum_analyze_threshold=2000000000,
    )
    set_storage_params(
        'account_transfer_signal',
        autovacuum_analyze_threshold=2000000000,
    )
    set_storage_params(
        'account_update_signal',
        autovacuum_analyze_threshold=2000000000,
    )
    set_storage_params(
        'account_purge_signal',
        autovacuum_analyze_threshold=2000000000,
    )
    set_storage_params(
        'rejected_config_signal',
        autovacuum_analyze_threshold=2000000000,
    )
    set_storage_params(
        'pending_balance_change_signal',
        autovacuum_analyze_threshold=2000000000,
    )
