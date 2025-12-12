"""storage params

Revision ID: aa6b6a4a1e95
Revises: 7a49b06e1eb6
Create Date: 2025-12-11 17:51:09.906283

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'aa6b6a4a1e95'
down_revision = '7a49b06e1eb6'
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
    op.execute("ALTER TABLE account ALTER COLUMN config_data SET STORAGE EXTERNAL")

    set_storage_params(
        'account',
        toast_tuple_target=420,
        fillfactor=80,
        autovacuum_vacuum_scale_factor=0.08,
        autovacuum_vacuum_insert_scale_factor=0.2,
    )
    set_storage_params(
        'prepared_transfer',
        fillfactor=100,
        autovacuum_vacuum_scale_factor=0.2,
        autovacuum_vacuum_insert_scale_factor=0.2,
    )
    set_storage_params(
        'registered_balance_change',
        fillfactor=100,
        autovacuum_vacuum_scale_factor=0.2,
        autovacuum_vacuum_insert_threshold=-1,
        autovacuum_analyze_threshold=2000000000,
    )

    # Buffer tables:
    set_storage_params(
        'pending_balance_change',
        fillfactor=100,
        autovacuum_vacuum_cost_delay=0.0,
        autovacuum_vacuum_insert_threshold=-1,
        autovacuum_analyze_threshold=2000000000,
    )
    set_storage_params(
        'finalization_request',
        fillfactor=100,
        autovacuum_vacuum_cost_delay=0.0,
        autovacuum_vacuum_insert_threshold=-1,
        autovacuum_analyze_threshold=2000000000,
    )
    set_storage_params(
        'transfer_request',
        fillfactor=100,
        autovacuum_vacuum_cost_delay=0.0,
        autovacuum_vacuum_insert_threshold=-1,
        autovacuum_analyze_threshold=2000000000,
    )

    # Signals:
    set_storage_params(
        'rejected_transfer_signal',
        fillfactor=100,
        autovacuum_vacuum_cost_delay=0.0,
        autovacuum_vacuum_insert_threshold=-1,
        autovacuum_analyze_threshold=2000000000,
    )
    set_storage_params(
        'prepared_transfer_signal',
        fillfactor=100,
        autovacuum_vacuum_cost_delay=0.0,
        autovacuum_vacuum_insert_threshold=-1,
        autovacuum_analyze_threshold=2000000000,
    )
    set_storage_params(
        'finalized_transfer_signal',
        fillfactor=100,
        autovacuum_vacuum_cost_delay=0.0,
        autovacuum_vacuum_insert_threshold=-1,
        autovacuum_analyze_threshold=2000000000,
    )
    set_storage_params(
        'account_transfer_signal',
        fillfactor=100,
        autovacuum_vacuum_cost_delay=0.0,
        autovacuum_vacuum_insert_threshold=-1,
        autovacuum_analyze_threshold=2000000000,
    )
    set_storage_params(
        'account_update_signal',
        fillfactor=100,
        autovacuum_vacuum_cost_delay=0.0,
        autovacuum_vacuum_insert_threshold=-1,
        autovacuum_analyze_threshold=2000000000,
    )
    set_storage_params(
        'account_purge_signal',
        fillfactor=100,
        autovacuum_vacuum_cost_delay=0.0,
        autovacuum_vacuum_insert_threshold=-1,
        autovacuum_analyze_threshold=2000000000,
    )
    set_storage_params(
        'rejected_config_signal',
        fillfactor=100,
        autovacuum_vacuum_cost_delay=0.0,
        autovacuum_vacuum_insert_threshold=-1,
        autovacuum_analyze_threshold=2000000000,
    )
    set_storage_params(
        'pending_balance_change_signal',
        fillfactor=100,
        autovacuum_vacuum_cost_delay=0.0,
        autovacuum_vacuum_insert_threshold=-1,
        autovacuum_analyze_threshold=2000000000,
    )


def downgrade():
    op.execute("ALTER TABLE account ALTER COLUMN config_data SET STORAGE DEFAULT")

    reset_storage_params(
        'account',
        [
            'toast_tuple_target',
            'fillfactor',
            'autovacuum_vacuum_scale_factor',
            'autovacuum_vacuum_insert_scale_factor',
        ]
    )
    reset_storage_params(
        'prepared_transfer',
        [
            'fillfactor',
            'autovacuum_vacuum_scale_factor',
            'autovacuum_vacuum_insert_scale_factor',
        ]
    )
    reset_storage_params(
        'registered_balance_change',
        [
            'fillfactor',
            'autovacuum_vacuum_scale_factor',
            'autovacuum_vacuum_insert_threshold',
            'autovacuum_analyze_threshold',
        ]
    )

    # Buffer tables:
    reset_storage_params(
        'pending_balance_change',
        [
            'fillfactor',
            'autovacuum_vacuum_cost_delay',
            'autovacuum_vacuum_insert_threshold',
            'autovacuum_analyze_threshold',
        ]
    )
    reset_storage_params(
        'finalization_request',
        [
            'fillfactor',
            'autovacuum_vacuum_cost_delay',
            'autovacuum_vacuum_insert_threshold',
            'autovacuum_analyze_threshold',
        ]
    )
    reset_storage_params(
        'transfer_request',
        [
            'fillfactor',
            'autovacuum_vacuum_cost_delay',
            'autovacuum_vacuum_insert_threshold',
            'autovacuum_analyze_threshold',
        ]
    )

    # Signals:
    reset_storage_params(
        'rejected_transfer_signal',
        [
            'fillfactor',
            'autovacuum_vacuum_cost_delay',
            'autovacuum_vacuum_insert_threshold',
            'autovacuum_analyze_threshold',
        ]
    )
    reset_storage_params(
        'prepared_transfer_signal',
        [
            'fillfactor',
            'autovacuum_vacuum_cost_delay',
            'autovacuum_vacuum_insert_threshold',
            'autovacuum_analyze_threshold',
        ]
    )
    reset_storage_params(
        'finalized_transfer_signal',
        [
            'fillfactor',
            'autovacuum_vacuum_cost_delay',
            'autovacuum_vacuum_insert_threshold',
            'autovacuum_analyze_threshold',
        ]
    )
    reset_storage_params(
        'account_transfer_signal',
        [
            'fillfactor',
            'autovacuum_vacuum_cost_delay',
            'autovacuum_vacuum_insert_threshold',
            'autovacuum_analyze_threshold',
        ]
    )
    reset_storage_params(
        'account_update_signal',
        [
            'fillfactor',
            'autovacuum_vacuum_cost_delay',
            'autovacuum_vacuum_insert_threshold',
            'autovacuum_analyze_threshold',
        ]
    )
    reset_storage_params(
        'account_purge_signal',
        [
            'fillfactor',
            'autovacuum_vacuum_cost_delay',
            'autovacuum_vacuum_insert_threshold',
            'autovacuum_analyze_threshold',
        ]
    )
    reset_storage_params(
        'rejected_config_signal',
        [
            'fillfactor',
            'autovacuum_vacuum_cost_delay',
            'autovacuum_vacuum_insert_threshold',
            'autovacuum_analyze_threshold',
        ]
    )
    reset_storage_params(
        'pending_balance_change_signal',
        [
            'fillfactor',
            'autovacuum_vacuum_cost_delay',
            'autovacuum_vacuum_insert_threshold',
            'autovacuum_analyze_threshold',
        ]
    )
