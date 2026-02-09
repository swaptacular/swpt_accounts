"""create pktypes

Revision ID: faa65a97e02e
Revises: aa6b6a4a1e95
Create Date: 2026-02-09 18:45:59.759624

"""
from alembic import op
import sqlalchemy as sa
from datetime import datetime
from sqlalchemy.inspection import inspect

# revision identifiers, used by Alembic.
revision = 'faa65a97e02e'
down_revision = 'aa6b6a4a1e95'
branch_labels = None
depends_on = None


def _pg_type(column_type):
    if column_type.python_type == datetime:
        if column_type.timezone:
            return "TIMESTAMP WITH TIME ZONE"
        else:
            return "TIMESTAMP"

    return str(column_type)


def _pktype_name(model):
    return f"{model.__table__.name}_pktype"


def create_pktype(model):
    mapper = inspect(model)
    type_declaration = ','.join(
        f"{c.key} {_pg_type(c.type)}" for c in mapper.primary_key
    )
    op.execute(
        f"CREATE TYPE {_pktype_name(model)} AS ({type_declaration})"
    )


def drop_pktype(model):
    op.execute(f"DROP TYPE IF EXISTS {_pktype_name(model)}")


def upgrade():
    from swpt_accounts import models

    create_pktype(models.Account)
    create_pktype(models.PreparedTransfer)
    create_pktype(models.RegisteredBalanceChange)
    create_pktype(models.RejectedTransferSignal)
    create_pktype(models.PreparedTransferSignal)
    create_pktype(models.FinalizedTransferSignal)
    create_pktype(models.AccountTransferSignal)
    create_pktype(models.AccountUpdateSignal)
    create_pktype(models.AccountPurgeSignal)
    create_pktype(models.RejectedConfigSignal)
    create_pktype(models.PendingBalanceChangeSignal)


def downgrade():
    from swpt_accounts import models

    drop_pktype(models.Account)
    drop_pktype(models.PreparedTransfer)
    drop_pktype(models.RegisteredBalanceChange)
    drop_pktype(models.RejectedTransferSignal)
    drop_pktype(models.PreparedTransferSignal)
    drop_pktype(models.FinalizedTransferSignal)
    drop_pktype(models.AccountTransferSignal)
    drop_pktype(models.AccountUpdateSignal)
    drop_pktype(models.AccountPurgeSignal)
    drop_pktype(models.RejectedConfigSignal)
    drop_pktype(models.PendingBalanceChangeSignal)
