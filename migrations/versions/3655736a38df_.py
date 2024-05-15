"""empty message

Revision ID: 3655736a38df
Revises: f6e0ff8ee775
Create Date: 2024-05-11 20:17:09.173430

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '3655736a38df'
down_revision = 'f6e0ff8ee775'
branch_labels = None
depends_on = None


def upgrade():
    op.alter_column('prepared_transfer',
                    column_name='final_interest_rate_ts',
                    existing_nullable=True,
                    nullable=False)
    op.alter_column('prepared_transfer_signal',
                    column_name='final_interest_rate_ts',
                    existing_nullable=True,
                    nullable=False)
    op.alter_column('transfer_request',
                    column_name='final_interest_rate_ts',
                    existing_nullable=True,
                    nullable=False)


def downgrade():
    op.alter_column('prepared_transfer',
                    column_name='final_interest_rate_ts',
                    existing_nullable=False,
                    nullable=True)
    op.alter_column('prepared_transfer_signal',
                    column_name='final_interest_rate_ts',
                    existing_nullable=False,
                    nullable=True)
    op.alter_column('transfer_request',
                    column_name='final_interest_rate_ts',
                    existing_nullable=False,
                    nullable=True)
