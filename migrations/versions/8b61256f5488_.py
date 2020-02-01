"""empty message

Revision ID: 8b61256f5488
Revises: 38f5b5252bdb
Create Date: 2020-02-01 17:36:57.138870

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '8b61256f5488'
down_revision = '38f5b5252bdb'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('prepared_transfer', sa.Column('last_remainder_ts', sa.TIMESTAMP(timezone=True), nullable=True, comment='The moment at which the last `PreparedTransferSignal` was sent as a remainder that the prepared transfer waits to be finalized. A `NULL` means that no remainders have been sent yet. This column helps to prevent sending remainders too often.'))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('prepared_transfer', 'last_remainder_ts')
    # ### end Alembic commands ###
