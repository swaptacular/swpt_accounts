"""empty message

Revision ID: 3509522161f0
Revises: 3c7a812d8f39
Create Date: 2019-05-06 19:02:32.251713

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '3509522161f0'
down_revision = '3c7a812d8f39'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('account_change_signal', sa.Column('interest', sa.BigInteger(), nullable=False))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('account_change_signal', 'interest')
    # ### end Alembic commands ###
