"""empty message

Revision ID: 98394a7e5cfc
Revises: 66a4d1614abf
Create Date: 2019-05-20 18:41:54.487309

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = '98394a7e5cfc'
down_revision = '66a4d1614abf'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('account_policy', sa.Column('last_change_ts', sa.TIMESTAMP(timezone=True), nullable=True))
    op.add_column('debtor_policy', sa.Column('last_change_ts', sa.TIMESTAMP(timezone=True), nullable=True))
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('debtor_policy', 'last_change_ts')
    op.drop_column('account_policy', 'last_change_ts')
    # ### end Alembic commands ###
