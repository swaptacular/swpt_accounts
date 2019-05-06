"""empty message

Revision ID: 3c7a812d8f39
Revises: 5e487e137ad8
Create Date: 2019-05-06 17:34:02.691961

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = '3c7a812d8f39'
down_revision = '5e487e137ad8'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column('debtor_policy', 'last_interest_rate_change_ts')
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column('debtor_policy', sa.Column('last_interest_rate_change_ts', postgresql.TIMESTAMP(timezone=True), autoincrement=False, nullable=True))
    # ### end Alembic commands ###
