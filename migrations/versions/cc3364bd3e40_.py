"""empty message

Revision ID: cc3364bd3e40
Revises: c1b4d8274865
Create Date: 2019-06-09 21:58:50.842090

"""
from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision = 'cc3364bd3e40'
down_revision = 'c1b4d8274865'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column('account', 'last_change_seqnum',
               existing_type=sa.INTEGER(),
               comment='Incremented (with wrapping) on every change in `principal`, `interest_rate`, `interest`, or `status`.',
               existing_comment='Incremented (with wrapping) on every change in `principal`, `interest_rate` or `status`.',
               existing_nullable=False)
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.alter_column('account', 'last_change_seqnum',
               existing_type=sa.INTEGER(),
               comment='Incremented (with wrapping) on every change in `principal`, `interest_rate` or `status`.',
               existing_comment='Incremented (with wrapping) on every change in `principal`, `interest_rate`, `interest`, or `status`.',
               existing_nullable=False)
    # ### end Alembic commands ###
