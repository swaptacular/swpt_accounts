"""empty message

Revision ID: df1c9760c6f4
Revises: 4709625c39f7
Create Date: 2019-05-04 21:52:39.417679

"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision = 'df1c9760c6f4'
down_revision = '4709625c39f7'
branch_labels = None
depends_on = None


def upgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('account_update_signal',
    sa.Column('debtor_id', sa.BigInteger(), nullable=False),
    sa.Column('creditor_id', sa.BigInteger(), nullable=False),
    sa.Column('last_change_seqnum', sa.BigInteger(), nullable=False),
    sa.Column('balance', sa.BigInteger(), nullable=False),
    sa.Column('discount_demurrage_rate', sa.REAL(), nullable=False),
    sa.PrimaryKeyConstraint('debtor_id', 'creditor_id', 'last_change_seqnum')
    )
    op.create_table('committed_transfer_signal',
    sa.Column('debtor_id', sa.BigInteger(), nullable=False),
    sa.Column('prepared_transfer_seqnum', sa.BigInteger(), nullable=False),
    sa.Column('sender_creditor_id', sa.BigInteger(), nullable=False),
    sa.Column('recipient_creditor_id', sa.BigInteger(), nullable=False),
    sa.Column('transfer_type', sa.SmallInteger(), nullable=False),
    sa.Column('transfer_info', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    sa.Column('amount', sa.BigInteger(), nullable=False),
    sa.Column('sender_locked_amount', sa.BigInteger(), nullable=False),
    sa.Column('prepared_at_ts', sa.TIMESTAMP(timezone=True), nullable=False),
    sa.Column('committed_at_ts', sa.TIMESTAMP(timezone=True), nullable=False),
    sa.PrimaryKeyConstraint('debtor_id', 'prepared_transfer_seqnum')
    )
    op.create_table('prepared_direct_transfer_signal',
    sa.Column('sender_creditor_id', sa.BigInteger(), nullable=False),
    sa.Column('sender_transfer_request_id', sa.BigInteger(), nullable=False),
    sa.Column('prepared_transfer_seqnum', sa.BigInteger(), nullable=False),
    sa.Column('prepared_at_ts', sa.TIMESTAMP(timezone=True), nullable=False),
    sa.PrimaryKeyConstraint('sender_creditor_id', 'sender_transfer_request_id')
    )
    op.create_table('rejected_direct_transfer_signal',
    sa.Column('sender_creditor_id', sa.BigInteger(), nullable=False),
    sa.Column('sender_transfer_request_id', sa.BigInteger(), nullable=False),
    sa.Column('details', postgresql.JSONB(astext_type=sa.Text()), nullable=False),
    sa.PrimaryKeyConstraint('sender_creditor_id', 'sender_transfer_request_id')
    )
    op.drop_table('transaction_signal')
    # ### end Alembic commands ###


def downgrade():
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table('transaction_signal',
    sa.Column('debtor_id', sa.BIGINT(), autoincrement=False, nullable=False),
    sa.Column('prepared_transfer_seqnum', sa.BIGINT(), autoincrement=True, nullable=False),
    sa.Column('sender_creditor_id', sa.BIGINT(), autoincrement=False, nullable=False),
    sa.Column('recipient_creditor_id', sa.BIGINT(), autoincrement=False, nullable=False),
    sa.Column('amount', sa.BIGINT(), autoincrement=False, nullable=False),
    sa.Column('transaction_info', postgresql.JSONB(astext_type=sa.Text()), autoincrement=False, nullable=False),
    sa.Column('committed_at_ts', postgresql.TIMESTAMP(timezone=True), autoincrement=False, nullable=False),
    sa.PrimaryKeyConstraint('debtor_id', 'prepared_transfer_seqnum', name='transaction_signal_pkey')
    )
    op.drop_table('rejected_direct_transfer_signal')
    op.drop_table('prepared_direct_transfer_signal')
    op.drop_table('committed_transfer_signal')
    op.drop_table('account_update_signal')
    # ### end Alembic commands ###
