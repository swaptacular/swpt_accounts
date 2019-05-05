import datetime
import math
import dramatiq
from sqlalchemy.dialects import postgresql as pg
from sqlalchemy.sql.expression import and_
from .extensions import db, broker

ROOT_CREDITOR_ID = -2**63
BEGINNING_OF_TIME = datetime.datetime(datetime.MINYEAR, 1, 1, tzinfo=datetime.timezone.utc)


def get_now_utc():
    return datetime.datetime.now(tz=datetime.timezone.utc)


class DebtorPolicy(db.Model):
    debtor_id = db.Column(db.BigInteger, primary_key=True, autoincrement=False)
    interest_rate = db.Column(db.REAL, nullable=False, default=0.0)
    interest_rate_floor = db.Column(db.REAL, nullable=False, default=0.0)


class Signal(db.Model):
    __abstract__ = True

    queue_name = None

    @property
    def event_name(self):
        model = type(self)
        return f'on_{model.__tablename__}'

    def send_signalbus_message(self):
        model = type(self)
        if model.queue_name is None:
            assert not hasattr(model, 'actor_name'), \
                'SignalModel.actor_name is set, but SignalModel.queue_name is not'
            actor_name = self.event_name
        else:
            actor_name = model.actor_name
        data = model.__marshmallow_schema__.dump(self)
        message = dramatiq.Message(
            queue_name=model.queue_name,
            actor_name=actor_name,
            args=(),
            kwargs=data,
            options={},
        )
        broker.publish_message(message, exchange='')


class Account(db.Model):
    debtor_id = db.Column(db.BigInteger, db.ForeignKey('debtor_policy.debtor_id'), primary_key=True)
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    concession_interest_rate = db.Column(db.REAL, nullable=False, default=math.inf)
    balance = db.Column(
        db.BigInteger,
        nullable=False,
        default=0,
        comment='The total owed amount',
    )
    interest = db.Column(
        db.BigInteger,
        nullable=False,
        default=0,
        comment='The amount of interest accumulated on the account. Can be negative. '
                'Interest accumulates at an annual rate (in percents) that is equal to '
                'the maximum of the following values: `concession_interest_rate`, '
                '`debtor_policy.interest_rate`, `debtor_policy.interest_rate_floor`.',
    )
    avl_balance = db.Column(
        db.BigInteger,
        nullable=False,
        default=0,
        comment='The `balance`, plus `interest`, minus pending transfer locks',
    )
    last_change_seqnum = db.Column(
        db.BigInteger,
        nullable=False,
        default=1,
        comment='Incremented on every change in `balance`, `concession_interest_rate`, '
                '`debtor_policy.interest_rate`, or `debtor_policy.interest_rate_floor`.',
    )
    last_change_ts = db.Column(
        db.TIMESTAMP(timezone=True),
        nullable=False,
        default=BEGINNING_OF_TIME,
        comment='Updated on every increment of `last_change_seqnum`.',
    )
    last_activity_ts = db.Column(
        db.TIMESTAMP(timezone=True),
        nullable=False,
        default=BEGINNING_OF_TIME,
        comment='Updated on every account activity. Can be used to remove stale accounts.',
    )

    debtor_policy = db.relationship(
        'DebtorPolicy',
        backref=db.backref('account_list'),
    )


class PreparedTransfer(db.Model):
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    prepared_transfer_seqnum = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    coordinator_type = db.Column(
        db.String(30),
        nullable=False,
        comment='Must be a valid python identifier.',
    )
    sender_creditor_id = db.Column(db.BigInteger, nullable=False)
    recipient_creditor_id = db.Column(db.BigInteger, nullable=False)
    transfer_info = db.Column(pg.JSONB, nullable=False, default={})
    amount = db.Column(db.BigInteger, nullable=False)
    sender_locked_amount = db.Column(
        db.BigInteger,
        nullable=False,
        default=lambda context: context.get_current_parameters()['amount'],
    )
    prepared_at_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False, default=get_now_utc)
    __table_args__ = (
        db.ForeignKeyConstraint(
            ['debtor_id', 'sender_creditor_id'],
            ['account.debtor_id', 'account.creditor_id'],
        ),
        db.Index(
            'idx_prepared_transfer_sender_creditor_id',
            debtor_id,
            sender_creditor_id,
        ),
        db.CheckConstraint(amount >= 0),
        db.CheckConstraint(sender_locked_amount >= 0),
    )

    sender_account = db.relationship(
        'Account',
        backref=db.backref('prepared_transfer_list'),
    )


class PreparedTransferSignal(Signal):
    coordinator_type = db.Column(db.String(30), primary_key=True)
    coordinator_id = db.Column(db.BigInteger, primary_key=True)
    coordinator_transfer_request_id = db.Column(db.BigInteger, primary_key=True)
    prepared_transfer_seqnum = db.Column(db.BigInteger, nullable=False)
    prepared_at_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    amount = db.Column(db.BigInteger, nullable=False)

    @property
    def event_name(self):
        return f'on_prepared_{self.coordinator_type}_transfer_signal'


class RejectedTransferSignal(Signal):
    coordinator_type = db.Column(db.String(30), primary_key=True)
    coordinator_id = db.Column(db.BigInteger, primary_key=True)
    coordinator_transfer_request_id = db.Column(db.BigInteger, primary_key=True)
    details = db.Column(pg.JSONB, nullable=False, default={})

    @property
    def event_name(self):
        return f'on_rejected_{self.coordinator_type}_transfer_signal'


class AccountChangeSignal(Signal):
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    change_seqnum = db.Column(db.BigInteger, primary_key=True)
    change_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    balance = db.Column(db.BigInteger, nullable=False)
    concession_interest_rate = db.Column(db.REAL, nullable=False)
    interest_rate = db.Column(db.REAL, nullable=False)
    interest_rate_floor = db.Column(db.REAL, nullable=False)


class CommittedTransferSignal(Signal):
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    prepared_transfer_seqnum = db.Column(db.BigInteger, primary_key=True)
    coordinator_type = db.Column(db.String(30), nullable=False)
    sender_creditor_id = db.Column(db.BigInteger, nullable=False)
    recipient_creditor_id = db.Column(db.BigInteger, nullable=False)
    transfer_info = db.Column(pg.JSONB, nullable=False, default={})
    amount = db.Column(db.BigInteger, nullable=False)
    sender_locked_amount = db.Column(db.BigInteger, nullable=False)
    prepared_at_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    committed_at_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    committed_amount = db.Column(db.BigInteger, nullable=False)

    __table_args__ = (
        db.CheckConstraint(and_(committed_amount > 0, committed_amount <= amount)),
    )
