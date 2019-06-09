import datetime
import dramatiq
from sqlalchemy.dialects import postgresql as pg
from .extensions import db, broker

MIN_INT32 = -1 << 31
MAX_INT32 = (1 << 31) - 1
MIN_INT64 = -1 << 63
MAX_INT64 = (1 << 63) - 1


def get_now_utc():
    return datetime.datetime.now(tz=datetime.timezone.utc)


def increment_seqnum(n):
    return MIN_INT32 if n == MAX_INT32 else n + 1


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


class Issuer(db.Model):
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    creditor_id = db.Column(db.BigInteger, nullable=False)


class IssuerPolicy(db.Model):
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    max_total_credit = db.Column(
        db.BigInteger,
        nullable=False,
        default=0,
        comment='The total amount owed to creditors should not surpass this value.',
    )
    last_change_seqnum = db.Column(db.Integer)
    last_change_ts = db.Column(db.TIMESTAMP(timezone=True))
    __table_args__ = (
        db.CheckConstraint(max_total_credit >= 0),
    )


class Account(db.Model):
    STATUS_DELETED_FLAG = 1
    STATUS_ESTABLISHED_INTEREST_RATE_FLAG = 2
    STATUS_OVERFLOWN_FLAG = 4
    STATUS_ISSUER_ACCOUNT_FLAG = 8

    debtor_id = db.Column(db.BigInteger, primary_key=True)
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    principal = db.Column(
        db.BigInteger,
        nullable=False,
        default=0,
        comment='The total owed amount. Can be negative. At any time, the sum of the '
                'principals of all accounts (including the issuer account) for a given '
                'debtor will be zero.',
    )
    interest_rate = db.Column(
        db.REAL,
        nullable=False,
        default=0.0,
        comment='Annual rate (in percents) at which interest accumulates on the account',
    )
    interest_rate_last_change_seqnum = db.Column(db.Integer)
    interest_rate_last_change_ts = db.Column(db.TIMESTAMP(timezone=True))
    interest = db.Column(
        db.FLOAT,
        nullable=False,
        default=0.0,
        comment='The amount of interest accumulated on the account before `last_change_ts`, '
                'but not added to the `principal` yet. Can be a negative number. `interest`'
                'gets zeroed and added to the principal once in while (like once per week).',
    )
    locked_amount = db.Column(
        db.BigInteger,
        nullable=False,
        default=0,
        comment='The total sum of all pending transfer locks',
    )
    prepared_transfers_count = db.Column(
        db.SmallInteger,
        nullable=False,
        default=0,
        comment='The number of `prepared_transfer` records for this account.',
    )
    last_change_seqnum = db.Column(
        db.Integer,
        nullable=False,
        default=1,
        comment='Incremented (with wrapping) on every change in `principal`, `interest_rate`, '
                '`interest`, or `status`.',
    )
    last_change_ts = db.Column(
        db.TIMESTAMP(timezone=True),
        nullable=False,
        default=get_now_utc,
        comment='Updated on every increment of `last_change_seqnum`. Must never decrease.',
    )
    last_transfer_date = db.Column(
        db.DATE,
        comment='Updated for each committed transfer which is not an interest or demurrage '
                'payment',
    )
    status = db.Column(
        db.SmallInteger,
        nullable=False,
        default=0,
        comment='Additional account status flags.',
    )
    __table_args__ = (
        db.CheckConstraint(interest_rate > -100.0),
        db.CheckConstraint(locked_amount >= 0),
        db.CheckConstraint(prepared_transfers_count >= 0),
    )


class PreparedTransfer(db.Model):
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    sender_creditor_id = db.Column(
        db.BigInteger,
        primary_key=True,
        comment='The payer',
    )
    transfer_id = db.Column(
        db.BigInteger,
        primary_key=True,
        autoincrement=True,
        comment='Along with `debtor_id` and `sender_creditor_id` uniquely identifies a transfer',
    )
    coordinator_type = db.Column(
        db.String(30),
        nullable=False,
        comment='Indicates which subsystem has initiated the transfer and is responsible for '
                'finalizing it. The value must be a valid python identifier, all lowercase, '
                'no double underscores. Example: direct, circular.',
    )
    recipient_creditor_id = db.Column(
        db.BigInteger,
        nullable=False,
        comment='The payee',
    )
    amount = db.Column(
        db.BigInteger,
        nullable=False,
        comment='The actual transferred (committed) amount may not exceed this number.',
    )
    sender_locked_amount = db.Column(
        db.BigInteger,
        nullable=False,
        default=lambda context: context.get_current_parameters()['amount'],
        comment="This amount has been added to sender's `account.locked_amount`.",
    )
    prepared_at_ts = db.Column(
        db.TIMESTAMP(timezone=True),
        nullable=False,
        default=get_now_utc,
    )
    __table_args__ = (
        db.ForeignKeyConstraint(
            ['debtor_id', 'sender_creditor_id'],
            ['account.debtor_id', 'account.creditor_id'],
            ondelete='CASCADE',
        ),
        db.CheckConstraint(amount > 0),
        db.CheckConstraint(sender_locked_amount >= 0),
    )

    sender_account = db.relationship(
        'Account',
        backref=db.backref('prepared_transfers'),
    )


class PreparedTransferSignal(Signal):
    # These fields are taken from `PreparedTransfer`.
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    sender_creditor_id = db.Column(db.BigInteger, primary_key=True)
    transfer_id = db.Column(db.BigInteger, primary_key=True)
    coordinator_type = db.Column(db.String(30), nullable=False)
    recipient_creditor_id = db.Column(db.BigInteger, nullable=False)
    amount = db.Column(db.BigInteger, nullable=False)
    sender_locked_amount = db.Column(db.BigInteger, nullable=False)
    prepared_at_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False)

    coordinator_id = db.Column(db.BigInteger, nullable=False)
    coordinator_request_id = db.Column(db.BigInteger, nullable=False)

    @property
    def event_name(self):
        return f'on_prepared_{self.coordinator_type}_transfer_signal'


class RejectedTransferSignal(Signal):
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    signal_id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    coordinator_type = db.Column(db.String(30), nullable=False)
    coordinator_id = db.Column(db.BigInteger, nullable=False)
    coordinator_request_id = db.Column(db.BigInteger, nullable=False)
    details = db.Column(pg.JSON, nullable=False, default={})

    @property
    def event_name(self):
        return f'on_rejected_{self.coordinator_type}_transfer_signal'


class AccountChangeSignal(Signal):
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    change_seqnum = db.Column(db.Integer, primary_key=True)
    change_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    principal = db.Column(db.BigInteger, nullable=False)
    interest = db.Column(db.FLOAT, nullable=False)
    interest_rate = db.Column(db.REAL, nullable=False)
    last_transfer_date = db.Column(db.DATE)
    status = db.Column(db.SmallInteger, nullable=False)


class CommittedTransferSignal(Signal):
    # These fields are taken from `PreparedTransfer`.
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    sender_creditor_id = db.Column(db.BigInteger, primary_key=True)
    transfer_id = db.Column(db.BigInteger, primary_key=True)
    coordinator_type = db.Column(db.String(30), nullable=False)
    recipient_creditor_id = db.Column(db.BigInteger, nullable=False)
    prepared_at_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False)

    committed_at_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    committed_amount = db.Column(db.BigInteger, nullable=False)
    transfer_info = db.Column(pg.JSON, nullable=False, default={})
