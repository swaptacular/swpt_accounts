import datetime
import math
import dramatiq
from sqlalchemy.dialects import postgresql as pg
from .extensions import db, broker

MIN_INT64 = -1 << 63
ISSUER_CREDITOR_ID = MIN_INT64
BEGINNING_OF_TIME = datetime.datetime(datetime.MINYEAR, 1, 1, tzinfo=datetime.timezone.utc)


def get_now_utc():
    return datetime.datetime.now(tz=datetime.timezone.utc)


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


class DebtorPolicy(db.Model):
    debtor_id = db.Column(db.BigInteger, primary_key=True, autoincrement=False)
    interest_rate = db.Column(db.REAL, nullable=False, default=0.0)
    last_interest_rate_change_seqnum = db.Column(db.BigInteger, nullable=False, default=0)


class Account(db.Model):
    debtor_id = db.Column(db.BigInteger, db.ForeignKey('debtor_policy.debtor_id'), primary_key=True)
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    balance = db.Column(
        db.BigInteger,
        nullable=False,
        default=0,
        comment='The total owed amount',
    )
    concession_interest_rate = db.Column(
        db.REAL,
        nullable=False,
        default=math.inf,
        comment='An interest rate exclusive for this account, presumably more '
                'advantageous for the account owner than the standard one. '
                'Interest accumulates at an annual rate (in percents) that is '
                'equal to the maximum of `concession_interest_rate` and '
                '`debtor_policy.interest_rate`.',
    )
    interest = db.Column(
        db.BigInteger,
        nullable=False,
        default=0,
        comment='The amount of interest accumulated on the account before `last_change_ts`, '
                'but not added to the `balance` yet. Can be a negative number. `interest`'
                'gets zeroed and added to the ballance one in while (like once per year).',
    )
    avl_balance = db.Column(
        db.BigInteger,
        nullable=False,
        default=0,
        comment='The `balance` minus pending transfer locks',
    )
    last_change_seqnum = db.Column(
        db.BigInteger,
        nullable=False,
        default=1,
        comment='Incremented on every change in `balance`, `concession_interest_rate`, '
                'or `debtor_policy.interest_rate`.',
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

    debtor_policy = db.relationship('DebtorPolicy')


class PreparedTransfer(db.Model):
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    sender_creditor_id = db.Column(
        db.BigInteger,
        primary_key=True,
        comment='The payer',
    )
    transfer_seqnum = db.Column(
        db.BigInteger,
        primary_key=True,
        autoincrement=True,
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
        comment='This amount has been subtracted from the available account balance.',
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
        db.CheckConstraint(amount >= 0),
        db.CheckConstraint(sender_locked_amount >= 0),
    )

    sender_account = db.relationship('Account')


class PreparedTransferSignal(Signal):
    coordinator_type = db.Column(db.String(30), primary_key=True)
    coordinator_id = db.Column(db.BigInteger, primary_key=True)
    coordinator_request_id = db.Column(db.BigInteger, primary_key=True)

    # These fields are taken from `PreparedTransfer`.
    debtor_id = db.Column(db.BigInteger, nullable=False)
    sender_creditor_id = db.Column(db.BigInteger, nullable=False)
    transfer_seqnum = db.Column(db.BigInteger, nullable=False)
    recipient_creditor_id = db.Column(db.BigInteger, nullable=False)
    amount = db.Column(db.BigInteger, nullable=False)
    sender_locked_amount = db.Column(db.BigInteger, nullable=False)
    prepared_at_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False)

    @property
    def event_name(self):
        return f'on_prepared_{self.coordinator_type}_transfer_signal'


class RejectedTransferSignal(Signal):
    coordinator_type = db.Column(db.String(30), primary_key=True)
    coordinator_id = db.Column(db.BigInteger, primary_key=True)
    coordinator_request_id = db.Column(db.BigInteger, primary_key=True)
    details = db.Column(pg.JSON, nullable=False, default={})

    @property
    def event_name(self):
        return f'on_rejected_{self.coordinator_type}_transfer_signal'


class AccountChangeSignal(Signal):
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    change_seqnum = db.Column(db.BigInteger, primary_key=True)
    change_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    balance = db.Column(db.BigInteger, nullable=False)
    interest = db.Column(db.BigInteger, nullable=False)
    concession_interest_rate = db.Column(db.REAL, nullable=False)
    standard_interest_rate = db.Column(db.REAL, nullable=False)


class CommittedTransferSignal(Signal):
    # These fields are taken from `PreparedTransfer`.
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    sender_creditor_id = db.Column(db.BigInteger, primary_key=True)
    transfer_seqnum = db.Column(db.BigInteger, primary_key=True)
    coordinator_type = db.Column(db.String(30), nullable=False)
    recipient_creditor_id = db.Column(db.BigInteger, nullable=False)
    prepared_at_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False)

    committed_at_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    committed_amount = db.Column(db.BigInteger, nullable=False)
    transfer_info = db.Column(pg.JSON, nullable=False, default={})
