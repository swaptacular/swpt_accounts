import datetime
import dramatiq
from marshmallow import Schema, fields
from sqlalchemy.dialects import postgresql as pg
from sqlalchemy.sql.expression import null, or_
from swpt_lib.utils import date_to_int24
from .extensions import db, broker, MAIN_EXCHANGE_NAME

MIN_INT32 = -1 << 31
MAX_INT32 = (1 << 31) - 1
MIN_INT64 = -1 << 63
MAX_INT64 = (1 << 63) - 1
INTEREST_RATE_FLOOR = -50.0
INTEREST_RATE_CEIL = 100.0
DATE_2020_01_01 = datetime.date(2020, 1, 1)


def get_now_utc() -> datetime.datetime:
    return datetime.datetime.now(tz=datetime.timezone.utc)


def increment_seqnum(n: int) -> int:
    return MIN_INT32 if n == MAX_INT32 else n + 1


class Signal(db.Model):
    __abstract__ = True

    # TODO: Define `send_signalbus_messages` class method, set
    #      `ModelClass.signalbus_autoflush = False` and
    #      `ModelClass.signalbus_burst_count = N` in models.

    queue_name = None

    @property
    def event_name(self):  # pragma: no cover
        model = type(self)
        return f'on_{model.__tablename__}'

    def send_signalbus_message(self):  # pragma: no cover
        model = type(self)
        if model.queue_name is None:
            assert not hasattr(model, 'actor_name'), \
                'SignalModel.actor_name is set, but SignalModel.queue_name is not'
            actor_name = self.event_name
            routing_key = f'events.{actor_name}'
        else:
            actor_name = model.actor_name
            routing_key = model.queue_name
        data = model.__marshmallow_schema__.dump(self)
        message = dramatiq.Message(
            queue_name=model.queue_name,
            actor_name=actor_name,
            args=(),
            kwargs=data,
            options={},
        )
        broker.publish_message(message, exchange=MAIN_EXCHANGE_NAME, routing_key=routing_key)


class Account(db.Model):
    STATUS_DELETED_FLAG = 1
    STATUS_ESTABLISHED_INTEREST_RATE_FLAG = 2
    STATUS_OVERFLOWN_FLAG = 4
    STATUS_SCHEDULED_FOR_DELETION_FLAG = 8

    debtor_id = db.Column(db.BigInteger, primary_key=True)
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    creation_date = db.Column(
        db.DATE,
        nullable=False,
        comment='The date at which the account was created. This also becomes the value of '
                'the `committed_transfer_signal.transfer_epoch` column for each committed '
                'transfer for the account.',
    )
    principal = db.Column(
        db.BigInteger,
        nullable=False,
        default=0,
        comment='The total owed amount. Can be negative.',
    )
    interest_rate = db.Column(
        db.REAL,
        nullable=False,
        default=0.0,
        comment='Annual rate (in percents) at which interest accumulates on the account. Can '
                'be negative.',
    )
    interest_rate_last_change_seqnum = db.Column(
        db.Integer,
        comment='The value of the `change_seqnum` attribute, received with the most recent '
                '`change_interest_rate` signal. It is used to decide whether to change the '
                'interest rate when a (potentially old) `change_interest_rate` signal is '
                'received.',
    )
    interest_rate_last_change_ts = db.Column(
        db.TIMESTAMP(timezone=True),
        comment='The value of the `change_ts` attribute, received with the most recent '
                '`change_interest_rate` signal. It is used to decide whether to change the '
                'interest rate when a (potentially old) `change_interest_rate` signal is '
                'received.',
    )
    interest = db.Column(
        db.FLOAT,
        nullable=False,
        default=0.0,
        comment='The amount of interest accumulated on the account before `last_change_ts`, '
                'but not added to the `principal` yet. Can be a negative number. `interest` '
                'gets zeroed and added to the principal once in a while (like once per week).',
    )
    locked_amount = db.Column(
        db.BigInteger,
        nullable=False,
        default=0,
        comment='The total sum of all pending transfer locks (the total sum of the values of '
                'the `pending_transfer.sender_locked_amount` column) for this account. This '
                'value has been reserved and must be subtracted from the available amount, to '
                'avoid double-spending.',
    )
    pending_transfers_count = db.Column(
        db.Integer,
        nullable=False,
        default=0,
        comment='The number of `pending_transfer` records for this account.',
    )
    last_change_seqnum = db.Column(
        db.Integer,
        nullable=False,
        default=1,
        comment='Incremented (with wrapping) on every meaningful change on the account. Every '
                'change in `principal`, `interest_rate`, `interest`, or `status` is considered '
                'meaningful. This column, along with the `last_change_ts` column, allows to '
                'reliably determine the correct order of changes, even if they occur in a very '
                'short period of time.',
    )
    last_change_ts = db.Column(
        db.TIMESTAMP(timezone=True),
        nullable=False,
        default=get_now_utc,
        comment='The moment at which the last meaningful change on the account happened. Must '
                'never decrease. Every change in `principal`, `interest_rate`, `interest`, or '
                '`status` is considered meaningful.',
    )
    last_outgoing_transfer_date = db.Column(
        db.DATE,
        comment='Updated on each transfer for which this account is the sender. It is not updated '
                'on interest/demurrage payments. This field is used to determine when an account '
                'with negative balance can be zeroed out for deletion.',
    )
    last_transfer_id = db.Column(
        db.BigInteger,
        nullable=False,
        default=(lambda context: date_to_int24(context.get_current_parameters()['creation_date']) << 40),
        comment='Incremented when a new `prepared_transfer` record is inserted. It is used '
                'to generate sequential numbers for the `prepared_transfer.transfer_id` '
                'column.',
    )
    last_transfer_seqnum = db.Column(
        db.BigInteger,
        nullable=False,
        default=(lambda context: date_to_int24(context.get_current_parameters()['creation_date']) << 40),
        comment='Incremented when a new `committed_transfer_signal` record is inserted. It is used '
                'to generate sequential numbers for the `committed_transfer_signal.transfer_seqnum`. '
                'column. Must never decrease. When the account is created, `last_transfer_seqnum` '
                'has its lowest 40 bits set to zero, and its highest 24 bits calculated from the '
                'value of `creation_date`.',
    )
    status = db.Column(
        db.SmallInteger,
        nullable=False,
        comment='Additional account status flags.',
    )
    __table_args__ = (
        db.CheckConstraint((interest_rate >= INTEREST_RATE_FLOOR) & (interest_rate <= INTEREST_RATE_CEIL)),
        db.CheckConstraint(locked_amount >= 0),
        db.CheckConstraint(pending_transfers_count >= 0),
        db.CheckConstraint(principal > MIN_INT64),
        db.CheckConstraint(last_transfer_seqnum >= 0),
        {
            'comment': 'Tells who owes what to whom.',
        }
    )


class TransferRequest(db.Model):
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    sender_creditor_id = db.Column(db.BigInteger, primary_key=True)
    transfer_request_id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    coordinator_type = db.Column(
        db.String(30),
        nullable=False,
        comment='Indicates which subsystem has initiated the transfer and is responsible for '
                'finalizing it (coordinates the transfer). The value must be a valid python '
                'identifier, all lowercase, no double underscores. Example: direct, interest, '
                'circular.',
    )
    coordinator_id = db.Column(
        db.BigInteger,
        nullable=False,
        comment='Along with `coordinator_type`, uniquely identifies who initiated the transfer.',
    )
    coordinator_request_id = db.Column(
        db.BigInteger,
        nullable=False,
        comment="Along with `coordinator_type` and `coordinator_id` uniquely identifies the "
                "transfer request from the coordinator's point of view. When the transfer is "
                "prepared, those three values will be included in the generated "
                "`on_prepared_{coordinator_type}_transfer_signal` event, so that the "
                "coordinator can match the event with the originating transfer request.",
    )
    min_amount = db.Column(
        db.BigInteger,
        nullable=False,
        comment='The minimum amount that should be secured for the transfer. '
                '(`prepared_transfer.sender_locked_amount` will be no smaller than this value.)',
    )
    max_amount = db.Column(
        db.BigInteger,
        nullable=False,
        comment='The maximum amount that should be secured for the transfer, if possible. '
                '(`prepared_transfer.sender_locked_amount` will be no bigger than this value.)',
    )
    minimum_account_balance = db.Column(
        db.BigInteger,
        nullable=False,
        comment="Determines the amount that must remain available on sender's account after "
                "the requested amount has been secured. This is useful when the coordinator "
                "does not want to expend everything available on the account.",
    )
    recipient_creditor_id = db.Column(db.BigInteger, nullable=False)

    __table_args__ = (
        db.CheckConstraint(min_amount > 0),
        db.CheckConstraint(min_amount <= max_amount),
        {
            'comment': 'Represents a request to secure (prepare) some amount for transfer, if '
                       'it is available on a given account. If the request is fulfilled, a new '
                       'row will be inserted in the `prepared_transfer` table. Requests are '
                       'queued to this table, before being processed, because this allows many '
                       'requests from one sender to be processed at once, reducing the lock '
                       'contention on `account` table rows.',
        }
    )


class PreparedTransfer(db.Model):
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    sender_creditor_id = db.Column(db.BigInteger, primary_key=True)
    transfer_id = db.Column(db.BigInteger, primary_key=True)
    coordinator_type = db.Column(db.String(30), nullable=False)
    recipient_creditor_id = db.Column(db.BigInteger, nullable=False)
    sender_locked_amount = db.Column(
        db.BigInteger,
        nullable=False,
        comment="The actual transferred (committed) amount may not exceed this number.",
    )
    prepared_at_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False, default=get_now_utc)
    __table_args__ = (
        db.ForeignKeyConstraint(
            ['debtor_id', 'sender_creditor_id'],
            ['account.debtor_id', 'account.creditor_id'],
            ondelete='CASCADE',
        ),
        db.CheckConstraint(sender_locked_amount > 0),
        {
            'comment': 'A prepared transfer represent a guarantee that a particular transfer of '
                       'funds will be successful if ordered (committed). A record will remain in '
                       'this table until the transfer has been commited or dismissed.',
        }
    )


class PendingAccountChange(db.Model):
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    change_id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    principal_delta = db.Column(
        db.BigInteger,
        nullable=False,
        comment='The change in `account.principal`.',
    )
    interest_delta = db.Column(
        db.BigInteger,
        nullable=False,
        comment='The change in `account.interest`.',
    )
    unlocked_amount = db.Column(
        db.BigInteger,
        comment='If not NULL, the value must be subtracted from `account.locked_amount`, and '
                '`account.pending_transfers_count` must be decremented.',
    )
    coordinator_type = db.Column(db.String(30), nullable=False)
    transfer_info = db.Column(
        pg.JSON,
        comment='Notes from the sender. Can be any JSON object that the sender wants the '
                'recipient to see. If the account change represents a committed transfer, '
                'the notes will be included in the generated `on_committed_transfer_signal` '
                'event. Can be NULL only if `principal_delta` is zero.',
    )
    other_creditor_id = db.Column(
        db.BigInteger,
        nullable=False,
        comment='If the account change represents a committed transfer, this is the other '
                'party in the transfer. When `principal_delta` is positive, this is the '
                'sender. When `principal_delta` is negative, this is the recipient. When '
                '`principal_delta` is zero, the value is irrelevant.',
    )
    inserted_at_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False, default=get_now_utc)

    __table_args__ = (
        db.CheckConstraint(or_(principal_delta == 0, transfer_info != null())),
        db.CheckConstraint(unlocked_amount >= 0),
        {
            'comment': 'Represents a pending change to a given account. Pending updates to '
                       '`account.principal`, `account.interest`, and `account.locked_amount` are '
                       'queued to this table, before being processed, because this allows '
                       'multiple updates to one account to coalesce, reducing the lock '
                       'contention on `account` table rows.',
        }
    )


class PreparedTransferSignal(Signal):
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    sender_creditor_id = db.Column(db.BigInteger, primary_key=True)
    transfer_id = db.Column(db.BigInteger, primary_key=True)
    coordinator_type = db.Column(db.String(30), nullable=False)
    coordinator_id = db.Column(db.BigInteger, nullable=False)
    coordinator_request_id = db.Column(db.BigInteger, nullable=False)
    sender_locked_amount = db.Column(db.BigInteger, nullable=False)
    recipient_creditor_id = db.Column(db.BigInteger, nullable=False)
    prepared_at_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False)

    @property
    def event_name(self):  # pragma: no cover
        return f'on_prepared_{self.coordinator_type}_transfer_signal'


class RejectedTransferSignal(Signal):
    class __marshmallow__(Schema):
        coordinator_type = fields.String()
        coordinator_id = fields.Integer()
        coordinator_request_id = fields.Integer()
        details = fields.Raw()

    debtor_id = db.Column(db.BigInteger, primary_key=True)
    signal_id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    coordinator_type = db.Column(db.String(30), nullable=False)
    coordinator_id = db.Column(db.BigInteger, nullable=False)
    coordinator_request_id = db.Column(db.BigInteger, nullable=False)
    details = db.Column(pg.JSON, nullable=False, default={})

    @property
    def event_name(self):  # pragma: no cover
        return f'on_rejected_{self.coordinator_type}_transfer_signal'


class AccountChangeSignal(Signal):
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    change_seqnum = db.Column(db.Integer, primary_key=True)
    change_ts = db.Column(db.TIMESTAMP(timezone=True), primary_key=True)
    principal = db.Column(db.BigInteger, nullable=False)
    interest = db.Column(db.FLOAT, nullable=False)
    interest_rate = db.Column(db.REAL, nullable=False)
    last_transfer_seqnum = db.Column(db.BigInteger, nullable=False)
    last_outgoing_transfer_date = db.Column(db.DATE)
    creation_date = db.Column(db.DATE, nullable=False)
    status = db.Column(db.SmallInteger, nullable=False)


class AccountPurgeSignal(Signal):
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    creation_date = db.Column(db.DATE, primary_key=True)


class CommittedTransferSignal(Signal):
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    transfer_seqnum = db.Column(db.BigInteger, primary_key=True)
    transfer_epoch = db.Column(db.DATE, nullable=False)
    coordinator_type = db.Column(db.String(30), nullable=False)
    other_creditor_id = db.Column(db.BigInteger, nullable=False)
    committed_at_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    committed_amount = db.Column(db.BigInteger, nullable=False)
    transfer_info = db.Column(pg.JSON, nullable=False, default={})
    new_account_principal = db.Column(db.BigInteger, nullable=False)
