import math
from datetime import datetime, timezone
from decimal import Decimal
from flask import current_app
from sqlalchemy.dialects import postgresql as pg
from sqlalchemy.sql.expression import null, or_
from swpt_lib.utils import date_to_int24
from .extensions import db
from .events import *  # noqa

MIN_INT16 = -1 << 15
MAX_INT16 = (1 << 15) - 1
MIN_INT32 = -1 << 31
MAX_INT32 = (1 << 31) - 1
MIN_INT64 = -1 << 63
MAX_INT64 = (1 << 63) - 1
SECONDS_IN_DAY = 24 * 60 * 60
SECONDS_IN_YEAR = 365.25 * SECONDS_IN_DAY
BEGINNING_OF_TIME = datetime(1970, 1, 1, tzinfo=timezone.utc)

INTEREST_RATE_FLOOR = -50.0
INTEREST_RATE_CEIL = 100.0
PRISTINE_ACCOUNT_STATUS = 0

# Reserved coordinator types:
CT_INTEREST = 'interest'
CT_NULLIFY = 'nullify'
CT_DELETE = 'delete'
CT_DIRECT = 'direct'

# The account `(debtor_id, ROOT_CREDITOR_ID)` is special. This is the
# debtor's account. It issuers all the money. Also, all interest and
# demurrage payments come from/to this account.
ROOT_CREDITOR_ID = 0


def get_now_utc() -> datetime:
    return datetime.now(tz=timezone.utc)


class Account(db.Model):
    # Status's lower 16 bits are configured by the owner of the account:
    STATUS_SCHEDULED_FOR_DELETION_FLAG = 1 << 0

    # Status's higher 16 bits contain internal flags:
    STATUS_DELETED_FLAG = 1 << 16
    STATUS_ESTABLISHED_INTEREST_RATE_FLAG = 1 << 17
    STATUS_OVERFLOWN_FLAG = 1 << 18

    debtor_id = db.Column(db.BigInteger, primary_key=True)
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    creation_date = db.Column(
        db.DATE,
        nullable=False,
        comment='The date at which the account was created. This also becomes the value of '
                'the `account_transfer_signal.account_creation_date` column for each transfer '
                'committed from/to the account.',
    )
    principal = db.Column(
        db.BigInteger,
        nullable=False,
        default=0,
        comment='The owed amount, without the interest. Can be negative.',
    )
    interest_rate = db.Column(
        db.REAL,
        nullable=False,
        default=0.0,
        comment='Annual rate (in percents) at which interest accumulates on the account. Can '
                'be negative.',
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
        default=0,
        comment='Incremented (with wrapping) on every meaningful change on the account. Every '
                'change in `principal`, `interest_rate`, `interest`, `negligible_amount`, or  '
                '`status` is considered meaningful. This column, along with the `last_change_ts` '
                'column, allows to reliably determine the correct order of changes, even if '
                'they occur in a very short period of time.',
    )
    last_change_ts = db.Column(
        db.TIMESTAMP(timezone=True),
        nullable=False,
        default=get_now_utc,
        comment='The moment at which the last meaningful change on the account happened. Must '
                'never decrease. Every change in `principal`, `interest_rate`, `interest`, '
                '`negligible_amount`, or `status` is considered meaningful.',
    )
    last_outgoing_transfer_date = db.Column(
        db.DATE,
        nullable=False,
        default=BEGINNING_OF_TIME.date(),
        comment='Updated on each transfer for which this account is the sender. It is not updated '
                'on interest/demurrage payments. This field is used to determine when an account '
                'with negative balance can be zeroed out.',
    )
    last_transfer_id = db.Column(
        db.BigInteger,
        nullable=False,
        default=(lambda context: date_to_int24(context.get_current_parameters()['creation_date']) << 40),
        comment='Incremented when a new `prepared_transfer` record is inserted. It is used '
                'to generate sequential numbers for the `prepared_transfer.transfer_id` column. '
                'When the account is created, `last_transfer_id` has its lower 40 bits set '
                'to zero, and its higher 24 bits calculated from the value of `creation_date` '
                '(the number of days since Jan 1st, 1970).',
    )
    last_transfer_seqnum = db.Column(
        db.BigInteger,
        nullable=False,
        default=(lambda context: date_to_int24(context.get_current_parameters()['creation_date']) << 40),
        comment='Incremented when a new `account_transfer_signal` record is inserted. It is used '
                'to generate sequential numbers for the `account_transfer_signal.transfer_seqnum` '
                'column. Must never decrease. '
                'When the account is created, `last_transfer_seqnum` has its lower 40 bits set '
                'to zero, and its higher 24 bits calculated from the value of `creation_date` '
                '(the number of days since Jan 1st, 1970).',
    )
    status = db.Column(
        db.Integer,
        nullable=False,
        default=PRISTINE_ACCOUNT_STATUS,
        comment="Contain additional account status bits. "
                "The lower 16 bits are configured by the owner of the account: "
                f"{STATUS_SCHEDULED_FOR_DELETION_FLAG} - scheduled for deletion. "
                "The higher 16 bits contain internal flags: "
                f"{STATUS_DELETED_FLAG} - deleted, "
                f"{STATUS_ESTABLISHED_INTEREST_RATE_FLAG} - established interest rate, "
                f"{STATUS_OVERFLOWN_FLAG} - overflown."
    )
    negligible_amount = db.Column(
        db.REAL,
        nullable=False,
        default=0.0,
        comment='An amount that is considered negligible. It is used to: 1) '
                'decide whether an account can be safely deleted; 2) decide '
                'whether a transfer is insignificant.',
    )
    last_config_signal_ts = db.Column(
        db.TIMESTAMP(timezone=True),
        nullable=False,
        default=BEGINNING_OF_TIME,
        comment='The value of the `signal_ts` attribute, received with the most recent '
                '`configure_account` signal. It is used to decide whether to update the '
                'configuration when a (potentially old) `configure_account` signal is received.',
    )
    last_config_signal_seqnum = db.Column(
        db.Integer,
        nullable=False,
        default=0,
        comment='The value of the `signal_seqnum` attribute, received with the most recent '
                '`configure_account` signal. It is used to decide whether to update the '
                'configuration when a (potentially old) `configure_account` signal is received.',
    )
    last_reminder_ts = db.Column(
        db.TIMESTAMP(timezone=True),
        nullable=False,
        default=BEGINNING_OF_TIME,
        comment='The moment at which the last `AccountChangeSignal` was sent to remind that '
                'the account still exists. This column helps to prevent sending reminders too '
                'often.',
    )
    __table_args__ = (
        db.CheckConstraint((interest_rate >= INTEREST_RATE_FLOOR) & (interest_rate <= INTEREST_RATE_CEIL)),
        db.CheckConstraint(locked_amount >= 0),
        db.CheckConstraint(pending_transfers_count >= 0),
        db.CheckConstraint(principal > MIN_INT64),
        db.CheckConstraint(last_transfer_id >= 0),
        db.CheckConstraint(last_transfer_seqnum >= 0),
        db.CheckConstraint(negligible_amount >= 0.0),
        {
            'comment': 'Tells who owes what to whom.',
        }
    )

    def calc_current_balance(self, current_ts: datetime = None) -> Decimal:
        current_balance = Decimal(self.principal)

        # Note that any interest accumulated on the debtor's account
        # will not be included in the current balance. Thus,
        # accumulating interest on the debtor's account has no effect.
        if self.creditor_id != ROOT_CREDITOR_ID:
            current_balance += Decimal.from_float(self.interest)
            if current_balance > 0:
                k = math.log(1.0 + self.interest_rate / 100.0) / SECONDS_IN_YEAR
                current_ts = current_ts or datetime.now(tz=timezone.utc)
                passed_seconds = max(0.0, (current_ts - self.last_change_ts).total_seconds())
                current_balance *= Decimal.from_float(math.exp(k * passed_seconds))

        return current_balance

    def calc_last_transfer_seqnum(self):
        """Return `0` if there have not been any transfers yet."""

        initial_seqnum = date_to_int24(self.creation_date) << 40
        last_transfer_seqnum = self.last_transfer_seqnum
        return last_transfer_seqnum if last_transfer_seqnum > initial_seqnum else 0

    def set_config_flags(self, value):
        """Set the lower 16 account status bits."""

        value &= 0xffff
        self.status &= 0xffff0000
        self.status |= value


class TransferRequest(db.Model):
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    sender_creditor_id = db.Column(db.BigInteger, primary_key=True)
    transfer_request_id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    coordinator_type = db.Column(
        db.String(30),
        nullable=False,
        comment='Indicates which subsystem has initiated the transfer and is responsible for '
                'finalizing it (coordinating the transfer). The value must be a valid python '
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
        comment="Determines the amount that must remain available on the sender's account after "
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
                       'queued to the `transfer_request` table, before being processed, because '
                       'this allows many requests from one sender to be processed at once, '
                       'reducing the lock contention on `account` table rows.',
        }
    )


class PreparedTransfer(db.Model):
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    sender_creditor_id = db.Column(db.BigInteger, primary_key=True)
    transfer_id = db.Column(db.BigInteger, primary_key=True)
    coordinator_type = db.Column(db.String(30), nullable=False)
    coordinator_id = db.Column(db.BigInteger, nullable=False)
    coordinator_request_id = db.Column(db.BigInteger, nullable=False)
    sender_locked_amount = db.Column(
        db.BigInteger,
        nullable=False,
        comment="The actual transferred (committed) amount may not exceed this number.",
    )
    recipient_creditor_id = db.Column(db.BigInteger, nullable=False)
    prepared_at_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False, default=get_now_utc)
    last_reminder_ts = db.Column(
        db.TIMESTAMP(timezone=True),
        comment='The moment at which the last `PreparedTransferSignal` was sent to remind '
                'that the prepared transfer must be finalized. A `NULL` means that no reminders '
                'have been sent yet. This column helps to prevent sending reminders too often.',
    )
    __table_args__ = (
        db.ForeignKeyConstraint(
            ['debtor_id', 'sender_creditor_id'],
            ['account.debtor_id', 'account.creditor_id'],
            ondelete='CASCADE',
        ),
        db.CheckConstraint(transfer_id > 0),
        db.CheckConstraint(sender_locked_amount > 0),
        {
            'comment': 'A prepared transfer represent a guarantee that a particular transfer of '
                       'funds will be successful if ordered (committed). A record will remain in '
                       'this table until the transfer has been committed or dismissed.',
        }
    )

    def get_status_code(self, committed_amount: int, current_ts: datetime) -> str:
        if not (0 <= committed_amount <= self.sender_locked_amount):  # pragma: no cover
            return 'INCORRECT_COMMITTED_AMOUNT'

        # A regular transfer should not be allowed if it took too long
        # to be committed, and the amount secured for the transfer
        # *might* have been consumed by accumulated negative
        # interest. This is necessary in order to prevent a trick that
        # creditors may use to evade incurring negative interests on
        # their accounts. The trick is to prepare a transfer from one
        # account to another for the whole available amount, wait for
        # some long time, then commit the prepared transfer and
        # abandon the account (which at that point would be
        # significantly in red).
        if self.sender_creditor_id != ROOT_CREDITOR_ID and self.recipient_creditor_id != ROOT_CREDITOR_ID:
            passed_seconds = max(0.0, (current_ts - self.prepared_at_ts).total_seconds())
            if passed_seconds > current_app.config['APP_TRANSFER_MAX_DELAY_SECONDS']:
                k = math.log(1.0 + INTEREST_RATE_FLOOR / 100.0) / SECONDS_IN_YEAR
                permitted_amount = self.sender_locked_amount * math.exp(k * passed_seconds)
                if committed_amount > permitted_amount:
                    assert committed_amount > 0
                    return 'TERMINATED_DUE_TO_TIMEOUT'

        return 'OK'


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
    transfer_id = db.Column(
        db.BigInteger,
        comment="When the account change represents a committed direct transfer, and `creditor_id` "
                f"is the creditor ID of the sender (that is: `coordinator_type = '{CT_DIRECT}' and "
                "principal_delta < 0`), this column contains the ID of the corresponding prepared "
                "transfer (`prepared_transfer.transfer_id`). Otherwise it is NULL.",
    )
    transfer_message = db.Column(
        pg.TEXT,
        comment='Notes from the sender. Can be any string that the sender wants the '
                'recipient to see. If the account change represents a committed transfer, '
                'the notes will be included in the generated `on_account_transfer_signal` '
                'event, otherwise the notes are ignored. Can be NULL only if '
                '`principal_delta` is zero.',
    )
    transfer_flags = db.Column(
        db.Integer,
        comment='Contains various flags that characterize the committed transfer. If the '
                'account change represents a committed transfer, the flags will be included '
                'in the generated `on_account_transfer_signal` event, otherwise the flags are '
                'ignored. Can be NULL only if `principal_delta` is zero.',
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
        db.CheckConstraint(or_(principal_delta == 0, transfer_message != null())),
        db.CheckConstraint(or_(principal_delta == 0, transfer_flags != null())),
        db.CheckConstraint(transfer_id > 0),
        db.CheckConstraint(unlocked_amount >= 0),
        {
            'comment': 'Represents a pending change to a given account. Pending updates to '
                       '`account.principal`, `account.interest`, and `account.locked_amount` are '
                       'queued to this table, before being processed, because this allows '
                       'multiple updates to one account to coalesce, reducing the lock '
                       'contention on `account` table rows.',
        }
    )
