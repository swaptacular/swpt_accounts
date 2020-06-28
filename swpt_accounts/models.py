import math
from datetime import datetime, timezone
from decimal import Decimal
from sqlalchemy.dialects import postgresql as pg
from sqlalchemy.sql.expression import null, or_, and_
from swpt_lib.utils import date_to_int24
from .extensions import db
from .events import INTEREST_RATE_FLOOR, INTEREST_RATE_CEIL
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
PRISTINE_ACCOUNT_STATUS_FLAGS = 0

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


def calc_k(interest_rate: float) -> float:
    return math.log(1.0 + interest_rate / 100.0) / SECONDS_IN_YEAR


class Account(db.Model):
    CONFIG_SCHEDULED_FOR_DELETION_FLAG = 1 << 0

    STATUS_UNREACHABLE_FLAG = 1 << 0
    STATUS_DELETED_FLAG = 1 << 16
    STATUS_ESTABLISHED_INTEREST_RATE_FLAG = 1 << 17
    STATUS_OVERFLOWN_FLAG = 1 << 18

    debtor_id = db.Column(db.BigInteger, primary_key=True)
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    creation_date = db.Column(db.DATE, nullable=False)
    last_change_seqnum = db.Column(db.Integer, nullable=False, default=0)
    last_change_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False, default=get_now_utc)
    principal = db.Column(db.BigInteger, nullable=False, default=0)
    interest_rate = db.Column(db.REAL, nullable=False, default=0.0)
    interest = db.Column(db.FLOAT, nullable=False, default=0.0)
    last_interest_rate_change_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False, default=BEGINNING_OF_TIME)
    last_config_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False, default=BEGINNING_OF_TIME)
    last_config_seqnum = db.Column(db.Integer, nullable=False, default=0)
    last_transfer_number = db.Column(db.BigInteger, nullable=False, default=0)
    last_transfer_committed_at_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False, default=BEGINNING_OF_TIME)
    last_outgoing_transfer_date = db.Column(db.DATE, nullable=False, default=BEGINNING_OF_TIME.date())
    negligible_amount = db.Column(db.REAL, nullable=False, default=0.0)
    config_flags = db.Column(db.Integer, nullable=False, default=0)
    status_flags = db.Column(
        db.Integer,
        nullable=False,
        default=PRISTINE_ACCOUNT_STATUS_FLAGS,
        comment="Contain additional account status bits: "
                f"{STATUS_UNREACHABLE_FLAG} - unreachable, "
                f"{STATUS_DELETED_FLAG} - deleted, "
                f"{STATUS_ESTABLISHED_INTEREST_RATE_FLAG} - established interest rate, "
                f"{STATUS_OVERFLOWN_FLAG} - overflown."
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
    previous_interest_rate = db.Column(
        db.REAL,
        nullable=False,
        default=0.0,
        comment='The annual interest rate (in percents) as it was before the last change of '
                'the interest rate happened (see `last_interest_rate_change_ts`).',
    )
    last_reminder_ts = db.Column(
        db.TIMESTAMP(timezone=True),
        nullable=False,
        default=BEGINNING_OF_TIME,
        comment='The moment at which the last `AccountUpdateSignal` was sent to remind that '
                'the account still exists. This column helps to prevent sending reminders too '
                'often.',
    )
    __table_args__ = (
        db.CheckConstraint(and_(
            interest_rate >= INTEREST_RATE_FLOOR,
            interest_rate <= INTEREST_RATE_CEIL,
        )),
        db.CheckConstraint(and_(
            previous_interest_rate >= INTEREST_RATE_FLOOR,
            previous_interest_rate <= INTEREST_RATE_CEIL,
        )),
        db.CheckConstraint(locked_amount >= 0),
        db.CheckConstraint(pending_transfers_count >= 0),
        db.CheckConstraint(principal > MIN_INT64),
        db.CheckConstraint(last_transfer_id >= 0),
        db.CheckConstraint(last_transfer_number >= 0),
        db.CheckConstraint(negligible_amount >= 0.0),
        {
            'comment': 'Tells who owes what to whom.',
        }
    )

    def calc_current_balance(self, current_ts: datetime) -> Decimal:
        current_balance = Decimal(self.principal)

        # Note that any interest accumulated on the debtor's account
        # will not be included in the current balance. Thus,
        # accumulating interest on the debtor's account has no effect.
        if self.creditor_id != ROOT_CREDITOR_ID:
            current_balance += Decimal.from_float(self.interest)
            if current_balance > 0:
                k = calc_k(self.interest_rate)
                current_ts = current_ts or datetime.now(tz=timezone.utc)
                passed_seconds = max(0.0, (current_ts - self.last_change_ts).total_seconds())
                current_balance *= Decimal.from_float(math.exp(k * passed_seconds))

        return current_balance

    def calc_due_interest(self, amount: int, start_ts: datetime, end_ts: datetime) -> float:
        """Return the accumulated interest between `start_ts` and `end_ts`.

        When `amount` is a positive number, returns the amount of
        interest that would have been accumulated for the given
        `amount`, between `start_ts` and `end_ts`. When `amount` is a
        negative number, returns `-self.calc_due_interest(-amount,
        start_ts, end_ts)`.

        """

        end_ts = max(start_ts, end_ts)
        interest_rate_change_ts = min(self.last_interest_rate_change_ts, end_ts)
        t = (end_ts - start_ts).total_seconds()
        t1 = max((interest_rate_change_ts - start_ts).total_seconds(), 0)
        t2 = min((end_ts - interest_rate_change_ts).total_seconds(), t)
        k1 = calc_k(self.previous_interest_rate)
        k2 = calc_k(self.interest_rate)
        assert t >= 0
        assert 0 <= t1 <= t
        assert 0 <= t2 <= t
        assert abs(t1 + t2 - t) <= t / 1000
        return amount * (math.exp(k1 * t1 + k2 * t2) - 1)


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
    coordinator_id = db.Column(db.BigInteger, nullable=False)
    coordinator_request_id = db.Column(db.BigInteger, nullable=False)
    min_amount = db.Column(db.BigInteger, nullable=False)
    max_amount = db.Column(db.BigInteger, nullable=False)
    deadline = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    min_account_balance = db.Column(db.BigInteger, nullable=False)
    recipient_creditor_id = db.Column(db.BigInteger, nullable=False)

    __table_args__ = (
        db.CheckConstraint(min_amount >= 0),
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
    recipient_creditor_id = db.Column(db.BigInteger, nullable=False)
    prepared_at_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False, default=get_now_utc)
    gratis_period = db.Column(db.Integer, nullable=False)
    demurrage_rate = db.Column(db.FLOAT, nullable=False)
    deadline = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    sender_locked_amount = db.Column(
        db.BigInteger,
        nullable=False,
        comment="The actual transferred (committed) amount may not exceed this number.",
    )
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
        db.CheckConstraint(gratis_period >= 0),
        db.CheckConstraint((demurrage_rate > -100.0) & (demurrage_rate <= 0.0)),
        {
            'comment': 'A prepared transfer represent a guarantee that a particular transfer of '
                       'funds will be successful if ordered (committed). A record will remain in '
                       'this table until the transfer has been committed or dismissed.',
        }
    )

    def get_status_code(self, committed_amount: int, current_ts: datetime) -> str:
        if current_ts > self.deadline:
            return 'TRANSFER_TIMEOUT'

        if not (0 <= committed_amount <= self.sender_locked_amount):  # pragma: no cover
            return 'INSUFFICIENT_LOCKED_AMOUNT'

        is_regular_transfer = ROOT_CREDITOR_ID not in [self.sender_creditor_id, self.recipient_creditor_id]
        if is_regular_transfer:
            demurrage_seconds = (current_ts - self.prepared_at_ts).total_seconds() - self.gratis_period
            if demurrage_seconds > 0:
                k = calc_k(self.demurrage_rate)
                unconsumed_locked_amount = self.sender_locked_amount * math.exp(k * demurrage_seconds)
                if float(committed_amount) > unconsumed_locked_amount:
                    assert committed_amount > 0
                    return 'INSUFFICIENT_LOCKED_AMOUNT'

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
    transfer_note = db.Column(
        pg.TEXT,
        comment='A note from the sender. Can be any string that the sender wants the '
                'recipient to see. If the account change represents a committed transfer, '
                'the note will be included in the generated `on_account_transfer_signal` '
                'event, otherwise the note is ignored. Can be NULL only if '
                '`principal_delta` is zero.',
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
        db.CheckConstraint(or_(principal_delta == 0, transfer_note != null())),
        db.CheckConstraint(unlocked_amount >= 0),
        {
            'comment': 'Represents a pending change to a given account. Pending updates to '
                       '`account.principal`, `account.interest`, and `account.locked_amount` are '
                       'queued to this table, before being processed, because this allows '
                       'multiple updates to one account to coalesce, reducing the lock '
                       'contention on `account` table rows.',
        }
    )
