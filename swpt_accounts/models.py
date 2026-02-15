import json
import math
from typing import NamedTuple, Optional
from base64 import b16encode
from datetime import datetime, timezone
from decimal import Decimal
from marshmallow import Schema, fields
from flask import current_app
from sqlalchemy import text
from sqlalchemy.inspection import inspect
from sqlalchemy.dialects import postgresql as pg
from sqlalchemy.sql.expression import func, null, or_, and_
from swpt_pythonlib.utils import (
    date_to_int24,
    i64_to_u64,
    i64_to_hex_routing_key,
    calc_bin_routing_key,
)
from swpt_pythonlib import rabbitmq
from swpt_accounts.extensions import (
    db,
    publisher,
    TO_COORDINATORS_EXCHANGE,
    TO_DEBTORS_EXCHANGE,
    TO_CREDITORS_EXCHANGE,
    ACCOUNTS_IN_EXCHANGE,
)

MIN_INT16 = -1 << 15
MAX_INT16 = (1 << 15) - 1
MIN_INT32 = -1 << 31
MAX_INT32 = (1 << 31) - 1
MIN_INT64 = -1 << 63
MAX_INT64 = (1 << 63) - 1
T0 = datetime(1970, 1, 1, tzinfo=timezone.utc)
T_INFINITY = datetime(9999, 12, 31, 23, 59, 59, tzinfo=timezone.utc)
SECONDS_IN_DAY = 24 * 60 * 60
SECONDS_IN_YEAR = 365.25 * SECONDS_IN_DAY
INTEREST_RATE_FLOOR = -50.0
INTEREST_RATE_CEIL = 100.0
TRANSFER_NOTE_MAX_BYTES = 500
CONFIG_DATA_MAX_BYTES = 2000
IRI_MAX_LENGTH = 200
CONTENT_TYPE_MAX_BYTES = 100
CREDITOR_SUBNET_MASK = 0xffffff0000000000
DEBTOR_INFO_SHA256_REGEX = r"^([0-9A-F]{64}|[0-9a-f]{64})?$"
SET_SEQSCAN_ON = text("SET LOCAL enable_seqscan = on")
DISCARD_PLANS = text("DISCARD PLANS")

# The account `(debtor_id, ROOT_CREDITOR_ID)` is special. This is the
# debtor's account. It issuers all the money. Also, all interest and
# demurrage payments come from/to this account.
ROOT_CREDITOR_ID = 0

# Reserved coordinator types:
CT_INTEREST = "interest"
CT_DELETE = "delete"
CT_DIRECT = "direct"
CT_AGENT = "agent"
CT_ISSUING = "issuing"

# Transfer status codes:
SC_OK = "OK"
SC_TIMEOUT = "TIMEOUT"
SC_NEWER_INTEREST_RATE = "NEWER_INTEREST_RATE"
SC_SENDER_IS_UNREACHABLE = "SENDER_IS_UNREACHABLE"
SC_RECIPIENT_IS_UNREACHABLE = "RECIPIENT_IS_UNREACHABLE"
SC_INSUFFICIENT_AVAILABLE_AMOUNT = "INSUFFICIENT_AVAILABLE_AMOUNT"
SC_RECIPIENT_SAME_AS_SENDER = "RECIPIENT_SAME_AS_SENDER"
SC_TOO_MANY_TRANSFERS = "TOO_MANY_TRANSFERS"


class RootConfigData(NamedTuple):
    interest_rate_target: float = 0.0
    info_iri: Optional[str] = None
    info_sha256: Optional[bytes] = None
    info_content_type: Optional[str] = None
    issuing_limit: int = MAX_INT64


class classproperty(object):
    def __init__(self, f):
        self.f = f

    def __get__(self, obj, owner):
        return self.f(owner)


class ChooseRowsMixin:
    @classmethod
    def choose_rows(cls, primary_keys: list[tuple], name: str = "chosen"):
        pktype_name = f"{cls.__table__.name}_pktype"
        bindparam_name = f"{name}_rows"
        return (
            text(f"SELECT * FROM unnest(:{bindparam_name} :: {pktype_name}[])")
            .bindparams(**{bindparam_name: primary_keys})
            .columns(**{c.key: c.type for c in inspect(cls).primary_key})
            .cte(name=name)
        )


def get_now_utc() -> datetime:
    return datetime.now(tz=timezone.utc)


def calc_k(interest_rate: float) -> float:
    return math.log(1.0 + interest_rate / 100.0) / SECONDS_IN_YEAR


def is_valid_account(
    debtor_id: int, creditor_id: int, match_parent=False
) -> bool:
    sharding_realm = current_app.config["SHARDING_REALM"]
    return sharding_realm.match(
        debtor_id, creditor_id, match_parent=match_parent
    )


def contain_principal_overflow(value: int) -> int:
    if value <= MIN_INT64:
        return -MAX_INT64
    if value > MAX_INT64:
        return MAX_INT64
    return value


def calc_current_balance(
    *,
    creditor_id: int,
    principal: int,
    interest: float,
    interest_rate: float,
    last_change_ts: datetime,
    current_ts: datetime,
) -> Decimal:
    current_balance = Decimal(principal)

    # Any interest accumulated on the debtor's account will not be
    # included in the current balance. Thus, accumulating interest on
    # the debtor's account has no effect.
    if creditor_id != ROOT_CREDITOR_ID:
        current_balance += Decimal.from_float(interest)
        if current_balance > 0:
            k = calc_k(interest_rate)
            passed_seconds = max(
                0.0, (current_ts - last_change_ts).total_seconds()
            )
            current_balance *= Decimal.from_float(math.exp(k * passed_seconds))

    return current_balance


def is_negligible_balance(balance, negligible_amount):
    return balance <= negligible_amount or balance <= 2.0


def are_managed_by_same_agent(
        sender_creditor_id: int,
        recipient_creditor_id: int,
) -> bool:
    return (
        sender_creditor_id & CREDITOR_SUBNET_MASK
        == recipient_creditor_id & CREDITOR_SUBNET_MASK
        != 0  # Creditor IDs starting with 32 zero bits are reserved.
    )


class Account(db.Model, ChooseRowsMixin):
    CONFIG_SCHEDULED_FOR_DELETION_FLAG = 1 << 0

    STATUS_DELETED_FLAG = 1 << 0
    STATUS_OVERFLOWN_FLAG = 1 << 1

    debtor_id = db.Column(db.BigInteger, primary_key=True)
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    creation_date = db.Column(db.DATE, nullable=False)
    last_change_seqnum = db.Column(db.Integer, nullable=False, default=0)
    last_change_ts = db.Column(
        db.TIMESTAMP(timezone=True), nullable=False, default=get_now_utc
    )
    principal = db.Column(db.BigInteger, nullable=False, default=0)
    interest_rate = db.Column(db.REAL, nullable=False, default=0.0)
    interest = db.Column(db.FLOAT, nullable=False, default=0.0)
    last_interest_rate_change_ts = db.Column(
        db.TIMESTAMP(timezone=True), nullable=False, default=T0
    )
    last_config_ts = db.Column(
        db.TIMESTAMP(timezone=True), nullable=False, default=T0
    )
    last_config_seqnum = db.Column(db.Integer, nullable=False, default=0)
    last_transfer_number = db.Column(db.BigInteger, nullable=False, default=0)
    last_transfer_committed_at = db.Column(
        db.TIMESTAMP(timezone=True), nullable=False, default=T0
    )
    negligible_amount = db.Column(db.REAL, nullable=False, default=0.0)
    config_flags = db.Column(db.Integer, nullable=False, default=0)
    config_data = db.Column(db.String, nullable=False, default="")
    debtor_info_iri = db.Column(db.String)
    debtor_info_content_type = db.Column(db.String)
    debtor_info_sha256 = db.Column(db.LargeBinary)
    status_flags = db.Column(
        db.Integer,
        nullable=False,
        default=0,
        comment=(
            "Contain account status bits: "
            f"{STATUS_DELETED_FLAG} - deleted,"
            f"{STATUS_OVERFLOWN_FLAG} - overflown."
        ),
    )
    total_locked_amount = db.Column(
        db.BigInteger,
        nullable=False,
        default=0,
        comment=(
            "The total sum of all pending transfer locks (the total sum of the"
            " values of the `pending_transfer.locked_amount` column) for this"
            " account. This value has been reserved and must be subtracted"
            " from the available amount, to avoid double-spending."
        ),
    )
    pending_transfers_count = db.Column(
        db.Integer,
        nullable=False,
        default=0,
        comment="The number of `pending_transfer` records for this account.",
    )
    last_transfer_id = db.Column(
        db.BigInteger,
        nullable=False,
        default=(
            lambda context: date_to_int24(
                context.get_current_parameters()["creation_date"]
            )
            << 40
        ),
        comment=(
            "Incremented when a new `prepared_transfer` record is inserted. It"
            " is used to generate sequential numbers for the"
            " `prepared_transfer.transfer_id` column. When the account is"
            " created, `last_transfer_id` has its lower 40 bits set to zero,"
            " and its higher 24 bits calculated from the value of"
            " `creation_date` (the number of days since Jan 1st, 1970)."
        ),
    )
    previous_interest_rate = db.Column(
        db.REAL,
        nullable=False,
        default=0.0,
        comment=(
            "The annual interest rate (in percents) as it was before the last"
            " change of the interest rate happened (see"
            " `last_interest_rate_change_ts`)."
        ),
    )
    last_heartbeat_ts = db.Column(
        db.TIMESTAMP(timezone=True),
        nullable=False,
        default=get_now_utc,
        comment="The moment at which the last `AccountUpdateSignal` was sent.",
    )
    last_interest_capitalization_ts = db.Column(
        db.TIMESTAMP(timezone=True),
        nullable=False,
        default=T0,
        comment=(
            "The moment at which the last interest capitalization was"
            " preformed. It is used to avoid capitalizing interest too often."
        ),
    )
    last_deletion_attempt_ts = db.Column(
        db.TIMESTAMP(timezone=True),
        nullable=False,
        default=T0,
        comment=(
            "The moment at which the last deletion attempt was made. It is"
            " used to avoid trying to delete the account too often."
        ),
    )
    pending_account_update = db.Column(
        db.BOOLEAN,
        nullable=False,
        default=False,
        comment=(
            "Whether there has been a change in the record that requires an"
            " `AccountUpdate` message to be send."
        ),
    )
    __table_args__ = (
        db.CheckConstraint(
            and_(
                interest_rate >= INTEREST_RATE_FLOOR,
                interest_rate <= INTEREST_RATE_CEIL,
            )
        ),
        db.CheckConstraint(
            and_(
                previous_interest_rate >= INTEREST_RATE_FLOOR,
                previous_interest_rate <= INTEREST_RATE_CEIL,
            )
        ),
        db.CheckConstraint(total_locked_amount >= 0),
        db.CheckConstraint(pending_transfers_count >= 0),
        db.CheckConstraint(principal > MIN_INT64),
        db.CheckConstraint(last_transfer_id >= 0),
        db.CheckConstraint(last_transfer_number >= 0),
        db.CheckConstraint(negligible_amount >= 0.0),
        db.CheckConstraint(
            or_(
                debtor_info_sha256 == null(),
                func.octet_length(debtor_info_sha256) == 32,
            )
        ),
        {
            "comment": "Tells who owes what to whom.",
        },
    )

    def calc_current_balance(self, current_ts: datetime) -> Decimal:
        return calc_current_balance(
            creditor_id=self.creditor_id,
            principal=self.principal,
            interest=self.interest,
            interest_rate=self.interest_rate,
            last_change_ts=self.last_change_ts,
            current_ts=current_ts,
        )

    def calc_due_interest(
        self, amount: int, due_ts: datetime, current_ts: datetime
    ) -> float:
        """Return the accumulated interest between `due_ts` and `current_ts`.

        When `amount` is a positive number, returns the amount of
        interest that would have been accumulated for the given
        `amount`, between `due_ts` and `current_ts`. When `amount` is
        a negative number, returns `-self.calc_due_interest(-amount,
        due_ts, current_ts)`.

        To calculate the accumulated interest, this function assumes
        that: 1) `current_ts` is the current time; 2) The interest
        rate has not changed more than once between `due_ts` and
        `current_ts`.

        """

        start_ts, end_ts = due_ts, max(due_ts, current_ts)
        interest_rate_change_ts = min(
            self.last_interest_rate_change_ts, end_ts
        )
        t = (end_ts - start_ts).total_seconds()
        t1 = max((interest_rate_change_ts - start_ts).total_seconds(), 0)
        t2 = min((end_ts - interest_rate_change_ts).total_seconds(), t)
        k1 = calc_k(self.previous_interest_rate)
        k2 = calc_k(self.interest_rate)

        assert t >= 0
        assert 0 <= t1 <= t
        assert 0 <= t2 <= t
        assert abs(t1 + t2 - t) <= t / 1000

        return amount * (math.exp(k1 * t1 + k2 * t2) - 1.0)


class TransferRequest(db.Model):
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    sender_creditor_id = db.Column(db.BigInteger, primary_key=True)
    transfer_request_id = db.Column(
        db.BigInteger, primary_key=True, autoincrement=True
    )
    coordinator_type = db.Column(db.String(30), nullable=False)
    coordinator_id = db.Column(db.BigInteger, nullable=False)
    coordinator_request_id = db.Column(db.BigInteger, nullable=False)
    min_locked_amount = db.Column(db.BigInteger, nullable=False)
    max_locked_amount = db.Column(db.BigInteger, nullable=False)
    deadline = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    final_interest_rate_ts = db.Column(
        db.TIMESTAMP(timezone=True), nullable=False
    )
    recipient_creditor_id = db.Column(db.BigInteger, nullable=False)
    __table_args__ = (
        db.CheckConstraint(min_locked_amount >= 0),
        db.CheckConstraint(min_locked_amount <= max_locked_amount),
        {
            "comment": (
                "Represents a request to secure (prepare) some amount for"
                " transfer, if it is available on a given account. If the"
                " request is fulfilled, a new row will be inserted in the"
                " `prepared_transfer` table. Requests are queued to the"
                " `transfer_request` table, before being processed, because"
                " this allows many requests from one sender to be processed at"
                " once, reducing the lock contention on `account` table rows."
            ),
        },
    )


class FinalizationRequest(db.Model):
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    sender_creditor_id = db.Column(db.BigInteger, primary_key=True)
    transfer_id = db.Column(db.BigInteger, primary_key=True)
    coordinator_type = db.Column(db.String(30), nullable=False)
    coordinator_id = db.Column(db.BigInteger, nullable=False)
    coordinator_request_id = db.Column(db.BigInteger, nullable=False)
    committed_amount = db.Column(db.BigInteger, nullable=False)
    transfer_note_format = db.Column(pg.TEXT, nullable=False)
    transfer_note = db.Column(pg.TEXT, nullable=False)
    ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    __table_args__ = (
        db.CheckConstraint(committed_amount >= 0),
        {
            "comment": (
                "Represents a request to finalize a prepared transfer."
                " Requests are queued to the `finalization_request` table,"
                " before being processed, because this allows many requests"
                " from one sender to be processed at once, reducing the lock"
                " contention on `account` table rows."
            ),
        },
    )


class PreparedTransfer(db.Model, ChooseRowsMixin):
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    sender_creditor_id = db.Column(db.BigInteger, primary_key=True)
    transfer_id = db.Column(db.BigInteger, primary_key=True)
    coordinator_type = db.Column(db.String(30), nullable=False)
    coordinator_id = db.Column(db.BigInteger, nullable=False)
    coordinator_request_id = db.Column(db.BigInteger, nullable=False)
    recipient_creditor_id = db.Column(db.BigInteger, nullable=False)
    prepared_at = db.Column(
        db.TIMESTAMP(timezone=True), nullable=False, default=get_now_utc
    )
    final_interest_rate_ts = db.Column(
        db.TIMESTAMP(timezone=True), nullable=False
    )
    demurrage_rate = db.Column(db.FLOAT, nullable=False)
    deadline = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    locked_amount = db.Column(db.BigInteger, nullable=False)
    last_reminder_ts = db.Column(
        db.TIMESTAMP(timezone=True),
        comment=(
            "The moment at which the last `PreparedTransferSignal` was sent to"
            " remind that the prepared transfer must be finalized. A `NULL`"
            " means that no reminders have been sent yet. This column helps to"
            " prevent sending reminders too often."
        ),
    )
    __table_args__ = (
        db.ForeignKeyConstraint(
            ["debtor_id", "sender_creditor_id"],
            ["account.debtor_id", "account.creditor_id"],
            ondelete="CASCADE",
        ),
        db.CheckConstraint(transfer_id > 0),
        db.CheckConstraint(locked_amount >= 0),
        db.CheckConstraint(
            (demurrage_rate > -100.0) & (demurrage_rate <= 0.0)
        ),
        {
            "comment": (
                "A prepared transfer represent a guarantee that a particular"
                " transfer of funds will be successful if ordered (committed)."
                " A record will remain in this table until the transfer has"
                " been committed or dismissed."
            ),
        },
    )

    def calc_status_code(
        self,
        committed_amount: int,
        expendable_amount: int,
        last_interest_rate_change_ts: datetime,
        current_ts: datetime,
    ) -> str:
        assert committed_amount >= 0

        def get_is_expendable():
            return committed_amount <= expendable_amount + self.locked_amount

        def get_is_reserved():
            if committed_amount > self.locked_amount:
                return False

            elif self.sender_creditor_id == ROOT_CREDITOR_ID:
                # We do not need to calculate demurrage for transfers
                # from the debtor's account, because all interest
                # payments come from this account anyway.
                return True

            else:
                demurrage_seconds = max(
                    0.0, (current_ts - self.prepared_at).total_seconds()
                )
                ratio = math.exp(
                    calc_k(self.demurrage_rate) * demurrage_seconds
                )
                assert ratio <= 1.0

                # To avoid nasty surprises coming from precision loss,
                # we multiply `committed_amount` (a 64-bit integer) to
                # `1.0` before the comparison.
                return committed_amount * 1.0 <= self.locked_amount * ratio

        if committed_amount != 0:
            if current_ts > self.deadline:
                return SC_TIMEOUT

            if last_interest_rate_change_ts > self.final_interest_rate_ts:
                return SC_NEWER_INTEREST_RATE

            if not (get_is_expendable() or get_is_reserved()):
                return SC_INSUFFICIENT_AVAILABLE_AMOUNT

        return SC_OK


class RegisteredBalanceChange(db.Model, ChooseRowsMixin):
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    other_creditor_id = db.Column(db.BigInteger, primary_key=True)
    change_id = db.Column(db.BigInteger, primary_key=True)
    committed_at = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    is_applied = db.Column(db.BOOLEAN, nullable=False, default=False)
    __table_args__ = {
        "comment": (
            "Represents the fact that a given pending balance change has been "
            "registered already. This is necessary in order to avoid applying "
            "one balance change more than once, when the corresponding "
            "`PendingBalanceChangeSignal`s is received multiple times."
        ),
    }


class PendingBalanceChange(db.Model):
    debtor_id = db.Column(db.BigInteger, primary_key=True)
    other_creditor_id = db.Column(
        db.BigInteger,
        primary_key=True,
        comment=(
            "This is the other party in the transfer. When `principal_delta`"
            " is positive, this is the sender. When `principal_delta` is"
            " negative, this is the recipient."
        ),
    )
    change_id = db.Column(db.BigInteger, primary_key=True)
    creditor_id = db.Column(db.BigInteger, nullable=False)
    coordinator_type = db.Column(db.String(30), nullable=False)
    transfer_note_format = db.Column(pg.TEXT, nullable=False)
    transfer_note = db.Column(pg.TEXT, nullable=False)
    committed_at = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    principal_delta = db.Column(db.BigInteger, nullable=False)
    __table_args__ = (
        db.ForeignKeyConstraint(
            [
                "debtor_id",
                "other_creditor_id",
                "change_id",
            ],
            [
                "registered_balance_change.debtor_id",
                "registered_balance_change.other_creditor_id",
                "registered_balance_change.change_id",
            ],
        ),
        db.CheckConstraint(principal_delta != 0),
        db.Index("idx_changed_account", debtor_id, creditor_id),
        {
            "comment": (
                "Represents a pending change in the balance of a given"
                " account. Pending updates to `account.principal` are queued"
                " to this table before being processed, thus allowing multiple"
                " updates to one account to coalesce, reducing the lock"
                " contention on `account` table rows."
            ),
        },
    )


class Signal(db.Model, ChooseRowsMixin):
    """A pending message that needs to be send to the RabbitMQ server."""

    __abstract__ = True

    @classmethod
    def send_signalbus_messages(cls, objects):
        assert all(isinstance(obj, cls) for obj in objects)
        messages = (obj._create_message() for obj in objects)
        publisher.publish_messages([m for m in messages if m is not None])

    def send_signalbus_message(self):
        self.send_signalbus_messages([self])

    def _create_message(self):
        data = self.__marshmallow_schema__.dump(self)
        message_type = data["type"]
        creditor_id = data["creditor_id"]
        debtor_id = data["debtor_id"]

        if message_type != "PendingBalanceChange" and not is_valid_account(
            debtor_id, creditor_id
        ):
            if current_app.config[
                "DELETE_PARENT_SHARD_RECORDS"
            ] and is_valid_account(debtor_id, creditor_id, match_parent=True):
                # This message most probably is a left-over from the
                # previous splitting of the parent shard into children
                # shards. Therefore we should just ignore it.
                return None
            raise RuntimeError(
                "The shard is not responsible for this account."
            )  # pragma: no cover

        headers = {
            "message-type": message_type,
            "debtor-id": debtor_id,
            "creditor-id": creditor_id,
        }
        if "coordinator_id" in data:
            headers["coordinator-id"] = data["coordinator_id"]
            headers["coordinator-type"] = data["coordinator_type"]

        properties = rabbitmq.MessageProperties(
            delivery_mode=2,
            app_id="swpt_accounts",
            content_type="application/json",
            type=message_type,
            headers=headers,
        )
        body = json.dumps(
            data,
            ensure_ascii=False,
            check_circular=False,
            allow_nan=False,
            separators=(",", ":"),
        ).encode("utf8")

        return rabbitmq.Message(
            exchange=self.exchange_name,
            routing_key=self.routing_key,
            body=body,
            properties=properties,
            mandatory=not (
                # When deactivating "Debtor Agents", most probably we
                # will not be able to purge all root accounts, and for
                # them we will continue to send heartbeat
                # "AccountUpdate" messages.
                message_type == "AccountUpdate"
                and creditor_id == ROOT_CREDITOR_ID
                and (
                    data["config_flags"]
                    & Account.CONFIG_SCHEDULED_FOR_DELETION_FLAG
                )
            ),
        )

    inserted_at = db.Column(
        db.TIMESTAMP(timezone=True), nullable=False, default=get_now_utc
    )


class RejectedTransferSignal(Signal):
    exchange_name = TO_COORDINATORS_EXCHANGE

    class __marshmallow__(Schema):
        type = fields.Constant("RejectedTransfer")
        coordinator_type = fields.String()
        coordinator_id = fields.Integer()
        coordinator_request_id = fields.Integer()
        status_code = fields.String()
        total_locked_amount = fields.Integer()
        debtor_id = fields.Integer()
        sender_creditor_id = fields.Integer(data_key="creditor_id")
        inserted_at = fields.DateTime(data_key="ts")

    __marshmallow_schema__ = __marshmallow__()

    debtor_id = db.Column(db.BigInteger, primary_key=True)
    sender_creditor_id = db.Column(db.BigInteger, primary_key=True)
    signal_id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    coordinator_type = db.Column(db.String(30), nullable=False)
    coordinator_id = db.Column(db.BigInteger, nullable=False)
    coordinator_request_id = db.Column(db.BigInteger, nullable=False)
    status_code = db.Column(db.String(30), nullable=False)
    total_locked_amount = db.Column(db.BigInteger, nullable=False)

    @classproperty
    def signalbus_burst_count(self):
        return current_app.config["APP_FLUSH_REJECTED_TRANSFERS_BURST_COUNT"]

    @property
    def routing_key(self):
        return i64_to_hex_routing_key(self.coordinator_id)


class PreparedTransferSignal(Signal):
    exchange_name = TO_COORDINATORS_EXCHANGE

    class __marshmallow__(Schema):
        type = fields.Constant("PreparedTransfer")
        debtor_id = fields.Integer()
        sender_creditor_id = fields.Integer(data_key="creditor_id")
        transfer_id = fields.Integer()
        coordinator_type = fields.String()
        coordinator_id = fields.Integer()
        coordinator_request_id = fields.Integer()
        locked_amount = fields.Integer()
        recipient = fields.Function(
            lambda obj: str(i64_to_u64(obj.recipient_creditor_id))
        )
        prepared_at = fields.DateTime()
        inserted_at = fields.DateTime(data_key="ts")
        demurrage_rate = fields.Float()
        deadline = fields.DateTime()
        final_interest_rate_ts = fields.DateTime()

    __marshmallow_schema__ = __marshmallow__()

    debtor_id = db.Column(db.BigInteger, primary_key=True)
    sender_creditor_id = db.Column(db.BigInteger, primary_key=True)
    signal_id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    transfer_id = db.Column(db.BigInteger, nullable=False)
    coordinator_type = db.Column(db.String(30), nullable=False)
    coordinator_id = db.Column(db.BigInteger, nullable=False)
    coordinator_request_id = db.Column(db.BigInteger, nullable=False)
    locked_amount = db.Column(db.BigInteger, nullable=False)
    recipient_creditor_id = db.Column(db.BigInteger, nullable=False)
    prepared_at = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    demurrage_rate = db.Column(db.FLOAT, nullable=False)
    deadline = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    final_interest_rate_ts = db.Column(
        db.TIMESTAMP(timezone=True), nullable=False
    )

    @classproperty
    def signalbus_burst_count(self):
        return current_app.config["APP_FLUSH_PREPARED_TRANSFERS_BURST_COUNT"]

    @property
    def routing_key(self):
        return i64_to_hex_routing_key(self.coordinator_id)


class FinalizedTransferSignal(Signal):
    exchange_name = TO_COORDINATORS_EXCHANGE

    class __marshmallow__(Schema):
        type = fields.Constant("FinalizedTransfer")
        debtor_id = fields.Integer()
        sender_creditor_id = fields.Integer(data_key="creditor_id")
        transfer_id = fields.Integer()
        coordinator_type = fields.String()
        coordinator_id = fields.Integer()
        coordinator_request_id = fields.Integer()
        prepared_at = fields.DateTime()
        finalized_at = fields.DateTime(data_key="ts")
        committed_amount = fields.Integer()
        total_locked_amount = fields.Integer()
        status_code = fields.String()

    __marshmallow_schema__ = __marshmallow__()

    debtor_id = db.Column(db.BigInteger, primary_key=True)
    sender_creditor_id = db.Column(db.BigInteger, primary_key=True)
    transfer_id = db.Column(db.BigInteger, primary_key=True)
    coordinator_type = db.Column(db.String(30), nullable=False)
    coordinator_id = db.Column(db.BigInteger, nullable=False)
    coordinator_request_id = db.Column(db.BigInteger, nullable=False)
    prepared_at = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    finalized_at = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    committed_amount = db.Column(db.BigInteger, nullable=False)
    total_locked_amount = db.Column(db.BigInteger, nullable=False)
    status_code = db.Column(db.String(30), nullable=False)

    @classproperty
    def signalbus_burst_count(self):
        return current_app.config["APP_FLUSH_FINALIZED_TRANSFERS_BURST_COUNT"]

    @property
    def routing_key(self):
        return i64_to_hex_routing_key(self.coordinator_id)


class AccountTransferSignal(Signal):
    class __marshmallow__(Schema):
        type = fields.Constant("AccountTransfer")
        debtor_id = fields.Integer()
        creditor_id = fields.Integer()
        creation_date = fields.Date()
        transfer_number = fields.Integer()
        coordinator_type = fields.String()
        committed_at = fields.DateTime()
        acquired_amount = fields.Integer()
        transfer_note_format = fields.String()
        transfer_note = fields.String()
        principal = fields.Integer()
        previous_transfer_number = fields.Integer()
        sender = fields.Function(
            lambda obj: str(i64_to_u64(obj.sender_creditor_id))
        )
        recipient = fields.Function(
            lambda obj: str(i64_to_u64(obj.recipient_creditor_id))
        )
        inserted_at = fields.DateTime(data_key="ts")

    __marshmallow_schema__ = __marshmallow__()

    SYSTEM_FLAG_IS_NEGLIGIBLE = 1
    """Indicates that the absolute value of `committed_amount` is not
    bigger than the negligible amount configured for the account.
    """

    debtor_id = db.Column(db.BigInteger, primary_key=True)
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    creation_date = db.Column(db.DATE, primary_key=True)
    transfer_number = db.Column(db.BigInteger, primary_key=True)
    coordinator_type = db.Column(db.String(30), nullable=False)
    committed_at = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    acquired_amount = db.Column(db.BigInteger, nullable=False)
    other_creditor_id = db.Column(db.BigInteger, nullable=False)
    transfer_note_format = db.Column(pg.TEXT, nullable=False)
    transfer_note = db.Column(pg.TEXT, nullable=False)
    principal = db.Column(db.BigInteger, nullable=False)
    previous_transfer_number = db.Column(db.BigInteger, nullable=False)

    @classproperty
    def signalbus_burst_count(self):
        return current_app.config["APP_FLUSH_ACCOUNT_TRANSFERS_BURST_COUNT"]

    @property
    def exchange_name(self):
        return (
            TO_DEBTORS_EXCHANGE
            if self.creditor_id == ROOT_CREDITOR_ID
            else TO_CREDITORS_EXCHANGE
        )

    @property
    def routing_key(self):
        return i64_to_hex_routing_key(
            self.debtor_id
            if self.creditor_id == ROOT_CREDITOR_ID
            else self.creditor_id
        )

    @property
    def sender_creditor_id(self):
        return (
            self.other_creditor_id
            if self.acquired_amount >= 0
            else self.creditor_id
        )

    @property
    def recipient_creditor_id(self):
        return (
            self.other_creditor_id
            if self.acquired_amount < 0
            else self.creditor_id
        )


class AccountUpdateSignal(Signal):
    class __marshmallow__(Schema):
        type = fields.Constant("AccountUpdate")
        debtor_id = fields.Integer()
        creditor_id = fields.Integer()
        last_change_ts = fields.DateTime()
        last_change_seqnum = fields.Integer()
        principal = fields.Integer()
        interest = fields.Float()
        interest_rate = fields.Float()
        transfer_note_max_bytes = fields.Constant(TRANSFER_NOTE_MAX_BYTES)
        demurrage_rate = fields.Constant(INTEREST_RATE_FLOOR)
        commit_period = fields.Integer()
        last_interest_rate_change_ts = fields.DateTime()
        last_transfer_number = fields.Integer()
        last_transfer_committed_at = fields.DateTime()
        last_config_ts = fields.DateTime()
        last_config_seqnum = fields.Integer()
        creation_date = fields.Date()
        negligible_amount = fields.Float()
        config_data = fields.String()
        config_flags = fields.Integer()
        inserted_at = fields.DateTime(data_key="ts")
        ttl = fields.Integer()
        account_id = fields.Function(
            lambda obj: str(i64_to_u64(obj.creditor_id))
        )
        debtor_info_iri = fields.Function(
            lambda obj: obj.debtor_info_iri or ""
        )
        debtor_info_content_type = fields.Function(
            lambda obj: obj.debtor_info_content_type or ""
        )
        debtor_info_sha256 = fields.Function(
            lambda obj: b16encode(obj.debtor_info_sha256 or b"").decode()
        )

    __marshmallow_schema__ = __marshmallow__()

    debtor_id = db.Column(db.BigInteger, primary_key=True)
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    signal_id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    last_change_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    last_change_seqnum = db.Column(db.Integer, nullable=False)
    principal = db.Column(db.BigInteger, nullable=False)
    interest = db.Column(db.FLOAT, nullable=False)
    interest_rate = db.Column(db.REAL, nullable=False)
    last_interest_rate_change_ts = db.Column(
        db.TIMESTAMP(timezone=True), nullable=False
    )
    last_transfer_number = db.Column(db.BigInteger, nullable=False)
    last_transfer_committed_at = db.Column(
        db.TIMESTAMP(timezone=True), nullable=False
    )
    last_config_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    last_config_seqnum = db.Column(db.Integer, nullable=False)
    creation_date = db.Column(db.DATE, nullable=False)
    negligible_amount = db.Column(db.REAL, nullable=False)
    config_data = db.Column(db.String, nullable=False)
    config_flags = db.Column(db.Integer, nullable=False)
    debtor_info_iri = db.Column(db.String)
    debtor_info_content_type = db.Column(db.String)
    debtor_info_sha256 = db.Column(db.LargeBinary)

    @classproperty
    def signalbus_burst_count(self):
        return current_app.config["APP_FLUSH_ACCOUNT_UPDATES_BURST_COUNT"]

    @property
    def exchange_name(self):
        return (
            TO_DEBTORS_EXCHANGE
            if self.creditor_id == ROOT_CREDITOR_ID
            else TO_CREDITORS_EXCHANGE
        )

    @property
    def routing_key(self):
        return i64_to_hex_routing_key(
            self.debtor_id
            if self.creditor_id == ROOT_CREDITOR_ID
            else self.creditor_id
        )

    @property
    def ttl(self):
        return int(
            current_app.config["APP_MESSAGE_MAX_DELAY_DAYS"] * SECONDS_IN_DAY
        )

    @property
    def commit_period(self):
        return int(
            current_app.config["APP_PREPARED_TRANSFER_MAX_DELAY_DAYS"]
            * SECONDS_IN_DAY
        )


class AccountPurgeSignal(Signal):
    class __marshmallow__(Schema):
        type = fields.Constant("AccountPurge")
        debtor_id = fields.Integer()
        creditor_id = fields.Integer()
        creation_date = fields.Date()
        inserted_at = fields.DateTime(data_key="ts")

    __marshmallow_schema__ = __marshmallow__()

    debtor_id = db.Column(db.BigInteger, primary_key=True)
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    creation_date = db.Column(db.DATE, primary_key=True)

    @property
    def exchange_name(self):
        return (
            TO_DEBTORS_EXCHANGE
            if self.creditor_id == ROOT_CREDITOR_ID
            else TO_CREDITORS_EXCHANGE
        )

    @property
    def routing_key(self):
        return i64_to_hex_routing_key(
            self.debtor_id
            if self.creditor_id == ROOT_CREDITOR_ID
            else self.creditor_id
        )

    @classproperty
    def signalbus_burst_count(self):
        return current_app.config["APP_FLUSH_ACCOUNT_PURGES_BURST_COUNT"]


class RejectedConfigSignal(Signal):
    class __marshmallow__(Schema):
        type = fields.Constant("RejectedConfig")
        debtor_id = fields.Integer()
        creditor_id = fields.Integer()
        config_ts = fields.DateTime()
        config_seqnum = fields.Integer()
        negligible_amount = fields.Float()
        config_data = fields.String()
        config_flags = fields.Integer()
        inserted_at = fields.DateTime(data_key="ts")
        rejection_code = fields.String()

    __marshmallow_schema__ = __marshmallow__()

    debtor_id = db.Column(db.BigInteger, primary_key=True)
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    signal_id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    config_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    config_seqnum = db.Column(db.Integer, nullable=False)
    config_flags = db.Column(db.Integer, nullable=False)
    config_data = db.Column(db.String, nullable=False)
    negligible_amount = db.Column(db.REAL, nullable=False)
    rejection_code = db.Column(db.String(30), nullable=False)

    @property
    def exchange_name(self):
        return (
            TO_DEBTORS_EXCHANGE
            if self.creditor_id == ROOT_CREDITOR_ID
            else TO_CREDITORS_EXCHANGE
        )

    @property
    def routing_key(self):
        return i64_to_hex_routing_key(
            self.debtor_id
            if self.creditor_id == ROOT_CREDITOR_ID
            else self.creditor_id
        )

    @classproperty
    def signalbus_burst_count(self):
        return current_app.config["APP_FLUSH_REJECTED_CONFIGS_BURST_COUNT"]


class PendingBalanceChangeSignal(Signal):
    exchange_name = ACCOUNTS_IN_EXCHANGE

    class __marshmallow__(Schema):
        type = fields.Constant("PendingBalanceChange")
        debtor_id = fields.Integer()
        creditor_id = fields.Integer()
        change_id = fields.Integer()
        coordinator_type = fields.String()
        transfer_note_format = fields.String()
        transfer_note = fields.String()
        committed_at = fields.DateTime()
        principal_delta = fields.Integer()
        other_creditor_id = fields.Integer()

    __marshmallow_schema__ = __marshmallow__()

    debtor_id = db.Column(db.BigInteger, primary_key=True)
    other_creditor_id = db.Column(db.BigInteger, primary_key=True)
    change_id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    creditor_id = db.Column(db.BigInteger, nullable=False)
    coordinator_type = db.Column(db.String(30), nullable=False)
    transfer_note_format = db.Column(pg.TEXT, nullable=False)
    transfer_note = db.Column(pg.TEXT, nullable=False)
    committed_at = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    principal_delta = db.Column(db.BigInteger, nullable=False)

    @property
    def routing_key(self):
        return calc_bin_routing_key(self.debtor_id, self.creditor_id)

    @classproperty
    def signalbus_burst_count(self):
        return current_app.config[
            "APP_FLUSH_PENDING_BALANCE_CHANGES_BURST_COUNT"
        ]
