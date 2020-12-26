import re
import math
import iso8601
from .extensions import broker, APP_QUEUE_NAME
from .models import MIN_INT32, MAX_INT32, MIN_INT64, MAX_INT64, BEGINNING_OF_TIME, TRANSFER_NOTE_MAX_BYTES, \
    CONFIG_DATA_MAX_BYTES
from . import procedures

RE_TRANSFER_NOTE_FORMAT = re.compile(r'^[0-9A-Za-z.-]{0,8}$')


@broker.actor(queue_name=APP_QUEUE_NAME)
def configure_account(
        debtor_id: int,
        creditor_id: int,
        ts: str,
        seqnum: int,
        negligible_amount: float = 0.0,
        config_flags: int = 0,
        config_data: str = '') -> None:

    """Make sure the account exists, and update its configuration settings."""

    parsed_ts = iso8601.parse_date(ts)

    assert MIN_INT64 <= debtor_id <= MAX_INT64
    assert MIN_INT64 <= creditor_id <= MAX_INT64
    assert parsed_ts > BEGINNING_OF_TIME
    assert MIN_INT32 <= seqnum <= MAX_INT32
    assert MIN_INT32 <= config_flags <= MAX_INT32
    assert len(config_data) <= CONFIG_DATA_MAX_BYTES and len(config_data.encode('utf8')) <= CONFIG_DATA_MAX_BYTES

    procedures.configure_account(
        debtor_id,
        creditor_id,
        parsed_ts,
        seqnum,
        negligible_amount,
        config_flags,
        config_data,
    )


@broker.actor(queue_name=APP_QUEUE_NAME)
def prepare_transfer(
        coordinator_type: str,
        coordinator_id: int,
        coordinator_request_id: int,
        min_locked_amount: int,
        max_locked_amount: int,
        debtor_id: int,
        creditor_id: int,
        recipient: str,
        ts: str,
        max_commit_delay: int,
        min_interest_rate: float = -100.0) -> None:

    """Try to secure some amount, to eventually transfer it to another account."""

    parsed_ts = iso8601.parse_date(ts)

    assert len(coordinator_type) <= 30 and coordinator_type.encode('ascii')
    assert MIN_INT64 <= coordinator_id <= MAX_INT64
    assert MIN_INT64 <= coordinator_request_id <= MAX_INT64
    assert 0 <= min_locked_amount <= max_locked_amount <= MAX_INT64
    assert MIN_INT64 <= debtor_id <= MAX_INT64
    assert MIN_INT64 <= creditor_id <= MAX_INT64
    assert parsed_ts > BEGINNING_OF_TIME
    assert 0 <= max_commit_delay <= MAX_INT32

    procedures.prepare_transfer(
        coordinator_type,
        coordinator_id,
        coordinator_request_id,
        min_locked_amount,
        max_locked_amount,
        debtor_id,
        creditor_id,
        recipient,
        parsed_ts,
        max_commit_delay,
        min_interest_rate,
    )


@broker.actor(queue_name=APP_QUEUE_NAME)
def finalize_transfer(
        debtor_id: int,
        creditor_id: int,
        transfer_id: int,
        coordinator_type: str,
        coordinator_id: int,
        coordinator_request_id: int,
        committed_amount: int,
        finalization_flags: int,
        transfer_note_format: str,
        transfer_note: str,
        ts: str) -> None:

    """Finalize a prepared transfer."""

    parsed_ts = iso8601.parse_date(ts)

    assert MIN_INT64 <= debtor_id <= MAX_INT64
    assert MIN_INT64 <= creditor_id <= MAX_INT64
    assert MIN_INT64 <= transfer_id <= MAX_INT64
    assert len(coordinator_type) <= 30 and coordinator_type.encode('ascii')
    assert MIN_INT64 <= coordinator_id <= MAX_INT64
    assert MIN_INT64 <= coordinator_request_id <= MAX_INT64
    assert 0 <= committed_amount <= MAX_INT64
    assert MIN_INT32 <= finalization_flags <= MAX_INT32
    assert RE_TRANSFER_NOTE_FORMAT.match(transfer_note_format)
    assert len(transfer_note) <= TRANSFER_NOTE_MAX_BYTES
    assert len(transfer_note.encode('utf8')) <= TRANSFER_NOTE_MAX_BYTES
    assert parsed_ts > BEGINNING_OF_TIME

    procedures.finalize_transfer(
        debtor_id,
        creditor_id,
        transfer_id,
        coordinator_type,
        coordinator_id,
        coordinator_request_id,
        committed_amount,
        finalization_flags,
        transfer_note_format,
        transfer_note,
        parsed_ts,
    )


# TODO: Consider passing a `demurrage_rate` argument here as
#       well. This would allow us to more accurately set the
#       `demurrage_rate` field in `AccountUpdate` messages.
@broker.actor(queue_name=APP_QUEUE_NAME)
def try_to_change_interest_rate(
        debtor_id: int,
        creditor_id: int,
        interest_rate: float,
        request_ts: str) -> None:

    """Try to change the interest rate on the account.

    The interest rate will not be changed if not enough time has
    passed since the previous change in the interest rate.

    """

    assert MIN_INT64 <= debtor_id <= MAX_INT64
    assert MIN_INT64 <= creditor_id <= MAX_INT64
    assert not math.isnan(interest_rate)

    procedures.try_to_change_interest_rate(
        debtor_id,
        creditor_id,
        interest_rate,
        iso8601.parse_date(request_ts),
    )


@broker.actor(queue_name=APP_QUEUE_NAME)
def capitalize_interest(
        debtor_id: int,
        creditor_id: int,
        accumulated_interest_threshold: int,
        request_ts: str) -> None:

    """Clear the interest accumulated on the account, adding it to the principal.

    Does nothing if the absolute value of the accumulated interest is
    smaller than `abs(accumulated_interest_threshold)`.

    """

    assert MIN_INT64 <= debtor_id <= MAX_INT64
    assert MIN_INT64 <= creditor_id <= MAX_INT64
    assert MIN_INT64 <= accumulated_interest_threshold <= MAX_INT64

    procedures.capitalize_interest(
        debtor_id,
        creditor_id,
        accumulated_interest_threshold,
        iso8601.parse_date(request_ts),
    )


@broker.actor(queue_name=APP_QUEUE_NAME)
def try_to_delete_account(
        debtor_id: int,
        creditor_id: int,
        request_ts: str) -> None:

    """Mark the account as deleted, if possible.

    If it is a "normal" account, it will be marked as deleted if it
    has been scheduled for deletion, there are no prepared transfers,
    and the current balance is not bigger than `max(2.0,
    account.negligible_amount)`.

    If it is the debtor's account, it will be marked as deleted if its
    principal is zero (`account.negligible_amount` is ignored in this
    case).

    Note that when a "normal" account has been successfully marked as
    deleted, it could be "resurrected" (with "scheduled for deletion"
    configuration flag) by a delayed incoming transfer. Therefore,
    this function does not guarantee that the account will be marked
    as deleted successfully, or that it will "stay" deleted for
    long. To achieve a reliable deletion, this function may need to be
    called repeatedly, until the account has been purged from the
    database.

    """

    assert MIN_INT64 <= debtor_id <= MAX_INT64
    assert MIN_INT64 <= creditor_id <= MAX_INT64

    procedures.try_to_delete_account(
        debtor_id,
        creditor_id,
        iso8601.parse_date(request_ts),
    )
