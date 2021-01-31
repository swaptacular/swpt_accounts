import re
from datetime import datetime
from swpt_lib.utils import u64_to_i64
from .extensions import protocol_broker, APP_QUEUE_NAME
from swpt_accounts.models import MIN_INT32, MAX_INT32, MIN_INT64, MAX_INT64, T0, TRANSFER_NOTE_MAX_BYTES, \
    CONFIG_DATA_MAX_BYTES
from swpt_accounts.fetch_api_client import get_if_account_is_reachable
from swpt_accounts import procedures
from swpt_accounts.chores import configure_account_and_set_interest_rate

RE_TRANSFER_NOTE_FORMAT = re.compile(r'^[0-9A-Za-z.-]{0,8}$')


@protocol_broker.actor(queue_name=APP_QUEUE_NAME)
def configure_account(
        debtor_id: int,
        creditor_id: int,
        ts: str,
        seqnum: int,
        negligible_amount: float = 0.0,
        config_flags: int = 0,
        config_data: str = '') -> None:

    """Make sure the account exists, and update its configuration settings."""

    parsed_ts = datetime.fromisoformat(ts)

    assert MIN_INT64 <= debtor_id <= MAX_INT64
    assert MIN_INT64 <= creditor_id <= MAX_INT64
    assert parsed_ts > T0
    assert MIN_INT32 <= seqnum <= MAX_INT32
    assert MIN_INT32 <= config_flags <= MAX_INT32
    assert len(config_data) <= CONFIG_DATA_MAX_BYTES and len(config_data.encode('utf8')) <= CONFIG_DATA_MAX_BYTES

    configure_account_and_set_interest_rate(
        debtor_id=debtor_id,
        creditor_id=creditor_id,
        ts=parsed_ts,
        seqnum=seqnum,
        negligible_amount=negligible_amount,
        config_flags=config_flags,
        config_data=config_data,
    )


@protocol_broker.actor(queue_name=APP_QUEUE_NAME)
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

    parsed_ts = datetime.fromisoformat(ts)

    assert len(coordinator_type) <= 30 and coordinator_type.isascii()
    assert MIN_INT64 <= coordinator_id <= MAX_INT64
    assert MIN_INT64 <= coordinator_request_id <= MAX_INT64
    assert 0 <= min_locked_amount <= max_locked_amount <= MAX_INT64
    assert MIN_INT64 <= debtor_id <= MAX_INT64
    assert MIN_INT64 <= creditor_id <= MAX_INT64
    assert parsed_ts > T0
    assert 0 <= max_commit_delay <= MAX_INT32

    try:
        recipient_creditor_id = u64_to_i64(int(recipient))
    except ValueError:
        is_reachable = False
    else:
        is_reachable = get_if_account_is_reachable(debtor_id, recipient_creditor_id)

    procedures.prepare_transfer(
        coordinator_type,
        coordinator_id,
        coordinator_request_id,
        min_locked_amount,
        max_locked_amount,
        debtor_id,
        creditor_id,
        recipient_creditor_id if is_reachable else None,
        parsed_ts,
        max_commit_delay,
        min_interest_rate,
    )


@protocol_broker.actor(queue_name=APP_QUEUE_NAME)
def finalize_transfer(
        debtor_id: int,
        creditor_id: int,
        transfer_id: int,
        coordinator_type: str,
        coordinator_id: int,
        coordinator_request_id: int,
        committed_amount: int,
        transfer_note_format: str,
        transfer_note: str,
        ts: str) -> None:

    """Finalize a prepared transfer."""

    parsed_ts = datetime.fromisoformat(ts)

    assert MIN_INT64 <= debtor_id <= MAX_INT64
    assert MIN_INT64 <= creditor_id <= MAX_INT64
    assert MIN_INT64 <= transfer_id <= MAX_INT64
    assert len(coordinator_type) <= 30 and coordinator_type.isascii()
    assert MIN_INT64 <= coordinator_id <= MAX_INT64
    assert MIN_INT64 <= coordinator_request_id <= MAX_INT64
    assert 0 <= committed_amount <= MAX_INT64
    assert RE_TRANSFER_NOTE_FORMAT.match(transfer_note_format)
    assert len(transfer_note) <= TRANSFER_NOTE_MAX_BYTES
    assert len(transfer_note.encode('utf8')) <= TRANSFER_NOTE_MAX_BYTES
    assert parsed_ts > T0

    procedures.finalize_transfer(
        debtor_id=debtor_id,
        creditor_id=creditor_id,
        transfer_id=transfer_id,
        coordinator_type=coordinator_type,
        coordinator_id=coordinator_id,
        coordinator_request_id=coordinator_request_id,
        committed_amount=committed_amount,
        transfer_note_format=transfer_note_format,
        transfer_note=transfer_note,
        ts=parsed_ts,
    )


@protocol_broker.actor(queue_name=APP_QUEUE_NAME, event_subscription=True)
def on_pending_balance_change_signal(
        debtor_id: int,
        creditor_id: int,
        change_id: int,
        coordinator_type: str,
        transfer_note_format: str,
        transfer_note: str,
        committed_at: str,
        principal_delta: int,
        other_creditor_id: int) -> None:

    """Queue a pendding balance change."""

    parsed_committed_at = datetime.fromisoformat(committed_at)

    assert MIN_INT64 <= debtor_id <= MAX_INT64
    assert MIN_INT64 <= creditor_id <= MAX_INT64
    assert MIN_INT64 <= change_id <= MAX_INT64
    assert len(coordinator_type) <= 30 and coordinator_type.isascii()
    assert RE_TRANSFER_NOTE_FORMAT.match(transfer_note_format)
    assert len(transfer_note) <= TRANSFER_NOTE_MAX_BYTES
    assert len(transfer_note.encode('utf8')) <= TRANSFER_NOTE_MAX_BYTES
    assert parsed_committed_at > T0
    assert -MAX_INT64 <= principal_delta <= MAX_INT64
    assert MIN_INT64 <= other_creditor_id <= MAX_INT64

    procedures.insert_pending_balance_change(
        debtor_id=debtor_id,
        creditor_id=creditor_id,
        change_id=change_id,
        coordinator_type=coordinator_type,
        transfer_note_format=transfer_note_format,
        transfer_note=transfer_note,
        committed_at=parsed_committed_at,
        principal_delta=principal_delta,
        other_creditor_id=other_creditor_id,
    )
