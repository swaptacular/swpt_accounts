from datetime import datetime
import iso8601
from .extensions import broker, APP_QUEUE_NAME
from .procedures import AB_IGNORE, AB_PRINCIPAL_ONLY, AB_PRINCIPAL_WITH_INTEREST  # noqa
from . import procedures


@broker.actor(queue_name=APP_QUEUE_NAME)
def prepare_transfer(
        *,
        coordinator_type: str,
        coordinator_id: int,
        coordinator_request_id: int,
        min_amount: int,
        max_amount: int,
        debtor_id: int,
        sender_creditor_id: int,
        recipient_creditor_id: int,
        avl_balance_check_mode: int,
        lock_amount: bool = True,
        recipient_account_must_exist: bool = True) -> None:

    """Try to greedily secure an amount between `min_amount` and
   `max_amount`, to transfer it from sender's account (`debtor_id`,
   `sender_creditor_id`) to recipient's account (`debtor_id`,
   `recipient_creditor_id`).

    The value of `avl_balance_check_mode` should be one of these:
    `AB_IGNORE`, `AB_PRINCIPAL_ONLY`, `AB_PRINCIPAL_WITH_INTEREST`.

    Before sending a message to this actor, the sender must create a
    Coordinator Request (CR) database record, with a primary key of
    `(coordinator_type, coordinator_id, coordinator_request_id)`, and
    status "initiated". This record will be used to act properly on
    `PreparedTransferSignal` and `RejectedTransferSignal` events.

    On received `PreparedTransferSignal`, the status of the
    corresponding CR record must be set to "prepared", and the
    received values for `debtor_id`, `sender_creditor_id`, and
    `transfer_id` -- recorded. The "prepared" CR record must be, at
    some point, finalized (using the `finalize_prepared_transfer`
    actor), and the status set to "finalized". The "finalized" CR
    record must not be deleted right away, to avoid problems when the
    event handler ends up being executed more than once.

    If a `PreparedTransferSignal` is received, but a corresponding CR
    record is not found, the newly prepared transfer must be
    immediately dismissed (by sending a message to the
    `finalize_prepared_transfer` actor with a zero `committed_amount`).

    If a `PreparedTransferSignal` is received for an already
    "prepared" or "finalized" CR record, the corresponding values of
    `debtor_id`, `sender_creditor_id`, and `transfer_id` must be
    compared. If they are the same, no action should be taken. If they
    differ, the newly prepared transfer must be immediately dismissed.

    If a `RejectedTransferSignal` is received, and the status of the
    corresponding CR record is "initiated", the CR record must be
    deleted. Otherwise, no action should be taken.

    """

    procedures.prepare_transfer(
        coordinator_type,
        coordinator_id,
        coordinator_request_id,
        min_amount,
        max_amount,
        debtor_id,
        sender_creditor_id,
        recipient_creditor_id,
        avl_balance_check_mode,
        lock_amount,
    )


@broker.actor(queue_name=APP_QUEUE_NAME)
def finalize_prepared_transfer(
        *,
        debtor_id: int,
        sender_creditor_id: int,
        transfer_id: int,
        committed_amount: int,
        transfer_info: dict = {}) -> None:

    """Execute a prepared transfer.

    To dismiss the transfer, `committed_amount` should be `0`.

    """

    procedures.finalize_prepared_transfer(
        debtor_id,
        sender_creditor_id,
        transfer_id,
        committed_amount,
        transfer_info,
    )


@broker.actor(queue_name=APP_QUEUE_NAME)
def set_interest_rate(
        *,
        debtor_id: int,
        creditor_id: int,
        interest_rate: float,
        change_seqnum: int,
        change_ts: str) -> None:

    """Change the interest rate on given account."""

    procedures.set_interest_rate(
        debtor_id,
        creditor_id,
        interest_rate,
        change_seqnum,
        iso8601.parse_date(change_ts),
    )


@broker.actor(queue_name=APP_QUEUE_NAME)
def capitalize_interest(
        *,
        debtor_id: int,
        creditor_id: int,
        issuer_creditor_id: int,
        accumulated_interest_threshold: int = 0) -> None:

    """Clear the interest accumulated on the account `(debtor_id,
    creditor_id)`, adding it to the principal. Does nothing if the
    absolute value of the accumulated interest is smaller than
    `abs(accumulated_interest_threshold)`.

    """

    procedures.capitalize_interest(
        debtor_id,
        creditor_id,
        issuer_creditor_id,
        accumulated_interest_threshold,
    )


@broker.actor(queue_name=APP_QUEUE_NAME)
def create_account_if_possible(
        *,
        debtor_id: int,
        creditor_id: int) -> None:

    """Make sure the account `(debtor_id, creditor_id)` exists.

    The name of this actor includes "if possible", because if a
    `delete_account_if_possible` asynchronous message is also sent for
    the given account, it is uncertain which one will be executed
    first. To avoid confusion, it should not be presumed that the
    account will always be created successfully. If must be sure, use
    a synchronous HTTP POST request instead.

    """

    procedures.get_or_create_account((debtor_id, creditor_id))


@broker.actor(queue_name=APP_QUEUE_NAME)
def delete_account_if_possible(
        *,
        debtor_id: int,
        creditor_id: int) -> None:

    """Mark the account `(debtor_id, creditor_id)` as deleted if there are
    no prepared transfers, the principal is zero, and the available
    balance is non-negative and very close to zero.

    Even if the account has been marked as deleted, it can be
    "resurrected" by an incoming transfer. To avoid confusion, in any
    circumstances this actor does not guarantee that the account will
    be marked as deleted successfully. (Also, see the comment for
    `create_account_if_possible`.)

    """

    procedures.delete_account_if_zeroed(
        debtor_id,
        creditor_id,
    )


@broker.actor(queue_name=APP_QUEUE_NAME)
def purge_deleted_account(
        *,
        debtor_id: int,
        creditor_id: int,
        if_deleted_before: datetime) -> None:

    """Removes the account `(debtor_id, creditor_id)` if it has been
    marked as deleted before the `if_deleted_before` moment.

    Some time should be allowed to pass between the marking of an
    account as "deleted", and its actual removal from the database.
    This is necessary to protect against the rare case when after the
    removal from the database, the account is quickly re-created with
    an older timestamp (due to un-synchronized system clocks). Since
    accounts that are marked as deleted behave exactly as removed,
    there is no rush to actually remove them from the database.

    """

    procedures.purge_deleted_account(
        debtor_id,
        creditor_id,
        if_deleted_before,
    )
