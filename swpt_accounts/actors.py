import iso8601
from .extensions import broker, APP_QUEUE_NAME
from . import procedures


@broker.actor(queue_name=APP_QUEUE_NAME)
def prepare_transfer(
        coordinator_type: str,
        coordinator_id: int,
        coordinator_request_id: int,
        min_amount: int,
        max_amount: int,
        debtor_id: int,
        sender_creditor_id: int,
        recipient_creditor_id: int,
        minimum_account_balance: int = 0) -> None:

    """Try to greedily secure an amount between `min_amount` (> 0) and
    `max_amount` (>= min_amount), to transfer it from sender's account
    (`debtor_id`, `sender_creditor_id`) to recipient's account
    (`debtor_id`, `recipient_creditor_id`).

    `minimum_account_balance` determines the amount that must remain
    available on sender's account after the requested amount has been
    secured. For normal accounts it should be a non-negative
    number. For the debtor's account it can be any number.

    Before sending a message to this actor, the sender must create a
    Coordinator Request (CR) database record, with a primary key of
    `(coordinator_type, coordinator_id, coordinator_request_id)`, and
    status "initiated". This record will be used to act properly on
    `PreparedTransferSignal` and `RejectedTransferSignal` events.


    PreparedTransferSignal
    ----------------------

    If a `PreparedTransferSignal` is received for an "initiated" CR
    record, the status of the corresponding CR record must be set to
    "prepared", and the received values for `debtor_id`,
    `sender_creditor_id`, and `transfer_id` -- recorded. The
    "prepared" CR record must be, at some point, finalized (using the
    `finalize_prepared_transfer` actor), and the status set to
    "finalized".

    If a `PreparedTransferSignal` is received for an already
    "prepared" or "finalized" CR record, the corresponding values of
    `debtor_id`, `sender_creditor_id`, and `transfer_id` must be
    compared. If they are the same, no action should be taken. If they
    differ, the newly prepared transfer must be immediately dismissed
    (by sending a message to the `finalize_prepared_transfer` actor
    with a zero `committed_amount`).

    If a `PreparedTransferSignal` is received but a corresponding CR
    record is not found, the newly prepared transfer must be
    immediately dismissed.


    RejectedTransferSignal
    ----------------------

    If a `RejectedTransferSignal` is received for an "initiated" CR
    record, the CR record must be deleted.

    If a `RejectedTransferSignal` is received in any other case, no
    action should be taken.


    IMPORTANT NOTES:

    1. "initiated" CR records can be deleted whenever considered
       appropriate.

    2. "prepared" CR records must not be deleted. Instead, they should
       be "finalized" first (by sending a message to the
       `finalize_prepared_transfer` actor).

    3. "finalized" CR records must not be deleted right away. Instead,
       after they have been finalized, they should stay in the
       database for at least few days.

       This is necessary in order to prevent problems caused by
       message re-delivery. Consider the following scenario: a
       transfer has been prepared and committed (finalized), but the
       `PreparedTransferSignal` message is re-delivered a second
       time. Had the CR record been deleted right away, the already
       committed transfer would be dismissed, and the fate of the
       transfer would be decided by the race between the two different
       finalizing messages.

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
        minimum_account_balance,
    )


@broker.actor(queue_name=APP_QUEUE_NAME)
def finalize_prepared_transfer(
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
def change_account_attributes(
        debtor_id: int,
        creditor_id: int,
        change_seqnum: int,
        change_ts: str,
        interest_rate: float) -> None:

    """Change attributes like the interest rate on a given account."""

    procedures.change_account_attributes(
        debtor_id,
        creditor_id,
        change_seqnum,
        iso8601.parse_date(change_ts),
        interest_rate,
    )


@broker.actor(queue_name=APP_QUEUE_NAME)
def capitalize_interest(
        debtor_id: int,
        creditor_id: int,
        accumulated_interest_threshold: int = 0) -> None:

    """Clear the interest accumulated on the account `(debtor_id,
    creditor_id)`, adding it to the principal. Does nothing if the
    absolute value of the accumulated interest is smaller than
    `abs(accumulated_interest_threshold)`.

    """

    procedures.capitalize_interest(
        debtor_id,
        creditor_id,
        accumulated_interest_threshold,
    )


@broker.actor(queue_name=APP_QUEUE_NAME)
def create_account(
        debtor_id: int,
        creditor_id: int) -> None:

    """Make sure the account `(debtor_id, creditor_id)` exists. Always
    sends a confirming `AccountChangeSignal` signal.

    """

    procedures.get_or_create_account(debtor_id, creditor_id)


@broker.actor(queue_name=APP_QUEUE_NAME)
def zero_out_negative_balance(
        debtor_id: int,
        creditor_id: int,
        last_outgoing_transfer_date: str) -> None:

    """Zero out the balance on the account `(debtor_id, creditor_id)` if
    the current balance is negative, and account's last outgoing
    transfer date is less or equal to `last_outgoing_transfer_date`.

    """

    procedures.zero_out_negative_balance(
        debtor_id,
        creditor_id,
        iso8601.parse_date(last_outgoing_transfer_date).date(),
    )


@broker.actor(queue_name=APP_QUEUE_NAME)
def delete_account_if_negligible(
        debtor_id: int,
        creditor_id: int,
        negligible_amount: int,
        ignore_after_ts: str) -> None:

    """Mark the account as deleted if there are no prepared transfers, and
    the current balance is non-negative and no bigger than
    `negligible_amount`. Otherwise, mark the account as "scheduled for
    deletion".

    Does nothing if the current timestamp is later than
    `ignore_after_ts`. This parameter is used to limit the lifespan of
    the message, which otherwise may be retained for a very long time
    by the massage bus.

    Note that even if the account has been marked as deleted, it could
    be "resurrected" by an incoming transfer. Therefore, this function
    does not guarantee neither that the account will be marked as
    deleted successfully, nor that it will "stay" deleted. To achieve
    a reliable deletion, the following procedure MAY be followed:

    1. Call `delete_account_if_negligible` with appropriate values for
       `negligible_amount` and `ignore_after_ts`.

    2. Wait for some time (depending on the circumstances).

    3. Check the current account status (as reported by the last
       received `AccountChangeSignal` for the account). If the account
       has a "deleted" status, YOU ARE DONE.

    4. If the account owner's action is not required, go to point 1.

    5. If the account owner hasn't already been prompted to take the
       required action, prompt the account owner to take the required
       action.

    6. Go to point 2.

    In rare cases, even when the above procedure has been successfully
    performed, the account will be "resurrected" with a "scheduled for
    deletion" status. This most probably means that some subsystem
    continues to actively use the account. In this case, the most
    appropriate action would be to explicitly call `create_account`,
    and "legitimate" the use of the account. (This will clear the
    "scheduled for deletion" flag.)

    """

    procedures.delete_account_if_negligible(
        debtor_id,
        creditor_id,
        negligible_amount,
        iso8601.parse_date(ignore_after_ts),
    )


@broker.actor(queue_name=APP_QUEUE_NAME)
def purge_deleted_account(
        debtor_id: int,
        creditor_id: int,
        if_deleted_before: str) -> None:

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
        iso8601.parse_date(if_deleted_before),
    )
