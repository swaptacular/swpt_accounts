import iso8601
from .extensions import broker, APP_QUEUE_NAME
from . import procedures


@broker.actor(queue_name=APP_QUEUE_NAME)
def configure_account(
        debtor_id: int,
        creditor_id: int,
        change_ts: str,
        change_seqnum: int,
        is_scheduled_for_deletion: bool = False,
        negligible_amount: float = 2.0) -> None:

    """Make sure the account `(debtor_id, creditor_id)` exists, then
    update its configuration settings.

    * `change_ts` is the current timestamp. For a given account, later
      calls to `configure_account` should have later or equal
      timestamps, compared to earlier calls.

    * `change_seqnum` is the sequential number of the call (a signed
      32-bit integer). For a given account, later calls to
      `configure_account` should have bigger sequential numbers,
      compared to earlier calls (except for the possible signed 32-bit
      integer wrapping, in case of an overflow).

    * `is_scheduled_for_deletion` determines whether the account is
      scheduled for deletion.

    * `negligible_amount` is the maximum account balance that should
       be considered negligible. It is used to decide whether an
       account can be safely deleted. Should be at least `2.0`.

    An `AccountChangeSignal` is always sent as a confirmation.

    NOTE: In order to decide whether to update the configuration when
    a (potentially old) `configure_account` signal is received, the
    implementation compares the `change_ts` of the current call, to
    the `change_ts` of the latest call. Only if they are equal, the
    `change_seqnum`s are compared as well (correctly dealing with
    possible integer wrapping).

    """

    procedures.configure_account(
        debtor_id,
        creditor_id,
        iso8601.parse_date(change_ts),
        change_seqnum,
        is_scheduled_for_deletion,
        negligible_amount,
    )


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
    "prepared" CR record must be, at some point, finalized (committed
    or dismissed), and the status set to "finalized".

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

    3. "finalized" CR records, which have been committed (i.e. not
       dismissed), MUST NOT be deleted right away. Instead, after they
       have been committed, they should stay in the database for some
       time. The delay should be long enough to allow all other
       messages that were queued to the message-bus at the time of
       commit to be successfully processed before the deletion.

       This is necessary in order to prevent problems caused by
       message re-delivery. Consider the following scenario: a
       transfer has been prepared and committed (finalized), but the
       `PreparedTransferSignal` message is re-delivered a second
       time. Had the CR record been deleted right away, the already
       committed transfer would be dismissed the second time, and the
       fate of the transfer would be decided by the race between the
       two different finalizing messages.

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

    `committed_amount` should be non-negative. To dismiss the
    transfer, `committed_amount` should be `0`.

    """

    procedures.finalize_prepared_transfer(
        debtor_id,
        sender_creditor_id,
        transfer_id,
        committed_amount,
        transfer_info,
    )


@broker.actor(queue_name=APP_QUEUE_NAME)
def change_interest_rate(
        debtor_id: int,
        creditor_id: int,
        interest_rate: float) -> None:

    """Change the interest rate on the account `(debtor_id, creditor_id)`."""

    procedures.change_interest_rate(
        debtor_id,
        creditor_id,
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
def try_to_delete_account(
        debtor_id: int,
        creditor_id: int) -> None:

    """Mark the account as deleted, if possible.

    If it is a "normal" account, it will be marked as deleted if it
    has been scheduled for deletion, there are no prepared transfers,
    and the current balance is non-negative and no bigger than
    `account.negligible_amount`.

    If it is the debtor's account, it will be marked as deleted if
    there are no prepared transfers and it is the only account left
    (`account.negligible_amount` is ignored in this case).

    Note that when a "normal" account has been successfully marked as
    deleted, it could be "resurrected" (with "scheduled for deletion"
    status) by a delayed incoming transfer. Therefore, this function
    does not guarantee that the account will be marked as deleted
    successfully, or that it will "stay" deleted for long. To achieve
    a reliable deletion, this function may need to be called
    repeatedly, until the account has been purged from the database.

    """

    procedures.try_to_delete_account(
        debtor_id,
        creditor_id,
    )


@broker.actor(queue_name=APP_QUEUE_NAME)
def purge_deleted_account(
        debtor_id: int,
        creditor_id: int,
        if_deleted_before: str) -> None:

    """Removes the account `(debtor_id, creditor_id)` if it has been
    marked as deleted before the `if_deleted_before` moment.

    An `AccountPurgeSignal` is always sent as a confirmation.

    Some time should be allowed to pass between the marking of an
    account as "deleted", and its actual removal from the database.
    This is necessary to protect against various edge cases. The delay
    should be long enough to allow all prepared transfers at the time
    of marking the account as "deleted", to be successfully finalized
    before the purge.

    Since accounts that are marked as deleted behave exactly as if
    they were removed, there is no rush to actually remove them from
    the database.

    """

    procedures.purge_deleted_account(
        debtor_id,
        creditor_id,
        iso8601.parse_date(if_deleted_before),
    )
