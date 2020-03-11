import iso8601
from .extensions import broker, APP_QUEUE_NAME
from . import procedures


@broker.actor(queue_name=APP_QUEUE_NAME)
def configure_account(
        debtor_id: int,
        creditor_id: int,
        signal_ts: str,
        signal_seqnum: int,
        is_scheduled_for_deletion: bool = False,
        negligible_amount: float = 0.0) -> None:

    """Make sure the account `(debtor_id, creditor_id)` exists, then
    update its configuration settings.

    * `signal_ts` is the current timestamp. For a given account, later
      calls to `configure_account` MUST have later or equal
      timestamps, compared to earlier calls.

    * `signal_seqnum` is the sequential number of the call (a signed
      32-bit integer). For a given account, later calls to
      `configure_account` SHOULD have bigger sequential numbers,
      compared to earlier calls (except for the possible signed 32-bit
      integer wrapping, in case of an overflow).

    * `is_scheduled_for_deletion` determines whether the account is
      scheduled for deletion.

    * `negligible_amount` is the maximum amount that should be
       considered negligible. It is used to: 1) decide whether an
       account can be safely deleted; 2) decide whether an incoming
       transfer is insignificant. MUST be non-negative.

    An `AccountChangeSignal` is always sent as a confirmation.

    NOTE: In order to decide whether to update the configuration when
    a (potentially old) `configure_account` signal is received, the
    implementation compares the `signal_ts` of the current call, to
    the `signal_ts` of the latest call. Only if they are equal, the
    `signal_seqnum`s are compared as well (correctly dealing with
    possible integer wrapping).

    """

    procedures.configure_account(
        debtor_id,
        creditor_id,
        iso8601.parse_date(signal_ts),
        signal_seqnum,
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
        signal_ts: str,
        minimum_account_balance: int = 0) -> None:

    """Try to greedily secure an amount between `min_amount` (> 0) and
    `max_amount` (>= min_amount), to transfer it from sender's account
    (`debtor_id`, `sender_creditor_id`) to recipient's account
    (`debtor_id`, `recipient_creditor_id`).

    `signal_ts` MUST be the current timestamp.

    `minimum_account_balance` determines the amount that must remain
    available on sender's account after the requested amount has been
    secured. For normal accounts it should be a non-negative
    number. For the debtor's account it can be any number.

    Before sending a message to this actor, the sender MUST create a
    Coordinator Request (CR) database record, with a primary key of
    `(coordinator_type, coordinator_id, coordinator_request_id)`, and
    status "initiated". This record will be used to act properly on
    `PreparedTransferSignal` and `RejectedTransferSignal` events.


    PreparedTransferSignal
    ----------------------

    If a `PreparedTransferSignal` is received for an "initiated" CR
    record, the status of the corresponding CR record MUST be set to
    "prepared", and the received values for `debtor_id`,
    `sender_creditor_id`, and `transfer_id` -- recorded. The
    "prepared" CR record MUST be, at some point, finalized (committed
    or dismissed), and the status set to "finalized".

    If a `PreparedTransferSignal` is received for a "prepared" CR
    record, the corresponding values of `debtor_id`,
    `sender_creditor_id`, and `transfer_id` MUST be compared. If they
    are the same, no action MUST be taken. If they differ, the newly
    prepared transfer MUST be immediately dismissed (by sending a
    message to the `finalize_prepared_transfer` actor with a zero
    `committed_amount`).

    If a `PreparedTransferSignal` is received for a "finalized" CR
    record, the corresponding values of `debtor_id`,
    `sender_creditor_id`, and `transfer_id` MUST be compared. If they
    are the same, the original message to the
    `finalize_prepared_transfer` actor MUST be sent again. If they
    differ, the newly prepared transfer MUST be immediately dismissed.

    If a `PreparedTransferSignal` is received but a corresponding CR
    record is not found, the newly prepared transfer MUST be
    immediately dismissed.


    RejectedTransferSignal
    ----------------------

    If a `RejectedTransferSignal` is received for an "initiated" CR
    record, the CR record SHOULD be deleted.

    If a `RejectedTransferSignal` is received in any other case, no
    action MUST be taken.


    IMPORTANT NOTES:

    1. "initiated" CR records MAY be deleted whenever considered
       appropriate.

    2. "prepared" CR records MUST NOT be deleted. Instead, they MUST
       be "finalized" first (by sending a message to the
       `finalize_prepared_transfer` actor).

    3. "finalized" CR records, which have been committed (i.e. not
       dismissed), SHOULD NOT be deleted right away. Instead, they
       SHOULD stay in the database until a corresponding
       `FinalizedTransferSignal` is received for them. (It MUST be
       verified that the signal has the same `debtor_id`,
       `sender_creditor_id`, and `transfer_id` as the CR record.)

       Only when the corresponding `FinalizedTransferSignal` has not
       been received for a very long time (1 year for example), the
       "finalized" CR record MAY be deleted with a warning.

       NOTE: The retention of committed CR records is necessary to
       prevent problems caused by message re-delivery. Consider the
       following scenario: a transfer has been prepared and committed
       (finalized), but the `PreparedTransferSignal` message is
       re-delivered a second time. Had the CR record been deleted
       right away, the already committed transfer would be dismissed
       the second time, and the fate of the transfer would be decided
       by the race between the two different finalizing messages. In
       most cases, this would be a serious problem.

    4. "finalized" CR records, which have been dismissed (i.e. not
       committed), MAY be deleted either right away, or when a
       corresponding `FinalizedTransferSignal` is received for them.

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
        iso8601.parse_date(signal_ts),
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

    `committed_amount` MUST be non-negative. To dismiss the transfer,
    `committed_amount` should be `0`.

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
        interest_rate: float,
        request_ts: str) -> None:

    """Change the interest rate on the account `(debtor_id, creditor_id)`."""

    procedures.change_interest_rate(
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

    """Clear the interest accumulated on the account `(debtor_id,
    creditor_id)`, adding it to the principal. Does nothing if the
    absolute value of the accumulated interest is smaller than
    `abs(accumulated_interest_threshold)`.

    """

    procedures.capitalize_interest(
        debtor_id,
        creditor_id,
        accumulated_interest_threshold,
        iso8601.parse_date(request_ts),
    )


@broker.actor(queue_name=APP_QUEUE_NAME)
def zero_out_negative_balance(
        debtor_id: int,
        creditor_id: int,
        last_outgoing_transfer_date: str,
        request_ts: str) -> None:

    """Zero out the balance on the account `(debtor_id, creditor_id)` if
    the current balance is negative, and account's last outgoing
    transfer date is less or equal to `last_outgoing_transfer_date`.

    """

    procedures.zero_out_negative_balance(
        debtor_id,
        creditor_id,
        iso8601.parse_date(last_outgoing_transfer_date).date(),
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
    and the current balance is non-negative and no bigger than
    `max(2.0, account.negligible_amount)`.

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
        iso8601.parse_date(request_ts),
    )
