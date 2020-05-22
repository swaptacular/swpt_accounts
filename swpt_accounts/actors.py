import iso8601
from .extensions import broker, APP_QUEUE_NAME
from . import procedures


@broker.actor(queue_name=APP_QUEUE_NAME)
def configure_account(
        debtor_id: int,
        creditor_id: int,
        ts: str,
        seqnum: int,
        negligible_amount: float = 0.0,
        config_flags: int = 0,
        config: str = '') -> None:

    """Make sure the account exists, and update its configuration settings."""

    procedures.configure_account(
        debtor_id,
        creditor_id,
        iso8601.parse_date(ts),
        seqnum,
        negligible_amount,
        config_flags,
        config,
    )


@broker.actor(queue_name=APP_QUEUE_NAME)
def prepare_transfer(
        coordinator_type: str,
        coordinator_id: int,
        coordinator_request_id: int,
        min_amount: int,
        max_amount: int,
        debtor_id: int,
        creditor_id: int,
        recipient: str,
        ts: str,
        minimum_account_balance: int = 0) -> None:

    """Try to greedily secure an amount between `min_amount` (> 0) and
    `max_amount` (>= min_amount), to transfer it from sender's account
    to recipient's account.

    * `ts` MUST be the current timestamp.

    * `minimum_account_balance` determines the amount that must remain
      available on sender's account after the requested amount has
      been secured. For normal accounts it should be a non-negative
      number. For the debtor's account it can be any number.

    * `creditor_id` (along with `debtor_id`) identify the sender's
      account. Note that `creditor_id` is an integer, while
      `recipient` is a string. The reason for this is that
      implementations will often want to use the `creditor_id` field
      as a key in a lookup table, to obtain addition information about
      sender's account (account authentication secrets for example).

    * `recipient` is a string, which (along with `debtor_id`)
      identifies the recipient's account. Different implementations
      may use different formats for the identifier of recipient's
      account.

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
    `creditor_id`, and `transfer_id` -- recorded. The "prepared" CR
    record MUST be, at some point, finalized (committed or dismissed),
    and the status set to "finalized".

    If a `PreparedTransferSignal` is received for a "prepared" CR
    record, the corresponding values of `debtor_id`, `creditor_id`,
    and `transfer_id` MUST be compared. If they are the same, no
    action MUST be taken. If they differ, the newly prepared transfer
    MUST be immediately dismissed (by sending a message to the
    `finalize_prepared_transfer` actor with a zero
    `committed_amount`).

    If a `PreparedTransferSignal` is received for a "finalized" CR
    record, the corresponding values of `debtor_id`, `creditor_id`,
    and `transfer_id` MUST be compared. If they are the same, the
    original message to the `finalize_prepared_transfer` actor MUST be
    sent again. If they differ, the newly prepared transfer MUST be
    immediately dismissed.

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
       `creditor_id`, and `transfer_id` as the CR record.)

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
        creditor_id,
        recipient,
        iso8601.parse_date(ts),
        minimum_account_balance,
    )


@broker.actor(queue_name=APP_QUEUE_NAME)
def finalize_prepared_transfer(
        debtor_id: int,
        creditor_id: int,
        transfer_id: int,
        committed_amount: int,
        transfer_message: str,
        ts: str) -> None:

    """Finalize a prepared transfer."""

    procedures.finalize_prepared_transfer(
        debtor_id,
        creditor_id,
        transfer_id,
        committed_amount,
        transfer_message,
        iso8601.parse_date(ts),
    )


@broker.actor(queue_name=APP_QUEUE_NAME)
def change_interest_rate(
        debtor_id: int,
        creditor_id: int,
        interest_rate: float,
        request_ts: str) -> None:

    """Change the interest rate on the account."""

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

    """Clear the interest accumulated on the account, adding it to the principal.

    Does nothing if the absolute value of the accumulated interest is
    smaller than `abs(accumulated_interest_threshold)`.

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

    """Zero out the balance on the account, if possible.

    The balance will be zeroed out only if the current balance is
    negative, and account's last outgoing transfer date is less or
    equal to `last_outgoing_transfer_date`.

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

    If it is the debtor's account, it will be marked as deleted if its
    principal is zero (`account.negligible_amount` is ignored in this
    case).

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
