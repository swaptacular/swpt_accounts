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
        max_commit_delay: int,
        min_account_balance: int = 0) -> None:

    """Try to secure some amount, to eventually transfer it to another account."""

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
        max_commit_delay,
        min_account_balance,
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
        transfer_note: str,
        ts: str) -> None:

    """Finalize a prepared transfer."""

    procedures.finalize_transfer(
        debtor_id,
        creditor_id,
        transfer_id,
        coordinator_type,
        coordinator_id,
        coordinator_request_id,
        committed_amount,
        finalization_flags,
        transfer_note,
        iso8601.parse_date(ts),
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
    and the current balance is between `-2.0` and `max(2.0,
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

    procedures.try_to_delete_account(
        debtor_id,
        creditor_id,
        iso8601.parse_date(request_ts),
    )
