import datetime
from .extensions import broker, APP_QUEUE_NAME
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
):
    """Try to greedily secure an amount between `min_amount` and `max_amount`.

    `avl_balance_check_mode` should be one of these:

    * `procedures.AVL_BALANCE_IGNORE`
    * `procedures.AVL_BALANCE_ONLY`
    * `procedures.AVL_BALANCE_WITH_INTEREST`

    """

    procedures.prepare_transfer(
        coordinator_type,
        coordinator_id,
        coordinator_request_id,
        (debtor_id, sender_creditor_id),
        min_amount,
        max_amount,
        recipient_creditor_id,
        avl_balance_check_mode,
        lock_amount,
    )


@broker.actor(queue_name=APP_QUEUE_NAME)
def execute_prepared_transfer(
        *,
        debtor_id: int,
        sender_creditor_id: int,
        transfer_id: int,
        committed_amount: int,
        transfer_info: dict,
):
    """Execute a prepared transfer.

    To dismiss the transfer, `committed_amount` should be `0`.

    """

    procedures.execute_prepared_transfer(
        (debtor_id, sender_creditor_id, transfer_id),
        committed_amount,
        transfer_info,
    )


@broker.actor(queue_name=APP_QUEUE_NAME)
def update_account_interest_rate(
        # TODO: seqnum?
        *,
        debtor_id: int,
        creditor_id: int,
        concession_interest_rate: float = None,
):
    """Recalculates the interest on a given account."""

    procedures.update_account_interest_rate((debtor_id, creditor_id), concession_interest_rate)


@broker.actor(queue_name=APP_QUEUE_NAME)
def on_debtor_interest_rate_change_signal(
        *,
        debtor_id: int,
        interest_rate: float,
        change_seqnum: int,
        change_ts: datetime.datetime,
):
    """Update `DebtorPolicy.interest_rate`."""

    if procedures.set_debtor_policy_interest_rate(debtor_id, interest_rate, change_seqnum):
        for creditor_id in procedures.get_debtor_creditor_ids(debtor_id):
            # TODO: is this fast enough?
            update_account_interest_rate.send(debtor_id, creditor_id)
