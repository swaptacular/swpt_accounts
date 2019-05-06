from .extensions import broker
from .models import ROOT_CREDITOR_ID


@broker.actor(queue_name='swpt_accounts')
def prepare_transfer(
        *,
        coordinator_type,
        coordinator_id,
        coordinator_transfer_request_id,
        min_amount,
        max_amount,
        debtor_id,
        sender_creditor_id,
        recipient_creditor_id,
):
    """Try to greedily secure an amount between `min_amount` and `max_amount`.

    When `coordinator_type` is 'direct', and `recipient_creditor_id`
    is `ROOT_CREDITOR_ID`, this is a withdrawal. For withdrawals the
    interest accumulated on the account (positive or negative) should
    not be added to the available balance.

    """

    assert ROOT_CREDITOR_ID <= 0


@broker.actor(queue_name='swpt_accounts')
def execute_prepared_transfer(
        *,
        debtor_id,
        prepared_transfer_seqnum,
        committed_amount,
        transfer_info,
):
    """Execute a prepared transfer.

    To dismiss the transfer, `committed_amount` should be `0`.

    """


@broker.actor(queue_name='swpt_accounts')
def set_account_concession_interest_rate(
        *,
        debtor_id,
        creditor_id,
        concession_interest_rate,
):
    """Set an interest rate exclusive for the given account."""


@broker.actor(queue_name='swpt_accounts')
def on_debtor_interest_rate_change_signal(
        *,
        debtor_id,
        interest_rate,
):
    """Update `DebtorPolicy.interest_rate`."""
