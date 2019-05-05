from .extensions import broker
from .models import ROOT_CREDITOR_ID


@broker.actor(queue_name='swpt_accounts')
def prepare_direct_transfer(
        *,
        sender_transfer_request_id,
        min_amount,
        max_amount,
        debtor_id,
        sender_creditor_id,
        recipient_creditor_id,
        amount,
        transfer_info,
):
    """Try to greedily secure an amount between `min_amount` and
    `max_amount` for a direct transfer.

    When `recipient_creditor_id` equals `ROOT_CREDITOR_ID`, this is a
    withdrawal. For withdrawals the interest accumulated on the
    account (positive or negative) should not be added to the
    available balance.

    """

    assert ROOT_CREDITOR_ID < 0


@broker.actor(queue_name='swpt_accounts')
def prepare_coordinated_transfer(
        *,
        coordinator_id,
        coordinator_transfer_request_id,
        min_amount,
        max_amount,
        debtor_id,
        sender_creditor_id,
        recipient_creditor_id,
        transfer_info,
):
    """Try to greedily secure an amount between `min_amount` and
    `max_amount` for a coordinated transfer.

    """


@broker.actor(queue_name='swpt_accounts')
def close_prepared_transfer(
        *,
        debtor_id,
        prepared_transfer_seqnum,
        committed_amount,
):
    """Closes a prepared transfer.

    To dismiss the transfer, `committed_amount` should be `0`.

    """
