from .extensions import broker


@broker.actor(queue_name='swpt_accounts')
def prepare_direct_transfer(
        sender_transfer_request_id,
        debtor_id,
        sender_creditor_id,
        recipient_creditor_id,
        amount,
        transfer_info,
):
    """Try to secure the requested `amount` for a direct transfer."""


@broker.actor(queue_name='swpt_accounts')
def prepare_circular_transfer(
        coordinator_id,
        coordinator_transfer_request_id,
        debtor_id,
        sender_creditor_id,
        recipient_creditor_id,
        amount,
        transfer_info,
):
    """Secure the requested `amount` or less for a circular transfer.

    The secured (prepared) amount can be smaller than the requested
    amount if the available balance is insufficient. If the available
    balance is zero, the prepared amount will also be zero.

    """


@broker.actor(queue_name='swpt_accounts')
def close_prepared_transfer(
        debtor_id,
        prepared_transfer_seqnum,
        committed_amount,
):
    """Closes a prepared transfer.

    To dismiss the transfer, `committed_amount` should be `0`.

    """
