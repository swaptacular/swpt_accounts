from .extensions import broker


@broker.actor(queue_name='swpt_accounts')
def prepare_direct_transfer(
        sender_creditor_id,
        sender_transfer_request_id,
        debtor_id,
        recipient_creditor_id,
        amount,
        transfer_info,
):
    pass


@broker.actor(queue_name='swpt_accounts')
def commit_prepared_transfer(
        debtor_id,
        prepared_transfer_seqnum,
):
    pass


@broker.actor(queue_name='swpt_accounts')
def rollback_prepared_transfer(
        debtor_id,
        prepared_transfer_seqnum,
):
    pass
