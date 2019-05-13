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
