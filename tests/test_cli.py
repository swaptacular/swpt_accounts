from swpt_accounts import procedures as p
from swpt_accounts.models import RejectedTransferSignal, TransferRequest


D_ID = -1
C_ID = 1


def test_process_transfers_pending_changes(app, db_session):
    p.make_debtor_payment('test', D_ID, C_ID, 1000)
    assert p.get_available_balance(D_ID, C_ID) is None
    runner = app.test_cli_runner()
    result = runner.invoke(args=['swpt_accounts', 'process_transfers'])
    assert not result.output
    assert p.get_available_balance(D_ID, C_ID) == 1000


def test_process_transfers_transfer_requests(app, db_session):
    p.prepare_transfer(
        coordinator_type='test',
        coordinator_id=1,
        coordinator_request_id=2,
        min_amount=1,
        max_amount=200,
        debtor_id=D_ID,
        sender_creditor_id=C_ID,
        recipient_creditor_id=1234,
    )
    assert len(TransferRequest.query.all()) == 1
    runner = app.test_cli_runner()
    result = runner.invoke(args=['swpt_accounts', 'process_transfers'])
    assert not result.output
    assert len(RejectedTransferSignal.query.all()) == 1
    assert len(TransferRequest.query.all()) == 0
