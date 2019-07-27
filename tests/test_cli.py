from swpt_accounts import procedures as p
from swpt_accounts.models import RejectedTransferSignal


D_ID = -1
C_ID = 1


def test_process_pending_changes(app, db_session):
    p.make_debtor_payment('test', D_ID, C_ID, 1000)
    assert p.get_available_balance(D_ID, C_ID, ignore_interest=True) == 0
    assert p.get_available_balance(D_ID, p.ROOT_CREDITOR_ID, ignore_interest=True) == 0
    runner = app.test_cli_runner()
    result = runner.invoke(args=['swpt_accounts', 'process-pending-changes'])
    assert not result.output
    assert p.get_available_balance(D_ID, C_ID, ignore_interest=True) == 1000
    assert p.get_available_balance(D_ID, p.ROOT_CREDITOR_ID, ignore_interest=True) == -1000


def test_process_transfer_requests(app, db_session):
    p.prepare_transfer(
        coordinator_type='test',
        coordinator_id=1,
        coordinator_request_id=2,
        min_amount=1,
        max_amount=200,
        debtor_id=D_ID,
        sender_creditor_id=C_ID,
        recipient_creditor_id=1234,
        ignore_interest=False,
    )
    runner = app.test_cli_runner()
    result = runner.invoke(args=['swpt_accounts', 'process-transfer-requests'])
    assert not result.output
    assert RejectedTransferSignal.query.one()
