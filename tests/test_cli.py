from datetime import datetime, timezone
from swpt_accounts import procedures as p
from swpt_accounts.models import RejectedTransferSignal, TransferRequest, FinalizationRequest, \
    FinalizedTransferSignal, PreparedTransfer


D_ID = -1
C_ID = 1


def test_process_transfers_pending_changes(app, db_session):
    p.make_debtor_payment('test', D_ID, C_ID, 1000)
    assert p.get_available_amount(D_ID, p.ROOT_CREDITOR_ID) is None
    runner = app.test_cli_runner()
    result = runner.invoke(args=['swpt_accounts', 'process_transfers'])
    assert not result.output
    assert p.get_available_amount(D_ID, p.ROOT_CREDITOR_ID) == -1000


def test_process_transfers_transfer_requests(app, db_session, mock_account_is_reachable):
    current_ts = datetime.now(tz=timezone.utc)
    p.configure_account(D_ID, 1234, current_ts, 0)
    p.prepare_transfer(
        coordinator_type='test',
        coordinator_id=1,
        coordinator_request_id=2,
        min_locked_amount=1,
        max_locked_amount=200,
        debtor_id=D_ID,
        creditor_id=C_ID,
        recipient='1234',
        ts=current_ts,
    )
    assert len(TransferRequest.query.all()) == 1
    runner = app.test_cli_runner()
    result = runner.invoke(args=['swpt_accounts', 'process_transfers'])
    assert not result.output
    assert len(RejectedTransferSignal.query.all()) == 1
    assert len(TransferRequest.query.all()) == 0


def test_process_transfers_finalization_requests(app, db_session, mock_account_is_reachable):
    p.make_debtor_payment('test', D_ID, C_ID, 1000)
    p.process_pending_account_changes(D_ID, C_ID)
    p.prepare_transfer(
        coordinator_type='test',
        coordinator_id=1,
        coordinator_request_id=2,
        min_locked_amount=1,
        max_locked_amount=200,
        debtor_id=D_ID,
        creditor_id=C_ID,
        recipient='0',
        ts=datetime.now(tz=timezone.utc),
    )
    p.process_transfer_requests(D_ID, C_ID)
    pt = PreparedTransfer.query.one()
    p.finalize_transfer(D_ID, C_ID, pt.transfer_id, 'test', 1, 2, 1)
    assert len(FinalizationRequest.query.all()) == 1
    runner = app.test_cli_runner()
    result = runner.invoke(args=['swpt_accounts', 'process_transfers'])
    assert not result.output
    assert len(FinalizedTransferSignal.query.all()) == 1
    assert len(FinalizationRequest.query.all()) == 0
