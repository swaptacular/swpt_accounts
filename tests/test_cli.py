import pytest
from unittest.mock import Mock
from datetime import datetime, timezone
from sqlalchemy.sql.expression import true
from swpt_accounts.extensions import db
from swpt_accounts import procedures as p
from swpt_accounts.models import RejectedTransferSignal, TransferRequest, FinalizationRequest, \
    FinalizedTransferSignal, PreparedTransfer, PendingBalanceChangeSignal, RegisteredBalanceChange, \
    Account, AccountUpdateSignal, AccountTransferSignal, PendingBalanceChange, \
    PreparedTransferSignal
from swpt_pythonlib.utils import ShardingRealm


def _flush_balance_change_signals():
    signals = PendingBalanceChangeSignal.query.all()
    for s in signals:
        p.insert_pending_balance_change(
            debtor_id=s.debtor_id,
            creditor_id=s.creditor_id,
            change_id=s.change_id,
            coordinator_type=s.coordinator_type,
            transfer_note_format=s.transfer_note_format,
            transfer_note=s.transfer_note,
            committed_at=s.committed_at,
            principal_delta=s.principal_delta,
            other_creditor_id=s.other_creditor_id,
        )


D_ID = -1
C_ID = 1


@pytest.mark.unsafe
def test_process_transfers_pending_balance_changes(app_unsafe_session):
    Account.query.delete()
    AccountUpdateSignal.query.delete()
    PreparedTransfer.query.delete()
    PreparedTransferSignal.query.delete()
    AccountTransferSignal.query.delete()
    PendingBalanceChange.query.delete()
    PendingBalanceChangeSignal.query.delete()
    RegisteredBalanceChange.query.delete()
    db.session.commit()

    app = app_unsafe_session
    p.make_debtor_payment('test', D_ID, C_ID, 1000)
    assert p.get_available_amount(D_ID, p.ROOT_CREDITOR_ID) is None
    _flush_balance_change_signals()
    _flush_balance_change_signals()
    _flush_balance_change_signals()
    runner = app.test_cli_runner()
    result = runner.invoke(args=['swpt_accounts', 'process_balance_changes', '--quit-early', '--wait=0'])
    assert result.exit_code == 0
    assert not result.output
    assert p.get_available_amount(D_ID, p.ROOT_CREDITOR_ID) == -1000
    assert RegisteredBalanceChange.query.filter(RegisteredBalanceChange.is_applied == true()).all()

    Account.query.delete()
    AccountUpdateSignal.query.delete()
    PreparedTransfer.query.delete()
    PreparedTransferSignal.query.delete()
    AccountTransferSignal.query.delete()
    PendingBalanceChange.query.delete()
    PendingBalanceChangeSignal.query.delete()
    RegisteredBalanceChange.query.delete()
    db.session.commit()


@pytest.mark.unsafe
def test_process_transfers_transfer_requests(app_unsafe_session):
    Account.query.delete()
    AccountUpdateSignal.query.delete()
    PreparedTransfer.query.delete()
    PreparedTransferSignal.query.delete()
    AccountTransferSignal.query.delete()
    TransferRequest.query.delete()
    RejectedTransferSignal.query.delete()
    PendingBalanceChange.query.delete()
    RegisteredBalanceChange.query.delete()
    PendingBalanceChangeSignal.query.delete()
    db.session.commit()

    app = app_unsafe_session
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
        recipient_creditor_id=1234,
        ts=current_ts,
    )
    assert len(TransferRequest.query.all()) == 1
    runner = app.test_cli_runner()
    result = runner.invoke(args=['swpt_accounts', 'process_transfer_requests', '--quit-early', '--wait=0'])
    assert result.exit_code == 0
    assert not result.output
    assert len(RejectedTransferSignal.query.all()) == 1
    assert len(TransferRequest.query.all()) == 0

    Account.query.delete()
    AccountUpdateSignal.query.delete()
    PreparedTransfer.query.delete()
    PreparedTransferSignal.query.delete()
    AccountTransferSignal.query.delete()
    TransferRequest.query.delete()
    RejectedTransferSignal.query.delete()
    PendingBalanceChange.query.delete()
    RegisteredBalanceChange.query.delete()
    PendingBalanceChangeSignal.query.delete()
    db.session.commit()


@pytest.mark.unsafe
def test_process_transfers_finalization_requests(app_unsafe_session):
    Account.query.delete()
    AccountUpdateSignal.query.delete()
    PreparedTransfer.query.delete()
    PreparedTransferSignal.query.delete()
    AccountTransferSignal.query.delete()
    PendingBalanceChange.query.delete()
    RegisteredBalanceChange.query.delete()
    PendingBalanceChangeSignal.query.delete()
    db.session.commit()

    app = app_unsafe_session
    p.make_debtor_payment('test', D_ID, C_ID, 1000)
    p.process_pending_balance_changes(D_ID, C_ID)
    p.prepare_transfer(
        coordinator_type='test',
        coordinator_id=1,
        coordinator_request_id=2,
        min_locked_amount=1,
        max_locked_amount=200,
        debtor_id=D_ID,
        creditor_id=C_ID,
        recipient_creditor_id=0,
        ts=datetime.now(tz=timezone.utc),
    )
    p.process_transfer_requests(D_ID, C_ID)
    pt = PreparedTransfer.query.one()
    p.finalize_transfer(D_ID, C_ID, pt.transfer_id, 'test', 1, 2, 1)
    assert len(FinalizationRequest.query.all()) == 1
    runner = app.test_cli_runner()
    result = runner.invoke(args=['swpt_accounts', 'process_finalization_requests', '--quit-early', '--wait=0'])
    assert result.exit_code == 0
    assert not result.output
    assert len(FinalizedTransferSignal.query.all()) == 1
    assert len(FinalizationRequest.query.all()) == 0

    Account.query.delete()
    AccountUpdateSignal.query.delete()
    PreparedTransfer.query.delete()
    PreparedTransferSignal.query.delete()
    AccountTransferSignal.query.delete()
    PendingBalanceChange.query.delete()
    RegisteredBalanceChange.query.delete()
    PendingBalanceChangeSignal.query.delete()
    db.session.commit()


@pytest.mark.unsafe
def test_ignore_transfers_finalization_requests(app_unsafe_session):
    Account.query.delete()
    AccountUpdateSignal.query.delete()
    PreparedTransfer.query.delete()
    PreparedTransferSignal.query.delete()
    AccountTransferSignal.query.delete()
    PendingBalanceChange.query.delete()
    RegisteredBalanceChange.query.delete()
    PendingBalanceChangeSignal.query.delete()
    FinalizedTransferSignal.query.delete()
    FinalizationRequest.query.delete()
    db.session.commit()

    app = app_unsafe_session
    orig_sharding_realm = app.config['SHARDING_REALM']
    app.config['SHARDING_REALM'] = ShardingRealm('0.#')
    app.config['DELETE_PARENT_SHARD_RECORDS'] = True
    p.make_debtor_payment('test', D_ID, C_ID, 1000)
    p.process_pending_balance_changes(D_ID, C_ID)
    p.prepare_transfer(
        coordinator_type='test',
        coordinator_id=1,
        coordinator_request_id=2,
        min_locked_amount=1,
        max_locked_amount=200,
        debtor_id=D_ID,
        creditor_id=C_ID,
        recipient_creditor_id=0,
        ts=datetime.now(tz=timezone.utc),
    )
    p.process_transfer_requests(D_ID, C_ID)
    pt = PreparedTransfer.query.one()
    p.finalize_transfer(D_ID, C_ID, pt.transfer_id, 'test', 1, 2, 1)
    assert len(FinalizationRequest.query.all()) == 1
    runner = app.test_cli_runner()
    result = runner.invoke(args=['swpt_accounts', 'process_finalization_requests', '--quit-early', '--wait=0'])
    assert result.exit_code == 0
    assert not result.output
    assert len(FinalizedTransferSignal.query.all()) == 0
    assert len(FinalizationRequest.query.all()) == 0
    app.config['DELETE_PARENT_SHARD_RECORDS'] = False
    app.config['SHARDING_REALM'] = orig_sharding_realm

    Account.query.delete()
    AccountUpdateSignal.query.delete()
    PreparedTransfer.query.delete()
    PreparedTransferSignal.query.delete()
    AccountTransferSignal.query.delete()
    PendingBalanceChange.query.delete()
    RegisteredBalanceChange.query.delete()
    PendingBalanceChangeSignal.query.delete()
    FinalizedTransferSignal.query.delete()
    FinalizationRequest.query.delete()
    db.session.commit()


@pytest.mark.unsafe
def test_flush_messages(mocker, app_unsafe_session):
    send_signalbus_message = Mock()
    mocker.patch('swpt_accounts.models.RejectedTransferSignal.send_signalbus_message',
                 new_callable=send_signalbus_message)
    RejectedTransferSignal.query.delete()
    db.session.commit()
    rts = RejectedTransferSignal(
        debtor_id=D_ID,
        sender_creditor_id=C_ID,
        coordinator_type='direct',
        coordinator_id=C_ID,
        coordinator_request_id=777,
        status_code='FAILURE',
        total_locked_amount=0,
    )
    db.session.add(rts)
    db.session.commit()
    assert len(RejectedTransferSignal.query.all()) == 1
    db.session.commit()
    app = app_unsafe_session

    runner = app.test_cli_runner()
    result = runner.invoke(args=['swpt_accounts', 'flush_messages',
                                 'RejectedTransferSignal', '--wait', '0.1', '--quit-early'])
    assert result.exit_code == 1
    assert send_signalbus_message.called_once()
    assert len(RejectedTransferSignal.query.all()) == 0


def test_consume_messages(app):
    runner = app.test_cli_runner()
    result = runner.invoke(args=['swpt_accounts', 'consume_messages', '--url=INVALID'])
    assert result.exit_code == 1


def test_consume_chore_messages(app):
    runner = app.test_cli_runner()
    result = runner.invoke(args=['swpt_accounts', 'consume_chore_messages', '--url=INVALID'])
    assert result.exit_code == 1
