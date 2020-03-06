from datetime import date, datetime, timezone, timedelta
from swpt_accounts.extensions import db


D_ID = -1
C_ID = 1


def test_scan_accounts(app_unsafe_session):
    from swpt_accounts.models import Account, AccountChangeSignal, AccountPurgeSignal

    # db.signalbus.autoflush = False
    current_ts = datetime.now(tz=timezone.utc)
    past_ts = datetime(1900, 1, 1, tzinfo=timezone.utc)
    app = app_unsafe_session
    Account.query.delete()
    AccountChangeSignal.query.delete()
    AccountPurgeSignal.query.delete()
    db.session.commit()
    account = Account(
        debtor_id=D_ID,
        creditor_id=12,
        creation_date=date(2020, 1, 1),
        principal=1000,
        locked_amount=500,
        pending_transfers_count=1,
        last_transfer_id=3,
        status=0,
        last_change_ts=past_ts,
    )
    db.session.add(account)
    db.session.add(Account(
        debtor_id=D_ID,
        creditor_id=123,
        creation_date=date(2020, 1, 1),
        principal=1000,
        locked_amount=500,
        pending_transfers_count=1,
        last_transfer_id=3,
        status=Account.STATUS_DELETED_FLAG,
        last_change_ts=past_ts,
    ))
    db.session.add(Account(
        debtor_id=D_ID,
        creditor_id=1234,
        creation_date=date(2020, 1, 1),
        principal=1000,
        locked_amount=500,
        pending_transfers_count=1,
        last_transfer_id=2,
        status=0,
    ))
    db.session.add(Account(
        debtor_id=D_ID,
        creditor_id=12345,
        creation_date=date(2020, 1, 1),
        principal=1000,
        locked_amount=500,
        pending_transfers_count=1,
        last_transfer_id=1,
        status=0,
        last_change_ts=past_ts,
        last_reminder_ts=current_ts - timedelta(seconds=10),
    ))
    db.session.commit()
    db.engine.execute('ANALYZE account')
    assert len(Account.query.all()) == 4
    runner = app.test_cli_runner()
    result = runner.invoke(args=['swpt_accounts', 'scan_accounts', '--days', '0.000001', '--quit-early'])
    assert result.exit_code == 0
    assert len(Account.query.all()) == 3
    assert len(AccountChangeSignal.query.all()) == 1
    acs = AccountChangeSignal.query.one()
    assert acs.debtor_id == account.debtor_id
    assert acs.creditor_id == account.creditor_id
    assert acs.change_ts == account.last_change_ts == past_ts
    assert acs.change_seqnum == account.last_change_seqnum == 0
    assert acs.principal == account.principal
    assert acs.interest == account.interest
    assert acs.interest_rate == account.interest_rate
    assert acs.last_transfer_seqnum == account.last_transfer_seqnum
    assert acs.last_outgoing_transfer_date == account.last_outgoing_transfer_date
    assert acs.last_config_signal_ts == account.last_config_signal_ts
    assert acs.last_config_signal_seqnum == account.last_config_signal_seqnum
    assert acs.creation_date == account.creation_date
    assert acs.negligible_amount == account.negligible_amount
    assert acs.status == account.status

    assert len(Account.query.filter_by(creditor_id=123).all()) == 0
    aps = AccountPurgeSignal.query.filter_by(debtor_id=D_ID, creditor_id=123).one()
    assert aps.creation_date == date(2020, 1, 1)

    accounts = Account.query.order_by(Account.creditor_id).all()
    assert accounts[0].last_reminder_ts >= current_ts
    assert accounts[1].last_reminder_ts < current_ts
    assert accounts[2].last_reminder_ts < current_ts

    db.engine.execute('ANALYZE account')
    result = runner.invoke(args=['swpt_accounts', 'scan_prepared_transfers', '--days', '0.000001', '--quit-early'])
    assert result.exit_code == 0
    assert len(Account.query.all()) == 3
    assert len(AccountChangeSignal.query.all()) == 1

    Account.query.delete()
    AccountChangeSignal.query.delete()
    AccountPurgeSignal.query.delete()
    db.session.commit()


def test_scan_prepared_transfers(app_unsafe_session):
    from swpt_accounts.models import Account, PreparedTransfer, PreparedTransferSignal

    # db.signalbus.autoflush = False
    current_ts = datetime.now(tz=timezone.utc)
    past_ts = datetime(1900, 1, 1, tzinfo=timezone.utc)
    app = app_unsafe_session
    Account.query.delete()
    PreparedTransfer.query.delete()
    PreparedTransferSignal.query.delete()
    db.session.commit()
    db.session.add(Account(
        debtor_id=D_ID,
        creditor_id=C_ID,
        creation_date=date(2020, 1, 1),
        principal=1000,
        locked_amount=500,
        pending_transfers_count=1,
        last_transfer_id=2,
        status=0,
    ))
    db.session.flush()
    db.session.add(PreparedTransfer(
        debtor_id=D_ID,
        sender_creditor_id=C_ID,
        transfer_id=1,
        coordinator_type='direct',
        coordinator_id=11,
        coordinator_request_id=111,
        sender_locked_amount=400,
        recipient_creditor_id=1234,
        prepared_at_ts=current_ts,
    ))
    db.session.add(PreparedTransfer(
        debtor_id=D_ID,
        sender_creditor_id=C_ID,
        transfer_id=2,
        coordinator_type='direct',
        coordinator_id=11,
        coordinator_request_id=112,
        sender_locked_amount=100,
        recipient_creditor_id=1234,
        prepared_at_ts=past_ts,
    ))
    db.session.commit()
    db.engine.execute('ANALYZE account')
    assert len(Account.query.all()) == 1
    assert len(PreparedTransfer.query.all()) == 2
    assert len(PreparedTransferSignal.query.all()) == 0
    runner = app.test_cli_runner()
    result = runner.invoke(args=['swpt_accounts', 'scan_prepared_transfers', '--days', '0.000001', '--quit-early'])
    assert result.exit_code == 0
    assert len(Account.query.all()) == 1
    assert len(PreparedTransfer.query.all()) == 2
    pt1 = PreparedTransfer.query.filter_by(transfer_id=1).one()
    assert pt1.last_reminder_ts is None
    pt2 = PreparedTransfer.query.filter_by(transfer_id=2).one()
    assert pt2.last_reminder_ts is not None
    assert len(PreparedTransferSignal.query.all()) == 1

    pts = PreparedTransferSignal.query.all()[0]
    assert pts.debtor_id == D_ID
    assert pts.sender_creditor_id == C_ID
    assert pts.transfer_id == 2
    assert pts.coordinator_type == 'direct'
    assert pts.coordinator_id == 11
    assert pts.coordinator_request_id == 112
    assert pts.sender_locked_amount == 100
    assert pts.recipient_creditor_id == 1234
    assert pts.prepared_at_ts == past_ts

    db.engine.execute('ANALYZE account')
    result = runner.invoke(args=['swpt_accounts', 'scan_prepared_transfers', '--days', '0.000001', '--quit-early'])
    assert result.exit_code == 0
    assert len(Account.query.all()) == 1
    assert len(PreparedTransfer.query.all()) == 2
    assert len(PreparedTransferSignal.query.all()) == 1

    Account.query.delete()
    PreparedTransfer.query.delete()
    PreparedTransferSignal.query.delete()
    db.session.commit()
