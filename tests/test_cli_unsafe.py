from datetime import date, datetime, timezone
from swpt_accounts.extensions import db


D_ID = -1
C_ID = 1


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
    pt = PreparedTransfer.query.filter_by(transfer_id=2).one()
    assert pt.last_remainder_ts is not None
    assert len(PreparedTransferSignal.query.all()) == 1

    pts = PreparedTransferSignal.query.all()[0]
    assert pts.debtor_id == D_ID
    assert pts.sender_creditor_id == C_ID
    assert pts.transfer_id == 2

    Account.query.delete()
    PreparedTransfer.query.delete()
    PreparedTransferSignal.query.delete()
    db.session.commit()
