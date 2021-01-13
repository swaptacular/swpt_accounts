import logging
import time
import dramatiq
from datetime import date, datetime, timezone, timedelta
from flask import current_app
from swpt_accounts.extensions import db, chores_broker
from swpt_accounts.schemas import RootConfigData
from swpt_accounts.fetch_api_client import get_if_account_is_reachable, get_root_config_data_dict
from swpt_accounts import procedures as p
from swpt_accounts import actors


D_ID = -1
C_ID = 1


def test_scan_accounts(app_unsafe_session):
    from swpt_accounts.models import Account, AccountUpdateSignal, AccountPurgeSignal, AccountTransferSignal, \
        PendingBalanceChangeSignal
    from swpt_accounts.fetch_api_client import _clear_root_config_data

    # db.signalbus.autoflush = False
    current_ts = datetime.now(tz=timezone.utc)
    past_ts = datetime(1970, 1, 1, tzinfo=timezone.utc)
    app = app_unsafe_session
    Account.query.delete()
    AccountUpdateSignal.query.delete()
    AccountPurgeSignal.query.delete()
    AccountTransferSignal.query.delete()
    PendingBalanceChangeSignal.query.delete()
    db.session.commit()

    p.configure_account(D_ID, p.ROOT_CREDITOR_ID, current_ts, 0, config_data='{"rate": 0.0}')
    AccountUpdateSignal.query.delete()
    account = Account(
        debtor_id=D_ID,
        creditor_id=12,
        creation_date=date(1970, 1, 1),
        principal=1000,
        total_locked_amount=500,
        pending_transfers_count=1,
        last_transfer_id=3,
        last_change_ts=past_ts,
        last_heartbeat_ts=past_ts,
    )
    db.session.add(account)
    db.session.add(Account(
        debtor_id=D_ID,
        creditor_id=123,
        creation_date=date(1970, 1, 1),
        principal=1000,
        total_locked_amount=500,
        pending_transfers_count=1,
        last_transfer_id=3,
        status_flags=Account.STATUS_DELETED_FLAG,
        last_change_ts=past_ts,
        last_heartbeat_ts=past_ts,
    ))
    db.session.add(Account(
        debtor_id=D_ID,
        creditor_id=1234,
        creation_date=date(1970, 1, 1),
        principal=1000,
        interest=20.0,
        interest_rate=2.0,
        total_locked_amount=500,
        pending_transfers_count=1,
        last_transfer_id=2,
        last_change_ts=current_ts - timedelta(seconds=10),
        last_heartbeat_ts=current_ts - timedelta(seconds=10),
    ))
    db.session.add(Account(
        debtor_id=D_ID,
        creditor_id=12345,
        creation_date=date(1970, 1, 1),
        principal=1000,
        total_locked_amount=500,
        pending_transfers_count=1,
        last_transfer_id=1,
        last_change_ts=past_ts,
        last_heartbeat_ts=current_ts - timedelta(seconds=10),
    ))
    db.session.add(Account(
        debtor_id=D_ID,
        creditor_id=123456,
        creation_date=date(1970, 1, 1),
        principal=0,
        total_locked_amount=0,
        pending_transfers_count=0,
        last_transfer_id=0,
        config_flags=Account.CONFIG_SCHEDULED_FOR_DELETION_FLAG,
        last_change_ts=current_ts,
        last_heartbeat_ts=current_ts,
    ))
    db.session.commit()
    db.engine.execute('ANALYZE account')
    assert len(Account.query.all()) == 6
    runner = app.test_cli_runner()
    result = runner.invoke(args=['swpt_accounts', 'scan_accounts', '--hours', '0.000024', '--quit-early'])
    assert result.exit_code == 0
    assert len(Account.query.all()) == 5
    assert len(AccountUpdateSignal.query.all()) == 1
    acs = AccountUpdateSignal.query.one()
    assert acs.debtor_id == account.debtor_id
    assert acs.creditor_id == account.creditor_id
    assert acs.last_change_ts == account.last_change_ts == past_ts
    assert acs.last_change_seqnum == account.last_change_seqnum == 0
    assert acs.principal == account.principal
    assert acs.interest == account.interest
    assert acs.interest_rate == account.interest_rate
    assert acs.last_transfer_number == account.last_transfer_number
    assert acs.last_config_ts == account.last_config_ts
    assert acs.last_config_seqnum == account.last_config_seqnum
    assert acs.creation_date == account.creation_date
    assert acs.negligible_amount == account.negligible_amount
    assert acs.config_data == ''
    assert acs.config_flags == account.config_flags

    assert len(Account.query.all()) == 5
    assert len(Account.query.filter_by(creditor_id=123).all()) == 0
    aps = AccountPurgeSignal.query.filter_by(debtor_id=D_ID, creditor_id=123).one()
    assert aps.creation_date == date(1970, 1, 1)

    assert len(AccountTransferSignal.query.all()) == 0
    assert len(PendingBalanceChangeSignal.query.all()) == 0

    db.session.commit()
    worker = dramatiq.Worker(chores_broker)
    worker.start()
    time.sleep(2.0)
    worker.join()

    accounts = Account.query.order_by(Account.creditor_id).all()
    assert accounts[0].creditor_id == 0
    assert accounts[1].last_heartbeat_ts >= current_ts
    assert accounts[2].last_heartbeat_ts >= current_ts and accounts[2].interest_rate == 0.0
    assert accounts[3].last_heartbeat_ts < current_ts
    assert accounts[4].status_flags & Account.STATUS_DELETED_FLAG

    assert AccountTransferSignal.query.one().creditor_id == 1234
    assert PendingBalanceChangeSignal.query.one().creditor_id == p.ROOT_CREDITOR_ID

    db.engine.execute('ANALYZE account')
    result = runner.invoke(args=['swpt_accounts', 'scan_prepared_transfers', '--days', '0.000001', '--quit-early'])
    assert result.exit_code == 0
    assert len(Account.query.all()) == 5
    assert len(AccountUpdateSignal.query.all()) == 3

    Account.query.delete()
    AccountUpdateSignal.query.delete()
    AccountPurgeSignal.query.delete()
    AccountTransferSignal.query.delete()
    PendingBalanceChangeSignal.query.delete()
    db.session.commit()

    _clear_root_config_data()


def test_scan_prepared_transfers(app_unsafe_session):
    from swpt_accounts.models import Account, PreparedTransfer, PreparedTransferSignal

    # db.signalbus.autoflush = False
    current_ts = datetime.now(tz=timezone.utc)
    past_ts = datetime(1970, 1, 1, tzinfo=timezone.utc)
    app = app_unsafe_session
    Account.query.delete()
    PreparedTransfer.query.delete()
    PreparedTransferSignal.query.delete()
    db.session.commit()
    db.session.add(Account(
        debtor_id=D_ID,
        creditor_id=C_ID,
        creation_date=date(1970, 1, 1),
        principal=1000,
        total_locked_amount=500,
        pending_transfers_count=1,
        last_transfer_id=2,
        status_flags=0,
    ))
    db.session.flush()
    db.session.add(PreparedTransfer(
        debtor_id=D_ID,
        sender_creditor_id=C_ID,
        transfer_id=1,
        coordinator_type='direct',
        coordinator_id=11,
        coordinator_request_id=111,
        locked_amount=400,
        recipient_creditor_id=1234,
        min_interest_rate=-100.0,
        prepared_at=current_ts,
        deadline=current_ts + timedelta(days=30),
        demurrage_rate=0.0,
    ))
    db.session.add(PreparedTransfer(
        debtor_id=D_ID,
        sender_creditor_id=C_ID,
        transfer_id=2,
        coordinator_type='direct',
        coordinator_id=11,
        coordinator_request_id=112,
        locked_amount=100,
        recipient_creditor_id=1234,
        min_interest_rate=-100.0,
        prepared_at=past_ts,
        deadline=current_ts + timedelta(days=30),
        demurrage_rate=0.0,
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
    assert pts.locked_amount == 100
    assert pts.recipient_creditor_id == 1234
    assert pts.prepared_at == past_ts

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


def test_get_if_account_is_reachable(app_unsafe_session, caplog):
    from swpt_accounts.models import Account, AccountUpdateSignal

    app_fetch_api_url = current_app.config['APP_FETCH_API_URL']

    Account.query.delete()
    AccountUpdateSignal.query.delete()
    db.session.commit()

    current_ts = datetime.now(tz=timezone.utc)
    p.configure_account(D_ID, C_ID, current_ts, 0)
    assert get_if_account_is_reachable(D_ID, C_ID)
    assert not get_if_account_is_reachable(666, C_ID)

    current_app.config['APP_FETCH_API_URL'] = 'localhost:1111'
    with caplog.at_level(logging.ERROR):
        assert not get_if_account_is_reachable(D_ID, C_ID)
        assert ["Caught error while making a fetch request."] == [rec.message for rec in caplog.records]
    current_app.config['APP_FETCH_API_URL'] = app_fetch_api_url

    Account.query.delete()
    AccountUpdateSignal.query.delete()
    db.session.commit()


def test_get_root_config_data_dict(app_unsafe_session, caplog):
    from swpt_accounts.models import Account, AccountUpdateSignal
    from swpt_accounts.fetch_api_client import _clear_root_config_data

    app_fetch_api_url = current_app.config['APP_FETCH_API_URL']

    Account.query.delete()
    AccountUpdateSignal.query.delete()
    db.session.commit()

    current_ts = datetime.now(tz=timezone.utc)
    p.configure_account(D_ID, p.ROOT_CREDITOR_ID, current_ts, 0, config_data='{"rate": 2.0}')
    assert get_root_config_data_dict([D_ID, 666]) == {D_ID: RootConfigData(2.0), 666: None}

    current_app.config['APP_FETCH_API_URL'] = 'localhost:1111'
    with caplog.at_level(logging.ERROR):
        assert get_root_config_data_dict([777]) == {777: None}
        assert ["Caught error while making a fetch request."] == [rec.message for rec in caplog.records]

    current_app.config['APP_FETCH_API_URL'] = app_fetch_api_url
    caplog.clear()
    with caplog.at_level(logging.ERROR):
        assert get_root_config_data_dict([D_ID, 666, 777]) == {D_ID: RootConfigData(2.0), 666: None, 777: None}
        assert len(caplog.records) == 0

    Account.query.delete()
    AccountUpdateSignal.query.delete()
    db.session.commit()
    _clear_root_config_data()


def test_set_interest_rate_on_new_accounts(app_unsafe_session):
    from swpt_accounts.models import Account, AccountUpdateSignal
    from swpt_accounts.fetch_api_client import _clear_root_config_data

    # db.signalbus.autoflush = False
    current_ts = datetime.now(tz=timezone.utc)
    Account.query.delete()
    AccountUpdateSignal.query.delete()
    db.session.commit()

    p.configure_account(D_ID, p.ROOT_CREDITOR_ID, current_ts, 0, config_data='{"rate": 3.567}')
    actors.configure_account(D_ID, C_ID, current_ts.isoformat(), 0)

    signals = AccountUpdateSignal.query.filter_by(creditor_id=C_ID).all()
    assert any(s.interest_rate == 3.567 for s in signals)

    Account.query.delete()
    AccountUpdateSignal.query.delete()
    db.session.commit()

    _clear_root_config_data()
