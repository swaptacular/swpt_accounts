import pytest
import sqlalchemy
from unittest.mock import Mock
from datetime import date, datetime, timezone, timedelta
from sqlalchemy.sql.expression import true
from swpt_accounts.extensions import db
from swpt_accounts import procedures as p
from swpt_accounts.models import (
    RejectedTransferSignal,
    TransferRequest,
    FinalizationRequest,
    FinalizedTransferSignal,
    PreparedTransfer,
    PendingBalanceChangeSignal,
    RegisteredBalanceChange,
    T_INFINITY,
)
from swpt_accounts.chores import ChoresConsumer
from swpt_pythonlib.utils import ShardingRealm

D_ID = -1
C_ID = 1


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


def test_process_transfers_pending_balance_changes(app, db_session):
    p.make_debtor_payment("test", D_ID, C_ID, 1000)
    assert p.get_available_amount(D_ID, p.ROOT_CREDITOR_ID) is None
    _flush_balance_change_signals()
    _flush_balance_change_signals()
    _flush_balance_change_signals()
    runner = app.test_cli_runner()
    result = runner.invoke(
        args=[
            "swpt_accounts",
            "process_balance_changes",
            "--quit-early",
            "--wait=0",
        ]
    )
    assert result.exit_code == 0
    assert not result.output
    assert p.get_available_amount(D_ID, p.ROOT_CREDITOR_ID) == -1000
    assert RegisteredBalanceChange.query.filter(
        RegisteredBalanceChange.is_applied == true()
    ).all()


def test_process_transfers_transfer_requests(app, db_session):
    current_ts = datetime.now(tz=timezone.utc)
    p.configure_account(D_ID, 1234, current_ts, 0)
    p.prepare_transfer(
        coordinator_type="test",
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
    result = runner.invoke(
        args=[
            "swpt_accounts",
            "process_transfer_requests",
            "--quit-early",
            "--wait=0",
        ]
    )
    assert result.exit_code == 0
    assert not result.output
    db_session.close()
    assert len(RejectedTransferSignal.query.all()) == 1
    assert len(TransferRequest.query.all()) == 0


def test_process_transfers_finalization_requests(app, db_session):
    p.make_debtor_payment("test", D_ID, C_ID, 1000)
    p.process_pending_balance_changes(D_ID, C_ID)
    p.prepare_transfer(
        coordinator_type="test",
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
    p.finalize_transfer(D_ID, C_ID, pt.transfer_id, "test", 1, 2, 1)
    assert len(FinalizationRequest.query.all()) == 1
    runner = app.test_cli_runner()
    result = runner.invoke(
        args=[
            "swpt_accounts",
            "process_finalization_requests",
            "--quit-early",
            "--wait=0",
        ]
    )
    assert result.exit_code == 0
    assert not result.output
    db_session.close()
    assert len(FinalizedTransferSignal.query.all()) == 1
    assert len(FinalizationRequest.query.all()) == 0


def test_ignore_transfers_finalization_requests(app, db_session):
    orig_sharding_realm = app.config["SHARDING_REALM"]
    app.config["SHARDING_REALM"] = ShardingRealm("0.#")
    app.config["DELETE_PARENT_SHARD_RECORDS"] = True
    p.make_debtor_payment("test", D_ID, C_ID, 1000)
    p.process_pending_balance_changes(D_ID, C_ID)
    p.prepare_transfer(
        coordinator_type="test",
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
    p.finalize_transfer(D_ID, C_ID, pt.transfer_id, "test", 1, 2, 1)
    assert len(FinalizationRequest.query.all()) == 1
    runner = app.test_cli_runner()
    result = runner.invoke(
        args=[
            "swpt_accounts",
            "process_finalization_requests",
            "--quit-early",
            "--wait=0",
        ]
    )
    assert result.exit_code == 0
    assert not result.output
    db_session.close()
    assert len(FinalizedTransferSignal.query.all()) == 0
    assert len(FinalizationRequest.query.all()) == 0
    app.config["DELETE_PARENT_SHARD_RECORDS"] = False
    app.config["SHARDING_REALM"] = orig_sharding_realm


def test_flush_messages(mocker, app, db_session):
    send_signalbus_message = Mock()
    mocker.patch(
        "swpt_accounts.models.RejectedTransferSignal.send_signalbus_message",
        new_callable=send_signalbus_message,
    )
    rts = RejectedTransferSignal(
        debtor_id=D_ID,
        sender_creditor_id=C_ID,
        coordinator_type="direct",
        coordinator_id=C_ID,
        coordinator_request_id=777,
        status_code="FAILURE",
        total_locked_amount=0,
    )
    db.session.add(rts)
    db.session.commit()
    assert len(RejectedTransferSignal.query.all()) == 1
    db.session.commit()

    runner = app.test_cli_runner()
    result = runner.invoke(
        args=[
            "swpt_accounts",
            "flush_messages",
            "RejectedTransferSignal",
            "--wait",
            "0.1",
            "--quit-early",
        ]
    )
    assert result.exit_code == 1
    send_signalbus_message.assert_called_once()
    assert len(RejectedTransferSignal.query.all()) == 0


def test_consume_messages(app):
    runner = app.test_cli_runner()
    result = runner.invoke(
        args=["swpt_accounts", "consume_messages", "--url=INVALID"]
    )
    assert result.exit_code == 1


def test_consume_chore_messages(app):
    runner = app.test_cli_runner()
    result = runner.invoke(
        args=["swpt_accounts", "consume_chore_messages", "--url=INVALID"]
    )
    assert result.exit_code == 1


def test_scan_accounts(app, db_session, mocker):
    chores = []

    class MyPublisher:
        def publish_messages(self, messages):
            chores.extend(messages)

    mocker.patch(
        "swpt_accounts.extensions.chores_publisher", new=MyPublisher()
    )

    from swpt_accounts.models import (
        Account,
        AccountUpdateSignal,
        AccountPurgeSignal,
        AccountTransferSignal,
        PendingBalanceChangeSignal,
    )
    from swpt_accounts.fetch_api_client import _clear_root_config_data

    current_ts = datetime.now(tz=timezone.utc)
    past_ts = datetime(1970, 1, 1, tzinfo=timezone.utc)
    p.configure_account(
        D_ID,
        p.ROOT_CREDITOR_ID,
        current_ts,
        0,
        config_data='{"rate": 0.0, "info": {"iri": "http://example.com"}}',
    )
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
        debtor_info_iri="http://example.com",
    )
    db.session.add(account)
    db.session.add(
        Account(
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
            debtor_info_iri="http://example.com",
        )
    )
    db.session.add(
        Account(
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
            debtor_info_iri="http://example.com",
        )
    )
    db.session.add(
        Account(
            debtor_id=D_ID,
            creditor_id=12345,
            creation_date=date(1970, 1, 1),
            principal=1000,
            total_locked_amount=500,
            pending_transfers_count=1,
            last_transfer_id=1,
            last_change_ts=past_ts,
            last_heartbeat_ts=current_ts - timedelta(seconds=10),
            debtor_info_iri="http://example.com",
        )
    )
    db.session.add(
        Account(
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
            debtor_info_iri="http://example.com",
        )
    )
    db.session.add(
        Account(
            debtor_id=D_ID,
            creditor_id=1234567,
            creation_date=date(1970, 1, 1),
            principal=0,
            total_locked_amount=0,
            pending_transfers_count=0,
            last_transfer_id=0,
            last_change_ts=current_ts,
            last_heartbeat_ts=current_ts,
        )
    )
    db.session.commit()

    with db.engine.connect() as conn:
        conn.execute(sqlalchemy.text("ANALYZE account"))

    assert len(Account.query.all()) == 7
    runner = app.test_cli_runner()
    result = runner.invoke(
        args=[
            "swpt_accounts",
            "scan_accounts",
            "--hours",
            "0.000024",
            "--quit-early",
        ]
    )
    assert result.exit_code == 0
    assert len(Account.query.all()) == 6
    assert len(AccountUpdateSignal.query.all()) == 1

    # A heartbeat message
    acs = AccountUpdateSignal.query.one()
    account = Account.query.filter_by(debtor_id=D_ID, creditor_id=12).one()
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
    assert acs.config_data == ""
    assert acs.config_flags == account.config_flags

    assert len(Account.query.all()) == 6
    assert len(Account.query.filter_by(creditor_id=123).all()) == 0
    aps = AccountPurgeSignal.query.filter_by(
        debtor_id=D_ID, creditor_id=123
    ).one()
    assert aps.creation_date == date(1970, 1, 1)

    assert len(AccountTransferSignal.query.all()) == 0
    assert len(PendingBalanceChangeSignal.query.all()) == 0

    db.session.commit()

    chores_consumer = ChoresConsumer()
    for msg in chores:
        chores_consumer.process_message(msg.body, msg.properties)

    accounts = Account.query.order_by(Account.creditor_id).all()
    assert accounts[0].creditor_id == 0
    assert accounts[1].last_heartbeat_ts >= current_ts
    assert (
        accounts[2].last_heartbeat_ts >= current_ts
        and accounts[2].interest_rate == 0.0
    )
    assert accounts[3].last_heartbeat_ts < current_ts
    assert accounts[4].status_flags & Account.STATUS_DELETED_FLAG
    assert accounts[5].debtor_info_iri == "http://example.com"

    assert AccountTransferSignal.query.one().creditor_id == 1234
    assert (
        PendingBalanceChangeSignal.query.one().creditor_id
        == p.ROOT_CREDITOR_ID
    )

    with db.engine.connect() as conn:
        conn.execute(sqlalchemy.text("ANALYZE prepared_transfer"))

    result = runner.invoke(
        args=[
            "swpt_accounts",
            "scan_prepared_transfers",
            "--days",
            "0.000001",
            "--quit-early",
        ]
    )
    assert result.exit_code == 0
    assert len(Account.query.all()) == 6
    assert len(AccountUpdateSignal.query.all()) == 4

    _clear_root_config_data()


def test_delete_parent_accounts(app, db_session):
    from swpt_accounts.models import Account, AccountUpdateSignal
    from swpt_accounts.fetch_api_client import _clear_root_config_data

    current_ts = datetime.now(tz=timezone.utc)
    AccountUpdateSignal.query.delete()
    account = Account(
        debtor_id=D_ID,
        creditor_id=12,
        creation_date=date(1970, 1, 1),
        principal=1000,
        total_locked_amount=500,
        pending_transfers_count=1,
        last_transfer_id=3,
        last_change_ts=current_ts,
        last_heartbeat_ts=current_ts,
        debtor_info_iri="http://example.com",
    )
    db.session.add(account)
    db.session.commit()
    orig_sharding_realm = app.config["SHARDING_REALM"]
    app.config["SHARDING_REALM"] = ShardingRealm("0.#")
    app.config["DELETE_PARENT_SHARD_RECORDS"] = True

    with db.engine.connect() as conn:
        conn.execute(sqlalchemy.text("ANALYZE account"))

    assert len(Account.query.all()) == 1
    runner = app.test_cli_runner()
    result = runner.invoke(
        args=[
            "swpt_accounts",
            "scan_accounts",
            "--hours",
            "0.000024",
            "--quit-early",
        ]
    )
    assert result.exit_code == 0
    assert len(Account.query.all()) == 0

    app.config["DELETE_PARENT_SHARD_RECORDS"] = False
    app.config["SHARDING_REALM"] = orig_sharding_realm
    _clear_root_config_data()


def test_scan_prepared_transfers(app, db_session):
    from swpt_accounts.models import (
        Account,
        PreparedTransfer,
        PreparedTransferSignal,
    )

    current_ts = datetime.now(tz=timezone.utc)
    past_ts = datetime(1970, 1, 1, tzinfo=timezone.utc)
    db.session.add(
        Account(
            debtor_id=D_ID,
            creditor_id=C_ID,
            creation_date=date(1970, 1, 1),
            principal=1000,
            total_locked_amount=500,
            pending_transfers_count=1,
            last_transfer_id=2,
            status_flags=0,
        )
    )
    db.session.flush()
    db.session.add(
        PreparedTransfer(
            debtor_id=D_ID,
            sender_creditor_id=C_ID,
            transfer_id=1,
            coordinator_type="direct",
            coordinator_id=11,
            coordinator_request_id=111,
            locked_amount=400,
            recipient_creditor_id=1234,
            final_interest_rate_ts=T_INFINITY,
            prepared_at=current_ts,
            deadline=current_ts + timedelta(days=30),
            demurrage_rate=0.0,
        )
    )
    db.session.add(
        PreparedTransfer(
            debtor_id=D_ID,
            sender_creditor_id=C_ID,
            transfer_id=2,
            coordinator_type="direct",
            coordinator_id=11,
            coordinator_request_id=112,
            locked_amount=100,
            recipient_creditor_id=1234,
            final_interest_rate_ts=T_INFINITY,
            prepared_at=past_ts,
            deadline=current_ts + timedelta(days=30),
            demurrage_rate=0.0,
        )
    )
    db.session.commit()

    with db.engine.connect() as conn:
        conn.execute(sqlalchemy.text("ANALYZE prepared_transfer"))

    assert len(Account.query.all()) == 1
    assert len(PreparedTransfer.query.all()) == 2
    assert len(PreparedTransferSignal.query.all()) == 0
    runner = app.test_cli_runner()
    result = runner.invoke(
        args=[
            "swpt_accounts",
            "scan_prepared_transfers",
            "--days",
            "0.000001",
            "--quit-early",
        ]
    )
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
    assert pts.coordinator_type == "direct"
    assert pts.coordinator_id == 11
    assert pts.coordinator_request_id == 112
    assert pts.locked_amount == 100
    assert pts.recipient_creditor_id == 1234
    assert pts.prepared_at == past_ts

    with db.engine.connect() as conn:
        conn.execute(sqlalchemy.text("ANALYZE prepared_transfer"))

    result = runner.invoke(
        args=[
            "swpt_accounts",
            "scan_prepared_transfers",
            "--days",
            "0.000001",
            "--quit-early",
        ]
    )
    assert result.exit_code == 0
    assert len(Account.query.all()) == 1
    assert len(PreparedTransfer.query.all()) == 2
    assert len(PreparedTransferSignal.query.all()) == 1


def test_scan_registered_balance_changes(app, db_session):
    from swpt_accounts.models import RegisteredBalanceChange

    current_ts = datetime.now(tz=timezone.utc)
    past_ts = datetime(1970, 1, 1, tzinfo=timezone.utc)
    db.session.add(
        RegisteredBalanceChange(
            debtor_id=D_ID,
            other_creditor_id=C_ID,
            change_id=1,
            committed_at=past_ts,
            is_applied=False,
        )
    )
    db.session.add(
        RegisteredBalanceChange(
            debtor_id=D_ID,
            other_creditor_id=C_ID,
            change_id=2,
            committed_at=past_ts,
            is_applied=True,
        )
    )
    db.session.add(
        RegisteredBalanceChange(
            debtor_id=D_ID,
            other_creditor_id=C_ID,
            change_id=3,
            committed_at=current_ts,
            is_applied=True,
        )
    )
    db.session.flush()
    db.session.commit()

    with db.engine.connect() as conn:
        conn.execute(sqlalchemy.text("ANALYZE registered_balance_change"))

    assert len(RegisteredBalanceChange.query.all()) == 3
    runner = app.test_cli_runner()
    result = runner.invoke(
        args=[
            "swpt_accounts",
            "scan_registered_balance_changes",
            "--days",
            "0.000001",
            "--quit-early",
        ]
    )
    assert result.exit_code == 0
    assert len(RegisteredBalanceChange.query.all()) == 2
    assert RegisteredBalanceChange.query.filter_by(change_id=1).one()
    assert RegisteredBalanceChange.query.filter_by(change_id=3).one()


@pytest.mark.parametrize("realm", ["0.#", "1.#"])
def test_verify_shard_content(app, db_session, realm):
    orig_sharding_realm = app.config["SHARDING_REALM"]
    app.config["SHARDING_REALM"] = ShardingRealm(realm)
    current_ts = datetime.now(tz=timezone.utc)
    p.configure_account(D_ID, 1234, current_ts, 0)

    runner = app.test_cli_runner()
    result = runner.invoke(
        args=["swpt_accounts", "verify_shard_content"]
    )
    assert result.exit_code == int(realm[0])
    app.config["SHARDING_REALM"] = orig_sharding_realm


def test_alembic_current_head(app, request, capfd):
    if request.config.option.capture != "no":
        pytest.skip("needs to be run with --capture=no")

    runner = app.test_cli_runner()
    result = runner.invoke(
        args=["db", "current"]
    )
    assert result.exit_code == 0
    captured = capfd.readouterr()
    assert captured.out.strip().endswith(" (head)")
