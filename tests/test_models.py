from datetime import datetime, date, timezone, timedelta
from swpt_accounts.models import Account

D_ID = -1
C_ID = 1


def test_sibnalbus_burst_count(app):
    from swpt_accounts import models as m

    assert isinstance(m.RejectedTransferSignal.signalbus_burst_count, int)
    assert isinstance(m.PreparedTransferSignal.signalbus_burst_count, int)
    assert isinstance(m.FinalizedTransferSignal.signalbus_burst_count, int)
    assert isinstance(m.AccountTransferSignal.signalbus_burst_count, int)
    assert isinstance(m.AccountUpdateSignal.signalbus_burst_count, int)
    assert isinstance(m.AccountPurgeSignal.signalbus_burst_count, int)
    assert isinstance(m.RejectedConfigSignal.signalbus_burst_count, int)
    assert isinstance(m.PendingBalanceChangeSignal.signalbus_burst_count, int)


def test_send_signalbus_message(app, mocker):
    from swpt_accounts import models as m

    current_ts = datetime.now()
    publisher = mocker.patch("swpt_accounts.models.publisher")
    s = m.RejectedTransferSignal(
        debtor_id=1,
        sender_creditor_id=2,
        coordinator_type="direct",
        coordinator_id=666,
        coordinator_request_id=777,
        status_code="TEST_ERROR",
        total_locked_amount=0,
        inserted_at=current_ts,
    )
    s.send_signalbus_message()
    publisher.publish_messages.assert_called_once()
    args, kwargs = publisher.publish_messages.call_args
    assert len(args) == 1
    assert kwargs == {}
    messages = args[0]
    assert len(messages) == 1
    m = messages[0]
    assert m.properties.headers["debtor-id"] == 1
    assert m.properties.headers["creditor-id"] == 2
    assert m.properties.headers["coordinator-type"] == "direct"
    assert m.properties.headers["coordinator-id"] == 666
    assert m.properties.type == "RejectedTransfer"
    assert m.properties.content_type == "application/json"
    assert m.properties.app_id == "swpt_accounts"
    assert m.properties.delivery_mode == 2
    assert s.__marshmallow_schema__.loads(m.body.decode("utf-8")) == dict(
        type="RejectedTransfer",
        debtor_id=1,
        sender_creditor_id=2,
        coordinator_type="direct",
        coordinator_id=666,
        coordinator_request_id=777,
        status_code="TEST_ERROR",
        total_locked_amount=0,
        inserted_at=current_ts,
    )


def test_send_signalbus_message_wrong_shard(app, mocker):
    from swpt_accounts import models as m
    from swpt_pythonlib.utils import ShardingRealm

    orig_sharding_realm = app.config["SHARDING_REALM"]
    app.config["SHARDING_REALM"] = ShardingRealm("0.#")
    app.config["DELETE_PARENT_SHARD_RECORDS"] = True
    current_ts = datetime.now()
    publisher = mocker.patch("swpt_accounts.models.publisher")
    s = m.RejectedTransferSignal(
        debtor_id=1,
        sender_creditor_id=2,
        coordinator_type="direct",
        coordinator_id=666,
        coordinator_request_id=777,
        status_code="TEST_ERROR",
        total_locked_amount=0,
        inserted_at=current_ts,
    )
    s.send_signalbus_message()
    publisher.publish_messages.assert_called_once()
    args, kwargs = publisher.publish_messages.call_args
    assert len(args) == 1
    assert kwargs == {}
    messages = args[0]
    assert len(messages) == 0
    app.config["DELETE_PARENT_SHARD_RECORDS"] = False
    app.config["SHARDING_REALM"] = orig_sharding_realm


def test_properties(app):
    from swpt_accounts import models as m
    from swpt_accounts.extensions import (
        TO_COORDINATORS_EXCHANGE,
        TO_DEBTORS_EXCHANGE,
        TO_CREDITORS_EXCHANGE,
        ACCOUNTS_IN_EXCHANGE,
    )

    s = m.RejectedTransferSignal(coordinator_id=1)
    assert s.exchange_name == TO_COORDINATORS_EXCHANGE
    assert s.routing_key == "00.00.00.00.00.00.00.01"

    s = m.PreparedTransferSignal(coordinator_id=1)
    assert s.exchange_name == TO_COORDINATORS_EXCHANGE
    assert s.routing_key == "00.00.00.00.00.00.00.01"

    s = m.FinalizedTransferSignal(coordinator_id=1)
    assert s.exchange_name == TO_COORDINATORS_EXCHANGE
    assert s.routing_key == "00.00.00.00.00.00.00.01"

    s = m.AccountTransferSignal(creditor_id=1)
    assert s.exchange_name == TO_CREDITORS_EXCHANGE
    assert s.routing_key == "00.00.00.00.00.00.00.01"

    s = m.AccountTransferSignal(debtor_id=2, creditor_id=0)
    assert s.exchange_name == TO_DEBTORS_EXCHANGE
    assert s.routing_key == "00.00.00.00.00.00.00.02"

    s = m.AccountUpdateSignal(creditor_id=1)
    assert s.exchange_name == TO_CREDITORS_EXCHANGE
    assert s.routing_key == "00.00.00.00.00.00.00.01"

    s = m.AccountPurgeSignal(creditor_id=1)
    assert s.exchange_name == TO_CREDITORS_EXCHANGE
    assert s.routing_key == "00.00.00.00.00.00.00.01"

    s = m.RejectedConfigSignal(creditor_id=1)
    assert s.exchange_name == TO_CREDITORS_EXCHANGE
    assert s.routing_key == "00.00.00.00.00.00.00.01"

    s = m.RejectedConfigSignal(debtor_id=2, creditor_id=0)
    assert s.exchange_name == TO_DEBTORS_EXCHANGE
    assert s.routing_key == "00.00.00.00.00.00.00.02"

    s = m.PendingBalanceChangeSignal(debtor_id=2, creditor_id=1)
    assert s.exchange_name == ACCOUNTS_IN_EXCHANGE
    assert s.routing_key == "1.1.1.1.1.0.0.0.1.1.0.1.0.0.1.1.1.0.1.1.0.1.0.1"


def test_configure_account():
    one_year = timedelta(days=365.25)
    current_ts = datetime.now(tz=timezone.utc)
    committed_at = current_ts - 2 * one_year
    account = Account(
        debtor_id=D_ID,
        creditor_id=C_ID,
        creation_date=date(1970, 1, 1),
        principal=1000,
        total_locked_amount=0,
        pending_transfers_count=0,
        last_transfer_id=0,
        status_flags=0,
        last_change_ts=current_ts,
        previous_interest_rate=0.0,
        last_interest_rate_change_ts=current_ts - one_year,
        interest_rate=10.0,
    )
    i = account.calc_due_interest(1000, committed_at, current_ts)
    assert abs(i - 100) < 1e-12

    i = account.calc_due_interest(-1000, committed_at, current_ts)
    assert abs(i + 100) < 1e-12

    assert account.calc_due_interest(1000, committed_at, committed_at) == 0
    assert account.calc_due_interest(1000, current_ts, current_ts) == 0
    assert account.calc_due_interest(1000, current_ts, committed_at) == 0

    i = account.calc_due_interest(
        1000, current_ts - timedelta(days=1), current_ts
    )
    assert abs(i - 0.26098) < 1e-3

    i = account.calc_due_interest(
        1000, committed_at, committed_at + timedelta(days=1)
    )
    assert abs(i) == 0


def test_are_managed_by_same_agent(app):
    from swpt_accounts import models as m

    assert m.are_managed_by_same_agent(0x0000010000000001, 0x0000010000000002)
    assert m.are_managed_by_same_agent(-1, -1)
    assert m.are_managed_by_same_agent(-1, -2)
    assert m.are_managed_by_same_agent(
        0xffffff0000000000, 0xffffff1111111111
    )
    assert m.are_managed_by_same_agent(
        0xffffff0000000000, 0xffffff0000000000
    )
    assert not m.are_managed_by_same_agent(
        0xffffff0000000000, 0xfffffe0000000000
    )
    assert not m.are_managed_by_same_agent(
        0xffffff0000000000, 0xefffff0000000000
    )
    assert not m.are_managed_by_same_agent(-1, -1 - 0x0000010000000000)
