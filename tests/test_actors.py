import pytest
from datetime import datetime, timezone
from swpt_accounts import procedures as p
from swpt_accounts.models import RejectedTransferSignal
from swpt_pythonlib.rabbitmq import MessageProperties

D_ID = -1
C_ID = 1


@pytest.fixture(scope='function')
def actors():
    from swpt_accounts import actors
    return actors


def test_prepare_transfer(db_session, actors):
    actors._on_prepare_transfer_signal(
        coordinator_type='test',
        coordinator_id=1,
        coordinator_request_id=2,
        min_locked_amount=1,
        max_locked_amount=200,
        debtor_id=D_ID,
        creditor_id=C_ID,
        recipient='1234',
        min_interest_rate=-100.0,
        max_commit_delay=1000000,
        ts=datetime.now(tz=timezone.utc),
    )
    actors._on_prepare_transfer_signal(
        coordinator_type='test',
        coordinator_id=1,
        coordinator_request_id=2,
        min_locked_amount=1,
        max_locked_amount=200,
        debtor_id=D_ID,
        creditor_id=C_ID,
        recipient='invalid',
        min_interest_rate=-100.0,
        max_commit_delay=1000000,
        ts=datetime.now(tz=timezone.utc),
    )

    p.process_transfer_requests(D_ID, C_ID)
    signals = RejectedTransferSignal.query.all()
    assert len(signals) == 2

    for rts in signals:
        assert rts.debtor_id == D_ID
        assert rts.coordinator_type == 'test'
        assert rts.coordinator_id == 1
        assert rts.coordinator_request_id == 2


def test_finalize_transfer(db_session, actors):
    actors._on_finalize_transfer_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        transfer_id=666,
        coordinator_type='test',
        coordinator_id=1,
        coordinator_request_id=2,
        committed_amount=100,
        transfer_note_format='',
        transfer_note='',
        ts=datetime.now(tz=timezone.utc),
    )


def test_configure_account(db_session, actors):
    from swpt_accounts.fetch_api_client import _clear_root_config_data

    actors._on_configure_account_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        ts=datetime.fromisoformat('2099-12-31T00:00:00+00:00'),
        seqnum=0,
        negligible_amount=500.0,
        config_flags=0,
        config_data='',
    )
    _clear_root_config_data()


def test_on_pending_balance_change_signal(db_session, actors):
    actors._on_pending_balance_change_signal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        change_id=1,
        coordinator_type='direct',
        transfer_note_format='',
        transfer_note='',
        committed_at=datetime.fromisoformat('2099-12-31T00:00:00+00:00'),
        principal_delta=1000,
        other_creditor_id=123,
    )


def test_consumer(db_session, actors):
    consumer = actors.SmpConsumer()

    props = MessageProperties(content_type="xxx")
    assert consumer.process_message(b'body', props) is False

    props = MessageProperties(content_type="application/json", type="xxx")
    assert consumer.process_message(b'body', props) is False

    props = MessageProperties(content_type="application/json", type="ConfigureAccount")
    assert consumer.process_message(b'body', props) is False

    props = MessageProperties(content_type="application/json", type="ConfigureAccount")
    assert consumer.process_message(b'{}', props) is False

    props = MessageProperties(content_type="application/json", type="ConfigureAccount")
    assert consumer.process_message(b'''
    {
      "type": "ConfigureAccount",
      "debtor_id": 1,
      "creditor_id": 2,
      "ts": "2099-12-31T00:00:00+00:00",
      "seqnum": 0,
      "negligible_amount": 500.0,
      "config_flags": 0,
      "config_data": ""
    }
    ''', props) is True
