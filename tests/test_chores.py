import pytest
from datetime import datetime
from swpt_accounts import chores
from swpt_accounts import schemas
from marshmallow import ValidationError
from swpt_pythonlib.rabbitmq import MessageProperties

D_ID = -1
C_ID = 1


def test_set_interest_rate(db_session):
    chores._on_change_interest_rate(
        debtor_id=D_ID,
        creditor_id=C_ID,
        interest_rate=10.0,
        ts=datetime.fromisoformat('2019-12-31T00:00:00+00:00'),
    )


def test_capitalize_interest(db_session):
    chores._on_capitalize_interest(
        debtor_id=D_ID,
        creditor_id=C_ID,
    )


def test_update_debtor_info(db_session):
    chores._on_update_debtor_info(
        debtor_id=D_ID,
        creditor_id=C_ID,
        debtor_info_iri='http://example.com',
        debtor_info_content_type='text/plain',
        debtor_info_sha256='FF' * 32,
        ts=datetime.fromisoformat('2019-12-31T00:00:00+00:00'),
    )


def test_try_to_delete_account(db_session):
    chores._on_try_to_delete_account(
        debtor_id=D_ID,
        creditor_id=C_ID,
    )


def test_change_interest_rate_schema():
    s = schemas.ChangeInterestRateMessageSchema()

    data = s.loads("""{
    "type": "ChangeInterestRate",
    "debtor_id": -2,
    "creditor_id": -1,
    "interest_rate": 5.5,
    "ts": "2022-01-02T00:00:00Z",
    "unknown": "ignored"
    }""")

    assert data['type'] == 'ChangeInterestRate'
    assert data['debtor_id'] == -2
    assert data['creditor_id'] == -1
    assert data['interest_rate'] == 5.5
    assert type(data['interest_rate']) is float
    assert data['ts'] == datetime.fromisoformat('2022-01-02T00:00:00+00:00')
    assert "unknown" not in data

    wrong_type = data.copy()
    wrong_type['type'] = 'WrongType'
    wrong_type = s.dumps(wrong_type)
    with pytest.raises(ValidationError, match='Invalid type.'):
        s.loads(wrong_type)

    try:
        s.loads('{}')
    except ValidationError as e:
        assert len(e.messages) == len(data)
        assert all(m == ['Missing data for required field.'] for m in e.messages.values())


def test_update_debtor_info_schema():
    s = schemas.UpdateDebtorInfoMessageSchema()

    data = s.loads("""{
    "type": "UpdateDebtorInfo",
    "debtor_id": -2,
    "creditor_id": -1,
    "debtor_info_iri": "http://example.com/",
    "debtor_info_content_type": "text/plain",
    "debtor_info_sha256": "FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF",
    "ts": "2022-01-02T00:00:00Z",
    "unknown": "ignored"
    }""")

    assert data['type'] == 'UpdateDebtorInfo'
    assert data['debtor_id'] == -2
    assert data['creditor_id'] == -1
    assert data['debtor_info_iri'] == 'http://example.com/'
    assert data['debtor_info_content_type'] == 'text/plain'
    assert data['debtor_info_sha256'] == 32 * 'FF'
    assert data['ts'] == datetime.fromisoformat('2022-01-02T00:00:00+00:00')
    assert "unknown" not in data

    wrong_content_type = data.copy()
    wrong_content_type['debtor_info_content_type'] = 'Кирилица'
    wrong_content_type = s.dumps(wrong_content_type)
    with pytest.raises(ValidationError, match='The debtor_info_content_type field contains non-ASCII characters'):
        s.loads(wrong_content_type)

    try:
        s.loads('{}')
    except ValidationError as e:
        assert len(e.messages) == len(data)
        assert all(m == ['Missing data for required field.'] for m in e.messages.values())


def test_create_chore_message():
    current_ts = datetime.now()
    s = schemas.UpdateDebtorInfoMessageSchema()
    m = chores.create_chore_message({
        "type": "UpdateDebtorInfo",
        "debtor_id": -2,
        "creditor_id": -1,
        "debtor_info_iri": "",
        "debtor_info_content_type": "",
        "debtor_info_sha256": "",
        "ts": current_ts,
        "unknown": "ignored",
    })
    assert m.exchange == ''
    assert m.routing_key == 'swpt_accounts_chores'
    obj = s.loads(m.body.decode())
    assert obj["ts"] == current_ts
    assert obj["debtor_info_iri"] == ''
    assert obj["debtor_id"] == -2
    assert m.mandatory
    assert m.properties.app_id == 'swpt_accounts'
    assert m.properties.content_type == 'application/json'
    assert m.properties.delivery_mode == 2
    assert m.properties.type == 'UpdateDebtorInfo'


def test_consumer(db_session):
    consumer = chores.ChoresConsumer()

    props = MessageProperties(content_type="xxx")
    assert consumer.process_message(b'body', props) is False

    props = MessageProperties(content_type="application/json", type="xxx")
    assert consumer.process_message(b'body', props) is False

    props = MessageProperties(content_type="application/json", type="TryToDeleteAccount")
    assert consumer.process_message(b'body', props) is False

    props = MessageProperties(content_type="application/json", type="TryToDeleteAccount")
    assert consumer.process_message(b'{}', props) is False

    props = MessageProperties(content_type="application/json", type="TryToDeleteAccount")
    assert consumer.process_message(b'''
    {
      "type": "TryToDeleteAccount",
      "debtor_id": 1,
      "creditor_id": 2
    }
    ''', props) is True
