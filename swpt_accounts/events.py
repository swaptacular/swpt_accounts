from base64 import b16encode
from hashlib import md5
import dramatiq
from flask import current_app
from datetime import datetime, timezone
from marshmallow import Schema, fields
from sqlalchemy.dialects import postgresql as pg
from swpt_lib.utils import i64_to_u64
from swpt_accounts.extensions import db, publisher
from flask_signalbus import rabbitmq

__all__ = [
    'ROOT_CREDITOR_ID',
    'RejectedTransferSignal',
    'PreparedTransferSignal',
    'FinalizedTransferSignal',
    'AccountTransferSignal',
    'AccountUpdateSignal',
    'AccountPurgeSignal',
    'RejectedConfigSignal',
    'PendingBalanceChangeSignal',
]

SECONDS_IN_DAY = 24 * 60 * 60
INTEREST_RATE_FLOOR = -50.0
INTEREST_RATE_CEIL = 100.0
TRANSFER_NOTE_MAX_BYTES = 500

# The account `(debtor_id, ROOT_CREDITOR_ID)` is special. This is the
# debtor's account. It issuers all the money. Also, all interest and
# demurrage payments come from/to this account.
ROOT_CREDITOR_ID = 0


def get_now_utc():
    return datetime.now(tz=timezone.utc)


def i64_to_hex_routing_key(n):
    bytes_n = n.to_bytes(8, byteorder='big', signed=True)
    assert(len(bytes_n) == 8)
    return '.'.join([format(byte, '02x') for byte in bytes_n])


def calc_bin_routing_key(debtor_id, creditor_id):
    m = md5()
    m.update(debtor_id.to_bytes(8, byteorder='big', signed=True))
    m.update(creditor_id.to_bytes(8, byteorder='big', signed=True))
    s = ''.join([format(byte, '08b') for byte in m.digest()[:3]])
    assert(len(s) == 24)
    return '.'.join(s)


class classproperty(object):
    def __init__(self, f):
        self.f = f

    def __get__(self, obj, owner):
        return self.f(owner)


class Signal(db.Model):
    __abstract__ = True

    # TODO: Define `send_signalbus_messages` class method, and set
    #      `ModelClass.signalbus_burst_count = N` in models. Make sure
    #      RabbitMQ message headers are set properly for the messages.

    @property
    def actor_name(self):  # pragma: no cover
        model = type(self)
        return f'on_{model.__tablename__}'

    @classmethod
    def send_signalbus_messages(cls, objects):  # pragma: no cover
        assert(all(isinstance(obj, cls) for obj in objects))
        messages = [obj._create_message() for obj in objects]
        publisher.publish_messages(messages)

    def send_signalbus_message(self):  # pragma: no cover
        self.send_signalbus_messages([self])

    def _create_message(self):  # pragma: no cover
        model = type(self)
        data = model.__marshmallow_schema__.dump(self)
        dramatiq_message = dramatiq.Message(
            queue_name=None,
            actor_name=self.actor_name,
            args=(),
            kwargs=data,
            options={},
        )
        headers = {
            'debtor-id': data['debtor_id'],
            'creditor-id': data['creditor_id'],
        }
        if 'coordinator_id' in data:
            headers['coordinator-id'] = data['coordinator_id']
            headers['coordinator-type'] = data['coordinator_type']
        properties = rabbitmq.MessageProperties(
            delivery_mode=2,
            app_id='swpt_accounts',
            content_type='application/json',
            type=self.message_type,
            headers=headers,
        )
        return rabbitmq.Message(
            exchange=self.exchange_name,
            routing_key=self.routing_key,
            body=dramatiq_message.encode(),
            properties=properties,
            mandatory=True,
        )

    inserted_at = db.Column(db.TIMESTAMP(timezone=True), nullable=False, default=get_now_utc)


class RejectedTransferSignal(Signal):
    message_type = 'RejectedTransfer'
    exchange_name = 'to_coordinators'

    class __marshmallow__(Schema):
        coordinator_type = fields.String()
        coordinator_id = fields.Integer()
        coordinator_request_id = fields.Integer()
        status_code = fields.String()
        total_locked_amount = fields.Integer()
        debtor_id = fields.Integer()
        sender_creditor_id = fields.Integer(data_key='creditor_id')
        inserted_at = fields.DateTime(data_key='ts')

    debtor_id = db.Column(db.BigInteger, primary_key=True)
    sender_creditor_id = db.Column(db.BigInteger, primary_key=True)
    signal_id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    coordinator_type = db.Column(db.String(30), nullable=False)
    coordinator_id = db.Column(db.BigInteger, nullable=False)
    coordinator_request_id = db.Column(db.BigInteger, nullable=False)
    status_code = db.Column(db.String(30), nullable=False)
    total_locked_amount = db.Column(db.BigInteger, nullable=False)

    @classproperty
    def signalbus_burst_count(self):
        return current_app.config['APP_FLUSH_REJECTED_TRANSFERS_BURST_COUNT']

    @property
    def routing_key(self):  # pragma: no cover
        return i64_to_hex_routing_key(self.coordinator_id)

    @property
    def actor_name(self):  # pragma: no cover
        return f'on_rejected_{self.coordinator_type}_transfer_signal'


class PreparedTransferSignal(Signal):
    message_type = 'PreparedTransfer'
    exchange_name = 'to_coordinators'

    class __marshmallow__(Schema):
        debtor_id = fields.Integer()
        sender_creditor_id = fields.Integer(data_key='creditor_id')
        transfer_id = fields.Integer()
        coordinator_type = fields.String()
        coordinator_id = fields.Integer()
        coordinator_request_id = fields.Integer()
        locked_amount = fields.Integer()
        recipient = fields.Function(lambda obj: str(i64_to_u64(obj.recipient_creditor_id)))
        prepared_at = fields.DateTime()
        inserted_at = fields.DateTime(data_key='ts')
        demurrage_rate = fields.Float()
        deadline = fields.DateTime()
        min_interest_rate = fields.Float()

    debtor_id = db.Column(db.BigInteger, primary_key=True)
    sender_creditor_id = db.Column(db.BigInteger, primary_key=True)
    signal_id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    transfer_id = db.Column(db.BigInteger, nullable=False)
    coordinator_type = db.Column(db.String(30), nullable=False)
    coordinator_id = db.Column(db.BigInteger, nullable=False)
    coordinator_request_id = db.Column(db.BigInteger, nullable=False)
    locked_amount = db.Column(db.BigInteger, nullable=False)
    recipient_creditor_id = db.Column(db.BigInteger, nullable=False)
    prepared_at = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    demurrage_rate = db.Column(db.FLOAT, nullable=False)
    deadline = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    min_interest_rate = db.Column(db.REAL, nullable=False)

    @classproperty
    def signalbus_burst_count(self):
        return current_app.config['APP_FLUSH_PREPARED_TRANSFERS_BURST_COUNT']

    @property
    def routing_key(self):  # pragma: no cover
        return i64_to_hex_routing_key(self.coordinator_id)

    @property
    def actor_name(self):  # pragma: no cover
        return f'on_prepared_{self.coordinator_type}_transfer_signal'


class FinalizedTransferSignal(Signal):
    message_type = 'FinalizedTransfer'
    exchange_name = 'to_coordinators'

    class __marshmallow__(Schema):
        debtor_id = fields.Integer()
        sender_creditor_id = fields.Integer(data_key='creditor_id')
        transfer_id = fields.Integer()
        coordinator_type = fields.String()
        coordinator_id = fields.Integer()
        coordinator_request_id = fields.Integer()
        prepared_at = fields.DateTime()
        finalized_at = fields.DateTime(data_key='ts')
        committed_amount = fields.Integer()
        total_locked_amount = fields.Integer()
        status_code = fields.String()

    debtor_id = db.Column(db.BigInteger, primary_key=True)
    sender_creditor_id = db.Column(db.BigInteger, primary_key=True)
    transfer_id = db.Column(db.BigInteger, primary_key=True)
    coordinator_type = db.Column(db.String(30), nullable=False)
    coordinator_id = db.Column(db.BigInteger, nullable=False)
    coordinator_request_id = db.Column(db.BigInteger, nullable=False)
    prepared_at = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    finalized_at = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    committed_amount = db.Column(db.BigInteger, nullable=False)
    total_locked_amount = db.Column(db.BigInteger, nullable=False)
    status_code = db.Column(db.String(30), nullable=False)

    @classproperty
    def signalbus_burst_count(self):
        return current_app.config['APP_FLUSH_FINALIZED_TRANSFERS_BURST_COUNT']

    @property
    def routing_key(self):  # pragma: no cover
        return i64_to_hex_routing_key(self.coordinator_id)

    @property
    def actor_name(self):  # pragma: no cover
        return f'on_finalized_{self.coordinator_type}_transfer_signal'


class AccountTransferSignal(Signal):
    message_type = 'AccountTransfer'

    class __marshmallow__(Schema):
        debtor_id = fields.Integer()
        creditor_id = fields.Integer()
        creation_date = fields.Date()
        transfer_number = fields.Integer()
        coordinator_type = fields.String()
        committed_at = fields.DateTime()
        acquired_amount = fields.Integer()
        transfer_note_format = fields.String()
        transfer_note = fields.String()
        principal = fields.Integer()
        previous_transfer_number = fields.Integer()
        sender = fields.Function(lambda obj: str(i64_to_u64(obj.sender_creditor_id)))
        recipient = fields.Function(lambda obj: str(i64_to_u64(obj.recipient_creditor_id)))
        inserted_at = fields.DateTime(data_key='ts')

    SYSTEM_FLAG_IS_NEGLIGIBLE = 1
    """Indicates that the absolute value of `committed_amount` is not
    bigger than the negligible amount configured for the account.
    """

    debtor_id = db.Column(db.BigInteger, primary_key=True)
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    creation_date = db.Column(db.DATE, primary_key=True)
    transfer_number = db.Column(db.BigInteger, primary_key=True)
    coordinator_type = db.Column(db.String(30), nullable=False)
    committed_at = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    acquired_amount = db.Column(db.BigInteger, nullable=False)
    other_creditor_id = db.Column(db.BigInteger, nullable=False)
    transfer_note_format = db.Column(pg.TEXT, nullable=False)
    transfer_note = db.Column(pg.TEXT, nullable=False)
    principal = db.Column(db.BigInteger, nullable=False)
    previous_transfer_number = db.Column(db.BigInteger, nullable=False)

    @classproperty
    def signalbus_burst_count(self):
        return current_app.config['APP_FLUSH_ACCOUNT_TRANSFERS_BURST_COUNT']

    @property
    def exchange_name(self):  # pragma: no cover
        return 'to_debtors' if self.creditor_id == ROOT_CREDITOR_ID else 'to_creditors'

    @property
    def routing_key(self):  # pragma: no cover
        return i64_to_hex_routing_key(self.debtor_id if self.creditor_id == ROOT_CREDITOR_ID else self.creditor_id)

    @property
    def sender_creditor_id(self):
        return self.other_creditor_id if self.acquired_amount >= 0 else self.creditor_id

    @property
    def recipient_creditor_id(self):
        return self.other_creditor_id if self.acquired_amount < 0 else self.creditor_id


class AccountUpdateSignal(Signal):
    message_type = 'AccountUpdate'

    class __marshmallow__(Schema):
        debtor_id = fields.Integer()
        creditor_id = fields.Integer()
        last_change_ts = fields.DateTime()
        last_change_seqnum = fields.Integer()
        principal = fields.Integer()
        interest = fields.Float()
        interest_rate = fields.Float()
        transfer_note_max_bytes = fields.Constant(TRANSFER_NOTE_MAX_BYTES)
        demurrage_rate = fields.Constant(INTEREST_RATE_FLOOR)
        commit_period = fields.Integer()
        last_interest_rate_change_ts = fields.DateTime()
        last_transfer_number = fields.Integer()
        last_transfer_committed_at = fields.DateTime()
        last_config_ts = fields.DateTime()
        last_config_seqnum = fields.Integer()
        creation_date = fields.Date()
        negligible_amount = fields.Float()
        config_data = fields.String()
        config_flags = fields.Integer()
        inserted_at = fields.DateTime(data_key='ts')
        ttl = fields.Integer()
        account_id = fields.Function(lambda obj: str(i64_to_u64(obj.creditor_id)))
        debtor_info_iri = fields.Function(lambda obj: obj.debtor_info_iri or '')
        debtor_info_content_type = fields.Function(lambda obj: obj.debtor_info_content_type or '')
        debtor_info_sha256 = fields.Function(lambda obj: b16encode(obj.debtor_info_sha256 or b'').decode())

    debtor_id = db.Column(db.BigInteger, primary_key=True)
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    signal_id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    last_change_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    last_change_seqnum = db.Column(db.Integer, nullable=False)
    principal = db.Column(db.BigInteger, nullable=False)
    interest = db.Column(db.FLOAT, nullable=False)
    interest_rate = db.Column(db.REAL, nullable=False)
    last_interest_rate_change_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    last_transfer_number = db.Column(db.BigInteger, nullable=False)
    last_transfer_committed_at = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    last_config_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    last_config_seqnum = db.Column(db.Integer, nullable=False)
    creation_date = db.Column(db.DATE, nullable=False)
    negligible_amount = db.Column(db.REAL, nullable=False)
    config_data = db.Column(db.String, nullable=False)
    config_flags = db.Column(db.Integer, nullable=False)
    debtor_info_iri = db.Column(db.String)
    debtor_info_content_type = db.Column(db.String)
    debtor_info_sha256 = db.Column(db.LargeBinary)

    @classproperty
    def signalbus_burst_count(self):
        return current_app.config['APP_FLUSH_ACCOUNT_UPDATES_BURST_COUNT']

    @property
    def exchange_name(self):  # pragma: no cover
        return 'to_debtors' if self.creditor_id == ROOT_CREDITOR_ID else 'to_creditors'

    @property
    def routing_key(self):  # pragma: no cover
        return i64_to_hex_routing_key(self.debtor_id if self.creditor_id == ROOT_CREDITOR_ID else self.creditor_id)

    @property
    def ttl(self):
        return int(current_app.config['APP_SIGNALBUS_MAX_DELAY_DAYS'] * SECONDS_IN_DAY)

    @property
    def commit_period(self):
        return int(current_app.config['APP_PREPARED_TRANSFER_MAX_DELAY_DAYS'] * SECONDS_IN_DAY)


class AccountPurgeSignal(Signal):
    message_type = 'AccountPurge'

    class __marshmallow__(Schema):
        debtor_id = fields.Integer()
        creditor_id = fields.Integer()
        creation_date = fields.Date()
        inserted_at = fields.DateTime(data_key='ts')

    debtor_id = db.Column(db.BigInteger, primary_key=True)
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    creation_date = db.Column(db.DATE, primary_key=True)

    @property
    def exchange_name(self):  # pragma: no cover
        return 'to_debtors' if self.creditor_id == ROOT_CREDITOR_ID else 'to_creditors'

    @property
    def routing_key(self):  # pragma: no cover
        return i64_to_hex_routing_key(self.debtor_id if self.creditor_id == ROOT_CREDITOR_ID else self.creditor_id)

    @classproperty
    def signalbus_burst_count(self):
        return current_app.config['APP_FLUSH_ACCOUNT_PURGES_BURST_COUNT']


class RejectedConfigSignal(Signal):
    message_type = 'RejectedConfig'

    class __marshmallow__(Schema):
        debtor_id = fields.Integer()
        creditor_id = fields.Integer()
        config_ts = fields.DateTime()
        config_seqnum = fields.Integer()
        negligible_amount = fields.Float()
        config_data = fields.String()
        config_flags = fields.Integer()
        inserted_at = fields.DateTime(data_key='ts')
        rejection_code = fields.String()

    debtor_id = db.Column(db.BigInteger, primary_key=True)
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    signal_id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    config_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    config_seqnum = db.Column(db.Integer, nullable=False)
    config_flags = db.Column(db.Integer, nullable=False)
    config_data = db.Column(db.String, nullable=False)
    negligible_amount = db.Column(db.REAL, nullable=False)
    rejection_code = db.Column(db.String(30), nullable=False)

    @property
    def exchange_name(self):  # pragma: no cover
        return 'to_debtors' if self.creditor_id == ROOT_CREDITOR_ID else 'to_creditors'

    @property
    def routing_key(self):  # pragma: no cover
        return i64_to_hex_routing_key(self.debtor_id if self.creditor_id == ROOT_CREDITOR_ID else self.creditor_id)

    @classproperty
    def signalbus_burst_count(self):
        return current_app.config['APP_FLUSH_REJECTED_CONFIGS_BURST_COUNT']


class PendingBalanceChangeSignal(Signal):
    message_type = 'PendingBalanceChange'
    exchange_name = 'accounts_in'

    class __marshmallow__(Schema):
        debtor_id = fields.Integer()
        creditor_id = fields.Integer()
        change_id = fields.Integer()
        coordinator_type = fields.String()
        transfer_note_format = fields.String()
        transfer_note = fields.String()
        committed_at = fields.DateTime()
        principal_delta = fields.Integer()
        other_creditor_id = fields.Integer()

    debtor_id = db.Column(db.BigInteger, primary_key=True)
    other_creditor_id = db.Column(db.BigInteger, primary_key=True)
    change_id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    creditor_id = db.Column(db.BigInteger, nullable=False)
    coordinator_type = db.Column(db.String(30), nullable=False)
    transfer_note_format = db.Column(pg.TEXT, nullable=False)
    transfer_note = db.Column(pg.TEXT, nullable=False)
    committed_at = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    principal_delta = db.Column(db.BigInteger, nullable=False)

    @property
    def routing_key(self):  # pragma: no cover
        return calc_bin_routing_key(self.debtor_id, self.creditor_id)

    @classproperty
    def signalbus_burst_count(self):
        return current_app.config['APP_FLUSH_PENDING_BALANCE_CHANGES_BURST_COUNT']
