import json
from base64 import b16encode
from flask import current_app
from datetime import datetime, timezone
from marshmallow import Schema, fields
from sqlalchemy.dialects import postgresql as pg
from swpt_pythonlib.utils import i64_to_u64, i64_to_hex_routing_key, calc_bin_routing_key
from swpt_accounts.extensions import db, publisher, TO_COORDINATORS_EXCHANGE, TO_DEBTORS_EXCHANGE, \
    TO_CREDITORS_EXCHANGE, ACCOUNTS_IN_EXCHANGE
from swpt_pythonlib import rabbitmq

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


class classproperty(object):
    def __init__(self, f):
        self.f = f

    def __get__(self, obj, owner):
        return self.f(owner)


class Signal(db.Model):
    __abstract__ = True

    @classmethod
    def send_signalbus_messages(cls, objects):  # pragma: no cover
        assert(all(isinstance(obj, cls) for obj in objects))
        messages = [obj._create_message() for obj in objects]
        publisher.publish_messages(messages)

    def send_signalbus_message(self):  # pragma: no cover
        self.send_signalbus_messages([self])

    def _create_message(self):  # pragma: no cover
        data = self.__marshmallow_schema__.dump(self)
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
            type=data['type'],
            headers=headers,
        )
        body = json.dumps(
            data,
            ensure_ascii=False,
            check_circular=False,
            allow_nan=False,
            separators=(',', ':'),
        ).encode('utf8')

        return rabbitmq.Message(
            exchange=self.exchange_name,
            routing_key=self.routing_key,
            body=body,
            properties=properties,
            mandatory=True,
        )

    inserted_at = db.Column(db.TIMESTAMP(timezone=True), nullable=False, default=get_now_utc)


class RejectedTransferSignal(Signal):
    exchange_name = TO_COORDINATORS_EXCHANGE

    class __marshmallow__(Schema):
        type = fields.Constant('RejectedTransfer')
        coordinator_type = fields.String()
        coordinator_id = fields.Integer()
        coordinator_request_id = fields.Integer()
        status_code = fields.String()
        total_locked_amount = fields.Integer()
        debtor_id = fields.Integer()
        sender_creditor_id = fields.Integer(data_key='creditor_id')
        inserted_at = fields.DateTime(data_key='ts')

    __marshmallow_schema__ = __marshmallow__()

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


class PreparedTransferSignal(Signal):
    exchange_name = TO_COORDINATORS_EXCHANGE

    class __marshmallow__(Schema):
        type = fields.Constant('PreparedTransfer')
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

    __marshmallow_schema__ = __marshmallow__()

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


class FinalizedTransferSignal(Signal):
    exchange_name = TO_COORDINATORS_EXCHANGE

    class __marshmallow__(Schema):
        type = fields.Constant('FinalizedTransfer')
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

    __marshmallow_schema__ = __marshmallow__()

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


class AccountTransferSignal(Signal):
    class __marshmallow__(Schema):
        type = fields.Constant('AccountTransfer')
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

    __marshmallow_schema__ = __marshmallow__()

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
        return TO_DEBTORS_EXCHANGE if self.creditor_id == ROOT_CREDITOR_ID else TO_CREDITORS_EXCHANGE

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
    class __marshmallow__(Schema):
        type = fields.Constant('AccountUpdate')
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

    __marshmallow_schema__ = __marshmallow__()

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
        return TO_DEBTORS_EXCHANGE if self.creditor_id == ROOT_CREDITOR_ID else TO_CREDITORS_EXCHANGE

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
    class __marshmallow__(Schema):
        type = fields.Constant('AccountPurge')
        debtor_id = fields.Integer()
        creditor_id = fields.Integer()
        creation_date = fields.Date()
        inserted_at = fields.DateTime(data_key='ts')

    __marshmallow_schema__ = __marshmallow__()

    debtor_id = db.Column(db.BigInteger, primary_key=True)
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    creation_date = db.Column(db.DATE, primary_key=True)

    @property
    def exchange_name(self):  # pragma: no cover
        return TO_DEBTORS_EXCHANGE if self.creditor_id == ROOT_CREDITOR_ID else TO_CREDITORS_EXCHANGE

    @property
    def routing_key(self):  # pragma: no cover
        return i64_to_hex_routing_key(self.debtor_id if self.creditor_id == ROOT_CREDITOR_ID else self.creditor_id)

    @classproperty
    def signalbus_burst_count(self):
        return current_app.config['APP_FLUSH_ACCOUNT_PURGES_BURST_COUNT']


class RejectedConfigSignal(Signal):
    class __marshmallow__(Schema):
        type = fields.Constant('RejectedConfig')
        debtor_id = fields.Integer()
        creditor_id = fields.Integer()
        config_ts = fields.DateTime()
        config_seqnum = fields.Integer()
        negligible_amount = fields.Float()
        config_data = fields.String()
        config_flags = fields.Integer()
        inserted_at = fields.DateTime(data_key='ts')
        rejection_code = fields.String()

    __marshmallow_schema__ = __marshmallow__()

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
        return TO_DEBTORS_EXCHANGE if self.creditor_id == ROOT_CREDITOR_ID else TO_CREDITORS_EXCHANGE

    @property
    def routing_key(self):  # pragma: no cover
        return i64_to_hex_routing_key(self.debtor_id if self.creditor_id == ROOT_CREDITOR_ID else self.creditor_id)

    @classproperty
    def signalbus_burst_count(self):
        return current_app.config['APP_FLUSH_REJECTED_CONFIGS_BURST_COUNT']


class PendingBalanceChangeSignal(Signal):
    exchange_name = ACCOUNTS_IN_EXCHANGE

    class __marshmallow__(Schema):
        type = fields.Constant('PendingBalanceChange')
        debtor_id = fields.Integer()
        creditor_id = fields.Integer()
        change_id = fields.Integer()
        coordinator_type = fields.String()
        transfer_note_format = fields.String()
        transfer_note = fields.String()
        committed_at = fields.DateTime()
        principal_delta = fields.Integer()
        other_creditor_id = fields.Integer()

    __marshmallow_schema__ = __marshmallow__()

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
