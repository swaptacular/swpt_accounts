from base64 import b16encode
import dramatiq
from flask import current_app
from datetime import datetime, timezone
from marshmallow import Schema, fields
from sqlalchemy.dialects import postgresql as pg
from swpt_lib.utils import i64_to_u64
from swpt_accounts.extensions import db, protocol_broker, MAIN_EXCHANGE_NAME

__all__ = [
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


def get_now_utc():
    return datetime.now(tz=timezone.utc)


class Signal(db.Model):
    __abstract__ = True

    # TODO: Define `send_signalbus_messages` class method, set
    #      `ModelClass.signalbus_autoflush = False` and
    #      `ModelClass.signalbus_burst_count = N` in models. Make sure
    #      TTL is set properly for the messages.

    # TODO: Move this logic `swpt_lib`. Consider implementing a signal
    #       metaclass.

    queue_name = None

    @property
    def event_name(self):  # pragma: no cover
        model = type(self)
        return f'on_{model.__tablename__}'

    def send_signalbus_message(self):  # pragma: no cover
        model = type(self)
        if model.queue_name is None:
            assert not hasattr(model, 'actor_name'), \
                'SignalModel.actor_name is set, but SignalModel.queue_name is not'
            actor_name = self.event_name
            routing_key = f'events.{actor_name}'
        else:
            actor_name = model.actor_name
            routing_key = model.queue_name
        data = model.__marshmallow_schema__.dump(self)
        message = dramatiq.Message(
            queue_name=model.queue_name,
            actor_name=actor_name,
            args=(),
            kwargs=data,
            options={},
        )
        protocol_broker.publish_message(message, exchange=MAIN_EXCHANGE_NAME, routing_key=routing_key)

    inserted_at = db.Column(db.TIMESTAMP(timezone=True), nullable=False, default=get_now_utc)


class RejectedTransferSignal(Signal):
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

    @property
    def event_name(self):  # pragma: no cover
        return f'on_rejected_{self.coordinator_type}_transfer_signal'


class PreparedTransferSignal(Signal):
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

    @property
    def event_name(self):  # pragma: no cover
        return f'on_prepared_{self.coordinator_type}_transfer_signal'


class FinalizedTransferSignal(Signal):
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

    @property
    def event_name(self):  # pragma: no cover
        return f'on_finalized_{self.coordinator_type}_transfer_signal'


class AccountTransferSignal(Signal):
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

    @property
    def sender_creditor_id(self):
        return self.other_creditor_id if self.acquired_amount >= 0 else self.creditor_id

    @property
    def recipient_creditor_id(self):
        return self.other_creditor_id if self.acquired_amount < 0 else self.creditor_id


class AccountUpdateSignal(Signal):
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

    @property
    def ttl(self):
        return int(current_app.config['APP_SIGNALBUS_MAX_DELAY_DAYS'] * SECONDS_IN_DAY)

    @property
    def commit_period(self):
        return int(current_app.config['APP_PREPARED_TRANSFER_MAX_DELAY_DAYS'] * SECONDS_IN_DAY)


class AccountPurgeSignal(Signal):
    class __marshmallow__(Schema):
        debtor_id = fields.Integer()
        creditor_id = fields.Integer()
        creation_date = fields.Date()
        inserted_at = fields.DateTime(data_key='ts')

    debtor_id = db.Column(db.BigInteger, primary_key=True)
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    creation_date = db.Column(db.DATE, primary_key=True)


class RejectedConfigSignal(Signal):
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


class PendingBalanceChangeSignal(Signal):
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
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    change_id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    coordinator_type = db.Column(db.String(30), nullable=False)
    transfer_note_format = db.Column(pg.TEXT, nullable=False)
    transfer_note = db.Column(pg.TEXT, nullable=False)
    committed_at = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    principal_delta = db.Column(db.BigInteger, nullable=False)
    other_creditor_id = db.Column(db.BigInteger, nullable=False)
