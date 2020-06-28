import dramatiq
from flask import current_app
from datetime import datetime, timezone
from marshmallow import Schema, fields
from sqlalchemy.dialects import postgresql as pg
from swpt_lib.utils import i64_to_u64
from swpt_lib.endpoints import build_url
from .extensions import db, broker, MAIN_EXCHANGE_NAME

__all__ = [
    'RejectedTransferSignal',
    'PreparedTransferSignal',
    'FinalizedTransferSignal',
    'AccountTransferSignal',
    'AccountUpdateSignal',
    'AccountPurgeSignal',
    'RejectedConfigSignal',
    'AccountMaintenanceSignal',
]

INTEREST_RATE_FLOOR = -50.0
INTEREST_RATE_CEIL = 100.0


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
        broker.publish_message(message, exchange=MAIN_EXCHANGE_NAME, routing_key=routing_key)

    inserted_at_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False, default=get_now_utc)


class RejectedTransferSignal(Signal):
    class __marshmallow__(Schema):
        coordinator_type = fields.String()
        coordinator_id = fields.Integer()
        coordinator_request_id = fields.Integer()
        rejection_code = fields.String()
        available_amount = fields.Integer()
        total_locked_amount = fields.Integer()
        debtor_id = fields.Integer()
        sender_creditor_id = fields.Integer(data_key='creditor_id')
        inserted_at_ts = fields.DateTime(data_key='ts')
        recipient = fields.String()

    debtor_id = db.Column(db.BigInteger, primary_key=True)
    sender_creditor_id = db.Column(db.BigInteger, primary_key=True)
    signal_id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    coordinator_type = db.Column(db.String(30), nullable=False)
    coordinator_id = db.Column(db.BigInteger, nullable=False)
    coordinator_request_id = db.Column(db.BigInteger, nullable=False)
    rejection_code = db.Column(db.String(30), nullable=False)
    available_amount = db.Column(db.BigInteger, nullable=False)
    total_locked_amount = db.Column(db.BigInteger, nullable=False)
    recipient = db.Column(db.String, nullable=False)

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
        prepared_at_ts = fields.DateTime(data_key='prepared_at')
        inserted_at_ts = fields.DateTime(data_key='ts')
        gratis_period = fields.Integer()
        demurrage_rate = fields.Float()
        deadline = fields.DateTime()

    debtor_id = db.Column(db.BigInteger, primary_key=True)
    sender_creditor_id = db.Column(db.BigInteger, primary_key=True)
    signal_id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    transfer_id = db.Column(db.BigInteger, nullable=False)
    coordinator_type = db.Column(db.String(30), nullable=False)
    coordinator_id = db.Column(db.BigInteger, nullable=False)
    coordinator_request_id = db.Column(db.BigInteger, nullable=False)
    locked_amount = db.Column(db.BigInteger, nullable=False)
    recipient_creditor_id = db.Column(db.BigInteger, nullable=False)
    prepared_at_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    gratis_period = db.Column(db.Integer, nullable=False)
    demurrage_rate = db.Column(db.FLOAT, nullable=False)
    deadline = db.Column(db.TIMESTAMP(timezone=True), nullable=False)

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
        recipient = fields.Function(lambda obj: str(i64_to_u64(obj.recipient_creditor_id)))
        prepared_at_ts = fields.DateTime(data_key='prepared_at')
        finalized_at_ts = fields.DateTime(data_key='ts')
        committed_amount = fields.Integer()
        status_code = fields.String()

    debtor_id = db.Column(db.BigInteger, primary_key=True)
    sender_creditor_id = db.Column(db.BigInteger, primary_key=True)
    transfer_id = db.Column(db.BigInteger, primary_key=True)
    coordinator_type = db.Column(db.String(30), nullable=False)
    coordinator_id = db.Column(db.BigInteger, nullable=False)
    coordinator_request_id = db.Column(db.BigInteger, nullable=False)
    recipient_creditor_id = db.Column(db.BigInteger, nullable=False)
    prepared_at_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    finalized_at_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    committed_amount = db.Column(db.BigInteger, nullable=False)
    status_code = db.Column(db.String(30), nullable=False)

    @property
    def event_name(self):  # pragma: no cover
        return f'on_finalized_{self.coordinator_type}_transfer_signal'


class AccountTransferSignal(Signal):
    class __marshmallow__(Schema):
        debtor_id = fields.Integer()
        creditor_id = fields.Integer()
        transfer_number = fields.Integer()
        coordinator_type = fields.String()
        committed_at_ts = fields.DateTime(data_key='committed_at')
        acquired_amount = fields.Integer()
        transfer_note = fields.String()
        transfer_flags = fields.Integer()
        creation_date = fields.Date()
        principal = fields.Integer()
        previous_transfer_number = fields.Integer()
        sender = fields.Function(lambda obj: str(i64_to_u64(obj.sender_creditor_id)))
        recipient = fields.Function(lambda obj: str(i64_to_u64(obj.recipient_creditor_id)))
        inserted_at_ts = fields.DateTime(data_key='ts')

    SYSTEM_FLAG_IS_NEGLIGIBLE = 1
    """Indicates that the absolute value of `committed_amount` is not
    bigger than the negligible amount configured for the account.
    """

    debtor_id = db.Column(db.BigInteger, primary_key=True)
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    transfer_number = db.Column(db.BigInteger, primary_key=True)
    coordinator_type = db.Column(db.String(30), nullable=False)
    committed_at_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    acquired_amount = db.Column(db.BigInteger, nullable=False)
    other_creditor_id = db.Column(db.BigInteger, nullable=False)
    transfer_note = db.Column(pg.TEXT, nullable=False)
    transfer_flags = db.Column(db.Integer, nullable=False)
    creation_date = db.Column(db.DATE, nullable=False)
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
        demurrage_rate = fields.Constant(INTEREST_RATE_FLOOR)
        gratis_period = fields.Integer()
        commit_period = fields.Integer()
        last_interest_rate_change_ts = fields.DateTime()
        last_transfer_number = fields.Integer()
        last_transfer_committed_at_ts = fields.DateTime(data_key='last_transfer_committed_at')
        last_outgoing_transfer_date = fields.Date()
        last_config_ts = fields.DateTime()
        last_config_seqnum = fields.Integer()
        creation_date = fields.Date()
        negligible_amount = fields.Float()
        config = fields.Constant('')
        config_flags = fields.Integer()
        status_flags = fields.Integer()
        inserted_at_ts = fields.DateTime(data_key='ts')
        ttl = fields.Integer()
        account_identity = fields.Function(lambda obj: str(i64_to_u64(obj.creditor_id)))
        debtor_url = fields.Function(lambda obj: build_url('debtor', debtorId=obj.debtor_id))

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
    last_transfer_committed_at_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    last_outgoing_transfer_date = db.Column(db.DATE, nullable=False)
    last_config_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    last_config_seqnum = db.Column(db.Integer, nullable=False)
    creation_date = db.Column(db.DATE, nullable=False)
    negligible_amount = db.Column(db.REAL, nullable=False)
    config_flags = db.Column(db.Integer, nullable=False)
    status_flags = db.Column(db.Integer, nullable=False)

    @property
    def ttl(self):
        return int(current_app.config['APP_SIGNALBUS_MAX_DELAY_DAYS'] * 86400)

    @property
    def gratis_period(self):
        return int(current_app.config['APP_PREPARED_TRANSFER_GRATIS_SECONDS'])

    @property
    def commit_period(self):
        return self.get_commit_period()

    @classmethod
    def get_commit_period(cls) -> int:
        # NOTE: To avoid timing out prepared transfers due to signal
        # bus delays, here we ensure that prepared transfers' maximum
        # delay is not smaller than the allowed signal bus delay.
        days = max(
            current_app.config['APP_PREPARED_TRANSFER_MAX_DELAY_DAYS'],
            current_app.config['APP_SIGNALBUS_MAX_DELAY_DAYS'],
        )
        return int(86400 * days)


class AccountPurgeSignal(Signal):
    class __marshmallow__(Schema):
        debtor_id = fields.Integer()
        creditor_id = fields.Integer()
        creation_date = fields.Date()
        inserted_at_ts = fields.DateTime(data_key='ts')

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
        config = fields.String()
        config_flags = fields.Integer()
        inserted_at_ts = fields.DateTime(data_key='ts')
        rejection_code = fields.String()

    debtor_id = db.Column(db.BigInteger, primary_key=True)
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    signal_id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    config_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    config_seqnum = db.Column(db.Integer, nullable=False)
    config_flags = db.Column(db.Integer, nullable=False)
    negligible_amount = db.Column(db.REAL, nullable=False)
    config = db.Column(db.String, nullable=False)
    rejection_code = db.Column(db.String(30), nullable=False)


class AccountMaintenanceSignal(Signal):
    """"Emitted when a maintenance operation request is received for a
    given account.

    Maintenance operations are:

    - `actor.capitalize_interest`
    - `actor.zero_out_negative_balance`
    - `actor.try_to_delete_account`
    - `actor.try_to_change_interest_rate`

    The event indicates that more maintenance operation requests can
    be made for the given account, without the risk of flooding the
    signal bus with account maintenance requests.

    * `debtor_id` and `creditor_id` identify the account.

    * `request_ts` is the timestamp of the received maintenance
      operation request. It can be used to the match the
      `AccountMaintenanceSignal` with the originating request.

    * `ts` is the moment at which message was sent. (Note that
      `request_ts` and `ts` are generated on different servers, so
      there might be some discrepancies.)

    """

    class __marshmallow__(Schema):
        debtor_id = fields.Integer()
        creditor_id = fields.Integer()
        request_ts = fields.DateTime()
        inserted_at_ts = fields.DateTime(data_key='ts')

    debtor_id = db.Column(db.BigInteger, primary_key=True)
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    signal_id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    request_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
