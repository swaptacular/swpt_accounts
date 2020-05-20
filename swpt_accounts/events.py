import dramatiq
from flask import current_app
from datetime import datetime, timezone
from marshmallow import Schema, fields
from sqlalchemy.dialects import postgresql as pg
from swpt_lib.utils import i64_to_u64
from .extensions import db, broker, MAIN_EXCHANGE_NAME

__all__ = [
    'RejectedTransferSignal',
    'PreparedTransferSignal',
    'FinalizedTransferSignal',
    'AccountTransferSignal',
    'AccountChangeSignal',
    'AccountPurgeSignal',
    'RejectedConfigSignal',
    'AccountMaintenanceSignal',
]


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
    """Emitted when a request to prepare a transfer has been rejected.

    * `coordinator_type`, `coordinator_id`, and
      `coordinator_request_id` uniquely identify the transfer request
      from the coordinator's point of view, so that the coordinator
      can match the event with the originating transfer request.

    * `ts` is the moment at which this signal was emitted.

    * `rejection_code` gives the reason for the rejection of the
      transfer. Between 0 and 30 symbols, ASCII only.

    * `available_amount` is the amount currently available on the sender's
      account.

    * `debtor_id` and `sender_creditor_id` identify the sender's account.

    """

    class __marshmallow__(Schema):
        coordinator_type = fields.String()
        coordinator_id = fields.Integer()
        coordinator_request_id = fields.Integer()
        rejection_code = fields.String()
        available_amount = fields.Integer()
        debtor_id = fields.Integer()
        sender_creditor_id = fields.Integer()
        inserted_at_ts = fields.DateTime(data_key='ts')

    debtor_id = db.Column(db.BigInteger, primary_key=True)
    sender_creditor_id = db.Column(db.BigInteger, primary_key=True)
    signal_id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    coordinator_type = db.Column(db.String(30), nullable=False)
    coordinator_id = db.Column(db.BigInteger, nullable=False)
    coordinator_request_id = db.Column(db.BigInteger, nullable=False)
    rejection_code = db.Column(db.String(30), nullable=False)
    available_amount = db.Column(db.BigInteger, nullable=False)

    @property
    def event_name(self):  # pragma: no cover
        return f'on_rejected_{self.coordinator_type}_transfer_signal'


class PreparedTransferSignal(Signal):
    """Emitted when a new transfer has been prepared, or to remind that a
    prepared transfer must be finalized.

    * `debtor_id` and `sender_creditor_id` identify sender's account.

    * `transfer_id` is an opaque ID generated for the prepared
      transfer. It will never be `0`.

    * `coordinator_type`, `coordinator_id`, and
      `coordinator_request_id` uniquely identify the transfer request
      from the coordinator's point of view, so that the coordinator
      can match the event with the originating transfer request.

    * `sender_locked_amount` is the secured (prepared) amount for the
      transfer (always a positive number). The actual transferred
      (committed) amount may not exceed this number.

    * `recipient_identity` is a string, which (along with `debtor_id`)
      identifies recipient's account. Different implementations may
      use different formats for the identifier of recipient's account.

    * `ts` is the moment at which this signal was emitted.

    """

    class __marshmallow__(Schema):
        debtor_id = fields.Integer()
        sender_creditor_id = fields.Integer()
        transfer_id = fields.Integer()
        coordinator_type = fields.String()
        coordinator_id = fields.Integer()
        coordinator_request_id = fields.Integer()
        sender_locked_amount = fields.Integer()
        recipient_identity = fields.Function(lambda obj: str(i64_to_u64(obj.recipient_creditor_id)))
        inserted_at_ts = fields.DateTime(data_key='ts')

    debtor_id = db.Column(db.BigInteger, primary_key=True)
    sender_creditor_id = db.Column(db.BigInteger, primary_key=True)
    signal_id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    transfer_id = db.Column(db.BigInteger, nullable=False)
    coordinator_type = db.Column(db.String(30), nullable=False)
    coordinator_id = db.Column(db.BigInteger, nullable=False)
    coordinator_request_id = db.Column(db.BigInteger, nullable=False)
    sender_locked_amount = db.Column(db.BigInteger, nullable=False)
    recipient_creditor_id = db.Column(db.BigInteger, nullable=False)
    prepared_at_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False)

    @property
    def event_name(self):  # pragma: no cover
        return f'on_prepared_{self.coordinator_type}_transfer_signal'


class FinalizedTransferSignal(Signal):
    """Emitted when a transfer has been finalized and its corresponding
    prepared transfer record removed from the database.

    * `debtor_id` and `sender_creditor_id` identify sender's account.

    * `transfer_id` is the opaque ID generated for the prepared transfer.

    * `coordinator_type`, `coordinator_id`, and
      `coordinator_request_id` uniquely identify the transfer request
      from the coordinator's point of view, so that the coordinator
      can match the event with the originating transfer request.

    * `recipient_identity` is a string, which (along with `debtor_id`)
      identifies recipient's account. Different implementations may
      use different formats for the identifier of recipient's account.

    * `prepared_at` is the moment at which the transfer was prepared.

    * `ts` is the moment at which this signal was emitted.

    * `committed_amount` is the transferred (committed) amount. It is
      always a non-negative number. A `0` means that the transfer has
      been dismissed, or was committed but has been terminated for
      some reason.

    * `status_code` is the finalization status. Between 0 and 30
      symbols, ASCII only. If the transfer has been dismissed or
      committed successfully, the value will be "OK". If the transfer
      was committed, but has been terminated for some reason, the
      status code will be different from "OK", and will hint at the
      cause for the termination (in this case `committed_amount` will
      be zero).

    """

    class __marshmallow__(Schema):
        debtor_id = fields.Integer()
        sender_creditor_id = fields.Integer()
        transfer_id = fields.Integer()
        coordinator_type = fields.String()
        coordinator_id = fields.Integer()
        coordinator_request_id = fields.Integer()
        recipient_identity = fields.Function(lambda obj: str(i64_to_u64(obj.recipient_creditor_id)))
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
    """"Emitted when a transfer has been committed, affecting a given account.

    NOTE: Each committed transfer affects exactly two accounts: the
          sender's, and the recipient's. Therefore, exactly two
          `AccountTransferSignal`s will be emitted for each committed
          transfer.

    * `debtor_id` and `creditor_id` identify the affected account.

    * `transfer_seqnum` is the sequential number (> 0) of the
      transfer. For a newly created account, the sequential number of
      the first transfer will have its lower 40 bits set to
      `0x0000000001`, and its higher 24 bits calculated from the
      account's creation date (the number of days since Jan 1st,
      1970). Note that when an account has been removed from the
      database, and then recreated again, for this account, a gap will
      occur in the generated sequence of `transfer_seqnum`s.

    * `coordinator_type` indicates the subsystem which initiated the
      transfer.

    * `committed_at` is the moment at which the transfer was
      committed.

    * `committed_amount` is the increase in the account principal
      which the transfer caused. It can be positive (increase), or
      negative (decrease), but it can never be zero.

    * `other_party_identity` is a string, which (along with
      `debtor_id`) identifies the other party in the transfer. When
      `committed_amount` is positive, this is the sender; when
      `committed_amount` is negative, this is the recipient. Different
      implementations may use different formats for the identifier.

    * `transfer_message` contains notes from the sender. Can be any
      string that the sender wanted the recipient to see.

    * `transfer_flags` contains various flags set when the transfer
      was finalized. (This is the value of the `transfer_flags`
      parameter, with which the `finalize_prepared_transfer` actor was
      called.)

    * `account_creation_date` is the date on which the account was
      created. It can be used to differentiate transfers from
      different "epochs".

    * `account_new_principal` is the account principal, after the
      transfer has been committd (between -MAX_INT64 and MAX_INT64).

    * `previous_transfer_seqnum` is the sequential number (>= 0) of
      the previous transfer. It will always be smaller than
      `transfer_seqnum`, and sometimes the difference can be more than
      `1`. If there were no previous transfers, the value will have
      its lower 40 bits set to `0x0000000000`, and its higher 24 bits
      calculated from `account_creation_date` (the number of days
      since Jan 1st, 1970).

    * `system_flags` contains various bit-flags characterizing the
      transfer.

    * `creditor_identity` is a string, which (along with `debtor_id`)
      identifies the affected account. Different implementations may
      use different formats for the identifier. Note that while
      `creditor_id` could be a "local" identifier, recognized only by
      the system that created the account, `creditor_identity` is
      always a globally recognized identifier.

    * `transfer_id` will contain either `0`, or the ID of the
       corresponding prepared transfer. This allows the sender of a
       committed direct transfer, to reliably identify the
       corresponding prepared transfer record (using `debtor_id`,
       `creditor_id`, and `transfer_id` fields).

    """

    class __marshmallow__(Schema):
        debtor_id = fields.Integer()
        creditor_id = fields.Integer()
        transfer_seqnum = fields.Integer()
        coordinator_type = fields.String()
        committed_at_ts = fields.DateTime(data_key='committed_at')
        committed_amount = fields.Integer()
        other_party_identity = fields.Function(lambda obj: str(i64_to_u64(obj.other_creditor_id)))
        transfer_message = fields.String()
        transfer_flags = fields.Integer()
        account_creation_date = fields.Date()
        account_new_principal = fields.Integer()
        previous_transfer_seqnum = fields.Integer()
        system_flags = fields.Integer()
        creditor_identity = fields.Function(lambda obj: str(i64_to_u64(obj.creditor_id)))
        transfer_id = fields.Integer()

    TRANSFER_FLAG_IS_PUBLIC = 1
    """Indicates that all transfer details have been made public. This can
    be used, for example, to obtain a legal evidence.
    """

    SYSTEM_FLAG_IS_NEGLIGIBLE = 1
    """Indicates that the absolute value of `committed_amount` is not
    bigger than the negligible amount configured for the account.
    """

    debtor_id = db.Column(db.BigInteger, primary_key=True)
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    transfer_seqnum = db.Column(db.BigInteger, primary_key=True)
    coordinator_type = db.Column(db.String(30), nullable=False)
    committed_at_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    committed_amount = db.Column(db.BigInteger, nullable=False)
    other_creditor_id = db.Column(db.BigInteger, nullable=False)
    transfer_message = db.Column(pg.TEXT, nullable=False)
    transfer_flags = db.Column(db.Integer, nullable=False)
    account_creation_date = db.Column(db.DATE, nullable=False)
    account_new_principal = db.Column(db.BigInteger, nullable=False)
    previous_transfer_seqnum = db.Column(db.BigInteger, nullable=False)
    system_flags = db.Column(db.Integer, nullable=False)
    transfer_id = db.Column(db.BigInteger, nullable=False)


class AccountChangeSignal(Signal):
    """Emitted when there is a meaningful change in account's state, or to
    remind that the account still exists.

    * `debtor_id` and `creditor_id` identify the account.

    * `change_ts` and `change_seqnum` can be used to reliably
      determine the correct order of changes, even if they occured in
      a very short period of time. When considering two events, the
      `change_ts`s must be compared first, and only if they are equal,
      the `change_seqnum`s must be compared as well (care should be
      taken to correctly deal with the possible 32-bit integer
      wrapping).

    * `principal` is the owed amount, without the interest. (Can be
      negative, between -MAX_INT64 and MAX_INT64.)

    * `interest` is the amount of interest accumulated on the account
      before `change_ts`, but not added to the `principal` yet. (Can
      be negative.)

    * `interest_rate` is the annual rate (in percents) at which
      interest accumulates on the account. (Can be negative,
      INTEREST_RATE_FLOOR <= interest_rate <= INTEREST_RATE_CEIL.)

    * `last_transfer_seqnum` (>= 0) identifies the last account
      commit. If there were no previous account commits, the value
      will have its lower 40 bits set to `0x0000000000`, and its
      higher 24 bits calculated from `creation_date` (the number of
      days since Jan 1st, 1970).

    * `last_outgoing_transfer_date` is the date of the last committed
      transfer, for which the owner of the account was the sender. It
      can be used, for example, to determine when an account with
      negative balance can be zeroed out. (If there were no outgoing
      transfers, the value will be "1970-01-01".)

    * `last_config_ts` contains the value of the `ts` field of the
      last applied `configure_account` signal. This field can be used
      to determine whether a sent configuration signal has been
      processed. (If there were no applied configuration signals, the
      value will be "1970-01-01T00:00:00+00:00".)

    * `last_config_seqnum` contains the value of the `seqnum` field of
      the last applied `configure_account` signal. This field can be
      used to determine whether a sent configuration signal has been
      processed. (If there were no applied configuration signals, the
      value will be `0`.)

    * `creation_date` is the date on which the account was created.

    * `negligible_amount` is the maximum amount which is considered
      negligible. It is used to: 1) decide whether an account can be
      safely deleted; 2) decide whether a transfer is
      insignificant. Will always be non-negative.

    * `status` (a 32-bit integer) contains status bit-flags (see
      `models.Account`).

    * `config` contains the value of the `config` field of the most
      recently applied account configuration signal that contained a
      valid account configuration. This field can be used to determine
      whether a requested configuration change has been successfully
      applied. (Note that when the `config` field of an account
      configuration signal contains an invalid configuration, the
      signal MUST be applied, but the `config` SHOULD NOT be updated.)

    * `ts` is the moment at which this signal was emitted.

    * `ttl` is the time-to-live (in seconds) for this signal. The
      signal SHOULD be ignored if more than `ttl` seconds have elapsed
      since the signal was emitted (`ts`). Will always be bigger than
      `0.0`.

    * `creditor_identity` is a string, which (along with `debtor_id`)
      identifies the account. Different implementations may use
      different formats for the identifier. Note that while
      `creditor_id` could be a "local" identifier, recognized only by
      the system that created the account, `creditor_identity` is
      always a globally recognized identifier.

    """

    class __marshmallow__(Schema):
        debtor_id = fields.Integer()
        creditor_id = fields.Integer()
        change_ts = fields.DateTime()
        change_seqnum = fields.Integer()
        principal = fields.Integer()
        interest = fields.Float()
        interest_rate = fields.Float()
        last_transfer_seqnum = fields.Integer()
        last_outgoing_transfer_date = fields.Date()
        last_config_ts = fields.DateTime()
        last_config_seqnum = fields.Integer()
        creation_date = fields.Date()
        negligible_amount = fields.Float()
        status = fields.Integer()
        inserted_at_ts = fields.DateTime(data_key='ts')
        ttl = fields.Float()
        creditor_identity = fields.Function(lambda obj: str(i64_to_u64(obj.creditor_id)))
        config = fields.Constant('')

    debtor_id = db.Column(db.BigInteger, primary_key=True)
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    signal_id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    change_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    change_seqnum = db.Column(db.Integer, nullable=False)
    principal = db.Column(db.BigInteger, nullable=False)
    interest = db.Column(db.FLOAT, nullable=False)
    interest_rate = db.Column(db.REAL, nullable=False)
    last_transfer_seqnum = db.Column(db.BigInteger, nullable=False)
    last_outgoing_transfer_date = db.Column(db.DATE, nullable=False)
    last_config_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    last_config_seqnum = db.Column(db.Integer, nullable=False)
    creation_date = db.Column(db.DATE, nullable=False)
    negligible_amount = db.Column(db.REAL, nullable=False)
    status = db.Column(db.Integer, nullable=False)

    @property
    def ttl(self):
        return current_app.config['APP_SIGNALBUS_MAX_DELAY_DAYS'] * 86400.0


class AccountPurgeSignal(Signal):
    """Emitted when an account has been removed from the database.

    * `debtor_id` and `creditor_id` identify the account.

    * `creation_date` is the date on which the account was created.

    * `ts` is the moment at which this signal was emitted.

    * `creditor_identity` is a string, which (along with `debtor_id`)
      identifies the account. Different implementations may use
      different formats for the identifier. Note that while
      `creditor_id` could be a "local" identifier, recognized only by
      the system that created the account, `creditor_identity` is
      always globally recognized identifier.

    """

    class __marshmallow__(Schema):
        debtor_id = fields.Integer()
        creditor_id = fields.Integer()
        creation_date = fields.Date()
        inserted_at_ts = fields.DateTime(data_key='ts')
        creditor_identity = fields.Function(lambda obj: str(i64_to_u64(obj.creditor_id)))

    debtor_id = db.Column(db.BigInteger, primary_key=True)
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    creation_date = db.Column(db.DATE, primary_key=True)


class RejectedConfigSignal(Signal):
    """Emitted when a `configure_account` message has been received and
    rejected.

    * `debtor_id` and `creditor_id` identify the account.

    * `config_ts` containg the value of the `ts` field in the rejected
      `configure_account` message.

    * `config_seqnum` containg the value of the `seqnum` field in the
      rejected `configure_account` message.

    * `status_flags`, `negligible_amount`, `config` contain the values
      of the corresponding fields in the rejected `configure_account`
      message.

    * `ts` is the moment at which this signal was emitted.

    * `rejection_code` gives the reason for the rejection of the
      `configure_account` message. Between 0 and 30 symbols, ASCII
      only.

    """

    class __marshmallow__(Schema):
        debtor_id = fields.Integer()
        creditor_id = fields.Integer()
        config_ts = fields.DateTime()
        config_seqnum = fields.Integer()
        status_flags = fields.Integer()
        negligible_amount = fields.Float(),
        config = fields.String()
        inserted_at_ts = fields.DateTime(data_key='ts')
        rejection_code = fields.String()

    debtor_id = db.Column(db.BigInteger, primary_key=True)
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    signal_id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    config_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    config_seqnum = db.Column(db.Integer, nullable=False)
    status_flags = db.Column(db.SmallInteger, nullable=False)
    negligible_amount = db.Column(db.REAL, nullable=False)
    config = db.Column(db.String, nullable=False)
    rejection_code = db.Column(db.String(30), nullable=False)


class AccountMaintenanceSignal(Signal):
    """"Emitted when a maintenance operation request is received for a
    given account.

    Maintenance operations are:

    - `actor.change_interest_rate`
    - `actor.capitalize_interest`
    - `actor.zero_out_negative_balance`
    - `actor.try_to_delete_account`

    The event indicates that more maintenance operation requests can
    be made for the given account, without the risk of flooding the
    signal bus with account maintenance requests.

    * `debtor_id` and `creditor_id` identify the account.

    * `request_ts` is the timestamp of the received maintenance
      operation request. It can be used the match the
      `AccountMaintenanceSignal` with the originating request.

    * `received_at` is the moment at which the maintenance operation
      request was received. (Note that `request_ts` and `received_at`
      are generated on different servers, so there might be some
      discrepancies.)

    """

    class __marshmallow__(Schema):
        debtor_id = fields.Integer()
        creditor_id = fields.Integer()
        request_ts = fields.DateTime()
        inserted_at_ts = fields.DateTime(data_key='received_at')

    debtor_id = db.Column(db.BigInteger, primary_key=True)
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    signal_id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    request_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
