import dramatiq
from marshmallow import Schema, fields
from sqlalchemy.dialects import postgresql as pg
from .extensions import db, broker, MAIN_EXCHANGE_NAME

__all__ = [
    'PreparedTransferSignal',
    'RejectedTransferSignal',
    'FinalizedTransferSignal',
    'AccountChangeSignal',
    'AccountPurgeSignal',
    'AccountCommitSignal',
]


class Signal(db.Model):
    __abstract__ = True

    # TODO: Define `send_signalbus_messages` class method, set
    #      `ModelClass.signalbus_autoflush = False` and
    #      `ModelClass.signalbus_burst_count = N` in models.

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


class PreparedTransferSignal(Signal):
    """Emitted when a new transfer has been prepared.

    * `transfer_id` is an opaque ID generated for the prepared transfer.

    * `coordinator_type`, `coordinator_id`, and
      `coordinator_request_id` uniquely identify the transfer request
      from the coordinator's point of view, so that the coordinator
      can match the event with the originating transfer request.

    * `sender_locked_amount` is the secured (prepared) amount for the
      transfer (always a positive number). The actual transferred
      (committed) amount may not exceed this number.

    """

    debtor_id = db.Column(db.BigInteger, primary_key=True)
    sender_creditor_id = db.Column(db.BigInteger, primary_key=True)
    transfer_id = db.Column(db.BigInteger, primary_key=True)
    coordinator_type = db.Column(db.String(30), nullable=False)
    coordinator_id = db.Column(db.BigInteger, nullable=False)
    coordinator_request_id = db.Column(db.BigInteger, nullable=False)
    sender_locked_amount = db.Column(db.BigInteger, nullable=False)
    recipient_creditor_id = db.Column(db.BigInteger, nullable=False)
    prepared_at_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False)

    @property
    def event_name(self):  # pragma: no cover
        return f'on_prepared_{self.coordinator_type}_transfer_signal'


class RejectedTransferSignal(Signal):
    """Emitted when a request to prepare a transfer has been rejected.

    * `coordinator_type`, `coordinator_id`, and
      `coordinator_request_id` uniquely identify the transfer request
      from the coordinator's point of view, so that the coordinator
      can match the event with the originating transfer request.

    * `details` is a JSON object describing why the transfer has been
      rejected. For example: `{"errorCode": "ACC005", "message": "The
      available balance is insufficient.", "avlBalance": 0}`. The
      properties "errorCode", and "message" are guarenteed to be
      available.

    """

    class __marshmallow__(Schema):
        coordinator_type = fields.String()
        coordinator_id = fields.Integer()
        coordinator_request_id = fields.Integer()
        details = fields.Raw()

    debtor_id = db.Column(db.BigInteger, primary_key=True)
    signal_id = db.Column(db.BigInteger, primary_key=True, autoincrement=True)
    coordinator_type = db.Column(db.String(30), nullable=False)
    coordinator_id = db.Column(db.BigInteger, nullable=False)
    coordinator_request_id = db.Column(db.BigInteger, nullable=False)
    details = db.Column(pg.JSON, nullable=False)

    @property
    def event_name(self):  # pragma: no cover
        return f'on_rejected_{self.coordinator_type}_transfer_signal'


class FinalizedTransferSignal(Signal):
    """Emitted when a transfer has been finalized and its corresponding
    prepared transfer record removed from the database.

    * `transfer_id` is the opaque ID generated for the prepared transfer.

    * `coordinator_type`, `coordinator_id`, and
      `coordinator_request_id` uniquely identify the transfer request
      from the coordinator's point of view, so that the coordinator
      can match the event with the originating transfer request.

    * `committed_amount` is the transferred (committed) amount. It is
      always a non-negative number. A `0` means that the transfer has
      been dismissed.

    """

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

    @property
    def event_name(self):  # pragma: no cover
        return f'on_finalized_{self.coordinator_type}_transfer_signal'


class AccountChangeSignal(Signal):
    """Emitted when there is a meaningful change in account's state.

    * `change_ts` and `change_seqnum` can be used to reliably
      determine the correct order of changes, even if they occured in
      a very short period of time. When considering two events, the
      `change_ts`s must be compared first, and only if they are equal,
      the `change_seqnum`s must be compared as well (care should be
      taken to correctly deal with the possible 32-bit signed integer
      wrapping).

    * `principal` is the owed amount, without the interest. (Can be
      negative, between -MAX_INT64 and MAX_INT64.)

    * `interest` is the amount of interest accumulated on the account
      before `change_ts`, but not added to the `principal` yet. (Can
      be negative.)

    * `interest_rate` is the annual rate (in percents) at which
      interest accumulates on the account. (Can be negative.)

    * `last_transfer_seqnum` identifies the last account commit.

    * `last_outgoing_transfer_date` is the date of the last committed
      transfer, for which the owner of the account was the sender. It
      can be used, for example, to determine when an account with
      negative balance can be zeroed out. (If there were no outgoing
      transfers, the value will be "1900-01-01".)

    * `last_config_change_ts` is the timestamp of the last applied
      account configuration change. That is: the `change_ts` field of
      the last effectual `configure_account` signal. It can be used to
      determine whether a scheduled configuration change has been
      applied. (If there were no configuration changes, the value will
      be "1900-01-01T00:00:00+00:00".)

    * `last_config_change_seqnum` is the sequential number of the last
      applied account configuration change. That is: the
      `change_seqnum` field of the last effectual `configure_account`
      signal. It can be used to determine whether a scheduled
      configuration change has been applied. (If there were no
      configuration changes, the value will be `0`.)

    * `creation_date` is the date on which the account was created.

    * `negligible_amount` is the maximum amount which is considered
      negligible. It can be used, for example, to decide whether an
      account can be safely deleted.

    * `status` contains status bit-flags (see `models.Account`).

    """

    debtor_id = db.Column(db.BigInteger, primary_key=True)
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    change_ts = db.Column(db.TIMESTAMP(timezone=True), primary_key=True)
    change_seqnum = db.Column(db.Integer, primary_key=True)
    principal = db.Column(db.BigInteger, nullable=False)
    interest = db.Column(db.FLOAT, nullable=False)
    interest_rate = db.Column(db.REAL, nullable=False)
    last_transfer_seqnum = db.Column(db.BigInteger, nullable=False)
    last_outgoing_transfer_date = db.Column(db.DATE, nullable=False)
    last_config_change_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    last_config_change_seqnum = db.Column(db.Integer, nullable=False)
    creation_date = db.Column(db.DATE, nullable=False)
    negligible_amount = db.Column(db.REAL, nullable=False)
    status = db.Column(db.SmallInteger, nullable=False)


class AccountPurgeSignal(Signal):
    """Emitted when an account has been removed from the database.

    * `creation_date` is the date on which the account was created.

    """

    debtor_id = db.Column(db.BigInteger, primary_key=True)
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    creation_date = db.Column(db.DATE, primary_key=True)


class AccountCommitSignal(Signal):
    """"Emitted when a transfer has been committed, affecting a given account.

    NOTE: Each committed transfer affects exactly two accounts: the
          sender's, and the recipient's. Therefore, exactly two
          `AccountCommitSignal`s will be emitted for each committed
          transfer.

    * `debtor_id` and `creditor_id` identify the affected account.

    * `transfer_seqnum` is the sequential number of the transfer. For
      a newly created account, the sequential number of the first
      transfer will have its lower 40 bits set to `0x0000000001`, and
      its higher 24 bits calculated from the account's creation date
      (the number of days since Jan 1st, 2020). Note that when an
      account has been removed from the database, and then recreated
      again, for this account, a gap will occur in the generated
      sequence of `transfer_seqnum`s.

    * `coordinator_type` indicates the subsystem which initiated the
      transfer.

    * `committed_at_ts` is the moment at which the transfer was
      committed.

    * `committed_amount` is the increase in the account principal
      which the transfer caused. It can be positive (increase), or
      negative (decrease), but it can never be zero.

    * `other_creditor_id` is the other party in the transfer. When
      `committed_amount` is positive, this is the sender. When
      `committed_amount` is negative, this is the recipient.

    * `transfer_info` contains notes from the sender. Can be any JSON
      object that the sender wanted the recipient to see.

    * `account_creation_date` is the date on which the account was
      created. It can be used to differentiate transfers from
      different "epochs".

    * `account_new_principal` is the account principal, after the
      transfer has been committd.

    """

    debtor_id = db.Column(db.BigInteger, primary_key=True)
    creditor_id = db.Column(db.BigInteger, primary_key=True)
    transfer_seqnum = db.Column(db.BigInteger, primary_key=True)
    coordinator_type = db.Column(db.String(30), nullable=False)
    committed_at_ts = db.Column(db.TIMESTAMP(timezone=True), nullable=False)
    committed_amount = db.Column(db.BigInteger, nullable=False)
    other_creditor_id = db.Column(db.BigInteger, nullable=False)
    transfer_info = db.Column(pg.JSON, nullable=False)
    account_creation_date = db.Column(db.DATE, nullable=False)
    account_new_principal = db.Column(db.BigInteger, nullable=False)
