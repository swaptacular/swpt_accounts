Incoming messages
=================

ConfigureAccount
----------------

Upon receiving this message, the server makes sure that the specified
account exists, and updates its configuration settings.

debtor_id : int64
   The ID of the debtor.

creditor_id : int64
   Along with ``debtor_id``, identifies the account.

signal_ts : date-time
   The moment at which this message was sent (the message
   timestamp). For a given account, later `ConfigureAccount`_ messages
   MUST have later or equal timestamps, compared to earlier messages.

signal_seqnum : int32
   The sequential number of the message. For a given account, later
   `ConfigureAccount`_ messages SHOULD have bigger sequential numbers,
   compared to earlier messages. Note that when the maximum ``int32``
   value is reached, the next value SHOULD be ``-2147483648`` (signeld
   32-bit integer wrapping).

status_flags : int16
   Account configuration flags. Different server implementations may
   use these flags for different purposes. For the lowest bit (bit
   ``0``) is reserved the meaning "scheduled for deletion". If this
   bit is set, server implementations SHOULD continuously watch the
   account, and if it is reasonably certain that amount bigger that
   ``negligible_amount`` can not be lost on the account, and at least
   48 hours have passed since account's creation, the account SHOULD
   be removed from the server's database. If those condition are not
   met, accounts SHOULD NOT be removed. When an account has been
   removed from the server's database, a `AccountPurge`_ message MUST
   be sent.

negligible_amount : float
   The maximum amount that should be considered negligible. It MUST be
   a non-negative number. It can be used to: 1) decide whether an
   account can be safely deleted; 2) decide whether an incoming
   transfer is insignificant.

config : string
   Additional account configuration information. Different server
   implementations may use different formats for this field.

Server implementations SHOULD ignore `ConfigureAccount`_ messages
whose timestamps are too far in the past, because such messages may
"resurrect" accounts that have been removed from the database for
good.

When server implementations process a message, they MUST first verify
whether the specified account already exists:

* *If the specified account does not exist*, the server implementation
  MUST attempt to create a new account with the requested
  configuration settings. If the new account has been successfully
  created, an `AccountChange`_ message containing the configuration
  MUST be sent. Otherwise a `RejectedConfig`_ message MUST be sent.

* *If the specified account already exists*, the server implementation
  MUST decide whether the same or a later `ConfigureAccount`_ message
  has been processed already. To do this, the server implementation
  MUST compare the values of ``signal_ts`` and ``signal_seqnum``
  fields in the received message, to the values of these fields in the
  latest processed `ConfigureAccount`_ message [#]_ . If the received
  message turns out to be an old one, it MUST be ignored. Otherwise,
  an attempt MUST be made to update the account's configuration with
  the requested new configuration. If the new configuration has been
  successfully applied, an `AccountChange`_ message containing the new
  configuration MUST be sent. Otherwise a `RejectedConfig`_ message
  MUST be sent, containing the rejected requested configuration.

.. [#] Server implementations MUST first compare the ``signal_ts``
  fields, and only if they are equal, the ``signal_seqnum`` fields
  MUST be compared as well. Note that when comparing the
  ``signal_seqnum`` fields, server implementations MUST correctly deal
  with the possible 32-bit integer wrapping. For example, to decide
  whether ``seqnum2`` is later than ``seqnum1``, the following
  expression MAY be used: ``0 < (seqnum2 - seqnum1) % 0x100000000 <
  0x80000000``. Timestamps should also be compared with care, because
  precision might have been lost when they were saved to the database.


PrepareTransfer
---------------

Upon receiving this message, the server tries to secure some amount,
to eventually transfer it from sender's account to recipient's
account.

coordinator_type : string
   Indicates the subsystem which sent this message. MUST be between 1
   and 30 symbols, ASCII only.

coordinator_id : int64
   Along with ``coordinator_type``, identifies the client that sent
   this message (the *coordinator*).

coordinator_request_id : int64
   Along with ``coordinator_type`` and ``coordinator_id``, uniquely
   identifies this message from the coordinator's point of view, so
   that the coordinator can pair this request with the received
   response message.

min_amount : int64
   The secured amount MUST be equal or bigger than this value. This
   value MUST be a positive number.

max_amount : int64
   The secured amount SHOULD NOT exceed this value. This value MUST be
   equal or bigger than the value of ``min_amount``.

debtor_id : int64
   The ID of the debtor.

sender_creditor_id : int64
   Along with ``debtor_id``, identifies the sender's account.

recipient_identity : string
   A string which (along with ``debtor_id``) globally identifies the
   recipient's account. Different server implementations may use
   different formats for this string. Note that ``sender_creditor_id``
   is an ID which is recognizable only by the system that created the
   sender's account. This identifier (along with ``debtor_id``), on
   the other hand, MUST provide enough information to globally
   identify the recipient's account (an IBAN for example).
   
signal_ts : date-time
   The moment at which this message was sent (the message timestamp).

minimum_account_balance : int64
   Determines the minimum amount that SHOULD remain available on
   sender's account after the requested amount has been secured. This
   can be a negative number.

When server implementations process a `PrepareTransfer`_ message they:

* SHOULD try to secure as big amount as possible, within the requested
  limits (between ``min_amount`` and ``max_amount``).

* SHOULD NOT prepare a transfer without verifying that the recipient's
  account exists, and does accept incoming transfers.

* SHOULD NOT allow transfers in which the sender and the recipient is
  the same account.

* MUST send a `PreparedTransfer`_ message if the requested transfer
  has been successfully prepared.

* MUST send a `RejectedTransfer`_ message if the requested transfer
  can not be prepared.

* MUST guarantee that when a transfer has been prepared, the
  probability for the success of the eventual commit is very
  high. Notably, the secured amount MUST be locked, so that until the
  prepared transfer is finalized, the amount is not available for
  other transfers.

* MUST NOT impose unnecessary limitations on the time in which the
  prepared transfer can/should be committed. All imposed limitations
  MUST be precisely defined, and known in advance.


FinalizePreparedTransfer
------------------------

Upon receiving this message, the server finalizes a prepared transfer.

debtor_id : int64
   The ID of the debtor.

sender_creditor_id : int64
   Along with ``debtor_id``, identifies the sender's account.

transfer_id : int64
   The opaque ID generated for the prepared transfer. It MUST always
   be a positive number. This ID, along with ``debtor_id`` and
   ``sender_creditor_id``, uniquely identifies the prepared transfer
   that should be finalized.

committed_amount : int64
   The amount that should be transferred. This MUST be a non-negative
   number, which MUST NOT exceed the value of the
   ``sender_locked_amount`` field in the corresponding
   `PreparedTransfer`_ message. A ``0`` signifies that the transfer
   MUST be dismissed.

transfer_message : string
   A string that the coordinator (the client that finalizes the
   prepared transfer) wants the recipient and the sender to see. If
   the transfer is being dismissed, this MUST be an empty string.

transfer_flags : int32
   Various bit-flags that the coordinator (the client that finalizes
   the prepared transfer) wants the recipient and the sender to
   see. If the transfer is being dismissed, this MUST be ``0``.

When server implementations processes the message, they MUST first
verify whether the specified prepared transfer exists in server's
database:

* *If the specified prepared transfer does not exist*, the message
  MUST be ignored.

* *If the specified prepared transfer exists*, server implementations
  MUST:

  1. Try to transfer the ``committed_amount`` from sender's account to
     recipient's account. [#]_

  2. Remove the prepared transfer from server's database.

  3. Unlock the remainder of the secured amount, so that it becomes
     available for other transfers. [#]_

  4. Send a `FinalizedTransfer`_ message with an apropriate
     ``status_code`` field.

.. [#] When ``committed_amount`` is zero, this would be a no-op.

.. [#] Note that ``committed_amount`` can be smaller that
  ``sender_locked_amount``.


Outgoing messages
=================


RejectedTransfer
----------------

Emitted when a request to prepare a transfer has been rejected.

coordinator_type : string
   Indicates the subsystem which requested the transfer. MUST be
   between 1 and 30 symbols, ASCII only.

coordinator_id : int64
   Along with ``coordinator_type``, identifies the client that
   requested the transfer (the *coordinator*).

coordinator_request_id : int64
   Along with ``coordinator_type`` and ``coordinator_id``, uniquely
   identifies the rejected request from the coordinator's point of
   view, so that the coordinator can pair this message with the issued
   request to prepare a transfer.

rejected_at_ts : date-time
   The moment at which the request to prepare a transfer was rejected.

rejection_code : string
   The reason for the rejection of the transfer. MUST be between 0 and
   30 symbols, ASCII only.

available_amount : int64
   A non-negative number. If the transfer was rejected due to
   insufficient available amount, but there is a good chance for a new
   transfer request for a smaller amount to be successful, this field
   SHOULD contain the amount currently available on sender's account
   [#]_ . Otherwise this MUST be ``0``.

debtor_id : int64
   The ID of the debtor.
   
sender_creditor_id : int64
   Along with ``debtor_id`` identifies the sender's account.

.. [#] This MUST NOT be a negative number.


PreparedTransfer
----------------

Emitted when a new transfer has been prepared, or to remind that a
prepared transfer must be finalized.

debtor_id : int64
   The ID of the debtor.

sender_creditor_id : int64
   Along with ``debtor_id`` identifies the sender's account.

transfer_id : int64
   An opaque ID generated for the prepared transfer. It MUST always be
   a positive number. This ID, along with ``debtor_id`` and
   ``sender_creditor_id``, uniquely identifies the prepared transfer.

coordinator_type : string
   Indicates the subsystem which requested the transfer. MUST be
   between 1 and 30 symbols, ASCII only.

coordinator_id : int64
   Along with ``coordinator_type``, identifies the client that
   requested the transfer (the *coordinator*).

coordinator_request_id : int64
   Along with ``coordinator_type`` and ``coordinator_id``, uniquely
   identifies the accepted request from the coordinator's point of
   view, so that the coordinator can pair this message with the
   issued request to prepare a transfer.

sender_locked_amount : int64
   The secured (prepared) amount for the transfer. It MUST always be a
   positive number. The actual transferred (committed) amount MUST NOT
   exceed this number.

recipient_identity : string
   The value of the ``recipient_identity`` field in the corresponding
   `PrepareTransfer`_ message.

prepared_at_ts : date-time
   The moment at which the transfer was prepared.

signal_ts : date-time
   The moment at which this signal was emitted (the message
   timestamp).

If a prepared transfer has not been finalized (committed or dismissed)
for a long while, the server SHOULD send another `PreparedTransfer`_
message, identical to the previous one (except for the **signal_ts**
field), to remind that a transfer has been prepared and is waiting for
a resolution. This guarantees that no prepared transfers will be
hanging in the server's database forever, even in the case of a lost
message, or a complete database loss on the client's side.


FinalizedTransfer
-----------------

Emitted when a transfer has been finalized.

debtor_id : int64
   The ID of the debtor.

sender_creditor_id : int64
   Along with ``debtor_id`` identifies the sender's account.

transfer_id : int64
   The opaque ID generated for the prepared transfer. It MUST always
   be a positive number. This ID, along with ``debtor_id`` and
   ``sender_creditor_id``, uniquely identifies the finalized prepared
   transfer.

coordinator_type : string
   Indicates the subsystem which requested the transfer. MUST be
   between 1 and 30 symbols, ASCII only.

coordinator_id : int64
   Along with ``coordinator_type``, identifies the client that
   requested the transfer (the *coordinator*).

coordinator_request_id : int64
   Along with ``coordinator_type`` and ``coordinator_id``, uniquely
   identifies the finalized prepared transfer from the coordinator's
   point of view, so that the coordinator can pair this message with
   the issued request to finalize the prepared transfer.

recipient_identity : string
   The value of the ``recipient_identity`` field in the corresponding
   `PreparedTransfer`_ message.

prepared_at_ts : date-time
   The moment at which the transfer was prepared.

finalized_at_ts : date-time
   The moment at which the transfer was finalized.

committed_amount : int64
   The transferred (committed) amount. It MUST always be a
   non-negative number. A ``0`` means either that the prepared
   transfer was dismissed, or that it was committed, but the commit
   was unsuccessful for some reason.

status_code : string
   The finalization status. MUST be between 0 and 30 symbols, ASCII
   only. If the prepared transfer was committed, but the commit was
   unsuccessful for some reason, this value MUST be different from
   ``"OK"``, and SHOULD hint at the reason for the failure [#]_ . In all
   other cases, this value MUST be ``"OK"``.

.. [#] In this case ``committed_amount`` MUST be zero.


AccountTransfer
---------------

Emitted when a committed transfer has affected a given account.

Each committed transfer affects exactly two accounts: the sender's,
and the recipient's. Therefore, exactly two ``AccountTransfer``
messages MUST be emitted for each committed transfer. The only
exception to this rule is for special-purpose accounts that have no
recipients for the message.

debtor_id : int64
   The ID of the debtor.

creditor_id : int64
   Along with ``debtor_id``, identifies the affected account.

transfer_seqnum : int64
   TODO: improve description
   The sequential number of the transfer. MUST be a positive
   number. For a newly created account, the sequential number of the
   first transfer will have its lower 40 bits set to `0x0000000001`,
   and its higher 24 bits calculated from the account's creation date
   (the number of days since Jan 1st, 1970). Note that when an account
   has been removed from the database, and then recreated again, for
   this account, a gap will occur in the generated sequence of
   seqnums.

coordinator_type : string
   Indicates the subsystem which requested the transfer. MUST be
   between 1 and 30 symbols, ASCII only.

committed_at_ts : date-time
   The moment at which the transfer was committed.

committed_amount : int64
   TODO: rename?
   The increase in the affected account's principal which the transfer
   caused. It can be positive (increase), or negative (decrease), but
   it MUST NOT be zero.

other_party_identity : string
   TODO: improve description
   A string which (along with ``debtor_id``) identifies the other
   party in the transfer. When ``committed_amount`` is positive, this
   is the sender; when ``committed_amount`` is negative, this is the
   recipient. Different server implementations may use different
   formats for the identifier.

transfer_message : string
   This MUST be the value of the ``transfer_message`` field in the
   ``FinalizePreparedTransfer`` message that fianlized the transfer.

transfer_flags : int32
   This MUST be the value of the ``transfer_flags`` field in the
   ``FinalizePreparedTransfer`` message that fianlized the transfer.

account_creation_date : date
   The date on which the affected account was created.

account_new_principal : int64
   The affected account's principal, as it is after the transfer has
   been committed.

previous_transfer_seqnum : int64
   TODO: improve description
   The sequential number of the previous transfer. MUST be a positive
   number. It will always be smaller than `transfer_seqnum`, and
   sometimes the difference can be more than `1`. If there were no
   previous transfers, the value will have its lower 40 bits set to
   `0x0000000000`, and its higher 24 bits calculated from
   `account_creation_date` (the number of days since Jan 1st, 1970).

system_flags : int32
   Various bit-flags characterizing the transfer.

creditor_identity : string
   A string which (along with ``debtor_id``) identifies the affected
   account. Different server implementations may use different formats
   for the identifier. Note that while ``creditor_id`` could be a
   "local" identifier, recognized only by the system that created the
   account, ``creditor_identity`` is always a globally recognized
   identifier.

transfer_id : int64
   TODO: improve description
   MUST contain either ``0``, or the ID of the corresponding prepared
   transfer. This allows the sender of a committed direct transfer, to
   reliably identify the corresponding prepared transfer record (using
   `debtor_id`, `creditor_id`, and `transfer_id` fields).


AccountChange
-------------

Emitted when there is a meaningful change in account's state, or to
remind that the account still exists.

* `debtor_id` and `creditor_id` identify the account.

* `change_ts` and `change_seqnum` can be used to reliably determine
  the correct order of changes, even if they occured in a very short
  period of time. When considering two events, the `change_ts`s must
  be compared first, and only if they are equal, the `change_seqnum`s
  must be compared as well (care should be taken to correctly deal
  with the possible 32-bit integer wrapping).

* `principal` is the owed amount, without the interest. (Can be
  negative, between -MAX_INT64 and MAX_INT64.)

* `interest` is the amount of interest accumulated on the account
  before `change_ts`, but not added to the `principal` yet. (Can be
  negative.)

* `interest_rate` is the annual rate (in percents) at which interest
  accumulates on the account. (Can be negative, INTEREST_RATE_FLOOR <=
  interest_rate <= INTEREST_RATE_CEIL.)

* `last_transfer_seqnum` (>= 0) identifies the last account commit. If
  there were no previous account commits, the value will have its
  lower 40 bits set to `0x0000000000`, and its higher 24 bits
  calculated from `creation_date` (the number of days since Jan 1st,
  1970).

* `last_outgoing_transfer_date` is the date of the last committed
  transfer, for which the owner of the account was the sender. It can
  be used, for example, to determine when an account with negative
  balance can be zeroed out. (If there were no outgoing transfers, the
  value will be "1970-01-01".)

* `last_config_signal_ts` contains the value of the `signal_ts` field
  of the last applied `configure_account` signal. This field can be
  used to determine whether a sent configuration signal has been
  processed. (If there were no applied configuration signals, the
  value will be "1970-01-01T00:00:00+00:00".)

* `last_config_signal_seqnum` contains the value of the
  `signal_seqnum` field of the last applied `configure_account`
  signal. This field can be used to determine whether a sent
  configuration signal has been processed. (If there were no applied
  configuration signals, the value will be `0`.)

* `creation_date` is the date on which the account was created.

* `negligible_amount` is the maximum amount which is considered
  negligible. It is used to: 1) decide whether an account can be
  safely deleted; 2) decide whether a transfer is insignificant. Will
  always be non-negative.

* `status` (a 32-bit integer) contains status bit-flags (see
  `models.Account`).

* `config` contains the value of the `config` field of the most
  recently applied account configuration signal that contained a valid
  account configuration. This field can be used to determine whether a
  requested configuration change has been successfully applied. (Note
  that when the `config` field of an account configuration signal
  contains an invalid configuration, the signal MUST be applied, but
  the `config` SHOULD NOT be updated.)

* `signal_ts` is the moment at which this signal was emitted (the
  message timestamp).

* `signal_ttl` is the time-to-live (in seconds) for this signal. The
  signal SHOULD be ignored if more than `signal_ttl` seconds have
  elapsed since the signal was emitted (`signal_ts`). Will always be
  bigger than `0.0`.

* `creditor_identity` is a string, which (along with `debtor_id`)
  identifies the account. Different server implementations may use
  different formats for the identifier. Note that while `creditor_id`
  could be a "local" identifier, recognized only by the system that
  created the account, `creditor_identity` is always a globally
  recognized identifier.


AccountPurge
------------

Emitted when an account has been removed from the server's database.

debtor_id : int64
   The ID of the debtor.

creditor_id : int64
   Along with ``debtor_id``, identifies the removed account.

creation_date : date
   The date on which the removed account was created.

purged_at_ts : date-time
   The moment at which the account was removed from the database.

creditor_identity : string
   A string which (along with ``debtor_id``) globally identifies the
   removed account. Different server implementations may use different
   formats for this string. Note that ``creditor_id`` is an ID which
   is recognizable only by the system that created the sender's
   account. This identifier (along with ``debtor_id``), on the other
   hand, MUST provide enough information to globally identify the
   removed account (an IBAN for example).


RejectedConfig
--------------

Emitted when a `ConfigureAccount`_ message has been received and
rejected.

debtor_id : int64
   The value of the ``debtor_id`` field in the rejected message.

creditor_id : int64
   The value of the ``creditor_id`` field in the rejected message.

config_signal_ts : date-time
   The value of the ``signal_ts`` field in the rejected message.

config_signal_seqnum : int32
   The value of the ``signal_seqnum`` field in the rejected message.

status_flags : int16
   The value of the ``status_flags`` field in the rejected message.

negligible_amount : float
   The value of the ``negligible_amount`` field in the rejected
   message.

config : string
   The value of the ``config`` field in the rejected message.

rejected_at_ts : date-time
   The moment at which the `ConfigureAccount`_ message was rejected.

rejection_code : string
   The reason for the rejection of the `ConfigureAccount`_
   message. Between 0 and 30 symbols, ASCII only.


Requirements for Client Implementations
=======================================

Before sending a `PrepareTransfer`_ message, the sender MUST create a
Coordinator Request (CR) database record, with a primary key of
`(coordinator_type, coordinator_id, coordinator_request_id)`, and
status "initiated". This record will be used to act properly on
`PreparedTransferSignal` and `RejectedTransferSignal` events.

`PreparedTransfer`_

If a `PreparedTransferSignal` is received for an "initiated" CR
record, the status of the corresponding CR record MUST be set to
"prepared", and the received values for `debtor_id`,
`sender_creditor_id`, and `transfer_id` -- recorded. The
"prepared" CR record MUST be, at some point, finalized (committed
or dismissed), and the status set to "finalized".

If a `PreparedTransferSignal` is received for a "prepared" CR
record, the corresponding values of `debtor_id`,
`sender_creditor_id`, and `transfer_id` MUST be compared. If they
are the same, no action MUST be taken. If they differ, the newly
prepared transfer MUST be immediately dismissed (by sending a
message to the `finalize_prepared_transfer` actor with a zero
`committed_amount`).

If a `PreparedTransferSignal` is received for a "finalized" CR
record, the corresponding values of `debtor_id`,
`sender_creditor_id`, and `transfer_id` MUST be compared. If they
are the same, the original message to the
`finalize_prepared_transfer` actor MUST be sent again. If they
differ, the newly prepared transfer MUST be immediately dismissed.

If a `PreparedTransferSignal` is received but a corresponding CR
record is not found, the newly prepared transfer MUST be
immediately dismissed.

`RejectedTransfer`_

If a `RejectedTransferSignal` is received for an "initiated" CR
record, the CR record SHOULD be deleted.

If a `RejectedTransferSignal` is received in any other case, no
action MUST be taken.

IMPORTANT NOTES:

1. "initiated" CR records MAY be deleted whenever considered
   appropriate.

2. "prepared" CR records MUST NOT be deleted. Instead, they MUST
   be "finalized" first (by sending a message to the
   `finalize_prepared_transfer` actor).

3. "finalized" CR records, which have been committed (i.e. not
   dismissed), SHOULD NOT be deleted right away. Instead, they
   SHOULD stay in the database until a corresponding
   `FinalizedTransferSignal` is received for them. (It MUST be
   verified that the signal has the same `debtor_id`,
   `sender_creditor_id`, and `transfer_id` as the CR record.)

   Only when the corresponding `FinalizedTransferSignal` has not
   been received for a very long time (1 year for example), the
   "finalized" CR record MAY be deleted with a warning.

   NOTE: The retention of committed CR records is necessary to
   prevent problems caused by message re-delivery. Consider the
   following scenario: a transfer has been prepared and committed
   (finalized), but the `PreparedTransferSignal` message is
   re-delivered a second time. Had the CR record been deleted
   right away, the already committed transfer would be dismissed
   the second time, and the fate of the transfer would be decided
   by the race between the two different finalizing messages. In
   most cases, this would be a serious problem.

4. "finalized" CR records, which have been dismissed (i.e. not
   committed), MAY be deleted either right away, or when a
   corresponding `FinalizedTransferSignal` is received for them.
