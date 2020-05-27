++++++++++++++++++++++++++++++
Swaptacular Messaging Protocol
++++++++++++++++++++++++++++++
:Description: Swaptacular Messaging Protocol Specification
:Author: Evgeni Pandurksi
:Contact: epandurski@gmail.com
:Date: 2020-05-24
:Version: 0.1
:Copyright: This document has been placed in the public domain.

.. contents::
   :depth: 3


Overview
========

This protocol is centered around two types of objects: *debtors* and
*creditors*. A debtor is a person or an organization that manages a
digital currency. A creditor is a person or an organization that owns
tokens in one or more debtors' digital currencies. The relationship is
asymmetrical: Currency tokens express the fact that the debtor owes
something to the creditor. Although a creditor owing something to a
debtor can be expressed with a negative account balance, the
relationship is not supposed to work in the reverse direction. The
protocol supports the following operations:

1. Creditors can open accounts with debtors. [#one-account-limit]_

2. Creditors can re-configure existing accounts. Notably, creditors
   can schedule accounts for deletion, and specify an amount on the
   account, that is considered negligible.

3. Creditors can safely delete existing accounts with debtors. The
   emphasis is on *safely*. When the balance on one account is not
   zero, deleting the account may result in a loss of non-negligible
   amount of money (tokens of the digital currency). Even if the
   balance was negligible at the moment of the deletion request, there
   might have been a pending incoming transfer to the account, which
   would be lost had the account been deleted without the necessary
   precautions. To achieve safe deletion, this protocol requires that
   the account is scheduled for deletion, and the system takes care to
   delete the account when (and if) it is safe to do so.

4. Creditors can transfer money from their account to other creditors'
   accounts. Transfers are possible only between account in the same
   currency (that is: same debtor). The execution of the transfer
   follows the "two phase commit" paradigm. First the transfer should
   be *prepared*, and then *finalized* (committed or dismissed). A
   successfully prepared transfer, gives a very high probability for
   the success of the eventual subsequent *commit*. This paradigm
   allows many transfers to be committed atomically.

5. Actors other than creditors (called *coordinators*), can make
   transfers from one creditor's account to another creditor's
   account. This can be useful for implementing automated payment and
   exchange systems.

6. Creditors receive notification events for every transfer in which
   they participate, either as senders (outgoing transfers), or as
   recipients (incoming transfers). Those notification events are
   properly ordered, so that the creditor can reliably assemble the
   transfer history for each account (the account ledger).

The protocol has been designed with these important properties in
mind:

1. In case of prolonged network disconnect, creditors can synchronize
   their state with the server, without losing data or money.

2. Messages may arrive out-of-order, or be delivered more than once,
   without causing any problems (with the exception of possible
   delays).

3. The protocol is generic enough to support different "backend"
   implementations. For example, it should be possible to implement a
   proxy/adapter that allows clients that "talk" this protocol to
   create bank accounts and make bank transfers.

.. [#one-account-limit] A given creditor can have *at most one
  account* with a given debtor. This limitation greatly simplifies the
  protocol, at the cost of making rare use cases less convenient. (To
  have more than one account with the same debtor, the creditor will
  have to use more that one ``creditor_id``.)


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

negligible_amount : float
   The maximum amount that can be considered negligible. This MUST be
   a non-negative number. It can be used to: 1) decide whether an
   account can be safely deleted; 2) decide whether an incoming
   transfer is insignificant.

config_flags : int32
   Account configuration bit-flags. Different server implementations
   may use these flags for different purposes. The lowest 16 bits are
   reserved. Bit ``0`` has the meaning "scheduled for
   deletion". [#forbid-transfers]_ If all of the following conditions
   are met, an account SHOULD be removed (or at least marked as
   deleted) from the server's database: **1)** the account is
   "scheduled for deletion"; **2)** the account has no prepared
   transfers that await finalization; **3)** enough time has passed
   since account's creation; [#creation-date]_ **4)** account's
   configuration have not been updated for some time; [#config-delay]_
   **5)** it is very unlikely that amount bigger that
   ``negligible_amount`` will be lost if the account is removed from
   server's database. If those condition are *not met*, accounts
   SHOULD NOT be removed. Some time after an account has been removed
   from the server's database, an `AccountPurge`_ message MUST be sent
   to inform about that. [#purge-delay]_

config : string
   Additional account configuration settings. Different server
   implementations may use different formats for this field.

ts : date-time
   The moment at which this message was sent (the message's
   timestamp). For a given account, later `ConfigureAccount`_ messages
   MUST have later or equal timestamps, compared to earlier messages.

seqnum : int32
   The sequential number of the message. For a given account, later
   `ConfigureAccount`_ messages SHOULD have bigger sequential numbers,
   compared to earlier messages. Note that when the maximum ``int32``
   value is reached, the next value SHOULD be ``-2147483648`` (signed
   32-bit integer wrapping).

When server implementations process a `ConfigureAccount`_ message,
they MUST first verify whether the specified account already exists:

1. If the specified account already exists, the server implementation
   MUST decide whether the same or a later `ConfigureAccount`_ message
   has been applied already. [#compare-config]_ [#compare-seqnums]_ If
   the received message turns out to be an old one, it MUST be
   ignored. Otherwise, an attempt MUST be made to update the account's
   configuration with the requested new configuration. If the new
   configuration has been successfully applied, an `AccountUpdate`_
   message MUST be sent; otherwise a `RejectedConfig`_ message MUST be
   sent.

2. If the specified account does not exist, the message's timestamp
   MUST be checked. If it is too far in the past, the message MUST be
   ignored. Otherwise, an attempt MUST be made to create a new account
   with the requested configuration settings. If the new account has
   been successfully created, an `AccountUpdate`_ message MUST be
   sent; otherwise a `RejectedConfig`_ message MUST be sent.

.. [#forbid-transfers] Server implementations SHOULD NOT accept
  incoming transfers for "scheduled for deletion" accounts.

.. [#config-delay] How long this "some time" is, depends on how far in
  the past an `ConfigureAccount`_ message has to be, in order to be
  ignored. The goal is to avoid the scenario in which an account is
  removed from server's database, but an old, wandering
  `ConfigureAccount`_ message "resurrects" it.

.. [#purge-delay] The delay MUST be at least as long as indicated by
  the value of the ``ttl`` field which is sent with `AccountUpdate`_
  messages. The goal is to ensure that after clients have received the
  `AccountPurge`_ message, if they continue to receive old
  `AccountUpdate`_ messages for the purged account, those messages
  will be ignored.

.. [#compare-config] To do this, server implementations MUST compare
  the values of ``ts`` and ``seqnum`` fields in the received message,
  to the values of these fields in the latest applied
  `ConfigureAccount`_ message. ``ts`` fields MUST be compared first,
  and only if they are equal, ``seqnum`` fields MUST be compared as
  well.

.. [#compare-seqnums] Note that when comparing "seqnum" fields, server
  implementations MUST correctly deal with the possible 32-bit integer
  wrapping. For example, to decide whether ``seqnum2`` is later than
  ``seqnum1``, the following expression may be used: ``0 < (seqnum2 -
  seqnum1) % 0x100000000 < 0x80000000``. Timestamps must also be
  compared with care, because precision might have been lost when they
  were saved to the database.


PrepareTransfer
---------------

Upon receiving this message, the server tries to secure some amount,
to eventually transfer it from sender's account to recipient's
account.

debtor_id : int64
   The ID of the debtor.

creditor_id : int64
   Along with ``debtor_id``, identifies the sender's account.

coordinator_type : string
   Indicates the subsystem which sent this message. MUST be between 1
   and 30 symbols, ASCII only. [#coordinator-type]_

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

recipient : string
   A string which (along with ``debtor_id``) globally identifies the
   recipient's account. [#account-identity]_
   
minimum_account_balance : int64
   Determines the minimum amount that SHOULD remain available on
   sender's account after the requested amount has been secured. This
   can be a negative number.

ts : date-time
   The moment at which this message was sent (the message's
   timestamp).

When server implementations process a `PrepareTransfer`_ message they:

* SHOULD try to secure as big amount as possible, within the requested
  limits (between ``min_amount`` and ``max_amount``).

* SHOULD NOT prepare a transfer without verifying that the recipient's
  account exists, and does accept incoming transfers.

* MUST NOT allow transfers in which the sender and the recipient is
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


.. [#coordinator-type] Random examples: ``"direct"`` might be used for
  payments initiated directly by the owner of the account,
  ``"interest"`` might be used for payments initiated by the interest
  capitalization service.


FinalizeTransfer
----------------

Upon receiving this message, the server finalizes a prepared transfer.

debtor_id : int64
   The ID of the debtor.

creditor_id : int64
   Along with ``debtor_id``, identifies the sender's account.

transfer_id : int64
   The opaque ID generated for the prepared transfer. This ID, along
   with ``debtor_id`` and ``creditor_id``, uniquely identifies the
   prepared transfer that has to be finalized.

committed_amount : int64
   The amount that has to be transferred. This MUST be a non-negative
   number, which MUST NOT exceed the value of the ``locked_amount``
   field in the corresponding `PreparedTransfer`_
   message. [#unlock-amount]_ A ``0`` signifies that the transfer MUST
   be dismissed.

transfer_message : string
   A string that the coordinator (the client that finalizes the
   prepared transfer) wants the recipient and the sender to see.  If
   the transfer is being dismissed, this MUST be an empty
   string. [#message-limitations]_

ts : date-time
   The moment at which this message was sent (the message's
   timestamp).

When server implementations process a `FinalizeTransfer`_ message,
they MUST first verify whether the specified prepared transfer exists
in server's database:

1. If the specified prepared transfer exists, server implementations
   MUST:

   * Try to transfer the ``committed_amount`` from sender's account to
     recipient's account. [#commit]_

   * Unlock the remainder of the secured amount, so that it becomes
     available for other transfers. [#unlock-amount]_

   * Remove the prepared transfer from server's database.

   * Send a `FinalizedTransfer`_ message with the appropriate
     ``status_code``.

2. If the specified prepared transfer does not exist, the message MUST
   be ignored.

.. [#message-limitations] Server implementations MAY impose additional
  restrictions on the format and the content of this string, as long
  as these restrictions are precisely defined, and known in advance.

.. [#commit] When ``committed_amount`` is zero, this would be a no-op.
  When the commit is successful, an `AccountUpdate`_ message, and
  `AccountTransfer`_ messages will be triggered eventually as well.

.. [#unlock-amount] Note that ``committed_amount`` can be smaller that
  ``locked_amount``.


Outgoing messages
=================


RejectedConfig
--------------

Emitted when a `ConfigureAccount`_ request has been rejected.

debtor_id : int64
   The value of the ``debtor_id`` field in the rejected message.

creditor_id : int64
   The value of the ``creditor_id`` field in the rejected message.

config_ts : date-time
   The value of the ``ts`` field in the rejected message.

config_seqnum : int32
   The value of the ``seqnum`` field in the rejected message.

config_flags : int32
   The value of the ``config_flags`` field in the rejected message.

negligible_amount : float
   The value of the ``negligible_amount`` field in the rejected
   message.

config : string
   The value of the ``config`` field in the rejected message.

rejection_code : string
   The reason for the rejection of the `ConfigureAccount`_
   request. Between 0 and 30 symbols, ASCII only.

ts : date-time
   The moment at which this message was sent (the message's
   timestamp).


RejectedTransfer
----------------

Emitted when a request to prepare a transfer has been rejected.

debtor_id : int64
   The ID of the debtor.

creditor_id : int64
   Along with ``debtor_id`` identifies the sender's account.

rejection_code : string
   The reason for the rejection of the transfer. MUST be between 0 and
   30 symbols, ASCII only.

coordinator_type : string
   Indicates the subsystem which requested the transfer. MUST be
   between 1 and 30 symbols, ASCII only. [#coordinator-type]_

coordinator_id : int64
   Along with ``coordinator_type``, identifies the client that
   requested the transfer (the *coordinator*).

coordinator_request_id : int64
   Along with ``coordinator_type`` and ``coordinator_id``, uniquely
   identifies the rejected request from the coordinator's point of
   view, so that the coordinator can pair this message with the issued
   request to prepare a transfer.

available_amount : int64
   MUST be a non-negative number. If the transfer was rejected due to
   insufficient available amount, but there is a good chance for a new
   transfer request for a smaller amount to be successful, this field
   SHOULD contain the amount currently available on sender's account;
   otherwise this MUST be ``0``.

recipient : string
   The value of the ``recipient`` field in the corresponding
   `PrepareTransfer`_ message.

ts : date-time
   The moment at which this message was sent (the message's
   timestamp).


PreparedTransfer
----------------

Emitted when a new transfer has been prepared, or to remind that a
prepared transfer has to be finalized.

debtor_id : int64
   The ID of the debtor.

creditor_id : int64
   Along with ``debtor_id`` identifies the sender's account.

transfer_id : int64
   An opaque ID generated for the prepared transfer. This ID, along
   with ``debtor_id`` and ``creditor_id``, uniquely identifies the
   prepared transfer.

coordinator_type : string
   Indicates the subsystem which requested the transfer. MUST be
   between 1 and 30 symbols, ASCII only. [#coordinator-type]_

coordinator_id : int64
   Along with ``coordinator_type``, identifies the client that
   requested the transfer (the *coordinator*).

coordinator_request_id : int64
   Along with ``coordinator_type`` and ``coordinator_id``, uniquely
   identifies the accepted request from the coordinator's point of
   view, so that the coordinator can pair this message with the
   issued request to prepare a transfer.

locked_amount : int64
   The secured (prepared) amount for the transfer. This MUST always be
   a positive number. The actual transferred (committed) amount MUST
   NOT exceed this number.

recipient : string
   The value of the ``recipient`` field in the corresponding
   `PrepareTransfer`_ message.

ts : date-time
   The moment at which this message was sent (the message's
   timestamp).

If a prepared transfer has not been finalized (committed or dismissed)
for a long while (1 week for example), the server MUST send another
`PreparedTransfer`_ message, identical to the previous one (except for
the **ts** field), to remind that a transfer has been prepared and is
waiting for a resolution. This guarantees that prepared transfers will
not be hanging in the server's database forever, even in the case of a
lost message, or a complete database loss on the client's side.


FinalizedTransfer
-----------------

Emitted when a transfer has been finalized.

debtor_id : int64
   The ID of the debtor.

creditor_id : int64
   Along with ``debtor_id`` identifies the sender's account.

transfer_id : int64
   The opaque ID generated for the prepared transfer. This ID, along
   with ``debtor_id`` and ``creditor_id``, uniquely identifies the
   finalized prepared transfer.

coordinator_type : string
   Indicates the subsystem which requested the transfer. MUST be
   between 1 and 30 symbols, ASCII only. [#coordinator-type]_

coordinator_id : int64
   Along with ``coordinator_type``, identifies the client that
   requested the transfer (the *coordinator*).

coordinator_request_id : int64
   Along with ``coordinator_type`` and ``coordinator_id``, uniquely
   identifies the finalized prepared transfer from the coordinator's
   point of view, so that the coordinator can pair this message with
   the issued request to finalize the prepared transfer.

committed_amount : int64
   The transferred (committed) amount. This MUST always be a
   non-negative number. A ``0`` means either that the prepared
   transfer was dismissed, or that it was committed, but the commit
   was unsuccessful for some reason.

recipient : string
   The value of the ``recipient`` field in the corresponding
   `PreparedTransfer`_ message.

status_code : string
   The finalization status. MUST be between 0 and 30 symbols, ASCII
   only. If the prepared transfer was committed, but the commit was
   unsuccessful for some reason, this value MUST be different from
   ``"OK"``, and SHOULD hint at the reason for the
   failure. [#failed-commit]_ In all other cases, this value MUST be
   ``"OK"``.

ts : date-time
   The moment at which this message was sent (the message's
   timestamp).

prepared_at : date-time
   The moment at which the transfer was prepared.

.. [#failed-commit] In this case ``committed_amount`` MUST be zero.


AccountUpdate
-------------

Emitted if there has been a meaningful change in the state of an
account [#meaningful-change]_, or to remind that an account still
exists.

debtor_id : int64
   The ID of the debtor.

creditor_id : int64
   Along with ``debtor_id``, identifies the account.

creation_date : date
   The date on which the account was created. Until the account is
   removed from the server's database, its ``creation_date`` MUST NOT
   be changed. [#creation-date]_

last_change_ts : date-time
   The moment at which the latest meaningful change in the state of
   the account has happened. For a given account, later
   `AccountUpdate`_ messages MUST have later or equal
   ``last_change_ts``\s, compared to earlier messages.

last_change_seqnum : int32
   The sequential number of the latest meaningful change. For a given
   account, later changes MUST have bigger sequential numbers,
   compared to earlier changes. Note that when the maximum ``int32``
   value is reached, the next value MUST be ``-2147483648`` (signed
   32-bit integer wrapping). [#compare-change]_ [#compare-seqnums]_

principal : int64
   The amount that the debtor owes to the creditor, without the
   interest. This can be a negative number.

interest : float
   The amount of interest accumulated on the account, that is not
   added to the ``principal`` yet. [#interest]_ This can be a negative
   number. The accumulated interest SHOULD be zeroed out and added to
   the principal once in a while (an interest payment).

interest_rate : float
   The annual rate (in percents) at which interest accumulates on the
   account. This can be a negative number.

status_flags : int32
   Account status bit-flags. Different server implementations may use
   these flags for different purposes. The lowest 16 bits are
   reserved. Bit ``0`` has the meaning "unreachable account",
   indicating that the account can not receive incoming transfers.

last_config_ts : date-time
   MUST contain the value of the ``ts`` field in the latest applied
   `ConfigureAccount`_ message. If there have not been any applied
   `ConfigureAccount`_ messages yet, the value MUST be
   "1970-01-01T00:00:00+00:00".

last_config_seqnum : int32
   MUST contain the value of the ``seqnum`` field in the latest
   applied `ConfigureAccount`_ message. If there have not been any
   applied `ConfigureAccount`_ messages yet, the value MUST be
   `0`. [#verify-config]_

negligible_amount : float
   MUST contain value of the ``negligible_amount`` field in the latest
   applied `ConfigureAccount`_ message. If there have not been any
   applied `ConfigureAccount`_ messages yet, the value SHOULD
   represent the default configuration settings.

config_flags : int32
   MUST contain the value of the ``config_flags`` field in the latest
   applied `ConfigureAccount`_ message. If there have not been any
   applied `ConfigureAccount`_ messages yet, the value SHOULD
   represent the default configuration settings.

config : string
   MUST contain the value of the ``config`` field in the latest
   applied `ConfigureAccount`_ message. If there have not been any
   applied `ConfigureAccount`_ messages yet, the value SHOULD
   represent the default configuration settings.

account_identity : string
   A string which (along with ``debtor_id``) globally identifies the
   account. [#account-identity]_ An empty string indicates that the
   account does not have an identity yet. [#missing-identity]_ Once
   the account have got an identity, the identity SHOULD NOT be
   changed until the account is removed from the server's database.

last_outgoing_transfer_date : date
   The date of the latest transfer (not counting interest payments),
   for which the owner of the account was the sender. If there have
   not been any outgoing transfers yet, the value MUST be
   "1970-01-01".

last_transfer_number : int64
   MUST contain the value of the ``transfer_number`` field in the
   latest emitted `AccountTransfer`_ message for the account. If since
   the creation of the account there have not been any emitted
   `AccountTransfer`_ messages, the value MUST be ``0``.

ts : date-time
   The moment at which this message was sent (the message's
   timestamp).

ttl : int32
   The time-to-live (in seconds) for this message. The message MUST be
   ignored if more than ``ttl`` seconds have elapsed since the message
   was emitted (``ts``). This MUST be a positive number.

If for a given account, no `AccountUpdate`_ messages have been sent
for a long while (1 week for example), the server MUST send a new
`AccountUpdate`_ message identical to the previous one (except for the
``ts`` field), to remind that the account still exist. This guarantees
that accounts will not be hanging in the server's database forever,
even in the case of a lost message, or a complete database loss on the
client's side. Also, this serves the purpose of a "heartbeat",
allowing clients to detect "dead" account records in their databases.

.. [#meaningful-change] For a given account, every change in the value
  of one of the fields included in `AccountUpdate`_ messages (except
  for the ``ts`` field) should be considered meaningful, and therefore
  an `AccountUpdate`_ message SHOULD *eventually* be emitted to inform
  about it. There is no requirement, though, `AccountUpdate`_ messages
  to be emitted instantly, following each individual change. For
  example, if a series of transactions are committed on the account in
  a short period of time, the server may emit only one
  `AccountUpdate`_ message, announcing only the final state of the
  account.

.. [#creation-date] Note that an account can be removed from the
  server's database, and then a new account with the same
  ``debtor_id`` and ``creditor_id`` can be created. Care MUST be taken
  so that in this case the newly created account always has a later
  ``creation_date``, compared to the preceding account.

.. [#compare-change] ``creation_date``, ``last_change_ts``, and
  ``last_change_seqnum`` can be used to reliably determine the correct
  order in a sequence of `AccountUpdate`_ massages, even if the
  changes occurred in a very short period of time. When considering
  two changes, ``creation_date`` fields MUST be compared first, if
  they are equal ``last_change_ts`` fields MUST be compared, and if
  they are equal, ``last_change_seqnum`` fields MUST be compared as
  well.

.. [#interest] Note that the ``interest`` field shows the amount of
  interest accumulated on the account only up to the
  ``last_change_ts`` moment. Also, any amount that is shown as
  accumulated interest, SHOULD be available for transfers. That is:
  the owner of the account has to be able to "wire" the accumulated
  interest to another account.

.. [#verify-config] Note that ``last_config_ts`` and
  ``last_config_seqnum`` can be used to determine whether a sent
  `ConfigureAccount`_ message has been applied successfully.

.. [#account-identity] Different server implementations may use
  different formats for this identifier. Note that ``creditor_id`` is
  an ID which is recognizable only by the system that created the
  account. This identifier (along with ``debtor_id``), on the other
  hand, MUST provide enough information to globally identify the
  account (an IBAN for example).

.. [#missing-identity] When the account does not have an identity yet,
  the ``status_flags`` field MUST indicate that the account is an
  "unreachable account".


AccountPurge
------------

Emitted some time after an account has been removed from the server's
database. [#purge-delay]_

debtor_id : int64
   The ID of the debtor.

creditor_id : int64
   Along with ``debtor_id``, identifies the removed account.

creation_date : date
   The date on which the removed account was created.

ts : date-time
   The moment at which this message was sent (the message's
   timestamp).

The purpose of `AccountPurge`_ messages is to inform clients that they
can safely remove a given account from their databases.


AccountTransfer
---------------

Emitted when a committed transfer has affected a given account.

debtor_id : int64
   The ID of the debtor.

creditor_id : int64
   Along with ``debtor_id``, identifies the affected account.

creation_date : date
   The date on which the affected account was created.

transfer_number : int64
   Along with ``debtor_id``, ``creditor_id``, and ``creation_date``,
   uniquely identifies the committed transfer. This MUST be a positive
   number. During the lifetime of a given account, later committed
   transfers MUST have bigger ``transfer_number``\s, compared to
   earlier transfers. [#transfer-number]_

coordinator_type : string
   Indicates the subsystem which requested the transfer. MUST be
   between 1 and 30 symbols, ASCII only. [#coordinator-type]_

sender : string
   A string which (along with ``debtor_id``) identifies the sender's
   account. [#account-identity]_

recipient : string
   A string which (along with ``debtor_id``) identifies the
   recipient's account. [#account-identity]_

amount : int64
   The increase in the affected account's principal (caused by the
   transfer). This MUST NOT be zero. If it is a positive number (an
   addition to the principal), the affected account would be the
   recipient. If it is a negative number (a subtraction from the
   principal), the affected account would be the sender.

committed_at : date-time
   The moment at which the transfer was committed.

transfer_message : string
   MUST contain the value of the ``transfer_message`` field from the
   `FinalizeTransfer`_ message that committed the transfer.

transfer_flags : int32
   Various bit-flags characterizing the transfer. Server
   implementations may use these flags for different purposes. The
   lowest 16 bits are reserved. Bit ``0`` has the meaning "negligible
   transfer", indicating that the transferred amount does not exceed
   the configured ``negligible_amount``. [#negligible-transfer]_

principal : int64
   The amount that the debtor owes to the creditor, without the
   interest, after the transfer has been committed. This can be a
   negative number.

ts : date-time
   The moment at which this message was sent (the message's
   timestamp).

previous_transfer_number : int64
   MUST contain the ``transfer_number`` of the previous
   `AccountTransfer`_ message that affected the same account. If since
   the creation of the account, there have not been any other
   committed transfers that affected it, the value MUST be ``0``.

Every committed transfer affects two accounts: the sender's, and the
recipient's. Therefore, two separate `AccountTransfer`_ messages would
be emitted for each committed transfer.

.. [#transfer-number] Note that when an account has been removed from
  the database, and then recreated again, the generation of transfer
  numbers MAY start from ``1`` again.

.. [#negligible-transfer] That is: ``abs(amount) <=
   negligible_amount``.


Requirements for Client Implementations
=======================================

RT record
---------

Before sending a `PrepareTransfer`_ message, client implementations
MUST create a *running transfer record* (RT record) in the client's
database, to track the progress of the requested transfer. The primary
key for running transfer records SHOULD be the (``coordinator_type``,
``coordinator_id``, ``coordinator_request_id``) tuple. `RT record`_\s
have 3 possible statuses:

initiated
   Indicates that a `PrepareTransfer`_ request has been sent, and no
   response has been received yet. `RT record`_\s with this status MAY
   be deleted whenever considered appropriate. Newly created records
   MUST receive this status.

prepared
   Indicates that a `PrepareTransfer`_ request has been sent, and a
   `PreparedTransfer`_ response has been received. `RT record`_\s with
   this status MUST NOT be deleted. Instead, they MUST be finalized
   first (committed or dismissed), by sending a `FinalizeTransfer`_
   message.

finalized
   Indicates that a `PrepareTransfer`_ request has been sent, a
   `PreparedTransfer`_ response has been received, and a
   `FinalizeTransfer`_ message has been sent to dismiss or commit the
   transfer. `RT record`_\s for *dismissed transfers* MAY be deleted
   whenever considered appropriate. RT records for *committed
   tranfers*, however, SHOULD NOT be deleted right away. Instead, they
   SHOULD stay in the database until a `FinalizedTransfer`_ message is
   received for them. [#cr-retention]_ [#staled-records]_
   [#dismissed-records]_

.. [#cr-retention] The retention of committed `RT record`_\s is
  necessary to prevent problems caused by message
  re-delivery. Consider the following scenario: a transfer has been
  prepared and committed (finalized), but the `PreparedTransfer`_
  message is re-delivered a second time. Had the RT record been
  deleted right away, the already committed transfer would be
  dismissed the second time, and the fate of the transfer would be
  decided by the race between the two different finalizing
  messages. In most cases, this would be a serious problem.

.. [#staled-records] If the corresponding `FinalizedTransfer`_ message
  has not been received for a very long time (1 year for example), the
  `RT record`_ for the committed transfer MAY be deleted,
  nevertheless.

.. [#dismissed-records] Note that `FinalizedTransfer`_ messages are
  emitted for dismissed transfers as well. Therefore, the most
  straightforward policy is to delete `RT record`_\s for both
  committed and dismissed transfers the same way.


Received `RejectedTransfer`_ message
````````````````````````````````````

When client implementations process a `RejectedTransfer`_ message,
they MUST first try to find a matching `RT record`_ in the client's
database. [#crr-match]_ If a matching record exists, and its status is
"initiated", the record SHOULD be deleted; otherwise the message MUST
be ignored.

.. [#crr-match] The matching `RT record`_ MUST have the same
  ``coordinator_type``, ``coordinator_id``, and
  ``coordinator_request_id`` values as the received
  `PreparedTransfer`_ message. Additionally, the values of other
  fields in the received message MAY be verified as well, so as to
  ensure that the server behaves as expected.


Received `PreparedTransfer`_ message
````````````````````````````````````

When client implementations process a `PreparedTransfer`_ message,
they MUST first try to find a matching `RT record`_ in the client's
database. [#crr-match]_ If a matching record does not exist, the newly
prepared transfer MUST be immediately dismissed [#dismiss-transfer]_;
otherwise, the way to proceed depends on the status of the RT record:

initiated
   The values of ``debtor_id``, ``creditor_id``, and ``transfer_id``
   fields in the received `PreparedTransfer`_ message MUST be stored
   in the `RT record`_, and the the status of the record MUST be set
   to "prepared". [#prepared-records]_

prepared
   The values of ``debtor_id``, ``creditor_id``, and ``transfer_id``
   fields in the received `PreparedTransfer`_ message MUST be compared
   to the values stored in the `RT record`_. If they are the same, no
   action MUST be taken; if they differ, the newly prepared transfer
   MUST be immediately dismissed. [#dismiss-transfer]_

finalized
   The values of ``debtor_id``, ``creditor_id``, and ``transfer_id``
   fields in the received `PreparedTransfer`_ message MUST be compared
   to the values stored in the `RT record`_. If they are the same, the
   same `FinalizeTransfer`_ message (except for the ``ts`` field),
   which was sent to finalize the transfer, MUST be sent again; if
   they differ, the newly prepared transfer MUST be immediately
   dismissed. [#dismiss-transfer]_

.. [#dismiss-transfer] A prepared transfer is dismissed by sending a
  `FinalizeTransfer`_ message, with zero ``committed_amount``.

.. [#prepared-records] Note that at some point a `FinalizeTransfer`_
  message MUST be sent for each "prepared" `RT record`_, and the
  record's status MUST be set to "finalized". Often this can be done
  immediately, in which case the RT record will change its status from
  "initiated", to "finalized" directly.


Received `FinalizedTransfer`_ message
`````````````````````````````````````

When client implementations process a `FinalizedTransfer`_ message,
they MUST first try to find a matching `RT record`_ in the client's
database. [#crr-match]_ If a matching record exists, and the values of
``debtor_id``, ``creditor_id``, and ``transfer_id`` fields in the
received message are the same as the values stored in the RT record,
the record SHOULD be deleted; otherwise the message MUST be ignored.


AD record
---------

Client implementations *that manage creditor accounts*, SHOULD
maintain *account data records* (AD records) in their databases, to
store accounts' current status data. The primary key for account data
records SHOULD be the (``creditor_id``, ``debtor_id``) tuple. As a
minimum, `AD record`_\s MUST also be able to store the values of
``creation_date``, ``last_change_ts``, ``last_change_seqnum``, and
``ts`` fields, from the latest received `AccountUpdate`_
message. [#latest-heartbeat]_


Received `AccountUpdate`_ message
`````````````````````````````````

When client implementations process an `AccountUpdate`_ message, they
MUST first verify message's ``ts`` and ``ttl`` fields. If the message
has "expired", it MUST be ignored. Otherwise, implementations MUST
verify whether a corresponding `AD record`_ already exists:

1. If a corresponding `AD record`_ already exists, the timestamp of
   the latest received account heartbeat (it is stored in the AD
   record) MUST be updated with the value of the ``ts`` field in the
   received message. [#heartbeat-update]_ Then it MUST be verified
   whether the same or a later `AccountUpdate`_ message has been
   received already. [#compare-change]_ [#compare-seqnums]_ If the
   received message turns out to be an old one, further actions MUST
   NOT be taken; otherwise, the corresponding AD record MUST be
   updated with the data contained in the message.

2. If a corresponding `AD record`_ does not exist, a new AD record
   SHOULD be created, storing the relevant data received with the
   message.

If for a given account, `AccountUpdate`_ messages have not been
received for a very long time (1 year for example), the account's `AD
record`_ SHOULD be removed from the client's
database. [#latest-heartbeat]_

.. [#latest-heartbeat] `AD record`_\'s ``ts`` field stores the
  timestamp of the latest received account heartbeat.

.. [#heartbeat-update] That is: the timestamp of the latest received account
  heartbeat, stored in the `AD record`_, MUST be changed only if the
  value of the ``ts`` field in the received `AccountUpdate`_ message
  represents a later timestamp.


Received `AccountPurge`_ message
````````````````````````````````

When client implementations process an `AccountPurge`_ message, they
MUST first verify whether a corresponding `AD record`_ already exists:

1. If a corresponding `AD record`_ already exists, the value of the
   ``creation_date`` field in the received `AccountPurge`_ message
   MUST be compared to the value stored in the AD record. If the value
   received with the message is smaller (an earlier date), the message
   MUST be ignored; otherwise the AD record SHOULD be removed.

2. If a corresponding `AD record`_ does not exist, the message MUST be
   ignored.


AL record
---------

Client implementations *that manage creditor accounts*, SHOULD
maintain *account ledger records* (AL records) in their databases, to
store accounts' ledger data. The primary key for account ledger
records SHOULD be the (``creditor_id``, ``debtor_id``,
``creation_date``) tuple. As a minimum, `AL record`_\s MUST also be
able to store a set of processed `AccountTransfer`_ messages, plus a
``last_transfer_number`` field, which contains the transfer number of
the latest transfer that has been added to the given account's
ledger. [#sequential-transfer]_ [#transfer-chain]_

.. [#sequential-transfer] Note that `AccountTransfer`_ messages can be
  received and processed out-of-order. For example, *transfer #3* can
  be processed right after *transfer #1*, and only then, *transfer #2*
  be received. In this case, the correct order of transfers in the
  ledger must be preserved, and therefore, *transfer #3* MUST NOT be
  added to the ledger before *transfer #2* has been processed as well.

.. [#transfer-chain] Note that `AccountTransfer`_ messages form a
  singly linked list. That is: the ``previous_transfer_number`` field
  in each message refers to the value of the ``transfer_number`` field
  in the previous message.


Received `AccountTransfer`_ message
```````````````````````````````````

When client implementations process an `AccountTransfer`_ message,
they MUST first verify whether a corresponding `AL record`_ already
exists. [#matching-alr]_ If it does not exist, a new AL record SHOULD
be created. [#new-alr]_ Then, if there is a corresponding AL record
(it may have been just created), the following steps MUST be
performed:

1. The currently processed message MUST be added to the set of
   processed `AccountTransfer`_ messages, stored in the corresponding
   `AL record`_.

2. If the value of the ``previous_transfer_number`` field in the
   currently processed message is the same as the value of the
   ``last_transfer_number`` field in the corresponding `AL record`_,
   the ``last_transfer_number``\'s value MUST be updated to contain
   the transfer number of the *latest sequential transfer* in the set
   of processed `AccountTransfer`_ messages. [#sequential-transfer]_
   [#transfer-chain]_

.. [#matching-alr] The corresponding `AL record`_ MUST have the same
  values for ``creditor_id``, ``debtor_id``, and ``creation_date`` as
  the currently processed `AccountTransfer`_ message.

.. [#new-alr] The newly created `AL record`_ MUST have the same values
  for ``creditor_id``, ``debtor_id``, and ``creation_date`` as the
  currently processed `AccountTransfer`_ message, an empty set of
  stored `AccountTransfer`_ massages, and a ``last_transfer_number``
  with the value of ``0``.
