swpt_accounts
=============

Swaptacular micro-service that manages user account balances

Currently it implements several `Dramatiq`_ actors (see
``swpt_accounts/actors.py``), and emits several types of events (see
``swpt_accounts/events.py``). Eventually, it should migrate to using
`Cap'n Proto`_.


How to run it
-------------

1. Install `Docker`_ and `Docker Compose`_.

2. Install `RabbitMQ`_ and either create a new RabbitMQ user, or allow
   the existing "guest" user to connect from other hosts (by default,
   only local connections are allowed for "guest"). You may need to
   alter the firewall rules on your computer as well, to allow docker
   containers to connect to the docker host.

3. To create an *.env* file with reasonable defalut values, run this
   command::

     $ cp env.development .env

4. To start the containers, run this command::

     $ docker-compose up --build -d


How to setup a development environment
--------------------------------------

1. Install `Poetry`_.

2. Create a new `Python`_ virtual environment and activate it.

3. To install dependencies, run this command::

     $ poetry install

4. You can use ``flask run`` to run a local Web server, or ``dramatiq
   tasks:broker`` to spawn local task workers.


Swaptacular Messaging Protocol
------------------------------

.. _`The protocol`: protocol.rst

This service implements a generic messaging protocol. `The protocol`_
is centered around two types of objects: *debtors* and *creditors*. A
debtor is a person or an organization that manages a digital
currency. A creditor is a person or an organization that owns tokens
in one or more debtors' digital currencies. The relationship is
asymmetrical: Currency tokens express the fact that the debtor owes
something to the creditor. Although a creditor owing something to a
debtor can be expressed with a negative account balance, the
relationship is not supposed to work in the reverse direction. The
protocol supports the following operations:

1. Creditors can open accounts with debtors.

2. Creditors can re-configure existing accounts. Notably, creditors
   can schedule accounts for deletion, and specify an amount on the
   account, that is considered negligible.

3. Creditors can safely delete existing accounts with debtors. The
   emphasis is on *safely*. When the balance on one account is not
   zero, deleting the account may result in a loss of non-negligible
   amount of money (tokens of the digital currency). Even if the
   balance was negligible at the moment of the deletion request, there
   might had been a pending incoming transfer to the account, which
   would be lost had the account been deleted without the necessary
   precautions. To achieve safe deletion, this protocal requires that
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


.. _Docker: https://docs.docker.com/
.. _Docker Compose: https://docs.docker.com/compose/
.. _RabbitMQ: https://www.rabbitmq.com/
.. _Poetry: https://poetry.eustace.io/docs/
.. _Python: https://docs.python.org/
.. _Dramatiq: https://dramatiq.io/
.. _`Cap'n Proto`: https://capnproto.org/
