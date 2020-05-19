swpt_accounts
=============

Swaptacular micro-service that manages user account balances

.. _`generic messaging protocol`: protocol.rst

This micro-service implements a `generic messaging
protocol`_. Currently it defines several `Dramatiq`_ `actors`_, and
emits several types of `events`_. Eventually, it should migrate to
using `Cap'n Proto`_. This micro-service needs the `swpt_debtors`_
helper micro-service to perform the following important maintenance
operations:

1. Accounts removal.
2. Interest rate capitalization.
3. Setting and updating interest rates.

.. _swpt_debtors: https://github.com/epandurski/swpt_debtors
.. _actors: swpt_accounts/actors.py
.. _events: swpt_accounts/events.py


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


.. _Docker: https://docs.docker.com/
.. _Docker Compose: https://docs.docker.com/compose/
.. _RabbitMQ: https://www.rabbitmq.com/
.. _Poetry: https://poetry.eustace.io/docs/
.. _Python: https://docs.python.org/
.. _Dramatiq: https://dramatiq.io/
.. _`Cap'n Proto`: https://capnproto.org/
