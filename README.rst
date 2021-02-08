swpt_accounts
=============

Swaptacular micro-service that manages user account balances

.. _`messaging protocol`: protocol.rst

This micro-service implements a generic `messaging
protocol`_. Currently it defines several `Dramatiq`_ `actors`_, and
emits several types of `events`_. Eventually, it should migrate to
using `Cap'n Proto`_.

.. _actors: swpt_accounts/actors.py
.. _events: swpt_accounts/events.py


How to run it
-------------

1. Install `Docker`_ and `Docker Compose`_.

2. To create an *.env* file with reasonable defalut values, run this
   command::

     $ cp development.env .env

3. To run the unit tests, use this command::

     $ docker-compose run tests-dummy test

4. To run the minimal set of services needed for development, use this
   command::

     $ docker-compose up --build


How to setup a development environment
--------------------------------------

1. Install `Poetry`_.

2. Create a new `Python`_ virtual environment and activate it.

3. To install dependencies, run this command::

     $ poetry install

4. You can use ``flask swpt_accounts`` to run management commands,
   ``dramatiq tasks:protocol_broker`` and ``dramatiq
   tasks:chores_broker`` to spawn local task workers, and
   ``pytest --cov=swpt_accounts --cov-report=html`` to run the tests
   and generate a test coverage report..


How to run all services (production-like)
-----------------------------------------

To start the containers, use this command::

     $ docker-compose -f docker-compose-all.yml up --build


.. _Docker: https://docs.docker.com/
.. _Docker Compose: https://docs.docker.com/compose/
.. _RabbitMQ: https://www.rabbitmq.com/
.. _Poetry: https://poetry.eustace.io/docs/
.. _Python: https://docs.python.org/
.. _Dramatiq: https://dramatiq.io/
.. _`Cap'n Proto`: https://capnproto.org/
