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

2. Install `RabbitMQ`_ and either create a new RabbitMQ user, or allow
   the existing "guest" user to connect from other hosts (by default,
   only local connections are allowed for "guest"). You may need to
   alter the firewall rules on your computer as well, to allow docker
   containers to connect to the docker host.

3. To create an *.env* file with reasonable defalut values, run this
   command::

     $ cp env.development .env

4. To create a minimal *docker-compose.yml* file for development, use
   this command::

     $ cp docker-compose-tests.yml docker-compose.yml

5. To run the unit tests, use this command::

     $ docker-compose run tests-tasks test

6. To run the minimal set of services needed for development, use this
   command::

     $ docker-compose up --build -d


How to setup a development environment
--------------------------------------

1. Install `Poetry`_.

2. Create a new `Python`_ virtual environment and activate it.

3. To install dependencies, run this command::

     $ poetry install

4. You can use ``flask swpt_accounts`` to run management commands,
   ``dramatiq tasks:broker`` to spawn local task workers, and
   ``pytest --cov=swpt_accounts --cov-report=html`` to run the tests
   and generate a test coverage report..


How to run all services (production-like)
-----------------------------------------

1. To create a production-like *docker-compose.yml* file, use this
   command::

     $ cp docker-compose-all.yml docker-compose.yml

2. To start the containers, use this command::

     $ docker-compose up --build -d


.. _Docker: https://docs.docker.com/
.. _Docker Compose: https://docs.docker.com/compose/
.. _RabbitMQ: https://www.rabbitmq.com/
.. _Poetry: https://poetry.eustace.io/docs/
.. _Python: https://docs.python.org/
.. _Dramatiq: https://dramatiq.io/
.. _`Cap'n Proto`: https://capnproto.org/
