swpt_accounts
=============

Swaptacular micro-service that manages user account balances

It implements several public `Dramatiq`_ actors (see
``swpt_accounts/actors.py``), and a very simple web API (see
``swpt_accounts/routes.py``).


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
