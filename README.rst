Swaptacular service that manages user account balances
======================================================

This service implements a `messaging protocol`_ server. The ultimate
deliverable is a docker image, which is generated from the project's
`Dockerfile`_. To find out what processes can be spawned from the
generated image, see the `entrypoint`_.  For the available
configuration options, see the `development.env`_ file. This
`example`_ shows how to use the generated image.


.. _`messaging protocol`: protocol.rst
.. _Dockerfile: Dockerfile
.. _entrypoint: docker/entrypoint.sh
.. _development.env: development.env
.. _`example`: docker-compose-all.yml


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
.. _Poetry: https://poetry.eustace.io/docs/
