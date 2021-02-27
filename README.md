Swaptacular service that manages user account balances
======================================================

This service implements
[Swaptacular](https://github.com/epandurski/swaptacular)'s [messaging
protocol](https://github.com/epandurski/swpt_accounts/blob/master/protocol.rst)
server. The ultimate deliverable is a docker image, which is generated
from the project's
[Dockerfile](https://github.com/epandurski/swpt_accounts/blob/master/Dockerfile). To
find out what processes can be spawned from the generated image, see
the
[entrypoint](https://github.com/epandurski/swpt_accounts/blob/master/docker/entrypoint.sh). For
the available configuration options, see the
[development.env](https://github.com/epandurski/swpt_accounts/blob/master/development.env)
file. This
[example](https://github.com/epandurski/swpt_accounts/blob/master/docker-compose-all.yml)
shows how to use the generated image.


How to run it
-------------

1.  Install [Docker](https://docs.docker.com/) and [Docker
    Compose](https://docs.docker.com/compose/).

2.  To create an *.env* file with reasonable defalut values, run this
    command:

        $ cp development.env .env

3.  To run the unit tests, use the following commands:

        $ docker-compose build
        $ docker-compose run tests-dummy test

4.  To run the minimal set of services needed for development, use this
    command:

        $ docker-compose up --build


How to setup a development environment
--------------------------------------

1.  Install [Poetry](https://poetry.eustace.io/docs/).

2.  Create a new [Python](https://docs.python.org/) virtual
    environment and activate it.

3.  To install dependencies, run this command:

        $ poetry install

4.  You can use `flask swpt_accounts` to run management commands,
    `dramatiq tasks:protocol_broker` and `dramatiq
    tasks:chores_broker` to spawn local task workers, and `pytest
    --cov=swpt_accounts --cov-report=html` to run the tests and
    generate a test coverage report..


How to run all services (production-like)
-----------------------------------------

To start the containers, use this command:

    $ docker-compose -f docker-compose-all.yml up --build
