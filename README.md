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
[entrypoint](https://github.com/epandurski/swpt_accounts/blob/master/docker/entrypoint.sh).


The behavior of the service can be tuned with environment variables.
Here are the most important settings with some example values:

```shell
# The port on which the container will listen for fetch-api
# requests. If not set, the default is 80.
PORT=8001

# Connection string for a PostgreSQL database server to connect to.
POSTGRES_URL=postgresql://swpt_accounts:swpt_accounts@localhost:5433/test

# Parameters for the communication with the RabbitMQ server which is
# responsible for brokering Swaptacular Messaging Protocol
# messages. The container will connect to "$PROTOCOL_BROKER_URL", will
# consume messages from the queue named "$PROTOCOL_BROKER_QUEUE",
# prefetching at most "$PROTOCOL_BROKER_PREFETCH_COUNT" messages at
# once (default 1). The specified number of processes
# ("$PROTOCOL_BROKER_PROCESSES") will be spawned to consume and
# process messages (default 1), each process will run
# "$PROTOCOL_BROKER_THREADS" threads in parallel (default 1). Note
# that PROTOCOL_BROKER_PROCESSES can be set to 0, in which case, the
# container will not consume any messages from the queue.
PROTOCOL_BROKER_URL=amqp://guest:guest@localhost:5672
PROTOCOL_BROKER_QUEUE=swpt_accounts
PROTOCOL_BROKER_PROCESSES=1
PROTOCOL_BROKER_THREADS=3
PROTOCOL_BROKER_PREFETCH_COUNT=10

# Parameters for the communication with the RabbitMQ server which is
# responsible for queuing *local* database tasks (chores). This may or
# may not be the same RabbitMQ server that is used for brokering
# Swaptacular Messaging Protocol messages. The container will connect
# to "$CHORES_BROKER_URL", will post and consume messages to/from the
# queue named "$CHORES_BROKER_QUEUE", prefetching at most
# "$CHORES_BROKER_PREFETCH_COUNT" messages at once (default 1). The
# specified number of processes ("$CHORES_BROKER_PROCESSES") will be
# spawned to consume and process chores (default 1), each process will
# run "$CHORES_BROKER_THREADS" threads in parallel (default 1). Note
# that CHORES_BROKER_PROCESSES can be set to 0, in which case, the
# container will not consume any chores from the queue, but may still
# post new chores.
CHORES_BROKER_URL=amqp://guest:guest@localhost:5672
CHORES_BROKER_QUEUE=swpt_accounts_chores
CHORES_BROKER_PROCESSES=1
CHORES_BROKER_THREADS=3
CHORES_BROKER_PREFETCH_COUNT=10

# The accounting authority should be able to access the configuration
# data of any existing account. For example, this is needed in order
# to ensure that the recipient's account exists, before initiating a
# new transfer. The "fetch API" is responsible to provide limited
# information about existing accounts, to other (internal) servers
# which may need it. When there is only one database server, the
# container will ask itself for the information. But when different
# accounts are located on several database servers (sharding), things
# get more complex. In such cases, one or more Web-proxies should be
# installed, which will forward each "fetch API" request to the
# corresponding server/container. However, even in a single server
# scenario, installing a Web-proxy is beneficial, for the caching
# only. If you have a "fetch API" Web-proxy installed (recommended),
# set the FETCH_API_URL variable to the base URL of your the proxy. If
# you do not have a proxy, set FETCH_API_URL to the address of the
# container itself (see PORT configuration variable).
FETCH_API_URL=http://localhost:8001

PROCESS_TRANSFER_REQUESTS_THREADS=1
PROCESS_FINALIZATION_REQUESTS_THREADS=1
PROCESS_BALANCE_CHANGES_THREADS=1
REGISTERED_BALANCE_CHANGES_RETENTION_DATETIME=1970-01-01

# Set the minimum level of severity for log messages ("info",
# "warning", or "error"). The default is "warning".
APP_LOG_LEVEL=info

# Set format for log messages ("text" or "json"). The default is
# "text".
APP_LOG_FORMAT=text
```

For more configuration options, see the
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

4.  To run the minimal set of services needed for development (not
    includuing RabbitMQ), use this command:

        $ docker-compose up --build


How to setup a development environment
--------------------------------------

1.  Install [Poetry](https://poetry.eustace.io/docs/).

2.  Create a new [Python](https://docs.python.org/) virtual
    environment and activate it.

3.  To install dependencies, run this command:

        $ poetry install


4.  You can use `flask swpt_accounts` to run management commands, and
    `pytest --cov=swpt_accounts --cov-report=html` to run the tests
    and generate a test coverage report.


How to run all services (production-like)
-----------------------------------------

To start the containers, use this command:

    $ docker-compose -f docker-compose-all.yml up --build
