Swaptacular service that manages user account balances
======================================================

This service implements
[Swaptacular](https://github.com/epandurski/swaptacular)'s [messaging
protocol](https://github.com/epandurski/swpt_accounts/blob/master/protocol.rst)
server. The ultimate deliverable is a docker image, which is generated
from the project's
[Dockerfile](https://github.com/epandurski/swpt_accounts/blob/master/Dockerfile).


Configuration
-------------

The behavior of the service can be tuned with environment variables.
Here are the most important settings with some example values:

```shell
# The port on which the container will listen for "fetch API"
# requests. If not set, the default is 80.
PORT=8001

# The specified number of processes ("$WEBSERVER_WORKERS") will be
# spawned to handle "fetch API" requests (default 1), each process
# will run "$WEBSERVER_THREADS" threads in parallel (default 3).
WEBSERVER_WORKERS=2
WEBSERVER_THREADS=10

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
# container can ask itself for the information. But when different
# accounts are located on different database servers (sharding),
# things get more complex. In such cases, one or more Web-proxies
# should be installed, which will forward each "fetch API" request to
# the server/container responsible fot the account. Note, however,
# that even in the single database server scenario, installing a
# Web-proxy will be beneficial, for the caching it provides.
#
# If you have a "fetch API" Web-proxy installed (recommended), set the
# FETCH_API_URL variable to the base URL of your proxy. If you do not
# have a proxy, set FETCH_API_URL to the "fetch API" address of the
# container itself (see PORT configuration variable).
FETCH_API_URL=http://localhost:8001

# The processing of each transfer consists of several stages. The
# following configuration variables control the number of worker
# threads that will be involved on each respective stage. You must set
# this to a reasonable value, and increase it when you start
# experiencing problems with performance.
PROCESS_TRANSFER_REQUESTS_THREADS=10
PROCESS_FINALIZATION_REQUESTS_THREADS=10
PROCESS_BALANCE_CHANGES_THREADS=10

# The ID of each committed transfer is archived to the database. Then,
# if the message that performed the transfer is received one more time
# (which is not likely, but is entirely possible), but the ID of the
# committed transfer is found in the archive, the transfer will not be
# performed again (as it should not be). As time passes, and the
# likelihood of receiving the same message again drops to zero, the
# transfer ID can be safely removed from the archive.
#
# The REMOVE_FROM_ARCHIVE_THRESHOLD_DATE configuration settings
# determines the date before which transfer IDs are safe to remove
# from the archive. Normally, this should be a date at least a few
# weeks in the past. The date must be given in ISO 8601 date
# format. It can also include time, for example:
# "1970-01-01T18:30:00Z".
REMOVE_FROM_ARCHIVE_THRESHOLD_DATE=1970-01-01

# Set the minimum level of severity for log messages ("info",
# "warning", or "error"). The default is "warning".
APP_LOG_LEVEL=info

# Set format for log messages ("text" or "json"). The default is
# "text".
APP_LOG_FORMAT=text
```

For more configuration options, check the
[development.env](https://github.com/epandurski/swpt_accounts/blob/master/development.env)
file.


Available commands
------------------

The
[entrypoint](https://github.com/epandurski/swpt_accounts/blob/master/docker/entrypoint.sh)
of the container allows you to execute the following *documented
commands*:

* `all`

  Starts all the necessary services in the container. This is the
  command that will be executed if no arguments are passed to the
  entrypoint.

* `configure`

  Initializes a new empty database, and creates the "chores" RabbitMQ
  queue. This needs to be run only once, but running it multiple times
  should not do any harm.

* `webserver`

  Starts only the "fetch API" server. This command allows you to start
  as many dedicated web servers as necessary, so as to handle the
  incoming load.

* `consume_messages`

  Starts only the processes that consume Swaptacular Messaging
  Protocol messages. This command allows you to start as many
  dedicated SMP message processors as necessary, so as to handle the
  incoming load.

* `consume_chore_messages`

  Starts only the processes that perform local database tasks. This
  command allows you to start as many dedicated chores processors as
  necessary, so as to handle the incoming load.


This
[example](https://github.com/epandurski/swpt_accounts/blob/master/docker-compose-all.yml)
shows how you can use the generated image.


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
