Swaptacular "Accounting Authority" reference implementation
===========================================================

This project implements a [Swaptacular] "Accounting Authority"
node. The ultimate deliverable is a [docker image], generated from the
project's [Dockerfile](../master/Dockerfile).

**Note:** This implementation uses [JSON Serialization for the
Swaptacular Messaging Protocol](../master/protocol-json.rst).


Dependencies
------------

Containers started from the generated docker image must have access to
the following servers:

1. [PostgreSQL] server instance, which stores accounts' data.

2. [RabbitMQ] server instance, which acts as broker for [Swaptacular
   Messaging Protocol](../master/protocol.rst) (SMP) messages.

   A [RabbitMQ queue] must be configured on the broker instance, so
   that all incoming SMP messages for the accounts stored on the
   PostgreSQL server instance, are routed to this queue.

   Also, the following [RabbitMQ exchanges] must be configured on the
   broker instance:

   - **`to_creditors`**: For messages that must be send to the creditors
     agents. The routing key will represent the creditor ID as hexadecimal
     (lowercase). For example, for creditor ID equal to 2, the routing key
     will be "00.00.00.00.00.00.00.02".

   - **`to_debtors`**: For messages that must be send to the debtors agents.
     The routing key will represent the debtor ID as hexadecimal
     (lowercase). For example, for debtor ID equal to -2, the routing key
     will be "ff.ff.ff.ff.ff.ff.ff.fe".

   - **`to_coordinators`**: For messages that must be send to the
     transfer coordinators. Different types of transfer coordinators
     are responsible for performing different types of transfers. The
     most important types are: "direct", "agent" (the message must be
     sent to the creditors agent), and "issuing" (the message must be
     sent to the debtors agent). All the messages sent to this
     exchange, will have a correctly set "coordinator_type" header.
     The routing key will represent the coordinator ID as hexadecimal
     (lowercase). Note that for "direct" transfers, the coordinator ID
     is guaranteed to be the same as the creditor ID; for "agent"
     transfers, the highest 24-bits of the coordinator ID are
     guaranteed to be the same as the highest 24-bits of the creditor
     ID; and for "issuing" transfers, the coordinator ID is guaranteed
     to be the same as the debtor ID.

   - **`accounts_in`**: For messages that must be send to this
     accounting authority itself (self-posting). The routing key will
     represent the highest 24 bits of the MD5 digest of the (debtor
     ID, creditor ID) pair. For example, if debtor ID is equal to 123,
     and creditor ID is equal to 456, the routing key will be
     "0.0.0.0.1.0.0.0.0.1.0.0.0.1.0.0.0.0.1.1.0.1.0.0". This allows
     different accounts to be located on different database servers
     (sharding).

   **Note:** If you execute the "configure" command (see below), with
   the environment variable `SETUP_RABBITMQ_BINDINGS` set to `yes`, an
   attempt will be made to automatically setup all the required
   RabbitMQ queues, exchanges, and the bindings between them.

3. *RabbitMQ server instance*, which is responsible for queuing local
   database tasks (chores).

   This can be the same RabbitMQ server instance that is used for
   brokering SMP messages, but can also be a different one. For
   example, when different accounts are located on different database
   servers, it might be a good idea to store the local database
   "chores" queue, as close to the database as possible.

   For this RabbitMQ server instance, the tolerance for data loss is
   relatively high, because it only contains messages which trigger
   periodic tasks.

Configuration
-------------

The behavior of the running container can be tuned with environment
variables. Here are the most important settings with some random
example values:

```shell
# The specified number of processes ("$WEBSERVER_PROCESSES") will be
# spawned to handle "fetch API" requests (default 1), each process
# will run "$WEBSERVER_THREADS" threads in parallel (default 3). The
# container will listen for "fetch API" requests on port
# "$WEBSERVER_PORT" (default 8080).
WEBSERVER_PROCESSES=2
WEBSERVER_THREADS=10
WEBSERVER_PORT=8001

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
# the server/container responsible for the account. Note, however,
# that even in the single database server scenario, installing a
# Web-proxy will be beneficial, for the caching it provides.
#
# If you have a "fetch API" Web-proxy installed, set the FETCH_API_URL
# variable to the base URL of your proxy. If you do not have a proxy,
# set FETCH_API_URL to the "fetch API" address of the container itself
# (see the WEBSERVER_PORT configuration variable).
FETCH_API_URL=http://localhost:8001

# Connection string for a PostgreSQL database server to connect to.
POSTGRES_URL=postgresql+psycopg://swpt_accounts:swpt_accounts@localhost:5433/test

# Parameters for the communication with the RabbitMQ server which is
# responsible for brokering SMP messages. The container will connect
# to "$PROTOCOL_BROKER_URL" (default
# "amqp://guest:guest@localhost:5672"), will consume messages from the
# queue named "$PROTOCOL_BROKER_QUEUE" (default "swpt_accounts"),
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

# The binding key with which the "$PROTOCOL_BROKER_QUEUE"
# RabbitMQ queue is bound to the "accounts_in" RabbitMQ topic
# exchange (default "#"). The binding key must consist of zero or
# more 0s or 1s, separated by dots, ending with a hash symbol.
# For example: "0.1.#", "1.#", or "#".
PROTOCOL_BROKER_QUEUE_ROUTING_KEY=#

# Parameters for the communication with the RabbitMQ server which is
# responsible for queuing local database tasks (chores). This may or
# may not be the same RabbitMQ server that is used for brokering SMP
# messages. The container will connect to "$CHORES_BROKER_URL"
# (default "amqp://guest:guest@localhost:5672"), will post and consume
# messages to/from the queue named "$CHORES_BROKER_QUEUE" (default
# "swpt_accounts_chores"), prefetching at most
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

# All outgoing Swaptacular Messaging Protocol messages are first
# recorded in the PostgreSQL database, and then are "fulshed" to
# the RabbitMQ message broker. The specified number of
# processes ("$FLUSH_PROCESSES") will be spawned to flush
# messages (default 1). Note that FLUSH_PROCESSES can be set to
# 0, in which case, the container will not flush any messages.
# The "$FLUSH_PERIOD" value specifies the number of seconds to
# wait between two sequential flushes (default 2).
FLUSH_PROCESSES=2
FLUSH_PERIOD=1.5

# The processing of each transfer consists of several stages. The
# following configuration variables control the number of worker
# threads that will be involved on each respective stage (default
# 1). You must set this to a reasonable value, and increase it when
# you start experiencing problems with performance.
PROCESS_TRANSFER_REQUESTS_THREADS=10
PROCESS_FINALIZATION_REQUESTS_THREADS=10
PROCESS_BALANCE_CHANGES_THREADS=10

# Set this to "true" after splitting a parent database shard into
# two children shards. You may set this back to "false", once all
# left-over records from the parent have been deleted from the
# child shard.
DELETE_PARENT_SHARD_RECORDS=false

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
# from the archive (default 1970-01-01). Normally, this should be a
# date at least a few weeks in the past. The date must be given in ISO
# 8601 date format. It can also include time, for example:
# "2022-07-30T18:59:59Z".
REMOVE_FROM_ARCHIVE_THRESHOLD_DATE=2022-07-30

# Set the minimum level of severity for log messages ("info",
# "warning", or "error"). The default is "warning".
APP_LOG_LEVEL=info

# Set format for log messages ("text" or "json"). The default is
# "text".
APP_LOG_FORMAT=text
```

For more configuration options, check the
[development.env](../master/development.env) file.


Available commands
------------------

The [entrypoint](../master/docker/entrypoint.sh) of the docker
container allows you to execute the following *documented commands*:

* `all`

  Starts all the necessary services in the container. Also, this is
  the command that will be executed if no arguments are passed to the
  entrypoint.

  **IMPORTANT NOTE: For each database instance, you must start exactly
  one container with this command.**

* `configure`

  Initializes a new empty PostgreSQL database, and creates the
  "chores" RabbitMQ queue.

  **IMPORTANT NOTE: This command has to be run only once (at the
  beginning), but running it multiple times should not do any harm.**

* `await_migrations`

  Blocks until the latest migration applied to the PostgreSQL server
  instance matches the latest known migration.

* `webserver`

  Starts only the "fetch API" server. This command allows you to start
  as many additional dedicated web servers as necessary, to handle the
  incoming load.

* `consume_messages`

  Starts only the processes that consume SMP messages. This command
  allows you to start as many additional dedicated SMP message
  processors as necessary, to handle the incoming load. If the
  `--draining-mode` option is specified, periodic pauses will be made
  during consumption, to allow the queue to be deleted safely.

* `consume_chore_messages`

  Starts only the processes that perform local database chores. This
  command allows you to start as many additional dedicated chores
  processors as necessary, to handle the incoming load.

* `flush_all`

  Starts only the worker processes that send outgoing messages to the
  RabbitMQ broker, and remove the messages from the PostgreSQL database.

* `flush_rejected_transfers`, `flush_prepared_transfers`
  `flush_finalized_transfers`, `flush_account_transfers`
  `flush_account_updates`, `flush_account_purges`, `flush_rejected_configs`,
  `flush_pending_balance_changes`

  Starts additional worker processes that send particular type of outgoing
  messages to the RabbitMQ broker, and remove the messages from the
  PostgreSQL database. These commands allow you to start processes dedicated
  to the flushing of particular type of messages. (See "FLUSH_PROCESSES" and
  "FLUSH_PERIOD" environment variables.)

* `subscribe`

  Declares a RabbitMQ queue, and subscribes it to receive incoming
  messages. Normally, this command should not be executed directly.

* `unsubscribe`

  Unsubscribes a RabbitMQ queue from receiving incoming messages.
  Normally, this command should not be executed directly.

This [docker-compose example](../master/docker-compose-all.yml) shows
how to use the generated docker image, along with the PostgerSQL
server, and the RabbitMQ server.


How to run the tests
--------------------

1.  Install [Docker Engine] and [Docker Compose].

2.  To create an *.env* file with reasonable defalut values, run this
    command:

        $ cp development.env .env

3.  To run the tests, use the following commands:

        $ docker-compose build
        $ docker-compose run tests-dummy test


How to setup a development environment
--------------------------------------

1.  Install [Poetry].

2.  Create a new [Python] virtual environment and activate it.

3.  To install dependencies, run this command:

        $ poetry install

4.  To run the minimal set of services needed for development, use
    this command:

        $ docker-compose up --build

    This will start its own PostgreSQL server instance in a docker
    container, but will rely on being able to connect to a RabbitMQ
    server instance at "amqp://guest:guest@localhost:5672".

    Note that because the RabbitMQ "guest" user [can only connect from
    localhost], you should either explicitly allow the "guest" user to
    connect from anywhere, or create a new RabbitMQ user, and change
    the RabbitMQ connection URLs accordingly (`PROTOCOL_BROKER_URL` and
    `CHORES_BROKER_URL` in the *.env* file).

5.  You can use `flask swpt_accounts` to run management commands, and
    `pytest --cov=swpt_accounts --cov-report=html` to run the tests
    and generate a test coverage report.


How to run all services (production-like)
-----------------------------------------

To start the "Accounting Authority" server, along with STOMP server, STOMP
client, PostgerSQL server, RabbitMQ server, and HTTP-proxy server, use this
command:

    $ docker-compose -f docker-compose-all.yml up --build

Note that the `docker/nodedata` directory contains an already populated
[root-CA database](https://github.com/swaptacular/swpt_ca_scripts), which
can be used for end-to-end testing.


[Swaptacular]: https://swaptacular.github.io/overview
[docker image]: https://www.geeksforgeeks.org/what-is-docker-images/
[PostgreSQL]: https://www.postgresql.org/
[RabbitMQ]: https://www.rabbitmq.com/
[RabbitMQ queue]: https://www.cloudamqp.com/blog/part1-rabbitmq-for-beginners-what-is-rabbitmq.html
[RabbitMQ exchanges]: https://www.cloudamqp.com/blog/part4-rabbitmq-for-beginners-exchanges-routing-keys-bindings.html
[Docker Engine]: https://docs.docker.com/engine/
[Docker Compose]: https://docs.docker.com/compose/
[Poetry]: https://poetry.eustace.io/docs/
[Python]: https://docs.python.org/
[can only connect from localhost]: https://www.rabbitmq.com/access-control.html#loopback-users
