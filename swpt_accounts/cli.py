import logging
import os
import time
import sys
import click
import signal
import pika
from typing import Optional, Any
from datetime import timedelta
from flask import current_app
from flask.cli import with_appcontext
from sqlalchemy import select
from flask_sqlalchemy.model import Model
from swpt_pythonlib.utils import ShardingRealm
from swpt_accounts import procedures
from swpt_accounts.extensions import db
from swpt_accounts.models import (
    Account,
    PendingBalanceChange,
    SECONDS_IN_DAY,
    is_valid_account,
)
from swpt_pythonlib.multiproc_utils import (
    ThreadPoolProcessor,
    spawn_worker_processes,
    try_unblock_signals,
    HANDLED_SIGNALS,
)
from swpt_pythonlib.flask_signalbus import SignalBus, get_models_to_flush


@click.group("swpt_accounts")
def swpt_accounts():
    """Perform operations on Swaptacular accounts."""


@swpt_accounts.command("subscribe")
@with_appcontext
@click.option(
    "-u",
    "--url",
    type=str,
    help="The RabbitMQ connection URL.",
)
@click.option(
    "-q",
    "--queue",
    type=str,
    help="The name of the queue to declare and subscribe.",
)
@click.option(
    "-k",
    "--queue-routing-key",
    type=str,
    help="The RabbitMQ binding key for the queue.",
)
def subscribe(url, queue, queue_routing_key):  # pragma: no cover
    """Declare a RabbitMQ queue, and subscribe it to receive incoming
    messages.

    If some of the available options are not specified directly, the
    values of the following environment variables will be used:

    * PROTOCOL_BROKER_URL (default "amqp://guest:guest@localhost:5672")

    * PROTOCOL_BROKER_QUEUE (defalut "swpt_accounts")

    * PROTOCOL_BROKER_QUEUE_ROUTING_KEY (default "#")
    """

    from .extensions import (
        ACCOUNTS_IN_EXCHANGE,
        TO_CREDITORS_EXCHANGE,
        TO_DEBTORS_EXCHANGE,
        TO_COORDINATORS_EXCHANGE,
    )

    logger = logging.getLogger(__name__)
    queue_name = queue or current_app.config["PROTOCOL_BROKER_QUEUE"]
    routing_key = (
        queue_routing_key
        or current_app.config["PROTOCOL_BROKER_QUEUE_ROUTING_KEY"]
    )
    dead_letter_queue_name = queue_name + ".XQ"
    broker_url = url or current_app.config["PROTOCOL_BROKER_URL"]
    connection = pika.BlockingConnection(pika.URLParameters(broker_url))
    channel = connection.channel()

    # declare exchanges
    channel.exchange_declare(
        ACCOUNTS_IN_EXCHANGE, exchange_type="topic", durable=True
    )
    channel.exchange_declare(
        TO_CREDITORS_EXCHANGE, exchange_type="topic", durable=True
    )
    channel.exchange_declare(
        TO_DEBTORS_EXCHANGE, exchange_type="topic", durable=True
    )
    channel.exchange_declare(
        TO_COORDINATORS_EXCHANGE, exchange_type="headers", durable=True
    )

    # declare exchange bindings
    channel.exchange_bind(
        source=TO_COORDINATORS_EXCHANGE,
        destination=TO_CREDITORS_EXCHANGE,
        arguments={
            "x-match": "all",
            "coordinator-type": "direct",
        },
    )
    channel.exchange_bind(
        source=TO_COORDINATORS_EXCHANGE,
        destination=TO_CREDITORS_EXCHANGE,
        arguments={
            "x-match": "all",
            "coordinator-type": "agent",
        },
    )
    channel.exchange_bind(
        source=TO_COORDINATORS_EXCHANGE,
        destination=TO_DEBTORS_EXCHANGE,
        arguments={
            "x-match": "all",
            "coordinator-type": "issuing",
        },
    )

    # declare a corresponding dead-letter queue
    channel.queue_declare(
        dead_letter_queue_name,
        durable=True,
        arguments={"x-queue-type": "stream"},
    )
    logger.info('Declared "%s" dead-letter queue.', dead_letter_queue_name)

    # declare the queue
    channel.queue_declare(
        queue_name,
        durable=True,
        arguments={
            "x-queue-type": "quorum",
            "overflow": "reject-publish",
            "x-dead-letter-exchange": "",
            "x-dead-letter-routing-key": dead_letter_queue_name,
        },
    )
    logger.info('Declared "%s" queue.', queue_name)

    # bind the queue
    channel.queue_bind(
        exchange=ACCOUNTS_IN_EXCHANGE,
        queue=queue_name,
        routing_key=routing_key,
    )
    logger.info(
        'Created a binding from "%s" to "%s" with routing key "%s".',
        ACCOUNTS_IN_EXCHANGE,
        queue_name,
        routing_key,
    )


@swpt_accounts.command("unsubscribe")
@with_appcontext
@click.option(
    "-u",
    "--url",
    type=str,
    help="The RabbitMQ connection URL.",
)
@click.option(
    "-q",
    "--queue",
    type=str,
    help="The name of the queue to unsubscribe.",
)
@click.option(
    "-k",
    "--queue-routing-key",
    type=str,
    help="The RabbitMQ binding key for the queue.",
)
def unsubscribe(url, queue, queue_routing_key):  # pragma: no cover
    """Unsubscribe a RabbitMQ queue from receiving incoming messages.

    If some of the available options are not specified directly, the
    values of the following environment variables will be used:

    * PROTOCOL_BROKER_URL (default "amqp://guest:guest@localhost:5672")

    * PROTOCOL_BROKER_QUEUE (defalut "swpt_accounts")

    * PROTOCOL_BROKER_QUEUE_ROUTING_KEY (default "#")
    """

    from .extensions import ACCOUNTS_IN_EXCHANGE

    logger = logging.getLogger(__name__)
    queue_name = queue or current_app.config["PROTOCOL_BROKER_QUEUE"]
    routing_key = (
        queue_routing_key
        or current_app.config["PROTOCOL_BROKER_QUEUE_ROUTING_KEY"]
    )
    broker_url = url or current_app.config["PROTOCOL_BROKER_URL"]
    connection = pika.BlockingConnection(pika.URLParameters(broker_url))
    channel = connection.channel()

    channel.queue_unbind(
        exchange=ACCOUNTS_IN_EXCHANGE,
        queue=queue_name,
        routing_key=routing_key,
    )
    logger.info(
        'Removed binding from "%s" to "%s" with routing key "%s".',
        ACCOUNTS_IN_EXCHANGE,
        queue_name,
        routing_key,
    )


@swpt_accounts.command("delete_queue")
@with_appcontext
@click.option(
    "-u",
    "--url",
    type=str,
    help="The RabbitMQ connection URL.",
)
@click.option(
    "-q",
    "--queue",
    type=str,
    help="The name of the queue to delete.",
)
def delete_queue(url, queue):  # pragma: no cover
    """Try to safely delete a RabbitMQ queue.

    When the queue is not empty or is currently in use, this command
    will continuously try to delete the queue, until the deletion
    succeeds or fails for some other reason.

    If some of the available options are not specified directly, the
    values of the following environment variables will be used:

    * PROTOCOL_BROKER_URL (default "amqp://guest:guest@localhost:5672")

    * PROTOCOL_BROKER_QUEUE (defalut "swpt_accounts")
    """

    logger = logging.getLogger(__name__)
    queue_name = queue or current_app.config["PROTOCOL_BROKER_QUEUE"]
    broker_url = url or current_app.config["PROTOCOL_BROKER_URL"]
    connection = pika.BlockingConnection(pika.URLParameters(broker_url))
    REPLY_CODE_PRECONDITION_FAILED = 406

    while True:
        channel = connection.channel()
        try:
            channel.queue_delete(
                queue=queue_name,
                if_unused=True,
                if_empty=True,
            )
            logger.info('Deleted "%s" queue.', queue_name)
            break
        except pika.exceptions.ChannelClosedByBroker as e:
            if e.reply_code != REPLY_CODE_PRECONDITION_FAILED:
                raise
            time.sleep(3.0)


@swpt_accounts.command("verify_shard_content")
@with_appcontext
def verify_shard_content():
    """Verify that the shard contains only records belonging to the
    shard.

    If the verification is successful, the exit code will be 0. If a
    record has been found that does not belong to the shard, the exit
    code will be 1.
    """

    class InvalidRecord(Exception):
        """The record does not belong the shard."""

    sharding_realm: ShardingRealm = current_app.config["SHARDING_REALM"]
    yield_per = current_app.config["APP_VERIFY_SHARD_YIELD_PER"]
    sleep_seconds = current_app.config["APP_VERIFY_SHARD_SLEEP_SECONDS"]

    def verify_table(conn, *table_columns):
        with conn.execution_options(yield_per=yield_per).execute(
                select(*table_columns)
        ) as result:
            for n, row in enumerate(result):
                if n % yield_per == 0 and sleep_seconds > 0.0:
                    time.sleep(sleep_seconds)
                if not sharding_realm.match(*row):
                    raise InvalidRecord

    with db.engine.connect() as conn:
        logger = logging.getLogger(__name__)
        try:
            verify_table(conn, Account.debtor_id, Account.creditor_id)
            verify_table(
                conn,
                PendingBalanceChange.debtor_id,
                PendingBalanceChange.creditor_id,
            )
        except InvalidRecord:
            logger.error(
                "At least one record has been found that does not belong to"
                " the shard."
            )
            sys.exit(1)


@swpt_accounts.command("create_chores_queue")
@with_appcontext
def create_chores_queue():  # pragma: no cover
    """Declare a RabbitMQ queue for accounts' chores."""

    logger = logging.getLogger(__name__)
    queue_name = current_app.config["CHORES_BROKER_QUEUE"]
    broker_url = current_app.config["CHORES_BROKER_URL"]
    connection = pika.BlockingConnection(pika.URLParameters(broker_url))
    channel = connection.channel()

    # declare the queue
    channel.queue_declare(
        queue_name,
        durable=True,
        arguments={"x-queue-type": "quorum"},
    )
    logger.info('Declared "%s" queue.', queue_name)


@swpt_accounts.command("process_balance_changes")
@with_appcontext
@click.option(
    "-t", "--threads", type=int, help="The number of worker threads."
)
@click.option(
    "-w",
    "--wait",
    type=float,
    help=(
        "The minimal number of seconds between"
        " the queries to obtain pending balance changes."
    ),
)
@click.option(
    "--quit-early",
    is_flag=True,
    default=False,
    help="Exit after some time (mainly useful during testing).",
)
def process_balance_changes(threads, wait, quit_early):
    """Process pending balance changes.

    If --threads is not specified, the value of the configuration
    variable PROCESS_BALANCE_CHANGES_THREADS is taken. If it is not
    set, the default number of threads is 1.

    If --wait is not specified, the value of the configuration
    variable APP_PROCESS_BALANCE_CHANGES_WAIT is taken. If it is not
    set, the default number of seconds is 2.

    """

    # TODO: Consider allowing load-sharing between multiple processes
    #       or containers. This may also be true for the other
    #       "process_*" CLI commands. A possible way to do this is to
    #       separate the `args collection` in multiple buckets,
    #       assigning a dedicated process/container for each bucket.
    #       Note that this would makes sense only if the load is
    #       CPU-bound, which is unlikely, especially if we
    #       re-implement the logic in stored procedures.

    threads = threads or int(
        current_app.config["PROCESS_BALANCE_CHANGES_THREADS"]
    )
    wait = (
        wait
        if wait is not None
        else current_app.config["APP_PROCESS_BALANCE_CHANGES_WAIT"]
    )
    max_count = current_app.config["APP_PROCESS_BALANCE_CHANGES_MAX_COUNT"]

    def get_args_collection():
        return procedures.get_accounts_with_pending_balance_changes(
            max_count=max_count
        )

    logger = logging.getLogger(__name__)
    logger.info("Started balance changes processor.")

    ThreadPoolProcessor(
        threads,
        get_args_collection=get_args_collection,
        process_func=procedures.process_pending_balance_changes,
        wait_seconds=wait,
        max_count=max_count,
    ).run(quit_early=quit_early)


@swpt_accounts.command("process_transfer_requests")
@with_appcontext
@click.option(
    "-t", "--threads", type=int, help="The number of worker threads."
)
@click.option(
    "-w",
    "--wait",
    type=float,
    help=(
        "The minimal number of seconds between"
        " the queries to obtain pending transfer requests."
    ),
)
@click.option(
    "--quit-early",
    is_flag=True,
    default=False,
    help="Exit after some time (mainly useful during testing).",
)
def process_transfer_requests(threads, wait, quit_early):
    """Process pending transfer requests.

    If --threads is not specified, the value of the configuration
    variable PROCESS_TRANSFER_REQUESTS_THREADS is taken. If it is not
    set, the default number of threads is 1.

    If --wait is not specified, the value of the configuration
    variable APP_PROCESS_TRANSFER_REQUESTS_WAIT is taken. If it is not
    set, the default number of seconds is 2.

    """

    threads = threads or int(
        current_app.config["PROCESS_TRANSFER_REQUESTS_THREADS"]
    )
    wait = (
        wait
        if wait is not None
        else current_app.config["APP_PROCESS_TRANSFER_REQUESTS_WAIT"]
    )
    commit_period = int(
        current_app.config["APP_PREPARED_TRANSFER_MAX_DELAY_DAYS"]
        * SECONDS_IN_DAY
    )
    max_count = current_app.config["APP_PROCESS_TRANSFER_REQUESTS_MAX_COUNT"]

    logger = logging.getLogger(__name__)
    logger.info("Started transfer requests processor.")

    def get_args_collection():
        rows = procedures.get_accounts_with_transfer_requests(
            max_count=max_count
        )
        return [
            (debtor_id, creditor_id, commit_period)
            for debtor_id, creditor_id in rows
        ]

    ThreadPoolProcessor(
        threads,
        get_args_collection=get_args_collection,
        process_func=procedures.process_transfer_requests,
        wait_seconds=wait,
        max_count=max_count,
    ).run(quit_early=quit_early)


@swpt_accounts.command("process_finalization_requests")
@with_appcontext
@click.option(
    "-t", "--threads", type=int, help="The number of worker threads."
)
@click.option(
    "-w",
    "--wait",
    type=float,
    help=(
        "The minimal number of seconds between"
        " the queries to obtain pending finalization requests."
    ),
)
@click.option(
    "--quit-early",
    is_flag=True,
    default=False,
    help="Exit after some time (mainly useful during testing).",
)
def process_finalization_requests(threads, wait, quit_early):
    """Process pending finalization requests.

    If --threads is not specified, the value of the configuration
    variable PROCESS_FINALIZATION_REQUESTS_THREADS is taken. If it is
    not set, the default number of threads is 1.

    If --wait is not specified, the value of the configuration
    variable APP_PROCESS_FINALIZATION_REQUESTS_WAIT is taken. If it is
    not set, the default number of seconds is 2.

    """

    threads = (
        threads
        if threads is not None
        else current_app.config["PROCESS_FINALIZATION_REQUESTS_THREADS"]
    )
    wait = (
        wait
        if wait is not None
        else current_app.config["APP_PROCESS_FINALIZATION_REQUESTS_WAIT"]
    )
    max_count = current_app.config[
        "APP_PROCESS_FINALIZATION_REQUESTS_MAX_COUNT"
    ]

    def should_ignore_requests(debtor_id: int, creditor_id: int) -> bool:
        if not is_valid_account(debtor_id, creditor_id):
            if current_app.config[
                "DELETE_PARENT_SHARD_RECORDS"
            ] and is_valid_account(debtor_id, creditor_id, match_parent=True):
                # NOTE: Finalization requests that have been created by the
                #       parent shard, should be processed only by one of the
                #       children shards.
                return True
            raise RuntimeError(
                "The shard is not responsible for this account."
            )  # pragma: no cover
        return False

    def get_args_collection():
        rows = procedures.get_accounts_with_finalization_requests(
            max_count=max_count
        )
        return [
            (
                debtor_id,
                creditor_id,
                should_ignore_requests(debtor_id, creditor_id),
            )
            for debtor_id, creditor_id in rows
        ]

    logger = logging.getLogger(__name__)
    logger.info("Started finalization requests processor.")

    ThreadPoolProcessor(
        threads,
        get_args_collection=get_args_collection,
        process_func=procedures.process_finalization_requests,
        wait_seconds=wait,
        max_count=max_count,
    ).run(quit_early=quit_early)


@swpt_accounts.command("scan_accounts")
@with_appcontext
@click.option("-h", "--hours", type=float, help="The number of hours.")
@click.option(
    "--quit-early",
    is_flag=True,
    default=False,
    help="Exit after some time (mainly useful during testing).",
)
def scan_accounts(hours, quit_early):
    """Start a process that executes accounts maintenance operations.

    The specified number of hours determines the intended duration of
    a single pass through the accounts table. If the number of hours
    is not specified, the value of the environment variable
    APP_ACCOUNTS_SCAN_HOURS is taken. If it is not set, the default
    number of hours is 8.

    """

    from swpt_accounts.table_scanners import AccountScanner

    logger = logging.getLogger(__name__)
    logger.info("Started accounts scanner.")
    hours = hours or current_app.config["APP_ACCOUNTS_SCAN_HOURS"]
    assert hours > 0.0
    scanner = AccountScanner()
    scanner.run(db.engine, timedelta(hours=hours), quit_early=quit_early)


@swpt_accounts.command("scan_prepared_transfers")
@with_appcontext
@click.option("-d", "--days", type=float, help="The number of days.")
@click.option(
    "--quit-early",
    is_flag=True,
    default=False,
    help="Exit after some time (mainly useful during testing).",
)
def scan_prepared_transfers(days, quit_early):
    """Start a process that attempts to finalize staled prepared transfers.

    The specified number of days determines the intended duration of a
    single pass through the accounts table. If the number of days is
    not specified, the value of the environment variable
    APP_PREPARED_TRANSFERS_SCAN_DAYS is taken. If it is not set, the
    default number of days is 1.

    """

    from swpt_accounts.table_scanners import PreparedTransferScanner

    logger = logging.getLogger(__name__)
    logger.info("Started prepared transfers scanner.")
    days = days or current_app.config["APP_PREPARED_TRANSFERS_SCAN_DAYS"]
    assert days > 0.0
    scanner = PreparedTransferScanner()
    scanner.run(db.engine, timedelta(days=days), quit_early=quit_early)


@swpt_accounts.command("scan_registered_balance_changes")
@with_appcontext
@click.option("-d", "--days", type=float, help="The number of days.")
@click.option(
    "--quit-early",
    is_flag=True,
    default=False,
    help="Exit after some time (mainly useful during testing).",
)
def scan_registered_balance_changes(days, quit_early):
    """Start a process that deletes stale registered balance changes.

    The specified number of days determines the intended duration of a
    single pass through the registered balance changes table. If the
    number of days is not specified, the value of the environment
    variable APP_REGISTERED_BALANCE_CHANGES_SCAN_DAYS is taken. If it
    is not set, the default number of days is 7.

    """

    from swpt_accounts.table_scanners import RegisteredBalanceChangeScanner

    logger = logging.getLogger(__name__)
    logger.info("Started registered balance changes scanner.")
    days = (
        days or current_app.config["APP_REGISTERED_BALANCE_CHANGES_SCAN_DAYS"]
    )
    assert days > 0.0
    scanner = RegisteredBalanceChangeScanner()
    scanner.run(db.engine, timedelta(days=days), quit_early=quit_early)


@swpt_accounts.command("consume_messages")
@with_appcontext
@click.option("-u", "--url", type=str, help="The RabbitMQ connection URL.")
@click.option(
    "-q", "--queue", type=str, help="The name the queue to consume from."
)
@click.option(
    "-p", "--processes", type=int, help="The number of worker processes."
)
@click.option(
    "-t",
    "--threads",
    type=int,
    help="The number of threads running in each process.",
)
@click.option(
    "-s",
    "--prefetch-size",
    type=int,
    help="The prefetch window size in bytes.",
)
@click.option(
    "-c",
    "--prefetch-count",
    type=int,
    help="The prefetch window in terms of whole messages.",
)
@click.option(
    "--draining-mode",
    is_flag=True,
    help="Make periodic pauses to allow the queue to be deleted safely.",
)
def consume_messages(
    url, queue, processes, threads, prefetch_size, prefetch_count,
    draining_mode
):
    """Consume and process incoming Swaptacular Messaging Protocol
    messages.

    If some of the available options are not specified directly, the
    values of the following environment variables will be used:

    * PROTOCOL_BROKER_URL (default "amqp://guest:guest@localhost:5672")

    * PROTOCOL_BROKER_QUEUE (defalut "swpt_accounts")

    * PROTOCOL_BROKER_PROCESSES (defalut 1)

    * PROTOCOL_BROKER_THREADS (defalut 1)

    * PROTOCOL_BROKER_PREFETCH_COUNT (default 1)

    * PROTOCOL_BROKER_PREFETCH_SIZE (default 0, meaning unlimited)

    """

    def _consume_messages(
        url, queue, threads, prefetch_size, prefetch_count
    ):  # pragma: no cover
        """Consume messages in a subprocess."""

        from swpt_accounts.actors import SmpConsumer, TerminatedConsumtion
        from swpt_accounts import create_app

        consumer = SmpConsumer(
            app=create_app(),
            config_prefix="PROTOCOL_BROKER",
            url=url,
            queue=queue,
            threads=threads,
            prefetch_size=prefetch_size,
            prefetch_count=prefetch_count,
            draining_mode=draining_mode,
        )
        for sig in HANDLED_SIGNALS:
            signal.signal(sig, consumer.stop)
        try_unblock_signals()

        pid = os.getpid()
        logger = logging.getLogger(__name__)
        logger.info("Worker with PID %i started processing messages.", pid)

        try:
            consumer.start()
        except TerminatedConsumtion:
            pass

        logger.info("Worker with PID %i stopped processing messages.", pid)

    spawn_worker_processes(
        processes=processes or current_app.config["PROTOCOL_BROKER_PROCESSES"],
        target=_consume_messages,
        url=url,
        queue=queue,
        threads=threads,
        prefetch_size=prefetch_size,
        prefetch_count=prefetch_count,
    )
    sys.exit(1)


@swpt_accounts.command("consume_chore_messages")
@with_appcontext
@click.option("-u", "--url", type=str, help="The RabbitMQ connection URL.")
@click.option(
    "-q", "--queue", type=str, help="The name the queue to consume from."
)
@click.option(
    "-p", "--processes", type=int, help="The number of worker processes."
)
@click.option(
    "-t",
    "--threads",
    type=int,
    help="The number of threads running in each process.",
)
@click.option(
    "-s",
    "--prefetch-size",
    type=int,
    help="The prefetch window size in bytes.",
)
@click.option(
    "-c",
    "--prefetch-count",
    type=int,
    help="The prefetch window in terms of whole messages.",
)
def consume_chore_messages(
    url, queue, processes, threads, prefetch_size, prefetch_count
):
    """Consume and process chore messages.

    If some of the available options are not specified directly, the
    values of the following environment variables will be used:

    * CHORES_BROKER_URL (default "amqp://guest:guest@localhost:5672")

    * CHORES_BROKER_QUEUE (defalut "swpt_accounts_chores")

    * CHORES_BROKER_PROCESSES (defalut 1)

    * CHORES_BROKER_THREADS (defalut 1)

    * CHORES_BROKER_PREFETCH_COUNT (default 1)

    * CHORES_BROKER_PREFETCH_SIZE (default 0, meaning unlimited)

    """

    def _consume_chore_messages(
        url, queue, threads, prefetch_size, prefetch_count
    ):  # pragma: no cover
        from swpt_accounts.chores import ChoresConsumer, TerminatedConsumtion
        from swpt_accounts import create_app

        consumer = ChoresConsumer(
            app=create_app(),
            config_prefix="CHORES_BROKER",
            url=url,
            queue=queue,
            threads=threads,
            prefetch_size=prefetch_size,
            prefetch_count=prefetch_count,
        )
        for sig in HANDLED_SIGNALS:
            signal.signal(sig, consumer.stop)
        try_unblock_signals()

        pid = os.getpid()
        logger = logging.getLogger(__name__)
        logger.info("Worker with PID %i started processing messages.", pid)

        try:
            consumer.start()
        except TerminatedConsumtion:
            pass

        logger.info("Worker with PID %i stopped processing messages.", pid)

    spawn_worker_processes(
        processes=processes or current_app.config["CHORES_BROKER_PROCESSES"],
        target=_consume_chore_messages,
        url=url,
        queue=queue,
        threads=threads,
        prefetch_size=prefetch_size,
        prefetch_count=prefetch_count,
    )
    sys.exit(1)


@swpt_accounts.command("flush_messages")
@with_appcontext
@click.option(
    "-p",
    "--processes",
    type=int,
    help=(
        "Then umber of worker processes."
        " If not specified, the value of the FLUSH_PROCESSES environment"
        " variable will be used, defaulting to 1 if empty."
    ),
)
@click.option(
    "-w",
    "--wait",
    type=float,
    help=(
        "Flush every FLOAT seconds."
        " If not specified, the value of the FLUSH_PERIOD environment"
        " variable will be used, defaulting to 2 seconds if empty."
    ),
)
@click.option(
    "--quit-early",
    is_flag=True,
    default=False,
    help="Exit after some time (mainly useful during testing).",
)
@click.argument("message_types", nargs=-1)
def flush_messages(
    message_types: list[str],
    processes: int,
    wait: float,
    quit_early: bool,
) -> None:
    """Send pending messages to the message broker.

    If a list of MESSAGE_TYPES is given, flushes only these types of
    messages. If no MESSAGE_TYPES are specified, flushes all messages.

    """
    logger = logging.getLogger(__name__)
    models_to_flush = get_models_to_flush(
        current_app.extensions["signalbus"], message_types
    )
    logger.info(
        "Started flushing %s.", ", ".join(m.__name__ for m in models_to_flush)
    )

    def _flush(
        models_to_flush: list[type[Model]],
        wait: Optional[float],
    ) -> None:  # pragma: no cover
        from swpt_accounts import create_app

        app = create_app()
        stopped = False

        def stop(signum: Any = None, frame: Any = None) -> None:
            nonlocal stopped
            stopped = True

        for sig in HANDLED_SIGNALS:
            signal.signal(sig, stop)
        try_unblock_signals()

        with app.app_context():
            signalbus: SignalBus = current_app.extensions["signalbus"]
            while not stopped:
                started_at = time.time()
                try:
                    count = signalbus.flushmany(models_to_flush)
                except Exception:
                    logger.exception(
                        "Caught error while sending pending signals."
                    )
                    sys.exit(1)

                if count > 0:
                    logger.info(
                        "%i signals have been successfully processed.", count
                    )
                else:
                    logger.debug("0 signals have been processed.")

                if quit_early:
                    break
                time.sleep(max(0.0, wait + started_at - time.time()))

    spawn_worker_processes(
        processes=(
            processes
            if processes is not None
            else current_app.config["FLUSH_PROCESSES"]
        ),
        target=_flush,
        models_to_flush=models_to_flush,
        wait=(
            wait if wait is not None else current_app.config["FLUSH_PERIOD"]
        ),
    )
    sys.exit(1)
