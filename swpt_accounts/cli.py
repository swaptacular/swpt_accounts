import logging
import os
import sys
import click
import signal
import pika
from datetime import timedelta
from flask import current_app
from flask.cli import with_appcontext
from swpt_accounts import procedures
from swpt_accounts.extensions import db
from swpt_accounts.models import SECONDS_IN_DAY
from swpt_accounts.multiproc_utils import ThreadPoolProcessor, spawn_worker_processes, \
    try_unblock_signals, HANDLED_SIGNALS


@click.group('swpt_accounts')
def swpt_accounts():
    """Perform operations on Swaptacular accounts."""


@swpt_accounts.command('subscribe')
@with_appcontext
def subscribe():  # pragma: no cover
    """Declare a RabbitMQ queue, and subscribe it to receive incoming
    messages.

    This is mainly useful during development and testing.
    """

    from .extensions import ACCOUNTS_IN_EXCHANGE, \
        TO_CREDITORS_EXCHANGE, TO_DEBTORS_EXCHANGE, TO_COORDINATORS_EXCHANGE, \
        DEBTORS_IN_EXCHANGE, DEBTORS_OUT_EXCHANGE, \
        CREDITORS_IN_EXCHANGE, CREDITORS_OUT_EXCHANGE

    logger = logging.getLogger(__name__)
    queue_name = current_app.config['PROTOCOL_BROKER_QUEUE']
    routing_key = current_app.config['PROTOCOL_BROKER_QUEUE_ROUTING_KEY']
    dead_letter_queue_name = queue_name + '.XQ'
    broker_url = current_app.config['PROTOCOL_BROKER_URL']
    connection = pika.BlockingConnection(pika.URLParameters(broker_url))
    channel = connection.channel()

    # declare exchanges
    channel.exchange_declare(ACCOUNTS_IN_EXCHANGE, exchange_type='topic', durable=True)
    channel.exchange_declare(TO_CREDITORS_EXCHANGE, exchange_type='topic', durable=True)
    channel.exchange_declare(TO_DEBTORS_EXCHANGE, exchange_type='topic', durable=True)
    channel.exchange_declare(TO_COORDINATORS_EXCHANGE, exchange_type='headers', durable=True)
    channel.exchange_declare(CREDITORS_IN_EXCHANGE, exchange_type='topic', durable=True)
    channel.exchange_declare(CREDITORS_OUT_EXCHANGE, exchange_type='topic', durable=True)
    channel.exchange_declare(DEBTORS_IN_EXCHANGE, exchange_type='topic', durable=True)
    channel.exchange_declare(DEBTORS_OUT_EXCHANGE, exchange_type='fanout', durable=True)

    # declare exchange bindings
    channel.exchange_bind(source=TO_CREDITORS_EXCHANGE, destination=CREDITORS_IN_EXCHANGE, routing_key="#")
    channel.exchange_bind(source=TO_DEBTORS_EXCHANGE, destination=DEBTORS_IN_EXCHANGE, routing_key="#")
    channel.exchange_bind(source=TO_COORDINATORS_EXCHANGE, destination=TO_CREDITORS_EXCHANGE, arguments={
        "x-match": "all",
        "coordinator-type": "direct",
    })
    channel.exchange_bind(source=TO_COORDINATORS_EXCHANGE, destination=TO_DEBTORS_EXCHANGE, arguments={
        "x-match": "all",
        "coordinator-type": "issuing",
    })
    channel.exchange_bind(source=CREDITORS_OUT_EXCHANGE, destination=ACCOUNTS_IN_EXCHANGE, routing_key="#")
    channel.exchange_bind(source=DEBTORS_OUT_EXCHANGE, destination=ACCOUNTS_IN_EXCHANGE)

    # declare a corresponding dead-letter queue
    channel.queue_declare(dead_letter_queue_name, durable=True, arguments={
        'x-message-ttl': 604800000,
    })
    logger.info('Declared "%s" dead-letter queue.', dead_letter_queue_name)

    # declare the queue
    channel.queue_declare(queue_name, durable=True, arguments={
        "x-dead-letter-exchange": "",
        "x-dead-letter-routing-key": dead_letter_queue_name,
    })
    logger.info('Declared "%s" queue.', queue_name)

    # bind the queue
    channel.queue_bind(exchange=ACCOUNTS_IN_EXCHANGE, queue=queue_name, routing_key=routing_key)
    logger.info('Created a binding from "%s" to "%s" with routing key "%s".',
                ACCOUNTS_IN_EXCHANGE, queue_name, routing_key)


@swpt_accounts.command('create_chores_queue')
@with_appcontext
def create_chores_queue():  # pragma: no cover
    """Declare a RabbitMQ queue for accounts' chores."""

    logger = logging.getLogger(__name__)
    queue_name = current_app.config['CHORES_BROKER_QUEUE']
    dead_letter_queue_name = queue_name + '.XQ'
    broker_url = current_app.config['CHORES_BROKER_URL']
    connection = pika.BlockingConnection(pika.URLParameters(broker_url))
    channel = connection.channel()

    # declare a corresponding dead-letter queue
    channel.queue_declare(dead_letter_queue_name, durable=True, arguments={
        'x-message-ttl': 604800000,
    })
    logger.info('Declared "%s" dead-letter queue.', dead_letter_queue_name)

    # declare the queue
    channel.queue_declare(queue_name, durable=True, arguments={
        "x-dead-letter-exchange": "",
        "x-dead-letter-routing-key": dead_letter_queue_name,
    })
    logger.info('Declared "%s" queue.', queue_name)


@swpt_accounts.command('process_balance_changes')
@with_appcontext
@click.option('-t', '--threads', type=int, help='The number of worker threads.')
@click.option('-w', '--wait', type=float, help='The minimal number of seconds between'
              ' the queries to obtain pending balance changes.')
@click.option('--quit-early', is_flag=True, default=False, help='Exit after some time (mainly useful during testing).')
def process_balance_changes(threads, wait, quit_early):
    """Process pending balance changes.

    If --threads is not specified, the value of the configuration
    variable APP_PROCESS_BALANCE_CHANGES_THREADS is taken. If it is
    not set, the default number of threads is 1.

    If --wait is not specified, the value of the configuration
    variable APP_PROCESS_BALANCE_CHANGES_WAIT is taken. If it is not
    set, the default number of seconds is 5.

    """

    # TODO: Consider allowing load-sharing between multiple processes
    #       or containers. This may also be true for the other
    #       "process_*" CLI commands. A possible way to do this is to
    #       separate the `args collection` in multiple buckets,
    #       assigning a dedicated process/container for each bucket.
    #       Note that this would makes sense only if the load is
    #       CPU-bound, which is unlikely, especially if we
    #       re-implement the logic in stored procedures.

    threads = threads or int(current_app.config['APP_PROCESS_BALANCE_CHANGES_THREADS'])
    wait = wait if wait is not None else current_app.config['APP_PROCESS_BALANCE_CHANGES_WAIT']
    max_count = current_app.config['APP_PROCESS_BALANCE_CHANGES_MAX_COUNT']

    def get_args_collection():
        return procedures.get_accounts_with_pending_balance_changes(max_count=max_count)

    logger = logging.getLogger(__name__)
    logger.info('Started balance changes processor.')

    ThreadPoolProcessor(
        threads,
        get_args_collection=get_args_collection,
        process_func=procedures.process_pending_balance_changes,
        wait_seconds=wait,
    ).run(quit_early=quit_early)


@swpt_accounts.command('process_transfer_requests')
@with_appcontext
@click.option('-t', '--threads', type=int, help='The number of worker threads.')
@click.option('-w', '--wait', type=float, help='The minimal number of seconds between'
              ' the queries to obtain pending transfer requests.')
@click.option('--quit-early', is_flag=True, default=False, help='Exit after some time (mainly useful during testing).')
def process_transfer_requests(threads, wait, quit_early):
    """Process pending transfer requests.

    If --threads is not specified, the value of the configuration
    variable APP_PROCESS_TRANSFER_REQUESTS_THREADS is taken. If it is
    not set, the default number of threads is 1.

    If --wait is not specified, the value of the configuration
    variable APP_PROCESS_TRANSFER_REQUESTS_WAIT is taken. If it is not
    set, the default number of seconds is 5.

    """

    threads = threads or int(current_app.config['APP_PROCESS_TRANSFER_REQUESTS_THREADS'])
    wait = wait if wait is not None else current_app.config['APP_PROCESS_TRANSFER_REQUESTS_WAIT']
    commit_period = current_app.config['APP_PREPARED_TRANSFER_MAX_DELAY_DAYS'] * SECONDS_IN_DAY
    max_count = current_app.config['APP_PROCESS_TRANSFER_REQUESTS_MAX_COUNT']

    logger = logging.getLogger(__name__)
    logger.info('Started transfer requests processor.')

    def get_args_collection():
        return [
            (debtor_id, creditor_id, commit_period)
            for debtor_id, creditor_id
            in procedures.get_accounts_with_transfer_requests(max_count=max_count)
        ]

    ThreadPoolProcessor(
        threads,
        get_args_collection=get_args_collection,
        process_func=procedures.process_transfer_requests,
        wait_seconds=wait,
    ).run(quit_early=quit_early)


@swpt_accounts.command('process_finalization_requests')
@with_appcontext
@click.option('-t', '--threads', type=int, help='The number of worker threads.')
@click.option('-w', '--wait', type=float, help='The minimal number of seconds between'
              ' the queries to obtain pending finalization requests.')
@click.option('--quit-early', is_flag=True, default=False, help='Exit after some time (mainly useful during testing).')
def process_finalization_requests(threads, wait, quit_early):
    """Process pending finalization requests.

    If --threads is not specified, the value of the configuration
    variable APP_PROCESS_FINALIZATION_REQUESTS_THREADS is taken. If it
    is not set, the default number of threads is 1.

    If --wait is not specified, the value of the configuration
    variable APP_PROCESS_FINALIZATION_REQUESTS_WAIT is taken. If it is
    not set, the default number of seconds is 5.

    """

    threads = threads if threads is not None else current_app.config['APP_PROCESS_FINALIZATION_REQUESTS_THREADS']
    wait = wait if wait is not None else current_app.config['APP_PROCESS_FINALIZATION_REQUESTS_WAIT']
    max_count = current_app.config['APP_PROCESS_FINALIZATION_REQUESTS_MAX_COUNT']

    def get_args_collection():
        return procedures.get_accounts_with_finalization_requests(max_count=max_count)

    logger = logging.getLogger(__name__)
    logger.info('Started finalization requests processor.')

    ThreadPoolProcessor(
        threads,
        get_args_collection=get_args_collection,
        process_func=procedures.process_finalization_requests,
        wait_seconds=wait,
    ).run(quit_early=quit_early)


@swpt_accounts.command('scan_accounts')
@with_appcontext
@click.option('-h', '--hours', type=float, help='The number of hours.')
@click.option('--quit-early', is_flag=True, default=False, help='Exit after some time (mainly useful during testing).')
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
    logger.info('Started accounts scanner.')
    hours = hours or current_app.config['APP_ACCOUNTS_SCAN_HOURS']
    assert hours > 0.0
    scanner = AccountScanner()
    scanner.run(db.engine, timedelta(hours=hours), quit_early=quit_early)


@swpt_accounts.command('scan_prepared_transfers')
@with_appcontext
@click.option('-d', '--days', type=float, help='The number of days.')
@click.option('--quit-early', is_flag=True, default=False, help='Exit after some time (mainly useful during testing).')
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
    logger.info('Started prepared transfers scanner.')
    days = days or current_app.config['APP_PREPARED_TRANSFERS_SCAN_DAYS']
    assert days > 0.0
    scanner = PreparedTransferScanner()
    scanner.run(db.engine, timedelta(days=days), quit_early=quit_early)


@swpt_accounts.command('scan_registered_balance_changes')
@with_appcontext
@click.option('-d', '--days', type=float, help='The number of days.')
@click.option('--quit-early', is_flag=True, default=False, help='Exit after some time (mainly useful during testing).')
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
    logger.info('Started registered balance changes scanner.')
    days = days or current_app.config['APP_REGISTERED_BALANCE_CHANGES_SCAN_DAYS']
    assert days > 0.0
    scanner = RegisteredBalanceChangeScanner()
    scanner.run(db.engine, timedelta(days=days), quit_early=quit_early)


@swpt_accounts.command('consume_messages')
@with_appcontext
@click.option('-u', '--url', type=str, help='The RabbitMQ connection URL.')
@click.option('-q', '--queue', type=str, help='The name the queue to consume from.')
@click.option('-p', '--processes', type=int, help='The number of worker processes.')
@click.option('-t', '--threads', type=int, help='The number of threads running in each process.')
@click.option('-s', '--prefetch-size', type=int, help='The prefetch window size in bytes.')
@click.option('-c', '--prefetch-count', type=int, help='The prefetch window in terms of whole messages.')
def consume_messages(url, queue, processes, threads, prefetch_size, prefetch_count):  # pragma: no cover
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

    def _consume_messages(url, queue, threads, prefetch_size, prefetch_count):
        """Consume messages in a subprocess."""

        from swpt_accounts.actors import SmpConsumer, TerminatedConsumtion
        from swpt_accounts import create_app

        consumer = SmpConsumer(
            app=create_app(),
            config_prefix='PROTOCOL_BROKER',
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
        logger.info('Worker with PID %i started processing messages.', pid)

        try:
            consumer.start()
        except TerminatedConsumtion:
            pass

        logger.info('Worker with PID %i stopped processing messages.', pid)

    spawn_worker_processes(
        processes=processes or current_app.config['PROTOCOL_BROKER_PROCESSES'],
        target=_consume_messages,
        url=url,
        queue=queue,
        threads=threads,
        prefetch_size=prefetch_size,
        prefetch_count=prefetch_count,
    )
    sys.exit(1)


@swpt_accounts.command('consume_chore_messages')
@with_appcontext
@click.option('-u', '--url', type=str, help='The RabbitMQ connection URL.')
@click.option('-q', '--queue', type=str, help='The name the queue to consume from.')
@click.option('-p', '--processes', type=int, help='The number of worker processes.')
@click.option('-t', '--threads', type=int, help='The number of threads running in each process.')
@click.option('-s', '--prefetch-size', type=int, help='The prefetch window size in bytes.')
@click.option('-c', '--prefetch-count', type=int, help='The prefetch window in terms of whole messages.')
def consume_chore_messages(url, queue, processes, threads, prefetch_size, prefetch_count):
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

    def _consume_chore_messages(url, queue, threads, prefetch_size, prefetch_count):  # pragma: no cover
        from swpt_accounts.chores import ChoresConsumer, TerminatedConsumtion
        from swpt_accounts import create_app

        consumer = ChoresConsumer(
            app=create_app(),
            config_prefix='CHORES_BROKER',
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
        logger.info('Worker with PID %i started processing messages.', pid)

        try:
            consumer.start()
        except TerminatedConsumtion:
            pass

        logger.info('Worker with PID %i stopped processing messages.', pid)

    spawn_worker_processes(
        processes=processes or current_app.config['CHORES_BROKER_PROCESSES'],
        target=_consume_chore_messages,
        url=url,
        queue=queue,
        threads=threads,
        prefetch_size=prefetch_size,
        prefetch_count=prefetch_count,
    )
    sys.exit(1)
