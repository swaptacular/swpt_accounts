import logging
import click
import time
import threading
import signal
import pika
from datetime import timedelta
from os import environ
from multiprocessing.dummy import Pool as ThreadPool
from flask import current_app
from flask.cli import with_appcontext
from swpt_accounts import procedures
from swpt_accounts.extensions import db
from swpt_accounts.models import SECONDS_IN_DAY


class ThreadPoolProcessor:
    def __init__(self, threads, *, get_args_collection, process_func, wait_seconds):
        self.logger = logging.getLogger(__name__)
        self.threads = threads
        self.get_args_collection = get_args_collection
        self.process_func = process_func
        self.wait_seconds = wait_seconds
        self.all_done = threading.Condition()
        self.pending = 0
        self.error_has_occurred = False

    def _wait_until_all_done(self):
        while self.pending > 0:
            self.all_done.wait()
        assert self.pending == 0

    def _mark_done(self, result=None):
        with self.all_done:
            self.pending -= 1
            if self.pending <= 0:
                self.all_done.notify()

    def _log_error(self, e):  # pragma: no cover
        self._mark_done()
        try:
            raise e
        except Exception:
            self.logger.exception('Caught error while processing objects.')

        self.error_has_occurred = True

    def run(self, *, quit_early=False):
        app = current_app._get_current_object()

        def push_app_context():
            ctx = app.app_context()
            ctx.push()

        pool = ThreadPool(self.threads, initializer=push_app_context)
        iteration_counter = 0

        while not (self.error_has_occurred or (quit_early and iteration_counter > 0)):
            iteration_counter += 1
            started_at = time.time()
            args_collection = self.get_args_collection()

            with self.all_done:
                self.pending += len(args_collection)

            for args in args_collection:
                pool.apply_async(self.process_func, args, callback=self._mark_done, error_callback=self._log_error)

            with self.all_done:
                self._wait_until_all_done()

            time.sleep(max(0.0, self.wait_seconds + started_at - time.time()))

        pool.close()
        pool.join()


@click.group('swpt_accounts')
def swpt_accounts():
    """Perform operations on Swaptacular accounts."""


@swpt_accounts.command()
@with_appcontext
@click.argument('queue_name')
@click.option('-r', '--routing-key', type=str, default='#', help='Specify a routing key (the default is "#").')
def subscribe(queue_name, routing_key):  # pragma: no cover
    """Declare a RabbitMQ queue, and subscribe it to receive incoming
    messages.

    QUEUE_NAME specifies the name of the queue.

    """

    from .extensions import ACCOUNTS_IN_EXCHANGE, \
        TO_CREDITORS_EXCHANGE, TO_DEBTORS_EXCHANGE, TO_COORDINATORS_EXCHANGE, \
        DEBTORS_IN_EXCHANGE, DEBTORS_OUT_EXCHANGE, \
        CREDITORS_IN_EXCHANGE, CREDITORS_OUT_EXCHANGE

    logger = logging.getLogger(__name__)
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

    # TODO: Consider allowing load-sharing between multiple
    #       containers. A possible way to do this is to separate the
    #       `args collection` in multiple buckets, assigning a
    #       dedicated container for each bucket. This may also be true
    #       for the other "process_*" CLI commands. Note that this
    #       would makes sense only if the load is CPU-bound, which is
    #       unlikely if we re-implement the logic in stored
    #       procedures.

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

    threads = threads or int(environ.get('APP_PROCESS_FINALIZATION_REQUESTS_THREADS', '1'))
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


@swpt_accounts.command('process_messages')
@with_appcontext
def process_messages():  # pragma: no cover
    """Process incoming Swaptacular Messaging Protocol messages.

    The following environment variables control the behavior of the
    message consumer:

    PROTOCOL_BROKER_URL: RabbimMQ connection URL (default
    "amqp://guest:guest@localhost:5672")

    PROTOCOL_BROKER_QUEUE: the queue name (defalut "swpt_accounts")

    PROTOCOL_BROKER_THREADS: number of worker threads (defalut 1)

    PROTOCOL_BROKER_PREFETCH_COUNT: max prefetched messages (default 1)

    PROTOCOL_BROKER_PREFETCH_SIZE: max prefetched bytes (default unlimited)

    """

    from swpt_accounts.actors import SmpConsumer, TerminatedConsumtion

    logger = logging.getLogger(__name__)
    logger.info('Started processing incoming messages.')
    app = current_app._get_current_object()
    consumer = SmpConsumer(config_prefix='PROTOCOL_BROKER')
    consumer.init_app(app)

    signal.signal(signal.SIGINT, consumer.stop)
    signal.signal(signal.SIGTERM, consumer.stop)
    try:
        consumer.start()
    except TerminatedConsumtion:
        pass
    logger.info('Stopped processing incoming messages.')
