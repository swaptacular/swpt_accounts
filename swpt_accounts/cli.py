import logging
import click
from datetime import timedelta
from os import environ
from multiprocessing.dummy import Pool as ThreadPool
from flask import current_app
from flask.cli import with_appcontext
from swpt_accounts import procedures
from swpt_accounts.extensions import db
from swpt_accounts.models import SECONDS_IN_DAY
from swpt_accounts.table_scanners import AccountScanner, PreparedTransferScanner


@click.group('swpt_accounts')
def swpt_accounts():
    """Perform operations on Swaptacular accounts."""


@swpt_accounts.command()
@with_appcontext
@click.argument('queue_name')
def subscribe(queue_name):  # pragma: no cover
    """Subscribe a queue for the observed events and messages.

    QUEUE_NAME specifies the name of the queue.

    """

    from .extensions import protocol_broker, MAIN_EXCHANGE_NAME
    from . import actors  # noqa

    logger = logging.getLogger(__name__)
    channel = protocol_broker.channel
    channel.exchange_declare(MAIN_EXCHANGE_NAME)
    logger.info(f'Declared "{MAIN_EXCHANGE_NAME}" direct exchange.')

    if environ.get('APP_USE_LOAD_BALANCING_EXCHANGE', '') not in ['', 'False']:
        bind = channel.exchange_bind
        unbind = channel.exchange_unbind
    else:
        bind = channel.queue_bind
        unbind = channel.queue_unbind
    bind(queue_name, MAIN_EXCHANGE_NAME, queue_name)
    logger.info(f'Subscribed "{queue_name}" to "{MAIN_EXCHANGE_NAME}.{queue_name}".')

    for actor in [protocol_broker.get_actor(actor_name) for actor_name in protocol_broker.get_declared_actors()]:
        if 'event_subscription' in actor.options:
            routing_key = f'events.{actor.actor_name}'
            if actor.options['event_subscription']:
                bind(queue_name, MAIN_EXCHANGE_NAME, routing_key)
                logger.info(f'Subscribed "{queue_name}" to "{MAIN_EXCHANGE_NAME}.{routing_key}".')
            else:
                unbind(queue_name, MAIN_EXCHANGE_NAME, routing_key)
                logger.info(f'Unsubscribed "{queue_name}" from "{MAIN_EXCHANGE_NAME}.{routing_key}".')


@swpt_accounts.command('process_transfers')
@with_appcontext
@click.option('-t', '--threads', type=int, help='The number of worker threads.')
def process_transfers(threads):
    """Process all pending account changes and all transfer requests."""

    # TODO: Python with SQLAlchemy can process about 1000 accounts per
    # second. (It is CPU bound!) This might be insufficient if we have
    # a highly perfomant database server. In this case we should
    # either distribute the processing to several machines, or improve
    # on python's code performance.

    # TODO: Consider separating the processing of pending transfers
    # from the processing of pending account changes. This would allow
    # to trigger them with different frequency.

    threads = threads or int(environ.get('APP_PROCESS_TRANSFERS_THREADS', '1'))
    commit_period = int(current_app.config['APP_PREPARED_TRANSFER_MAX_DELAY_DAYS'] * SECONDS_IN_DAY)
    app = current_app._get_current_object()

    def push_app_context():
        ctx = app.app_context()
        ctx.push()

    def log_error(e):  # pragma: no cover
        try:
            raise e
        except Exception:
            logger = logging.getLogger(__name__)
            logger.exception('Caught error while processing transfers.')

    pool1 = ThreadPool(threads, initializer=push_app_context)
    for account_pk in procedures.get_accounts_with_pending_balance_changes():
        pool1.apply_async(procedures.process_pending_balance_changes, account_pk, error_callback=log_error)
    pool1.close()
    pool1.join()

    pool2 = ThreadPool(threads, initializer=push_app_context)
    for debtor_id, creditor_id in procedures.get_accounts_with_transfer_requests():
        pool2.apply_async(
            procedures.process_transfer_requests,
            (debtor_id, creditor_id, commit_period),
            error_callback=log_error,
        )
    pool2.close()
    pool2.join()

    pool3 = ThreadPool(threads, initializer=push_app_context)
    for account_pk in procedures.get_accounts_with_finalization_requests():
        pool3.apply_async(procedures.process_finalization_requests, account_pk, error_callback=log_error)
    pool3.close()
    pool3.join()


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

    logger = logging.getLogger(__name__)
    logger.info('Started prepared transfers scanner.')
    days = days or current_app.config['APP_PREPARED_TRANSFERS_SCAN_DAYS']
    assert days > 0.0
    scanner = PreparedTransferScanner()
    scanner.run(db.engine, timedelta(days=days), quit_early=quit_early)


# TODO: Implement a CLI command (or a table scanner) that *safely*
#       deletes old applied `RegisteredBalanceChange` records.
