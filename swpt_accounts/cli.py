import logging
import click
from os import environ
from multiprocessing.dummy import Pool as ThreadPool
from flask import current_app
from flask.cli import with_appcontext
from . import procedures


@click.group('swpt_accounts')
def swpt_accounts():
    """Perform operations on Swaptacular accounts."""


@swpt_accounts.command()
@with_appcontext
@click.option('-t', '--threads', type=int, help='The number of worker threads.')
def process_pending_changes(threads):
    """Process all pending account changes."""

    threads = threads or int(environ.get('APP_PENDING_CHANGES_THREADS', '1'))
    app = current_app._get_current_object()

    def push_app_context():
        ctx = app.app_context()
        ctx.push()

    def log_error(e):  # pragma: no cover
        try:
            raise e
        except Exception:
            logger = logging.getLogger(__name__)
            logger.exception('Caught error while processing account pending changes.')

    # TODO: Python with SQLAlchemy can process about 1000 accounts per
    # second. (It is CPU bound!) This might be insufficient if we have
    # a highly perfomant database server. In this case we should
    # either distribute the processing to several machines, or improve
    # on python's code performance.
    pool = ThreadPool(threads, initializer=push_app_context)
    for account_pk in procedures.get_accounts_with_pending_changes():
        pool.apply_async(procedures.process_pending_changes, account_pk, error_callback=log_error)
    pool.close()
    pool.join()
