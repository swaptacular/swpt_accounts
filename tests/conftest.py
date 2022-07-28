import pytest
import sqlalchemy
import flask_migrate
from datetime import datetime, timezone
from unittest import mock
from swpt_accounts import create_app
from swpt_accounts.extensions import db

DB_SESSION = 'swpt_accounts.extensions.db.session'

server_name = 'example.com'
config_dict = {
    'TESTING': True,
    'SWPT_URL_SCHEME': 'https',
    'SWPT_SERVER_NAME': server_name,
    'APP_MAX_INTEREST_TO_PRINCIPAL_RATIO': 0.01,
    'APP_FETCH_DATA_CACHE_SIZE': 10,
    'APP_REGISTERED_BALANCE_CHANGES_RETENTION_DATETIME': datetime(2000, 1, 1, tzinfo=timezone.utc),
}


def _restart_savepoint(session, transaction):
    if transaction.nested and not transaction._parent.nested:
        session.expire_all()
        session.begin_nested()


@pytest.fixture(scope='module')
def app_unsafe_session():
    app = create_app(config_dict)
    db.signalbus.autoflush = False
    with app.app_context():
        flask_migrate.upgrade()
        yield app


@pytest.fixture(scope='module')
def app():
    """Create a Flask application object."""

    app = create_app(config_dict)
    with app.app_context():
        flask_migrate.upgrade()
        forbidden = mock.Mock()
        forbidden.side_effect = RuntimeError('Database accessed without "db_session" fixture.')
        with mock.patch(DB_SESSION, new=forbidden):
            yield app


@pytest.fixture(scope='function')
def db_session(app):
    """Create a mocked Flask-SQLAlchmey session object.

    The standard Flask-SQLAlchmey's session object is replaced with a
    mock session that perform all database operations in a
    transaction, which is rolled back at the end of the test.

    """

    db.signalbus.autoflush = False
    engines_by_table = db.get_binds()
    connections_by_engine = {engine: engine.connect() for engine in set(engines_by_table.values())}
    transactions = [connection.begin() for connection in connections_by_engine.values()]
    session_options = dict(
        binds={table: connections_by_engine[engine] for table, engine in engines_by_table.items()},
    )
    session = db.create_scoped_session(options=session_options)
    session.begin_nested()
    sqlalchemy.event.listen(session, 'after_transaction_end', _restart_savepoint)
    with mock.patch(DB_SESSION, new=session):
        yield session
    sqlalchemy.event.remove(session, 'after_transaction_end', _restart_savepoint)
    session.remove()
    for transaction in transactions:
        transaction.rollback()
    for connection in connections_by_engine.values():
        connection.close()
