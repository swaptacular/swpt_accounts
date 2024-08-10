import pytest
import sqlalchemy
import flask_migrate
from datetime import datetime, timezone
from swpt_accounts import create_app
from swpt_accounts.extensions import db

server_name = "example.com"
config_dict = {
    "TESTING": True,
    "SWPT_URL_SCHEME": "https",
    "SWPT_SERVER_NAME": server_name,
    "APP_MAX_INTEREST_TO_PRINCIPAL_RATIO": 0.01,
    "APP_FETCH_DATA_CACHE_SIZE": 10,
    "REMOVE_FROM_ARCHIVE_THRESHOLD_DATE": datetime(
        2000, 1, 1, tzinfo=timezone.utc
    ),
}


def pytest_addoption(parser):
    parser.addoption("--use-pgplsql", action="store", default="false")


@pytest.fixture(scope="module")
def app(request):
    """Get a Flask application object."""

    config_dict["APP_USE_PGPLSQL_FUNCTIONS"] = (
        request.config.option.use_pgplsql.lower() not in ["false", "no", "off"]
    )
    app = create_app(config_dict)
    with app.app_context():
        flask_migrate.upgrade()
        yield app


@pytest.fixture(scope="function")
def db_session(app):
    """Get a Flask-SQLAlchmey session, with an automatic cleanup."""

    yield db.session

    # Cleanup:
    db.session.remove()
    for cmd in [
        "TRUNCATE TABLE account CASCADE",
        "TRUNCATE TABLE transfer_request",
        "TRUNCATE TABLE finalization_request",
        "TRUNCATE TABLE registered_balance_change CASCADE",
        "TRUNCATE TABLE pending_balance_change",
        "TRUNCATE TABLE rejected_transfer_signal",
        "TRUNCATE TABLE prepared_transfer_signal",
        "TRUNCATE TABLE finalized_transfer_signal",
        "TRUNCATE TABLE account_transfer_signal",
        "TRUNCATE TABLE account_update_signal",
        "TRUNCATE TABLE account_purge_signal",
        "TRUNCATE TABLE rejected_config_signal",
        "TRUNCATE TABLE pending_balance_change_signal",
    ]:
        db.session.execute(sqlalchemy.text(cmd))
    db.session.commit()
