__version__ = "0.1.0"

import logging
import json
import sys
import os
import os.path
from typing import List
from datetime import datetime, timezone, timedelta
from swpt_pythonlib.utils import ShardingRealm


def _parse_datetime(s: str) -> datetime:
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt


def _parse_dict(s: str) -> dict:
    try:
        return json.loads(s)
    except ValueError:  # pragma: no cover
        raise ValueError(f"Invalid JSON configuration value: {s}")


def _excepthook(exc_type, exc_value, traceback):  # pragma: nocover
    logging.error(
        "Uncaught exception occured", exc_info=(exc_type, exc_value, traceback)
    )


def _remove_handlers(logger):
    for h in logger.handlers:
        logger.removeHandler(h)  # pragma: nocover


def _add_console_hander(logger, format: str):
    handler = logging.StreamHandler(sys.stdout)
    fmt = "%(asctime)s:%(levelname)s:%(name)s:%(message)s"

    if format == "text":
        handler.setFormatter(
            logging.Formatter(fmt, datefmt="%Y-%m-%d %H:%M:%S%z")
        )
    elif format == "json":  # pragma: nocover
        from pythonjsonlogger import jsonlogger

        handler.setFormatter(
            jsonlogger.JsonFormatter(fmt, datefmt="%Y-%m-%dT%H:%M:%S%z")
        )
    else:  # pragma: nocover
        raise RuntimeError(f"invalid log format: {format}")

    handler.addFilter(_filter_pika_connection_reset_errors)
    handler.addFilter(_filter_asyncio_unclosed_client_session_errors)
    logger.addHandler(handler)


def _configure_root_logger(format: str) -> logging.Logger:
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.WARNING)
    _remove_handlers(root_logger)
    _add_console_hander(root_logger, format)

    return root_logger


def _filter_pika_connection_reset_errors(
    record: logging.LogRecord,
) -> bool:  # pragma: nocover
    # NOTE: Currently, when one of Pika's connections to the RabbitMQ
    # server has not been used for some time, it will be closed by the
    # server. We successfully recover form these situations, but pika
    # logs a bunch of annoying errors. Here we filter out those
    # errors.

    message = record.getMessage()
    is_pika_connection_reset_error = record.levelno == logging.ERROR and (
        (
            record.name == "pika.adapters.utils.io_services_utils"
            and message.startswith(
                "_AsyncBaseTransport._produce() failed, aborting connection: "
                "error=ConnectionResetError(104, 'Connection reset by peer'); "
            )
        )
        or (
            record.name == "pika.adapters.base_connection"
            and message.startswith(
                'connection_lost: StreamLostError: ("Stream connection lost:'
                " ConnectionResetError(104, 'Connection reset by peer')\",)"
            )
        )
        or (
            record.name == "pika.adapters.blocking_connection"
            and message.startswith(
                "Unexpected connection close detected: StreamLostError:"
                ' ("Stream connection lost: ConnectionResetError(104,'
                " 'Connection reset by peer')\",)"
            )
        )
    )

    return not is_pika_connection_reset_error


def _filter_asyncio_unclosed_client_session_errors(
    record: logging.LogRecord,
) -> bool:  # pragma: nocover
    # NOTE: Currently, when using aiohttp's ClientSession,
    # periodically we get "Unclosed client session client_session:
    # <aiohttp.client.ClientSession object at ...>" error from the
    # "asyncio" module, which seems to be a harmless warning, but is
    # quite annoying. Here we filter out those errors.

    message = record.getMessage()
    is_unclosed_client_session_error = record.levelno == logging.ERROR and (
        record.name == "asyncio"
        and message.startswith(
            "Unclosed client session\nclient_session:"
            " <aiohttp.client.ClientSession object at "
        )
    )

    return not is_unclosed_client_session_error


def configure_logging(
    level: str, format: str, associated_loggers: List[str]
) -> None:
    root_logger = _configure_root_logger(format)

    # Set the log level for this app's logger.
    app_logger = logging.getLogger(__name__)
    app_logger.setLevel(level.upper())
    app_logger_level = app_logger.getEffectiveLevel()

    # Make sure that all loggers that are associated to this app have
    # their log levels set to the specified level as well.
    for qualname in associated_loggers:
        logging.getLogger(qualname).setLevel(app_logger_level)

    # Make sure that the root logger's log level (that is: the log
    # level for all third party libraires) is not lower than the
    # specified level.
    if app_logger_level > root_logger.getEffectiveLevel():
        root_logger.setLevel(app_logger_level)  # pragma: no cover

    # Delete all gunicorn's log handlers (they are not needed in a
    # docker container because everything goes to the stdout anyway),
    # and make sure that the gunicorn logger's log level is not lower
    # than the specified level.
    gunicorn_logger = logging.getLogger("gunicorn.error")
    gunicorn_logger.propagate = True
    _remove_handlers(gunicorn_logger)
    if app_logger_level > gunicorn_logger.getEffectiveLevel():
        gunicorn_logger.setLevel(app_logger_level)  # pragma: no cover


class MetaEnvReader(type):
    def __init__(cls, name, bases, dct):
        """MetaEnvReader class initializer.

        This function will get called when a new class which utilizes
        this metaclass is defined, as opposed to when an instance is
        initialized. This function overrides the default configuration
        from environment variables.

        """

        super().__init__(name, bases, dct)
        NoneType = type(None)
        annotations = dct.get("__annotations__", {})
        falsy_values = {"false", "off", "no", ""}
        for key, value in os.environ.items():
            if hasattr(cls, key):
                target_type = annotations.get(key) or type(getattr(cls, key))
                if target_type is NoneType:  # pragma: no cover
                    target_type = str

                if target_type is bool:
                    value = value.lower() not in falsy_values
                else:
                    value = target_type(value)

                setattr(cls, key, value)


class Configuration(metaclass=MetaEnvReader):
    SQLALCHEMY_DATABASE_URI = ""
    SQLALCHEMY_ENGINE_OPTIONS: _parse_dict = _parse_dict('{"pool_size": 0}')
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SQLALCHEMY_ECHO = False

    PROTOCOL_BROKER_URL = "amqp://guest:guest@localhost:5672"
    PROTOCOL_BROKER_QUEUE = "swpt_accounts"
    PROTOCOL_BROKER_QUEUE_ROUTING_KEY = "#"
    PROTOCOL_BROKER_PROCESSES = 1
    PROTOCOL_BROKER_THREADS = 1
    PROTOCOL_BROKER_PREFETCH_SIZE = 0
    PROTOCOL_BROKER_PREFETCH_COUNT = 1

    CHORES_BROKER_URL = "amqp://guest:guest@localhost:5672"
    CHORES_BROKER_QUEUE = "swpt_accounts_chores"
    CHORES_BROKER_PROCESSES = 1
    CHORES_BROKER_THREADS = 1
    CHORES_BROKER_PREFETCH_SIZE = 0
    CHORES_BROKER_PREFETCH_COUNT = 1

    FLUSH_PROCESSES = 1
    FLUSH_PERIOD = 2.0

    FETCH_API_URL: str = None

    PROCESS_TRANSFER_REQUESTS_THREADS = 1
    PROCESS_FINALIZATION_REQUESTS_THREADS = 1
    PROCESS_BALANCE_CHANGES_THREADS = 1

    DELETE_PARENT_SHARD_RECORDS = False
    REMOVE_FROM_ARCHIVE_THRESHOLD_DATE: _parse_datetime = _parse_datetime(
        "1970-01-01"
    )

    # NOTE: Some of the functionality has two implementations: A pure
    # Python implementation, and a PG/PLSQL implementation. The goal
    # of the pure Python implementation is to be easy to understand,
    # change, and test. The goal of PG/PLSQL implementations is to
    # avoid possible Python single-process performance bottlenecks, as
    # well as to avoid unnecessary network round trips. The PG/PLSQL
    # implementations will be used by default, because they are likely
    # to perform better. When running the tests however, the pure
    # Python implementations will be used. If you want to run the
    # tests with the PG/PLSQL implementations, start the tests with
    # the command: `pytest --use-pgplsql=true`.
    APP_USE_PGPLSQL_FUNCTIONS = True

    APP_PROCESS_BALANCE_CHANGES_WAIT = 2.0
    APP_PROCESS_BALANCE_CHANGES_MAX_COUNT = 100000
    APP_PROCESS_TRANSFER_REQUESTS_WAIT = 2.0
    APP_PROCESS_TRANSFER_REQUESTS_MAX_COUNT = 100000
    APP_PROCESS_FINALIZATION_REQUESTS_WAIT = 2.0
    APP_PROCESS_FINALIZATION_REQUESTS_MAX_COUNT = 100000
    APP_FLUSH_REJECTED_TRANSFERS_BURST_COUNT = 10000
    APP_FLUSH_PREPARED_TRANSFERS_BURST_COUNT = 10000
    APP_FLUSH_FINALIZED_TRANSFERS_BURST_COUNT = 10000
    APP_FLUSH_ACCOUNT_TRANSFERS_BURST_COUNT = 10000
    APP_FLUSH_ACCOUNT_UPDATES_BURST_COUNT = 10000
    APP_FLUSH_ACCOUNT_PURGES_BURST_COUNT = 10000
    APP_FLUSH_REJECTED_CONFIGS_BURST_COUNT = 10000
    APP_FLUSH_PENDING_BALANCE_CHANGES_BURST_COUNT = 10000
    APP_ACCOUNTS_SCAN_HOURS = 8.0
    APP_PREPARED_TRANSFERS_SCAN_DAYS = 1.0
    APP_REGISTERED_BALANCE_CHANGES_SCAN_DAYS = 7.0
    APP_INTRANET_EXTREME_DELAY_DAYS = 14.0
    APP_MESSAGE_MAX_DELAY_DAYS = 7.0
    APP_ACCOUNT_HEARTBEAT_DAYS = 7.0
    APP_PREPARED_TRANSFER_REMAINDER_DAYS = 7.0
    APP_PREPARED_TRANSFER_MAX_DELAY_DAYS = 90.0
    APP_FETCH_API_TIMEOUT_SECONDS = 5.0
    APP_FETCH_DNS_CACHE_SECONDS = 10.0
    APP_FETCH_CONNECTIONS = 100
    APP_FETCH_DATA_CACHE_SIZE = 1000
    APP_MIN_INTEREST_CAPITALIZATION_DAYS = 14.0
    APP_MAX_INTEREST_TO_PRINCIPAL_RATIO = 0.0001
    APP_DELETION_ATTEMPTS_MIN_DAYS = 14.0
    APP_ACCOUNTS_SCAN_BLOCKS_PER_QUERY = 40
    APP_ACCOUNTS_SCAN_BEAT_MILLISECS = 100
    APP_PREPARED_TRANSFERS_SCAN_BLOCKS_PER_QUERY = 40
    APP_PREPARED_TRANSFERS_SCAN_BEAT_MILLISECS = 100
    APP_REGISTERED_BALANCE_CHANGES_SCAN_BLOCKS_PER_QUERY = 40
    APP_REGISTERED_BALANCE_CHANGES_SCAN_BEAT_MILLISECS = 100
    APP_VERIFY_SHARD_YIELD_PER = 10000
    APP_VERIFY_SHARD_SLEEP_SECONDS = 0.005


def _check_config_sanity(c):  # pragma: nocover
    if c["APP_PREPARED_TRANSFER_MAX_DELAY_DAYS"] < 30:
        raise RuntimeError(
            "The configured value for APP_PREPARED_TRANSFER_MAX_DELAY_DAYS"
            " must not be smaller than 30 days."
        )

    if (
        c["APP_PREPARED_TRANSFER_MAX_DELAY_DAYS"]
        < c["APP_MESSAGE_MAX_DELAY_DAYS"]
    ):
        raise RuntimeError(
            "The configured value for APP_PREPARED_TRANSFER_MAX_DELAY_DAYS is"
            " too small compared to the configured value for"
            " APP_MESSAGE_MAX_DELAY_DAYS. This may result in frequent timing"
            " out of prepared transfers due to message delays. Choose more"
            " appropriate configuration values."
        )

    if (
        c["APP_PREPARED_TRANSFER_MAX_DELAY_DAYS"]
        < c["APP_INTRANET_EXTREME_DELAY_DAYS"]
    ):
        raise RuntimeError(
            "The configured value for APP_PREPARED_TRANSFER_MAX_DELAY_DAYS is"
            " too small compared to the configured value for"
            " APP_INTRANET_EXTREME_DELAY_DAYS. This may result in timing"
            " out of prepared transfers due to extreme message delays. Choose"
            " more appropriate configuration values."
        )

    if not 0.0 < c["APP_MAX_INTEREST_TO_PRINCIPAL_RATIO"] <= 0.10:
        raise RuntimeError(
            "The configured value for APP_MAX_INTEREST_TO_PRINCIPAL_RATIO is"
            " outside of the interval that is good for practical use. Choose a"
            " more appropriate value."
        )

    if c["APP_MIN_INTEREST_CAPITALIZATION_DAYS"] > 92:
        raise RuntimeError(
            "The configured value for APP_MIN_INTEREST_CAPITALIZATION_DAYS is"
            " too big. This may result in quirky capitalization of the"
            " accumulated interest. Choose a more appropriate value."
        )

    if c["APP_ACCOUNTS_SCAN_HOURS"] > 48:
        raise RuntimeError(
            "The configured value for APP_ACCOUNTS_SCAN_HOURS is too big."
            " This may result in lagging account status updates. Choose a more"
            " appropriate value."
        )

    if c["APP_ACCOUNT_HEARTBEAT_DAYS"] > 14:
        raise RuntimeError(
            "The configured value for APP_ACCOUNT_HEARTBEAT_DAYS is too big."
            " This may result in a missed account heartbeats. Choose a more"
            " appropriate value."
        )

    if c["REMOVE_FROM_ARCHIVE_THRESHOLD_DATE"] > datetime.now(
        tz=timezone.utc
    ) - timedelta(days=c["APP_INTRANET_EXTREME_DELAY_DAYS"]):
        raise RuntimeError(
            "The configured date for REMOVE_FROM_ARCHIVE_THRESHOLD_DATE is too"
            " recent. This may result in discarding balance change events."
            " Choose a more appropriate value."
        )


def create_app(config_dict={}):
    from werkzeug.middleware.proxy_fix import ProxyFix
    from flask import Flask
    from swpt_pythonlib.utils import Int64Converter
    from .extensions import db, migrate, publisher, chores_publisher
    from .routes import fetch_api
    from .cli import swpt_accounts
    from . import models  # noqa

    app = Flask(__name__)
    app.wsgi_app = ProxyFix(app.wsgi_app, x_port=1)
    app.url_map.converters["i64"] = Int64Converter
    app.config.from_object(Configuration)
    app.config.from_mapping(config_dict)
    app.config["SHARDING_REALM"] = ShardingRealm(
        Configuration.PROTOCOL_BROKER_QUEUE_ROUTING_KEY
    )
    db.init_app(app)
    migrate.init_app(app, db)
    publisher.init_app(app)
    chores_publisher.init_app(app)
    app.register_blueprint(fetch_api)
    app.cli.add_command(swpt_accounts)
    _check_config_sanity(app.config)

    return app


configure_logging(
    level=os.environ.get("APP_LOG_LEVEL", "warning"),
    format=os.environ.get("APP_LOG_FORMAT", "text"),
    associated_loggers=os.environ.get("APP_ASSOCIATED_LOGGERS", "").split(),
)
sys.excepthook = _excepthook
