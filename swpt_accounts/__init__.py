__version__ = '0.1.0'

import os
import os.path
import logging
import logging.config

# Configure app logging. If the value of "$APP_LOGGING_CONFIG_FILE" is
# a relative path, the directory of this (__init__.py) file will be
# used as a current directory.
config_filename = os.environ.get('APP_LOGGING_CONFIG_FILE')
if config_filename:  # pragma: no cover
    if not os.path.isabs(config_filename):
        current_dir = os.path.dirname(__file__)
        config_filename = os.path.join(current_dir, config_filename)
    logging.config.fileConfig(config_filename, disable_existing_loggers=False)
else:
    logging.basicConfig(level=logging.WARNING)


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
        annotations = dct.get('__annotations__', {})
        falsy_values = {'false', 'off', 'no', ''}
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
    SECRET_KEY = 'dummy-secret'
    SQLALCHEMY_DATABASE_URI = ''
    SQLALCHEMY_POOL_SIZE: int = None
    SQLALCHEMY_POOL_TIMEOUT: int = None
    SQLALCHEMY_POOL_RECYCLE: int = None
    SQLALCHEMY_MAX_OVERFLOW: int = None
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SQLALCHEMY_ECHO = False
    DRAMATIQ_BROKER_CLASS = 'RabbitmqBroker'
    DRAMATIQ_BROKER_URL = 'amqp://guest:guest@localhost:5672'
    APP_ACCOUNTS_SCAN_HOURS = 8
    APP_PREPARED_TRANSFERS_SCAN_DAYS = 1
    APP_SIGNALBUS_MAX_DELAY_DAYS = 7
    APP_ACCOUNT_HEARTBEAT_DAYS = 7
    APP_PREPARED_TRANSFER_REMAINDER_DAYS = 7
    APP_PREPARED_TRANSFER_MAX_DELAY_DAYS = 30


def create_app(config_dict={}):
    from flask import Flask
    from .extensions import db, migrate, broker
    from .cli import swpt_accounts
    from . import models  # noqa

    app = Flask(__name__)
    app.config.from_object(Configuration)
    app.config.from_mapping(config_dict)
    db.init_app(app)
    migrate.init_app(app, db)
    broker.init_app(app)
    app.cli.add_command(swpt_accounts)
    return app
