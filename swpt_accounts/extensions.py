import os
import warnings
import requests
from sqlalchemy.exc import SAWarning
from werkzeug.local import Local, LocalProxy
from flask import current_app
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from flask_signalbus import SignalBusMixin, AtomicProceduresMixin
from flask_melodramatiq import RabbitmqBroker
from dramatiq import Middleware

MAIN_EXCHANGE_NAME = 'dramatiq'
APP_QUEUE_NAME = os.environ.get('APP_QUEUE_NAME', 'swpt_accounts')

_local = Local()


warnings.filterwarnings(
    'ignore',
    r"this is a regular expression for the text of the warning",
    SAWarning,
)


class CustomAlchemy(AtomicProceduresMixin, SignalBusMixin, SQLAlchemy):
    pass


class EventSubscriptionMiddleware(Middleware):
    @property
    def actor_options(self):
        return {'event_subscription'}


def get_requests_session():
    if not hasattr(_local, 'requests_session'):
        session = requests.Session()
        session.timeout = current_app.config['APP_FETCH_API_TIMEOUT_SECONDS']
        _local.requests_session = session

    return _local.requests_session


db = CustomAlchemy()
migrate = Migrate()
requests_session = LocalProxy(get_requests_session)
broker = RabbitmqBroker(confirm_delivery=True)
broker.add_middleware(EventSubscriptionMiddleware())
