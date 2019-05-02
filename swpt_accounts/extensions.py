import warnings
from sqlalchemy.exc import SAWarning
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from flask_signalbus import SignalBusMixin, AtomicProceduresMixin
from flask_melodramatiq import RabbitmqBroker

warnings.filterwarnings(
    'ignore',
    r"this is a regular expression for the text of the warning",
    SAWarning,
)


class CustomAlchemy(AtomicProceduresMixin, SignalBusMixin, SQLAlchemy):
    pass


db = CustomAlchemy()
migrate = Migrate()
broker = RabbitmqBroker(confirm_delivery=True)
