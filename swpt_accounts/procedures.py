import datetime
import math
from .extensions import db
from .models import Account, PreparedTransfer

SECONDS_IN_YEAR = 365.25 * 24 * 60 * 60


class InsufficientFunds(Exception):
    """The required amount is not available at the moment."""


@db.atomic
def execute_prepared_transfer():
    pass


def _get_account(account):
    instance = Account.get_instance(account)
    if instance is None:
        debtor_id, creditor_id = Account.get_pk_values(account)
        instance = Account(debtor_id=debtor_id, creditor_id=creditor_id)
        with db.retry_on_integrity_error():
            db.session.add(instance)
    return instance


def _get_account_avl_balance(account, ignore_interest=False):
    account = _get_account(account)
    if ignore_interest:
        return account.avl_balance
    now = datetime.datetime.now(tz=datetime.timezone.utc)
    passed_seconds = max(0.0, (now - account.last_change_ts).total_seconds())
    interest_rate = max(account.concession_interest_rate, account.debtor_policy.interest_rate)
    try:
        k = math.log(1 + interest_rate / 100) / SECONDS_IN_YEAR
    except ValueError:
        k = -math.inf  # the interest rate is -100
    old_principal = max(0, account.balance + account.interest)
    new_principal = math.floor(old_principal * math.exp(k * passed_seconds))
    locked_amount = account.balance - account.avl_balance
    assert locked_amount >= 0
    return new_principal - locked_amount


def _prepare_account_transfer(account, coordinator_type, recipient_creditor_id, amount, locked_amount=None):
    assert amount >= 0
    if locked_amount is None:
        locked_amount = amount
    account = _get_account(account)
    account.avl_balance -= locked_amount
    prepared_transfer = PreparedTransfer(
        sender_account=account,
        coordinator_type=coordinator_type,
        recipient_creditor_id=recipient_creditor_id,
        amount=amount,
        sender_locked_amount=locked_amount,
    )
    db.session.add(prepared_transfer)
    return prepared_transfer
