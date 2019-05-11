import datetime
import math
from .extensions import db
from .models import Account, PreparedTransfer, RejectedTransferSignal, PreparedTransferSignal, MAX_INT64, \
    AccountChangeSignal, CommittedTransferSignal

SECONDS_IN_YEAR = 365.25 * 24 * 60 * 60

# Available balance check modes:
AVL_BALANCE_IGNORE = 0
AVL_BALANCE_ONLY = 1
AVL_BALANCE_WITH_INTEREST = 2


class InsufficientFunds(Exception):
    """The required amount is not available at the moment."""


class InvalidPreparedTransfer(Exception):
    """The specified prepared transfer does not exist."""


@db.atomic
def prepare_transfer(
        coordinator_type,
        coordinator_id,
        coordinator_request_id,
        account,
        min_amount,
        max_amount,
        recipient_creditor_id,
        avl_balance_check_mode=AVL_BALANCE_WITH_INTEREST,
        lock_amount=True,
):
    account = _get_account(account)
    current_ts = datetime.datetime.now(tz=datetime.timezone.utc)
    if avl_balance_check_mode == AVL_BALANCE_IGNORE:
        avl_balance = MAX_INT64
    elif avl_balance_check_mode == AVL_BALANCE_ONLY:
        avl_balance = _get_account_current_avl_balance(account, current_ts, ignore_interest=True)
    elif avl_balance_check_mode == AVL_BALANCE_WITH_INTEREST:
        avl_balance = _get_account_current_avl_balance(account, current_ts, ignore_interest=False)
    else:
        raise ValueError(f'invalid available balance check mode: {avl_balance_check_mode}')
    if avl_balance >= min_amount:
        amount = min(avl_balance, max_amount)
        locked_amount = amount if lock_amount else 0
        pt = _prepare_account_transfer(account, coordinator_type, recipient_creditor_id, amount, locked_amount)
        db.session.add(PreparedTransferSignal(
            debtor_id=pt.debtor_id,
            sender_creditor_id=pt.sender_creditor_id,
            transfer_id=pt.transfer_id,
            coordinator_type=pt.coordinator_type,
            recipient_creditor_id=pt.recipient_creditor_id,
            amount=pt.amount,
            sender_locked_amount=pt.sender_locked_amount,
            prepared_at_ts=pt.prepared_at_ts,
            coordinator_id=coordinator_id,
            coordinator_request_id=coordinator_request_id,
        ))
    else:
        db.session.add(RejectedTransferSignal(
            debtor_id=account.debtor_id,
            coordinator_type=coordinator_type,
            coordinator_id=coordinator_id,
            coordinator_request_id=coordinator_request_id,
            details={
                'error_code': 'ACC001',
                'avl_balance': avl_balance,
                'message': 'Insufficient available balance',
            }
        ))


@db.atomic
def execute_prepared_transfer(prepared_transfer, committed_amount, transfer_info):
    assert committed_amount >= 0
    if committed_amount == 0:
        _delete_prepared_transfer(prepared_transfer)
    else:
        _commit_prepared_transfer(prepared_transfer, committed_amount, transfer_info)


def _get_account(account):
    instance = Account.get_instance(account)
    if instance is None:
        debtor_id, creditor_id = Account.get_pk_values(account)
        instance = Account(debtor_id=debtor_id, creditor_id=creditor_id)
        with db.retry_on_integrity_error():
            db.session.add(instance)
    return instance


def _recalc_account_current_principal(account, current_ts):
    account = Account.get_instance(account)
    passed_seconds = max(0.0, (current_ts - account.last_change_ts).total_seconds())
    interest_rate = max(account.concession_interest_rate, account.debtor_policy.interest_rate)
    try:
        k = math.log(1 + interest_rate / 100) / SECONDS_IN_YEAR
    except ValueError:
        k = -math.inf  # the interest rate is -100
    old_principal = max(0, account.balance + account.interest)
    return math.floor(old_principal * math.exp(k * passed_seconds))


def _get_account_current_avl_balance(account, current_ts, ignore_interest=False):
    account = Account.get_instance(account)
    if ignore_interest:
        return account.avl_balance
    current_principal = _recalc_account_current_principal(account, current_ts)
    locked_amount = account.balance - account.avl_balance
    assert locked_amount >= 0
    return current_principal - locked_amount


def _change_account_balance(account, delta, current_ts):
    account = Account.get_instance(account)
    current_principal = _recalc_account_current_principal(account, current_ts)
    account.interest = current_principal - account.balance
    account.balance += delta
    account.avl_balance += delta
    account.last_change_seqnum += 1
    account.last_change_ts = current_ts
    account.last_activity_ts = current_ts
    db.session.add(AccountChangeSignal(
        debtor_id=account.debtor_id,
        creditor_id=account.creditor_id,
        change_seqnum=account.last_change_seqnum,
        change_ts=account.last_change_ts,
        balance=account.ballance,
        interest=account.interest,
        concession_interest_rate=account.concession_interest_rate,
        standard_interest_rate=account.debtor_policy.interest_rate
    ))


def _prepare_account_transfer(account, coordinator_type, recipient_creditor_id, amount, sender_locked_amount):
    assert amount >= 0
    account = Account.get_instance(account)
    account.avl_balance -= sender_locked_amount
    prepared_transfer = PreparedTransfer(
        sender_account=account,
        coordinator_type=coordinator_type,
        recipient_creditor_id=recipient_creditor_id,
        amount=amount,
        sender_locked_amount=sender_locked_amount,
    )
    db.session.add(prepared_transfer)
    return prepared_transfer


def _delete_prepared_transfer(pt):
    pt = PreparedTransfer.get_instance(pt, db.joinedload('sender_account', innerjoin=True))
    if pt:
        sender_account = pt.sender_account
        sender_account.avl_balance += pt.sender_locked_amount
        db.session.delete(pt)


def _insert_committed_transfer_signal(pt, committed_amount, committed_at_ts, transfer_info):
    pt = PreparedTransfer.get_instance(pt, db.joinedload('sender_account', innerjoin=True))
    db.session.add(CommittedTransferSignal(
        debtor_id=pt.debtor_id,
        sender_creditor_id=pt.sender_creditor_id,
        transfer_id=pt.transfer_id,
        coordinator_type=pt.coordinator_type,
        recipient_creditor_id=pt.recipient_creditor_id,
        prepared_at_ts=pt.prepared_at_ts,
        committed_at_ts=committed_at_ts,
        committed_amount=committed_amount,
        transfer_info=transfer_info,
    ))


def _commit_prepared_transfer(pt, committed_amount, transfer_info):
    pt = PreparedTransfer.get_instance(pt, db.joinedload('sender_account', innerjoin=True))
    if pt:
        committed_at_ts = datetime.datetime.now(tz=datetime.timezone.utc)
        sender_account = pt.sender_account
        recipient_account = _get_account((pt.debtor_id, pt.recipient_creditor_id))
        _change_account_balance(sender_account, -committed_amount, committed_at_ts)
        _change_account_balance(recipient_account, committed_amount, committed_at_ts)
        _insert_committed_transfer_signal(pt, committed_amount, committed_at_ts, transfer_info)
        _delete_prepared_transfer(pt)
