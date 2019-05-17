import datetime
import math
from typing import Tuple
from decimal import Decimal
from .extensions import db
from .models import Account, PreparedTransfer, RejectedTransferSignal, PreparedTransferSignal, \
    MAX_INT64, AccountChangeSignal, CommittedTransferSignal, DebtorPolicy, AccountPolicy, \
    increment_seqnum

SECONDS_IN_YEAR = 365.25 * 24 * 60 * 60

# Available balance check modes:
AVL_BALANCE_IGNORE = 0
AVL_BALANCE_ONLY = 1
AVL_BALANCE_WITH_INTEREST = 2


@db.atomic
def prepare_transfer(
        coordinator_type,
        coordinator_id,
        coordinator_request_id,
        account,
        min_amount,
        max_amount,
        recipient_creditor_id,
        avl_balance_check_mode,
        lock_amount,
):
    assert 0 < min_amount <= max_amount
    account, avl_balance = _get_account_avl_balance(account, avl_balance_check_mode)
    if avl_balance >= min_amount:
        account = _get_or_create_account_instance(account)
        amount = min(avl_balance, max_amount)
        locked_amount = amount if lock_amount else 0
        pt = _create_prepared_transfer(account, coordinator_type, recipient_creditor_id, amount, locked_amount)
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
        debtor_id, creditor_id = Account.get_pk_values(account)
        db.session.add(RejectedTransferSignal(
            debtor_id=debtor_id,
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
def execute_prepared_transfer(pt, committed_amount, transfer_info):
    assert committed_amount >= 0
    pt = PreparedTransfer.get_instance(pt, db.joinedload('sender_account', innerjoin=True))
    if pt:
        if committed_amount == 0:
            _delete_prepared_transfer(pt)
        else:
            committed_at_ts = datetime.datetime.now(tz=datetime.timezone.utc)
            _commit_prepared_transfer(pt, committed_amount, committed_at_ts, transfer_info)


def _insert_account_change_signal(account, last_change_ts=None):
    account.last_change_seqnum = increment_seqnum(account.last_change_seqnum)
    account.last_change_ts = last_change_ts or datetime.datetime.now(tz=datetime.timezone.utc)
    db.session.add(AccountChangeSignal(
        debtor_id=account.debtor_id,
        creditor_id=account.creditor_id,
        change_seqnum=account.last_change_seqnum,
        change_ts=account.last_change_ts,
        balance=account.ballance,
        interest=account.interest,
        interest_rate=account.interest_rate,
        status=account.status,
    ))


def _calc_account_interest_rate(account) -> float:
    debtor_id, creditor_id = Account.get_pk_values(account)
    debtor_policy = DebtorPolicy.lock_instance(debtor_id, read=True)
    account_policy = AccountPolicy.lock_instance((debtor_id, creditor_id), read=True)
    standard_interest_rate = debtor_policy.interest_rate if debtor_policy else 0.0
    concession_interest_rate = account_policy.interest_rate if account_policy else -100.0
    return max(standard_interest_rate, concession_interest_rate)


def _create_account(debtor_id, creditor_id) -> Account:
    account = Account(
        debtor_id=debtor_id,
        creditor_id=creditor_id,
        interest_rate=_calc_account_interest_rate((debtor_id, creditor_id)),
    )
    with db.retry_on_integrity_error():
        db.session.add(account)
    _insert_account_change_signal(account)
    return account


def _resurrect_account_if_deleted(account):
    if account.status & Account.STATUS_DELETED_FLAG:
        assert account.balance == 0
        assert account.locked_amount == 0
        assert account.interest == 0.0
        account.status = 0
        account.interest_rate = _calc_account_interest_rate(account)
        _insert_account_change_signal(account)


def _get_or_create_account_instance(account) -> Account:
    instance = Account.get_instance(account)
    if instance is None:
        debtor_id, creditor_id = Account.get_pk_values(account)
        instance = _create_account(debtor_id, creditor_id)
    _resurrect_account_if_deleted(instance)
    return instance


def _calc_account_current_principal(account, current_ts) -> Decimal:
    principal = account.balance + Decimal.from_float(account.interest)
    if principal > 0:
        k = math.log(1.0 + account.interest_rate / 100.0) / SECONDS_IN_YEAR
        passed_seconds = max(0.0, (current_ts - account.last_change_ts).total_seconds())
        principal *= Decimal.from_float(math.exp(k * passed_seconds))
    return principal


def _get_account_avl_balance(account, avl_balance_check_mode) -> Tuple[Account, int]:
    avl_balance = 0
    if avl_balance_check_mode == AVL_BALANCE_IGNORE:
        avl_balance = MAX_INT64
    elif avl_balance_check_mode == AVL_BALANCE_ONLY:
        instance = Account.get_instance(account)
        if instance:
            account = instance
            avl_balance = account.balance - account.locked_amount
    elif avl_balance_check_mode == AVL_BALANCE_WITH_INTEREST:
        instance = Account.get_instance(account)
        if instance:
            account = instance
            current_ts = datetime.datetime.now(tz=datetime.timezone.utc)
            current_principal = _calc_account_current_principal(account, current_ts)
            avl_balance = math.floor(current_principal) - account.locked_amount
    else:
        raise ValueError(f'invalid available balance check mode: {avl_balance_check_mode}')
    return account, avl_balance


def _create_prepared_transfer(account, coordinator_type, recipient_creditor_id, amount,
                              sender_locked_amount) -> PreparedTransfer:
    account.locked_amount += sender_locked_amount
    pt = PreparedTransfer(
        sender_account=account,
        coordinator_type=coordinator_type,
        recipient_creditor_id=recipient_creditor_id,
        amount=amount,
        sender_locked_amount=sender_locked_amount,
    )
    db.session.add(pt)
    db.session.flush()
    return pt


def _insert_committed_transfer_signal(pt, committed_amount, committed_at_ts, transfer_info):
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


def _change_account_balance(account, balance_delta, current_ts):
    current_principal = _calc_account_current_principal(account, current_ts)
    account.interest = float(current_principal - account.balance)
    account.balance += balance_delta
    if balance_delta != 0:
        _insert_account_change_signal(account, current_ts)


def _delete_prepared_transfer(pt):
    sender_account = pt.sender_account
    sender_account.locked_amount -= pt.sender_locked_amount
    db.session.delete(pt)


def _commit_prepared_transfer(pt, committed_amount, committed_at_ts, transfer_info):
    assert committed_amount <= pt.amount
    sender_account = pt.sender_account
    recipient_account = _get_or_create_account_instance((pt.debtor_id, pt.recipient_creditor_id))
    _change_account_balance(sender_account, -committed_amount, committed_at_ts)
    _change_account_balance(recipient_account, committed_amount, committed_at_ts)
    _insert_committed_transfer_signal(pt, committed_amount, committed_at_ts, transfer_info)
    _delete_prepared_transfer(pt)
