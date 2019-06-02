import math
from datetime import datetime, timezone, timedelta
from typing import TypeVar, Tuple, Union, Optional, Callable
from decimal import Decimal
from .extensions import db
from .models import Account, PreparedTransfer, RejectedTransferSignal, PreparedTransferSignal, \
    MAX_INT64, AccountChangeSignal, CommittedTransferSignal, increment_seqnum

T = TypeVar('T')
atomic: Callable[[T], T] = db.atomic
AccountId = Union[Account, Tuple[int, int]]
PreparedTransferId = Union[PreparedTransfer, Tuple[int, int, int]]

# Available balance check modes:
AVL_BALANCE_IGNORE = 0
AVL_BALANCE_ONLY = 1
AVL_BALANCE_WITH_INTEREST = 2

TD_ZERO = timedelta()
TD_SECOND = timedelta(seconds=1)
TD_MINUS_SECOND = -TD_SECOND
SECONDS_IN_YEAR = 365.25 * 24 * 60 * 60


@atomic
def prepare_transfer(*,
                     coordinator_type: str,
                     coordinator_id: int,
                     coordinator_request_id: int,
                     account: AccountId,
                     min_amount: int,
                     max_amount: int,
                     recipient_creditor_id: int,
                     avl_balance_check_mode: int,
                     lock_amount: bool) -> None:
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


@atomic
def execute_prepared_transfer(pt: PreparedTransferId, committed_amount: int, transfer_info: dict) -> None:
    assert committed_amount >= 0
    instance = PreparedTransfer.get_instance(pt, db.joinedload('sender_account', innerjoin=True))
    if instance:
        if committed_amount == 0:
            _delete_prepared_transfer(instance)
        else:
            committed_at_ts = datetime.now(tz=timezone.utc)
            _commit_prepared_transfer(instance, committed_amount, committed_at_ts, transfer_info)


@atomic
def update_account_interest_rate(account: AccountId, interest_rate: float,
                                 change_seqnum: int, change_ts: datetime) -> None:
    assert change_seqnum is not None
    assert change_ts is not None
    instance = Account.get_instance(account)
    if instance and not instance.status & Account.STATUS_DELETED_FLAG:
        this_update = (change_seqnum, change_ts)
        prev_update = (instance.interest_rate_last_change_seqnum, instance.interest_rate_last_change_ts)
        if _is_later_event(this_update, prev_update):
            current_ts = datetime.now(tz=timezone.utc)
            _update_accumulated_account_interest(instance, current_ts)
            instance.interest_rate = interest_rate
            instance.interest_rate_last_change_seqnum = change_seqnum
            instance.interest_rate_last_change_ts = change_ts
            instance.status = instance.status | Account.STATUS_ESTABLISHED_INTEREST_RATE_FLAG
            _insert_account_change_signal(instance, current_ts)


def _is_later_event(event: Tuple[int, datetime],
                    other_event: Tuple[Optional[int], Optional[datetime]]) -> bool:
    seqnum, ts = event
    other_seqnum, other_ts = other_event
    advance = (ts - other_ts) if other_ts else TD_ZERO
    return advance >= TD_MINUS_SECOND and (
        advance > TD_SECOND
        or other_seqnum is None
        or 0 < (seqnum - other_seqnum) % 0x100000000 < 0x80000000
    )


def _insert_account_change_signal(account: Account, last_change_ts: Optional[datetime] = None) -> None:
    last_change_ts = last_change_ts or datetime.now(tz=timezone.utc)
    account.last_change_seqnum = increment_seqnum(account.last_change_seqnum)
    account.last_change_ts = max(account.last_change_ts, last_change_ts)
    db.session.add(AccountChangeSignal(
        debtor_id=account.debtor_id,
        creditor_id=account.creditor_id,
        change_seqnum=account.last_change_seqnum,
        change_ts=account.last_change_ts,
        balance=account.balance,
        interest=account.interest,
        interest_rate=account.interest_rate,
        status=account.status,
    ))


def _create_account(debtor_id: int, creditor_id: int) -> Account:
    account = Account(
        debtor_id=debtor_id,
        creditor_id=creditor_id,
    )
    with db.retry_on_integrity_error():
        db.session.add(account)
    _insert_account_change_signal(account)
    return account


def _resurrect_account_if_deleted(account: Account) -> None:
    if account.status & Account.STATUS_DELETED_FLAG:
        assert account.balance == 0
        assert account.locked_amount == 0
        assert account.interest == 0.0
        account.status = 0
        account.interest_rate = 0.0
        account.interest_rate_last_change_seqnum = None
        account.interest_rate_last_change_ts = None
        _insert_account_change_signal(account)


def _get_or_create_account_instance(account: AccountId) -> Account:
    instance = Account.get_instance(account)
    if instance is None:
        debtor_id, creditor_id = Account.get_pk_values(account)
        instance = _create_account(debtor_id, creditor_id)
    _resurrect_account_if_deleted(instance)
    return instance


def _calc_account_current_principal(account: Account, current_ts: datetime) -> Decimal:
    principal = account.balance + Decimal.from_float(account.interest)
    if principal > 0:
        k = math.log(1.0 + account.interest_rate / 100.0) / SECONDS_IN_YEAR
        passed_seconds = max(0.0, (current_ts - account.last_change_ts).total_seconds())
        principal *= Decimal.from_float(math.exp(k * passed_seconds))
    return principal


def _get_account_avl_balance(account: AccountId, avl_balance_check_mode: int) -> Tuple[AccountId, int]:
    avl_balance = 0
    if avl_balance_check_mode == AVL_BALANCE_IGNORE:
        avl_balance = MAX_INT64
    elif avl_balance_check_mode == AVL_BALANCE_ONLY:
        instance = Account.get_instance(account)
        if instance:
            avl_balance = instance.balance - instance.locked_amount
            account = instance
    elif avl_balance_check_mode == AVL_BALANCE_WITH_INTEREST:
        instance = Account.get_instance(account)
        if instance:
            current_ts = datetime.now(tz=timezone.utc)
            current_principal = _calc_account_current_principal(instance, current_ts)
            avl_balance = math.floor(current_principal) - instance.locked_amount
            account = instance
    else:
        raise ValueError(f'invalid available balance check mode: {avl_balance_check_mode}')
    return account, avl_balance


def _create_prepared_transfer(account: Account,
                              coordinator_type: str,
                              recipient_creditor_id: int,
                              amount: int,
                              sender_locked_amount: int) -> PreparedTransfer:
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


def _insert_committed_transfer_signal(pt: PreparedTransfer,
                                      committed_amount: int,
                                      committed_at_ts: datetime,
                                      transfer_info: dict) -> None:
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


def _update_accumulated_account_interest(account: Account, current_ts: datetime) -> None:
    current_principal = _calc_account_current_principal(account, current_ts)
    account.interest = float(current_principal - account.balance)


def _change_account_balance(account: Account, balance_delta: int, current_ts: datetime) -> None:
    _update_accumulated_account_interest(account, current_ts)
    account.balance += balance_delta
    _insert_account_change_signal(account, current_ts)


def _delete_prepared_transfer(pt: PreparedTransfer) -> None:
    pt.sender_account.locked_amount -= pt.sender_locked_amount
    db.session.delete(pt)


def _commit_prepared_transfer(pt: PreparedTransfer,
                              committed_amount: int,
                              committed_at_ts: datetime,
                              transfer_info: dict) -> None:
    assert committed_amount <= pt.amount
    recipient_account = _get_or_create_account_instance((pt.debtor_id, pt.recipient_creditor_id))
    _change_account_balance(pt.sender_account, -committed_amount, committed_at_ts)
    _change_account_balance(recipient_account, committed_amount, committed_at_ts)
    _insert_committed_transfer_signal(pt, committed_amount, committed_at_ts, transfer_info)
    _delete_prepared_transfer(pt)
