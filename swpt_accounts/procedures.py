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

# Available balance check modes:
AB_IGNORE = 0
AB_PRINCIPAL_ONLY = 1
AB_PRINCIPAL_WITH_INTEREST = 2

TINY_PRINCIPAL_AMOUNT = 3
MAX_PREPARED_TRANSFERS_COUNT = 1000

TD_ZERO = timedelta()
TD_SECOND = timedelta(seconds=1)
TD_MINUS_SECOND = -TD_SECOND
SECONDS_IN_YEAR = 365.25 * 24 * 60 * 60


@atomic
def prepare_transfer(coordinator_type: str,
                     coordinator_id: int,
                     coordinator_request_id: int,
                     min_amount: int,
                     max_amount: int,
                     debtor_id: int,
                     sender_creditor_id: int,
                     recipient_creditor_id: int,
                     avl_balance_check_mode: int,
                     lock_amount: bool) -> None:
    assert 0 < min_amount <= max_amount
    account_or_pk, avl_balance = _calc_account_avl_balance((debtor_id, sender_creditor_id), avl_balance_check_mode)

    def reject_transfer(**kw):
        debtor_id, creditor_id = Account.get_pk_values(account_or_pk)
        db.session.add(RejectedTransferSignal(
            debtor_id=debtor_id,
            coordinator_type=coordinator_type,
            coordinator_id=coordinator_id,
            coordinator_request_id=coordinator_request_id,
            details=kw,
        ))

    if avl_balance >= min_amount:
        account = _get_or_create_account(account_or_pk)
        if account.prepared_transfers_count < MAX_PREPARED_TRANSFERS_COUNT:
            amount = min(avl_balance, max_amount)
            locked_amount = amount if lock_amount else 0
            pt = _create_prepared_transfer(coordinator_type, account, recipient_creditor_id, amount, locked_amount)
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
            reject_transfer(
                error_code='ACC002',
                message='Too many prepared transfers',
                prepared_transfers_count=account.prepared_transfers_count,
            )
    else:
        reject_transfer(
            error_code='ACC001',
            message='Insufficient available balance',
            avl_balance=avl_balance,
        )


@atomic
def finalize_prepared_transfer(debtor_id: int,
                               sender_creditor_id: int,
                               transfer_id: int,
                               committed_amount: int,
                               transfer_info: dict) -> None:
    assert committed_amount >= 0
    pt_pk = (debtor_id, sender_creditor_id, transfer_id)
    pt = PreparedTransfer.get_instance(pt_pk, db.joinedload('sender_account', innerjoin=True))
    if pt:
        if committed_amount == 0:
            _delete_prepared_transfer(pt)
        else:
            _commit_prepared_transfer(pt, committed_amount, datetime.now(tz=timezone.utc), transfer_info)


@atomic
def set_interest_rate(debtor_id: int,
                      creditor_id: int,
                      interest_rate: float,
                      change_seqnum: int,
                      change_ts: datetime) -> None:
    assert change_seqnum is not None
    assert change_ts is not None
    account = _get_account((debtor_id, creditor_id))
    if account:
        this_event = (change_seqnum, change_ts)
        prev_event = (account.interest_rate_last_change_seqnum, account.interest_rate_last_change_ts)
        if _is_later_event(this_event, prev_event):
            _change_account_interest_rate(account, interest_rate, change_seqnum, change_ts)


@atomic
def capitalize_interest(debtor_id: int,
                        creditor_id: int,
                        issuer_creditor_id: int,
                        accumulated_interest_threshold: int) -> None:
    account = _get_account((debtor_id, creditor_id))
    if account:
        positive_threshold = max(1, abs(accumulated_interest_threshold))
        current_ts = datetime.now(tz=timezone.utc)
        amount = math.floor(_calc_accumulated_account_interest(account, current_ts))

        # When the new account principal is positive and very close to
        # zero, we make it a zero. This behavior could be helpful when
        # the owner zeroes out the account before deleting it.
        if 0 <= account.principal + amount <= TINY_PRINCIPAL_AMOUNT:
            amount = -account.principal

        if amount >= positive_threshold:
            # The issuer pays interest to the owner of the account.
            issuer_account = _get_or_create_account((debtor_id, issuer_creditor_id))
            pt = _create_prepared_transfer('interest', issuer_account, creditor_id, amount, amount)
            _commit_prepared_transfer(pt, amount, current_ts)
        elif -amount >= positive_threshold:
            # The owner of the account pays demurrage to the issuer.
            pt = _create_prepared_transfer('demurrage', account, issuer_creditor_id, -amount, -amount)
            _commit_prepared_transfer(pt, -amount, current_ts)


@atomic
def delete_account_if_zeroed(debtor_id: int, creditor_id: int) -> None:
    account = _get_account((debtor_id, creditor_id))
    if (account
            and account.principal == 0
            and account.prepared_transfers_count == 0
            and 0 <= _calc_account_current_balance(account) <= TINY_PRINCIPAL_AMOUNT):
        assert account.locked_amount == 0
        account.interest = 0.0
        account.status = account.status | Account.STATUS_DELETED_FLAG
        _insert_account_change_signal(account)


@atomic
def purge_deleted_account(debtor_id: int, creditor_id: int, if_deleted_before: datetime) -> None:
    Account.query.filter_by(debtor_id=debtor_id, creditor_id=creditor_id)\
                 .filter(Account.status.op('&')(Account.STATUS_DELETED_FLAG) == 1)\
                 .filter(Account.last_change_ts < if_deleted_before)\
                 .delete(synchronize_session=False)


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


def _create_account(debtor_id: int, creditor_id: int) -> Account:
    account = Account(debtor_id=debtor_id, creditor_id=creditor_id)
    with db.retry_on_integrity_error():
        db.session.add(account)
    _insert_account_change_signal(account)
    return account


def _get_account(account_or_pk: AccountId) -> Optional[Account]:
    account = Account.get_instance(account_or_pk)
    if account and not account.status & Account.STATUS_DELETED_FLAG:
        return account
    return None


def _get_or_create_account(account_or_pk: AccountId) -> Account:
    account = Account.get_instance(account_or_pk)
    if account is None:
        debtor_id, creditor_id = Account.get_pk_values(account_or_pk)
        account = _create_account(debtor_id, creditor_id)
    _resurrect_account_if_deleted(account)
    return account


def _resurrect_account_if_deleted(account: Account) -> None:
    if account.status & Account.STATUS_DELETED_FLAG:
        assert account.principal == 0
        assert account.locked_amount == 0
        assert account.prepared_transfers_count == 0
        assert account.interest == 0.0
        account.status = 0
        account.interest_rate = 0.0
        account.interest_rate_last_change_seqnum = None
        account.interest_rate_last_change_ts = None
        _insert_account_change_signal(account)


def _calc_account_current_balance(account: Account, current_ts: datetime = None) -> Decimal:
    current_ts = current_ts or datetime.now(tz=timezone.utc)
    current_balance = account.principal + Decimal.from_float(account.interest)
    if current_balance > 0:
        k = math.log(1.0 + account.interest_rate / 100.0) / SECONDS_IN_YEAR
        passed_seconds = max(0.0, (current_ts - account.last_change_ts).total_seconds())
        current_balance *= Decimal.from_float(math.exp(k * passed_seconds))
    return current_balance


def _calc_account_avl_balance(account_or_pk: AccountId, avl_balance_check_mode: int) -> Tuple[AccountId, int]:
    avl_balance = 0
    if avl_balance_check_mode == AB_IGNORE:
        avl_balance = MAX_INT64
    elif avl_balance_check_mode == AB_PRINCIPAL_ONLY:
        account = _get_account(account_or_pk)
        if account:
            avl_balance = account.principal - account.locked_amount
            account_or_pk = account
    elif avl_balance_check_mode == AB_PRINCIPAL_WITH_INTEREST:
        account = _get_account(account_or_pk)
        if account:
            avl_balance = math.floor(_calc_account_current_balance(account)) - account.locked_amount
            account_or_pk = account
    else:
        raise ValueError(f'invalid available balance check mode: {avl_balance_check_mode}')
    return account_or_pk, avl_balance


def _create_prepared_transfer(coordinator_type: str,
                              sender_account: Account,
                              recipient_creditor_id: int,
                              amount: int,
                              sender_locked_amount: int) -> PreparedTransfer:
    pt = PreparedTransfer(
        sender_account=sender_account,
        coordinator_type=coordinator_type,
        recipient_creditor_id=recipient_creditor_id,
        amount=amount,
        sender_locked_amount=sender_locked_amount,
    )
    db.session.add(pt)
    db.session.flush()
    sender_account.locked_amount += sender_locked_amount
    sender_account.prepared_transfers_count += 1
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


def _insert_account_change_signal(account: Account, last_change_ts: Optional[datetime] = None) -> None:
    last_change_ts = last_change_ts or datetime.now(tz=timezone.utc)
    account.last_change_seqnum = increment_seqnum(account.last_change_seqnum)
    account.last_change_ts = max(account.last_change_ts, last_change_ts)
    db.session.add(AccountChangeSignal(
        debtor_id=account.debtor_id,
        creditor_id=account.creditor_id,
        change_seqnum=account.last_change_seqnum,
        change_ts=account.last_change_ts,
        principal=account.principal,
        interest=account.interest,
        interest_rate=account.interest_rate,
        status=account.status,
    ))


def _calc_accumulated_account_interest(account: Account, current_ts: datetime) -> Decimal:
    return _calc_account_current_balance(account, current_ts) - account.principal


def _change_account_principal(account: Account,
                              principal_delta: int,
                              current_ts: Optional[datetime] = None,
                              is_interest_payment: bool = False) -> None:
    current_ts = current_ts or datetime.now(tz=timezone.utc)
    interest = _calc_accumulated_account_interest(account, current_ts)
    account.interest = float(interest - principal_delta if is_interest_payment else interest)
    account.principal += principal_delta
    _insert_account_change_signal(account, current_ts)


def _change_account_interest_rate(account: Account,
                                  interest_rate: float,
                                  change_seqnum: int,
                                  change_ts: datetime,
                                  current_ts: Optional[datetime] = None) -> None:
    current_ts = current_ts or datetime.now(tz=timezone.utc)
    account.interest = float(_calc_accumulated_account_interest(account, current_ts))
    account.interest_rate = interest_rate
    account.interest_rate_last_change_seqnum = change_seqnum
    account.interest_rate_last_change_ts = change_ts
    account.status = account.status | Account.STATUS_ESTABLISHED_INTEREST_RATE_FLAG
    _insert_account_change_signal(account, current_ts)


def _delete_prepared_transfer(pt: PreparedTransfer) -> None:
    sender_account = pt.sender_account
    sender_account.locked_amount -= pt.sender_locked_amount
    sender_account.prepared_transfers_count -= 1
    db.session.delete(pt)


def _commit_prepared_transfer(pt: PreparedTransfer,
                              committed_amount: int,
                              committed_at_ts: datetime,
                              transfer_info: dict = {}) -> None:
    assert committed_amount <= pt.amount
    recipient_account = _get_or_create_account((pt.debtor_id, pt.recipient_creditor_id))
    _change_account_principal(pt.sender_account, -committed_amount, committed_at_ts, pt.coordinator_type == 'demurrage')
    _change_account_principal(recipient_account, committed_amount, committed_at_ts, pt.coordinator_type == 'interest')
    _insert_committed_transfer_signal(pt, committed_amount, committed_at_ts, transfer_info)
    _delete_prepared_transfer(pt)
