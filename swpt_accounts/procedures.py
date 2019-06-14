import math
from datetime import datetime, timezone, timedelta
from typing import TypeVar, Tuple, Union, Optional, Callable
from decimal import Decimal
from .extensions import db
from .models import Account, PreparedTransfer, RejectedTransferSignal, PreparedTransferSignal, \
    AccountChangeSignal, CommittedTransferSignal, ScheduledAccountChange, \
    increment_seqnum, MIN_INT64, MAX_INT64

T = TypeVar('T')
atomic: Callable[[T], T] = db.atomic
AccountId = Union[Account, Tuple[int, int]]

TINY_POSITIVE_AMOUNT = 3  # should be at least `2`
MAX_PREPARED_TRANSFERS_COUNT = 1000

TD_ZERO = timedelta(seconds=0)
TD_SECOND = timedelta(seconds=1)
TD_MINUS_SECOND = -TD_SECOND
SECONDS_IN_YEAR = 365.25 * 24 * 60 * 60

# The account `(debtor_id, ROOT_CREDITOR_ID)` is special. This is the
# debtor's account. It issuers all the money. Also, all interest and
# demurrage payments come from/to this account.
ROOT_CREDITOR_ID = MIN_INT64


@atomic
def get_or_create_account(debtor_id: int, creditor_id: int) -> Account:
    assert MIN_INT64 <= debtor_id <= MAX_INT64
    assert MIN_INT64 <= creditor_id <= MAX_INT64
    return _get_or_create_account((debtor_id, creditor_id))


@atomic
def prepare_transfer(coordinator_type: str,
                     coordinator_id: int,
                     coordinator_request_id: int,
                     min_amount: int,
                     max_amount: int,
                     debtor_id: int,
                     sender_creditor_id: int,
                     recipient_creditor_id: int,
                     ignore_interest: bool) -> None:
    assert MIN_INT64 <= coordinator_id <= MAX_INT64
    assert MIN_INT64 <= coordinator_request_id <= MAX_INT64
    assert 0 < min_amount <= max_amount <= MAX_INT64
    assert MIN_INT64 <= debtor_id <= MAX_INT64
    assert MIN_INT64 <= sender_creditor_id <= MAX_INT64
    assert MIN_INT64 <= recipient_creditor_id <= MAX_INT64

    def reject_transfer(**kw) -> None:
        db.session.add(RejectedTransferSignal(
            debtor_id=debtor_id,
            coordinator_type=coordinator_type,
            coordinator_id=coordinator_id,
            coordinator_request_id=coordinator_request_id,
            details=kw,
        ))

    if sender_creditor_id == ROOT_CREDITOR_ID:
        reject_transfer(
            error_code='ACC001',
            message="The sender account can not be the debtor's account.",
        )
        return

    if sender_creditor_id == recipient_creditor_id:
        reject_transfer(
            error_code='ACC002',
            message='Recipient and sender accounts are the same.',
        )
        return

    avl_balance, account_or_pk = _calc_account_avl_balance((debtor_id, sender_creditor_id), ignore_interest)

    if avl_balance < min_amount:
        reject_transfer(
            error_code='ACC003',
            message='The available balance is insufficient.',
            avl_balance=avl_balance,
        )
        return

    if not (recipient_creditor_id == ROOT_CREDITOR_ID or _get_account((debtor_id, recipient_creditor_id))):
        reject_transfer(
            error_code='ACC004',
            message='The recipient account does not exist.',
        )
        return

    sender_account = _get_or_create_account(account_or_pk)
    amount = min(avl_balance, max_amount)
    new_locked_amount = sender_account.locked_amount + amount

    if new_locked_amount > MAX_INT64:
        reject_transfer(
            error_code='ACC005',
            message='The locked amount is too big.',
            locked_amount=new_locked_amount,
        )
        return

    if sender_account.prepared_transfers_count >= MAX_PREPARED_TRANSFERS_COUNT:
        reject_transfer(
            error_code='ACC006',
            message='There are too many prepared transfers.',
            prepared_transfers_count=sender_account.prepared_transfers_count,
        )
        return

    sender_account.locked_amount = new_locked_amount
    sender_account.prepared_transfers_count += 1
    pt = PreparedTransfer(
        sender_account=sender_account,
        coordinator_type=coordinator_type,
        recipient_creditor_id=recipient_creditor_id,
        amount=amount,
        sender_locked_amount=amount,
    )
    db.session.add(pt)
    db.session.flush()
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


@atomic
def finalize_prepared_transfer(debtor_id: int,
                               sender_creditor_id: int,
                               transfer_id: int,
                               committed_amount: int,
                               transfer_info: dict = {}) -> None:
    assert committed_amount >= 0
    pt_pk = (debtor_id, sender_creditor_id, transfer_id)
    pt = PreparedTransfer.get_instance(pt_pk, db.joinedload('sender_account', innerjoin=True))
    if pt:
        if committed_amount == 0:
            _delete_prepared_transfer(pt)
        else:
            _commit_prepared_transfer(pt, committed_amount, transfer_info)


@atomic
def set_interest_rate(debtor_id: int,
                      creditor_id: int,
                      interest_rate: float,
                      change_seqnum: int,
                      change_ts: datetime) -> None:
    # Too big interest rates can cause account balance overflows. To
    # prevent this, the interest rates should be kept within
    # reasonable limits, and the accumulated interest should be
    # capitalized every once in a while (like once a month).
    assert -100.0 < interest_rate <= 100.0

    account = _get_account((debtor_id, creditor_id))
    if account:
        this_event = (change_seqnum, change_ts)
        prev_event = (account.interest_rate_last_change_seqnum, account.interest_rate_last_change_ts)
        if _is_later_event(this_event, prev_event):
            _change_interest_rate(account, interest_rate, change_seqnum, change_ts)
            if creditor_id == ROOT_CREDITOR_ID:
                # It is a nonsense to accumulate interest on debtor's
                # own account. Therefore, we only pretend that the
                # interest rate has been set, while leaving it zero.
                account.interest_rate = 0.0


@atomic
def capitalize_interest(debtor_id: int,
                        creditor_id: int,
                        accumulated_interest_threshold: int = 0,
                        current_ts: datetime = None) -> None:
    account = _get_account((debtor_id, creditor_id))
    if account:
        positive_threshold = max(1, abs(accumulated_interest_threshold))
        current_ts = current_ts or datetime.now(tz=timezone.utc)
        amount = math.floor(_calc_account_accumulated_interest(account, current_ts))

        # When the new account principal is positive and very close to
        # zero, we make it a zero. This behavior allows us to reliably
        # zero out the principal before deleting the account.
        if creditor_id != ROOT_CREDITOR_ID and 0 < account.principal + amount <= TINY_POSITIVE_AMOUNT:
            amount = -account.principal

        # Make sure `amount` and `-amount` are within INT64 limits.
        if amount > MAX_INT64:
            amount = MAX_INT64
        if amount < -MAX_INT64:
            amount = -MAX_INT64

        if abs(amount) >= positive_threshold:
            make_debtor_payment('interest', debtor_id, creditor_id, amount)


@atomic
def make_debtor_payment(
        coordinator_type: str,
        debtor_id: int,
        creditor_id: int,
        amount: int,
        transfer_info: dict = {}) -> None:
    assert MIN_INT64 <= debtor_id <= MAX_INT64
    assert MIN_INT64 <= creditor_id <= MAX_INT64
    assert -MAX_INT64 <= amount <= MAX_INT64

    # It could happen that the debtor must pay himself, for example,
    # when `capitalize_interest` is called for the debtor's
    # account. In this case we will simply discard the interest
    # accumulated on the account.
    is_self_payment = creditor_id == ROOT_CREDITOR_ID

    if amount > 0:
        # The debtor pays the creditor.
        _force_transfer(
            coordinator_type=coordinator_type,
            debtor_id=debtor_id,
            sender_creditor_id=ROOT_CREDITOR_ID,
            recipient_creditor_id=creditor_id,
            committed_at_ts=datetime.now(tz=timezone.utc),
            committed_amount=0 if is_self_payment else amount,
            transfer_info=transfer_info,
            recipient_interest_delta=0 if coordinator_type != 'interest' else -amount,
        )
    elif amount < 0:
        # The creditor pays the debtor.
        _force_transfer(
            coordinator_type=coordinator_type,
            debtor_id=debtor_id,
            sender_creditor_id=creditor_id,
            recipient_creditor_id=ROOT_CREDITOR_ID,
            committed_at_ts=datetime.now(tz=timezone.utc),
            committed_amount=0 if is_self_payment else -amount,
            transfer_info=transfer_info,
            sender_interest_delta=0 if coordinator_type != 'interest' else -amount,
        )


@atomic
def delete_account_if_zeroed(debtor_id: int, creditor_id: int) -> None:
    current_ts = datetime.now(tz=timezone.utc)
    account = _get_account((debtor_id, creditor_id))
    if (account
            and account.prepared_transfers_count == 0
            and account.locked_amount == 0
            and 0 <= _calc_account_current_balance(account, current_ts) <= TINY_POSITIVE_AMOUNT):
        if account.principal != 0:
            capitalize_interest(debtor_id, creditor_id, 0, current_ts)
        if account.principal == 0:
            account.interest = 0.0
            account.status = account.status | Account.STATUS_DELETED_FLAG
            _insert_account_change_signal(account, current_ts)


@atomic
def purge_deleted_account(debtor_id: int, creditor_id: int, if_deleted_before: datetime) -> None:
    Account.query.filter_by(debtor_id=debtor_id, creditor_id=creditor_id)\
                 .filter(Account.status.op('&')(Account.STATUS_DELETED_FLAG) == Account.STATUS_DELETED_FLAG)\
                 .filter(Account.last_change_ts < if_deleted_before)\
                 .delete(synchronize_session=False)


def _is_later_event(event: Tuple[int, datetime], other_event: Tuple[Optional[int], Optional[datetime]]) -> bool:
    seqnum, ts = event
    other_seqnum, other_ts = other_event
    if other_ts:
        advance = ts - other_ts
    else:
        advance = TD_ZERO
    return advance >= TD_MINUS_SECOND and (
        advance > TD_SECOND
        or other_seqnum is None
        or 0 < (seqnum - other_seqnum) % 0x100000000 < 0x80000000
    )


def _insert_account_change_signal(account: Account, current_ts: Optional[datetime] = None) -> None:
    current_ts = current_ts or datetime.now(tz=timezone.utc)
    account.last_change_seqnum = increment_seqnum(account.last_change_seqnum)
    account.last_change_ts = max(account.last_change_ts, current_ts)
    db.session.add(AccountChangeSignal(
        debtor_id=account.debtor_id,
        creditor_id=account.creditor_id,
        change_seqnum=account.last_change_seqnum,
        change_ts=account.last_change_ts,
        principal=account.principal,
        interest=account.interest,
        interest_rate=account.interest_rate,
        last_outgoing_transfer_date=account.last_outgoing_transfer_date,
        status=account.status,
    ))


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
        account.principal = 0
        account.prepared_transfers_count = 0
        account.locked_amount = 0
        account.status = 0
        account.interest = 0.0
        account.interest_rate = 0.0
        account.interest_rate_last_change_seqnum = None
        account.interest_rate_last_change_ts = None
        account.last_outgoing_transfer_date = None
        _insert_account_change_signal(account)


def _calc_account_current_balance(account: Account, current_ts: datetime = None) -> Decimal:
    current_ts = current_ts or datetime.now(tz=timezone.utc)
    current_balance = account.principal + Decimal.from_float(account.interest)
    if current_balance > 0:
        k = math.log(1.0 + account.interest_rate / 100.0) / SECONDS_IN_YEAR
        passed_seconds = max(0.0, (current_ts - account.last_change_ts).total_seconds())
        current_balance *= Decimal.from_float(math.exp(k * passed_seconds))
    return current_balance


def _calc_account_avl_balance(account_or_pk: AccountId, ignore_interest: bool) -> Tuple[int, AccountId]:
    avl_balance = 0
    account = _get_account(account_or_pk)
    if account:
        if ignore_interest:
            avl_balance = account.principal
        else:
            avl_balance = math.floor(_calc_account_current_balance(account))
        avl_balance -= account.locked_amount
        account_or_pk = account
    return avl_balance, account_or_pk


def _calc_account_accumulated_interest(account: Account, current_ts: datetime) -> Decimal:
    return _calc_account_current_balance(account, current_ts) - account.principal


def _change_interest_rate(account: Account, interest_rate: float, change_seqnum: int, change_ts: datetime) -> None:
    current_ts = datetime.now(tz=timezone.utc)
    account.interest = float(_calc_account_accumulated_interest(account, current_ts))
    account.interest_rate = interest_rate
    account.interest_rate_last_change_seqnum = change_seqnum
    account.interest_rate_last_change_ts = change_ts
    account.status |= Account.STATUS_ESTABLISHED_INTEREST_RATE_FLAG
    _insert_account_change_signal(account, current_ts)


def _delete_prepared_transfer(pt: PreparedTransfer) -> None:
    sender_account = pt.sender_account
    sender_account.locked_amount = max(0, sender_account.locked_amount - pt.sender_locked_amount)
    sender_account.prepared_transfers_count = max(0, sender_account.prepared_transfers_count - 1)
    db.session.delete(pt)


def _commit_prepared_transfer(pt: PreparedTransfer, committed_amount: int, transfer_info: dict) -> None:
    assert committed_amount > 0
    if committed_amount > pt.amount:
        committed_amount = pt.amount
    current_ts = datetime.now(tz=timezone.utc)
    sender_account = pt.sender_account
    sender_account.last_outgoing_transfer_date = current_ts.date()
    _apply_account_change(
        account=sender_account,
        principal_delta=-committed_amount,
        interest_delta=0,
        current_ts=current_ts,
    )
    _schedule_account_change(
        debtor_id=pt.debtor_id,
        creditor_id=pt.recipient_creditor_id,
        principal_delta=committed_amount,
        interest_delta=0,
    )
    db.session.add(CommittedTransferSignal(
        debtor_id=pt.debtor_id,
        coordinator_type=pt.coordinator_type,
        sender_creditor_id=pt.sender_creditor_id,
        recipient_creditor_id=pt.recipient_creditor_id,
        committed_at_ts=current_ts,
        committed_amount=committed_amount,
        committed_transfer_id=pt.transfer_id,
        transfer_info=transfer_info,
    ))
    _delete_prepared_transfer(pt)


def _force_transfer(coordinator_type: str,
                    debtor_id: int,
                    sender_creditor_id: int,
                    recipient_creditor_id,
                    committed_at_ts: datetime,
                    committed_amount: int,
                    transfer_info: dict = {},
                    sender_interest_delta: int = 0,
                    recipient_interest_delta: int = 0) -> None:
    assert committed_amount >= 0
    if committed_amount != 0 and sender_creditor_id != recipient_creditor_id:
        db.session.add(CommittedTransferSignal(
            debtor_id=debtor_id,
            coordinator_type=coordinator_type,
            sender_creditor_id=sender_creditor_id,
            recipient_creditor_id=recipient_creditor_id,
            committed_at_ts=committed_at_ts,
            committed_amount=committed_amount,
            transfer_info=transfer_info,
        ))
    _schedule_account_change(
        debtor_id=debtor_id,
        creditor_id=sender_creditor_id,
        principal_delta=-committed_amount,
        interest_delta=sender_interest_delta,
    )
    _schedule_account_change(
        debtor_id=debtor_id,
        creditor_id=recipient_creditor_id,
        principal_delta=committed_amount,
        interest_delta=recipient_interest_delta,
    )


def _schedule_account_change(debtor_id: int, creditor_id: int, principal_delta: int, interest_delta: int) -> None:
    if principal_delta != 0 or interest_delta != 0:
        db.session.add(ScheduledAccountChange(
            debtor_id=debtor_id,
            creditor_id=creditor_id,
            principal_delta=principal_delta,
            interest_delta=interest_delta,
        ))


def _apply_account_change(account: Account, principal_delta: int, interest_delta: int, current_ts: datetime) -> None:
    account.interest = float(_calc_account_accumulated_interest(account, current_ts) + interest_delta)
    new_principal = account.principal + principal_delta
    if new_principal < MIN_INT64:
        account.principal = MIN_INT64
        account.status |= Account.STATUS_OVERFLOWN_FLAG
    elif new_principal > MAX_INT64:
        account.principal = MAX_INT64
        account.status |= Account.STATUS_OVERFLOWN_FLAG
    else:
        account.principal = new_principal
    _insert_account_change_signal(account, current_ts)


# TODO: Process `ScheduledAccountChange` records.
