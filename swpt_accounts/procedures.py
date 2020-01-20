import math
from datetime import datetime, date, timezone, timedelta
from typing import TypeVar, Iterable, List, Tuple, Union, Optional, Callable
from decimal import Decimal
from sqlalchemy import func
from swpt_lib.utils import is_later_event
from .extensions import db
from .models import Account, PreparedTransfer, RejectedTransferSignal, PreparedTransferSignal, \
    AccountChangeSignal, AccountPurgeSignal, CommittedTransferSignal, PendingAccountChange, TransferRequest, \
    increment_seqnum, MAX_INT32, MIN_INT64, MAX_INT64, INTEREST_RATE_FLOOR, INTEREST_RATE_CEIL

T = TypeVar('T')
atomic: Callable[[T], T] = db.atomic

PRISTINE_ACCOUNT_STATUS = 0
SECONDS_IN_YEAR = 365.25 * 24 * 60 * 60
DELETE_ACCOUNT = 'delete_account'
INTEREST = 'interest'
ZERO_OUT_ACCOUNT = 'zero_out_account'

# The account `(debtor_id, ROOT_CREDITOR_ID)` is special. This is the
# debtor's account. It issuers all the money. Also, all interest and
# demurrage payments come from/to this account.
ROOT_CREDITOR_ID = 0


@atomic
def get_debtor_account_list(debtor_id: int, start_after: int = None, limit: bool = None) -> List[Account]:
    query = Account.query.filter_by(debtor_id=debtor_id).order_by(Account.creditor_id)
    if start_after is not None:
        query = query.filter(Account.creditor_id > start_after)
    if limit is not None:
        if limit < 1:
            return []
        query = query.limit(limit)
    return query.all()


@atomic
def get_account(debtor_id: int, creditor_id: int, lock: bool = False) -> Optional[Account]:
    account = _get_account_instance(debtor_id, creditor_id, lock=lock)
    if account and not account.status & Account.STATUS_DELETED_FLAG:
        return account
    return None


@atomic
def get_available_balance(debtor_id: int, creditor_id: int, minimum_account_balance: int = 0) -> Optional[int]:
    account = get_account(debtor_id, creditor_id)
    if account:
        return _get_available_balance(account, minimum_account_balance)
    return None


@atomic
def prepare_transfer(
        coordinator_type: str,
        coordinator_id: int,
        coordinator_request_id: int,
        min_amount: int,
        max_amount: int,
        debtor_id: int,
        sender_creditor_id: int,
        recipient_creditor_id: int,
        minimum_account_balance: int = 0) -> None:

    assert MIN_INT64 <= coordinator_id <= MAX_INT64
    assert MIN_INT64 <= coordinator_request_id <= MAX_INT64
    assert 0 < min_amount <= max_amount <= MAX_INT64
    assert MIN_INT64 <= debtor_id <= MAX_INT64
    assert MIN_INT64 <= sender_creditor_id <= MAX_INT64
    assert MIN_INT64 <= recipient_creditor_id <= MAX_INT64
    assert MIN_INT64 <= minimum_account_balance <= MAX_INT64
    assert minimum_account_balance >= 0 or sender_creditor_id == ROOT_CREDITOR_ID

    db.session.add(TransferRequest(
        debtor_id=debtor_id,
        coordinator_type=coordinator_type,
        coordinator_id=coordinator_id,
        coordinator_request_id=coordinator_request_id,
        min_amount=min_amount,
        max_amount=max_amount,
        sender_creditor_id=sender_creditor_id,
        recipient_creditor_id=recipient_creditor_id,
        minimum_account_balance=minimum_account_balance,
    ))


@atomic
def finalize_prepared_transfer(
        debtor_id: int,
        sender_creditor_id: int,
        transfer_id: int,
        committed_amount: int,
        transfer_info: dict = {}) -> None:

    pt = PreparedTransfer.lock_instance((debtor_id, sender_creditor_id, transfer_id))
    if pt:
        if committed_amount == 0:
            _insert_pending_account_change(
                debtor_id=pt.debtor_id,
                creditor_id=pt.sender_creditor_id,
                coordinator_type=pt.coordinator_type,
                other_creditor_id=pt.recipient_creditor_id,
                unlocked_amount=pt.sender_locked_amount,
            )
        elif committed_amount > 0:
            _execute_transfer(
                coordinator_type=pt.coordinator_type,
                debtor_id=pt.debtor_id,
                sender_creditor_id=pt.sender_creditor_id,
                recipient_creditor_id=pt.recipient_creditor_id,
                committed_at_ts=datetime.now(tz=timezone.utc),
                committed_amount=min(committed_amount, pt.sender_locked_amount),
                transfer_info=transfer_info,
                sender_unlocked_amount=pt.sender_locked_amount,
            )
        else:
            raise ValueError('The committed amount is negative.')
        db.session.delete(pt)


@atomic
def change_interest_rate(
        debtor_id: int,
        creditor_id: int,
        change_seqnum: int,
        change_ts: datetime,
        interest_rate: float) -> None:

    # Too big positive interest rates can cause account balance
    # overflows. To prevent this, the interest rates should be kept
    # within reasonable limits, and the accumulated interest should be
    # capitalized every once in a while (like once a month).
    if interest_rate > INTEREST_RATE_CEIL:
        interest_rate = INTEREST_RATE_CEIL

    # Too big negative interest rates are dangerous too. Chances are
    # that they have been entered either maliciously or by mistake. It
    # is a good precaution to not allow them at all.
    if interest_rate < INTEREST_RATE_FLOOR:
        interest_rate = INTEREST_RATE_FLOOR

    account = get_account(debtor_id, creditor_id, lock=True)
    if account:
        this_event = (change_ts, change_seqnum)
        prev_event = (account.interest_rate_last_change_ts, account.interest_rate_last_change_seqnum)
        if is_later_event(this_event, prev_event):
            _change_interest_rate(account, change_seqnum, change_ts, interest_rate)


@atomic
def capitalize_interest(
        debtor_id: int,
        creditor_id: int,
        accumulated_interest_threshold: int = 0,
        current_ts: datetime = None) -> None:

    account = get_account(debtor_id, creditor_id, lock=True)
    if account:
        positive_threshold = max(1, abs(accumulated_interest_threshold))
        current_ts = current_ts or datetime.now(tz=timezone.utc)
        amount = math.floor(_calc_account_accumulated_interest(account, current_ts))
        amount = _contain_principal_overflow(amount)
        if abs(amount) >= positive_threshold:
            make_debtor_payment(INTEREST, debtor_id, creditor_id, amount, current_ts=current_ts)


@atomic
def make_debtor_payment(
        coordinator_type: str,
        debtor_id: int,
        creditor_id: int,
        amount: int,
        transfer_info: dict = {},
        current_ts: datetime = None) -> None:

    assert MIN_INT64 <= debtor_id <= MAX_INT64
    assert MIN_INT64 <= creditor_id <= MAX_INT64
    assert -MAX_INT64 <= amount <= MAX_INT64
    current_ts = current_ts or datetime.now(tz=timezone.utc)
    is_account_deletion = coordinator_type == DELETE_ACCOUNT
    is_interest_payment = coordinator_type == INTEREST
    interest_delta = -amount if is_interest_payment else 0

    if creditor_id == ROOT_CREDITOR_ID:  # pragma: no cover
        # The debtor pays himself.
        pass
    elif amount > 0:
        # The debtor pays the creditor.
        _execute_transfer(
            coordinator_type=coordinator_type,
            debtor_id=debtor_id,
            sender_creditor_id=ROOT_CREDITOR_ID,
            recipient_creditor_id=creditor_id,
            committed_at_ts=current_ts,
            committed_amount=amount,
            transfer_info=transfer_info,
            recipient_interest_delta=interest_delta,

            # We must not insert a `PendingAccountChange` record when
            # an account is getting zeroed out for deletion, otherwise
            # the account would be resurrected immediately.
            omit_recipient_account_change=is_account_deletion,
        )
    elif amount < 0:
        # The creditor pays the debtor.
        _execute_transfer(
            coordinator_type=coordinator_type,
            debtor_id=debtor_id,
            sender_creditor_id=creditor_id,
            recipient_creditor_id=ROOT_CREDITOR_ID,
            committed_at_ts=current_ts,
            committed_amount=-amount,
            transfer_info=transfer_info,
            sender_interest_delta=interest_delta,

            # See the corresponding comment for `omit_recipient_account_change`.
            omit_sender_account_change=is_account_deletion,
        )


@atomic
def zero_out_negative_balance(debtor_id: int, creditor_id: int, last_outgoing_transfer_date: date) -> None:
    assert last_outgoing_transfer_date is not None
    account = get_account(debtor_id, creditor_id, lock=True)
    if account:
        account_date = account.last_outgoing_transfer_date
        account_date_is_ok = account_date is None or account_date <= last_outgoing_transfer_date
        zero_out_amount = -math.floor(_calc_account_current_balance(account))
        zero_out_amount = _contain_principal_overflow(zero_out_amount)
        if account_date_is_ok and zero_out_amount > 0:
            make_debtor_payment(ZERO_OUT_ACCOUNT, debtor_id, creditor_id, zero_out_amount)


@atomic
def configure_account(
        debtor_id: int,
        creditor_id: int,
        change_ts: datetime,
        change_seqnum: int,
        is_scheduled_for_deletion: bool = False,
        negligible_amount: float = 2.0) -> None:

    assert MIN_INT64 <= debtor_id <= MAX_INT64
    assert MIN_INT64 <= creditor_id <= MAX_INT64
    assert not (is_scheduled_for_deletion and creditor_id == ROOT_CREDITOR_ID)
    assert negligible_amount >= 2.0

    account = _get_or_create_account(debtor_id, creditor_id, lock=True, send_account_creation_signal=False)
    this_event = (change_ts, change_seqnum)
    prev_event = (account.config_last_change_ts, account.config_last_change_seqnum)
    if is_later_event(this_event, prev_event):
        # When a new account is created, this block is guaranteed to
        # be executed, because `account.config_last_change_ts` for
        # newly created accounts is always `None`, which means that
        # `is_later_event(this_event, prev_event)` is `True`.
        if is_scheduled_for_deletion:
            account.status |= Account.STATUS_SCHEDULED_FOR_DELETION_FLAG
        else:
            account.status &= ~Account.STATUS_SCHEDULED_FOR_DELETION_FLAG
        account.negligible_amount = negligible_amount
        account.config_last_change_ts = change_ts
        account.config_last_change_seqnum = change_seqnum
        _insert_account_change_signal(account)


@atomic
def try_to_delete_account(debtor_id: int, creditor_id: int) -> None:
    assert MIN_INT64 <= debtor_id <= MAX_INT64
    assert MIN_INT64 <= creditor_id <= MAX_INT64

    account = get_account(debtor_id, creditor_id, lock=True)
    if account and account.pending_transfers_count == 0 and account.locked_amount == 0:
        if creditor_id == ROOT_CREDITOR_ID:
            # The debtor's account can be marked as deleted only when
            # it is the only account left.
            if db.session.query(func.count(Account.creditor_id)).filter_by(debtor_id=debtor_id).scalar() == 1:
                _mark_account_as_deleted(account)
        else:
            current_ts = datetime.now(tz=timezone.utc)
            current_balance = _calc_account_current_balance(account, current_ts)
            has_negligible_balance = 0 <= current_balance <= account.negligible_amount
            is_scheduled_for_deletion = account.status & Account.STATUS_SCHEDULED_FOR_DELETION_FLAG
            if has_negligible_balance and is_scheduled_for_deletion:
                if account.principal != 0:
                    make_debtor_payment(
                        DELETE_ACCOUNT,
                        debtor_id,
                        creditor_id,
                        -account.principal,
                        current_ts=current_ts,
                    )
                    _insert_committed_transfer_signal(
                        account=account,
                        coordinator_type=DELETE_ACCOUNT,
                        other_creditor_id=ROOT_CREDITOR_ID,
                        committed_at_ts=current_ts,
                        committed_amount=-account.principal,
                        transfer_info={},
                        new_account_principal=0,
                    )
                _mark_account_as_deleted(account, current_ts)


@atomic
def purge_deleted_account(
        debtor_id: int,
        creditor_id: int,
        if_deleted_before: datetime,
        allow_hasty_purges: bool = False) -> None:

    account = _get_account_instance(debtor_id, creditor_id, lock=True)
    if account and account.status & Account.STATUS_DELETED_FLAG and account.last_change_ts < if_deleted_before:
        yesterday = date.today() - timedelta(days=1)

        # When one account is created, deleted, purged, and re-created
        # in a single day, the `creation_date` of the re-created
        # account will be the same as the `creation_date` of the
        # deleted account. This must be avoided, because we use the
        # creation date to differentiate `CommittedTransferSignal`s
        # from different "epochs" (the `transfer_epoch` column). The
        # `allow_hasty_purges` parameter exists used only for testing.
        if account.creation_date < yesterday or allow_hasty_purges:
            db.session.delete(account)
            db.session.add(AccountPurgeSignal(
                debtor_id=debtor_id,
                creditor_id=creditor_id,
                creation_date=account.creation_date,
            ))


@atomic
def get_accounts_with_transfer_requests() -> Iterable[Tuple[int, int]]:
    return set(db.session.query(TransferRequest.debtor_id, TransferRequest.sender_creditor_id).all())


@atomic
def get_accounts_with_pending_changes() -> Iterable[Tuple[int, int]]:
    return set(db.session.query(PendingAccountChange.debtor_id, PendingAccountChange.creditor_id).all())


@atomic
def process_transfer_requests(debtor_id: int, creditor_id: int) -> None:
    requests = TransferRequest.query.\
        filter_by(debtor_id=debtor_id, sender_creditor_id=creditor_id).\
        with_for_update(skip_locked=True).\
        all()

    if requests:
        sender_account = get_account(debtor_id, creditor_id, lock=True)
        new_objects = []
        for request in requests:
            new_objects.extend(_process_transfer_request(request, sender_account))
            db.session.delete(request)

        # TODO: `new_objects.sort(key=lambda o: id(type(o)))`
        #       `db.session.bulk_save_objects(new_objects)`
        # would be faster here, but it would not automatically flush
        # the signals. This should be changed when we decide to
        # disable auto-flushing.
        db.session.add_all(new_objects)


@atomic
def process_pending_account_changes(debtor_id: int, creditor_id: int) -> None:
    changes = PendingAccountChange.query.\
        filter_by(debtor_id=debtor_id, creditor_id=creditor_id).\
        with_for_update(skip_locked=True).\
        all()

    if changes:
        nonzero_deltas = False
        principal_delta = 0
        interest_delta = 0
        account = _get_or_create_account(debtor_id, creditor_id, lock=True)
        current_ts = datetime.now(tz=timezone.utc)
        current_date = current_ts.date()
        for change in changes:
            if change.principal_delta != 0 or change.interest_delta != 0:
                nonzero_deltas = True
                principal_delta += change.principal_delta
                interest_delta += change.interest_delta
            if change.unlocked_amount is not None:
                account.locked_amount = max(0, account.locked_amount - change.unlocked_amount)
                account.pending_transfers_count = max(0, account.pending_transfers_count - 1)
                if change.principal_delta < 0:
                    account.last_outgoing_transfer_date = current_date
            if change.principal_delta != 0:
                _insert_committed_transfer_signal(
                    account=account,
                    coordinator_type=change.coordinator_type,
                    other_creditor_id=change.other_creditor_id,
                    committed_at_ts=change.inserted_at_ts,
                    committed_amount=change.principal_delta,
                    transfer_info=change.transfer_info,
                    new_account_principal=_contain_principal_overflow(account.principal + principal_delta),
                )
            db.session.delete(change)

        if nonzero_deltas:
            _apply_account_change(
                account=account,
                principal_delta=principal_delta,
                interest_delta=interest_delta,
                current_ts=current_ts,
            )


@atomic
def get_dead_transfers(if_prepared_before: datetime = None) -> List[PreparedTransfer]:
    if_prepared_before = if_prepared_before or datetime.now(tz=timezone.utc) - timedelta(days=7)
    return PreparedTransfer.query.\
        filter(PreparedTransfer.prepared_at_ts < if_prepared_before).\
        all()


def _contain_principal_overflow(value: int) -> int:
    if value <= MIN_INT64:
        return -MAX_INT64
    if value > MAX_INT64:
        return MAX_INT64
    return value


def _insert_account_change_signal(account: Account, current_ts: datetime = None) -> None:
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
        last_transfer_seqnum=account.last_transfer_seqnum,
        last_outgoing_transfer_date=account.last_outgoing_transfer_date,
        creation_date=account.creation_date,
        negligible_amount=account.negligible_amount,
        status=account.status,
    ))


def _create_account(debtor_id: int, creditor_id: int) -> Account:
    account = Account(
        debtor_id=debtor_id,
        creditor_id=creditor_id,
        status=PRISTINE_ACCOUNT_STATUS,
        creation_date=datetime.now(tz=timezone.utc).date(),
    )
    with db.retry_on_integrity_error():
        db.session.add(account)
    return account


def _get_account_instance(debtor_id: int, creditor_id: int, lock: bool = False) -> Optional[Account]:
    if lock:
        account = Account.lock_instance((debtor_id, creditor_id))
    else:
        account = Account.get_instance((debtor_id, creditor_id))
    return account


def _get_or_create_account(
        debtor_id: int,
        creditor_id: int,
        lock: bool = False,
        send_account_creation_signal: bool = True) -> Account:

    account = _get_account_instance(debtor_id, creditor_id, lock=lock)
    if account is None:
        account = _create_account(debtor_id, creditor_id)
        if send_account_creation_signal:
            _insert_account_change_signal(account)
    if account.status & Account.STATUS_DELETED_FLAG:
        _resurrect_deleted_account(account)
    return account


def _resurrect_deleted_account(account: Account) -> None:
    assert account.status & Account.STATUS_DELETED_FLAG
    account.principal = 0
    account.pending_transfers_count = 0
    account.locked_amount = 0
    account.interest = 0.0
    account.interest_rate = 0.0
    account.interest_rate_last_change_seqnum = None
    account.interest_rate_last_change_ts = None
    account.last_outgoing_transfer_date = None
    account.status = PRISTINE_ACCOUNT_STATUS | account.status & Account.STATUS_SCHEDULED_FOR_DELETION_FLAG
    assert not account.status & Account.STATUS_DELETED_FLAG
    _insert_account_change_signal(account)


def _calc_account_current_balance(account: Account, current_ts: datetime = None) -> Decimal:
    if account.creditor_id == ROOT_CREDITOR_ID:
        # Any interest accumulated on the debtor's account will not be
        # included in the current balance. Thus, accumulating interest
        # on the debtor's account is has no real effect.
        return Decimal(account.principal)

    current_ts = current_ts or datetime.now(tz=timezone.utc)
    current_balance = account.principal + Decimal.from_float(account.interest)
    if current_balance > 0:
        k = math.log(1.0 + account.interest_rate / 100.0) / SECONDS_IN_YEAR
        passed_seconds = max(0.0, (current_ts - account.last_change_ts).total_seconds())
        current_balance *= Decimal.from_float(math.exp(k * passed_seconds))
    return current_balance


def _get_available_balance(account: Account, minimum_account_balance: int = 0) -> int:
    if account.creditor_id != ROOT_CREDITOR_ID:
        # Only the debtor's account is allowed to go deliberately
        # negative. This is because only the debtor's account is
        # allowed to issue money.
        minimum_account_balance = max(0, minimum_account_balance)

    available_balance = math.floor(_calc_account_current_balance(account)) - account.locked_amount
    return available_balance - minimum_account_balance


def _calc_account_accumulated_interest(account: Account, current_ts: datetime) -> Decimal:
    return _calc_account_current_balance(account, current_ts) - account.principal


def _change_interest_rate(
        account: Account,
        change_seqnum: int,
        change_ts: datetime,
        interest_rate: float) -> None:

    current_ts = datetime.now(tz=timezone.utc)
    account.interest = float(_calc_account_accumulated_interest(account, current_ts))
    account.interest_rate = interest_rate
    account.status |= Account.STATUS_ESTABLISHED_INTEREST_RATE_FLAG
    account.interest_rate_last_change_seqnum = change_seqnum
    account.interest_rate_last_change_ts = change_ts
    _insert_account_change_signal(account, current_ts)


def _execute_transfer(
        coordinator_type: str,
        debtor_id: int,
        sender_creditor_id: int,
        recipient_creditor_id,
        committed_at_ts: datetime,
        committed_amount: int,
        transfer_info: dict = {},
        sender_unlocked_amount: int = None,
        sender_interest_delta: int = 0,
        recipient_interest_delta: int = 0,
        omit_sender_account_change: bool = False,
        omit_recipient_account_change: bool = False) -> None:

    assert committed_amount > 0
    if not omit_sender_account_change:
        _insert_pending_account_change(
            debtor_id=debtor_id,
            creditor_id=sender_creditor_id,
            coordinator_type=coordinator_type,
            other_creditor_id=recipient_creditor_id,
            inserted_at_ts=committed_at_ts,
            transfer_info=transfer_info,
            principal_delta=-committed_amount,
            interest_delta=sender_interest_delta,
            unlocked_amount=sender_unlocked_amount,
        )
    if not omit_recipient_account_change:
        _insert_pending_account_change(
            debtor_id=debtor_id,
            creditor_id=recipient_creditor_id,
            coordinator_type=coordinator_type,
            other_creditor_id=sender_creditor_id,
            inserted_at_ts=committed_at_ts,
            transfer_info=transfer_info,
            principal_delta=committed_amount,
            interest_delta=recipient_interest_delta,
        )


def _insert_pending_account_change(
        debtor_id: int,
        creditor_id: int,
        coordinator_type: str,
        other_creditor_id: int,
        inserted_at_ts: datetime = None,
        transfer_info: dict = None,
        principal_delta: int = 0,
        interest_delta: int = 0,
        unlocked_amount: int = None) -> None:

    if principal_delta != 0 or interest_delta != 0 or unlocked_amount is not None:
        db.session.add(PendingAccountChange(
            debtor_id=debtor_id,
            creditor_id=creditor_id,
            coordinator_type=coordinator_type,
            other_creditor_id=other_creditor_id,
            inserted_at_ts=inserted_at_ts or datetime.now(tz=timezone.utc),
            transfer_info=transfer_info,
            principal_delta=principal_delta,
            interest_delta=interest_delta,
            unlocked_amount=unlocked_amount,
        ))


def _insert_committed_transfer_signal(
        account: Account,
        coordinator_type: str,
        other_creditor_id: int,
        committed_at_ts: datetime,
        committed_amount: int,
        transfer_info: dict,
        new_account_principal: int) -> None:

    assert committed_amount != 0
    account.last_transfer_seqnum += 1

    # We do not send notifications for transfers from/to the debtor's
    # account, because the debtor's account account does not have a
    # real owning creditor.
    if account.creditor_id != ROOT_CREDITOR_ID:
        db.session.add(CommittedTransferSignal(
            debtor_id=account.debtor_id,
            creditor_id=account.creditor_id,
            transfer_epoch=account.creation_date,
            transfer_seqnum=account.last_transfer_seqnum,
            coordinator_type=coordinator_type,
            other_creditor_id=other_creditor_id,
            committed_at_ts=committed_at_ts,
            committed_amount=committed_amount,
            transfer_info=transfer_info,
            new_account_principal=new_account_principal,
        ))


def _mark_account_as_deleted(account: Account, current_ts: datetime = None):
    current_ts = current_ts or datetime.now(tz=timezone.utc)
    account.principal = 0
    account.interest = 0.0
    account.status |= Account.STATUS_DELETED_FLAG
    _insert_account_change_signal(account, current_ts)


def _apply_account_change(account: Account, principal_delta: int, interest_delta: int, current_ts: datetime) -> None:
    account.interest = float(_calc_account_accumulated_interest(account, current_ts) + interest_delta)
    principal_possibly_overflown = account.principal + principal_delta
    principal = _contain_principal_overflow(principal_possibly_overflown)
    if principal != principal_possibly_overflown:
        account.status |= Account.STATUS_OVERFLOWN_FLAG
    account.principal = principal
    _insert_account_change_signal(account, current_ts)


def _process_transfer_request(tr: TransferRequest, sender_account: Optional[Account]) -> list:

    def reject(**kw) -> List[RejectedTransferSignal]:
        return [RejectedTransferSignal(
            debtor_id=tr.debtor_id,
            coordinator_type=tr.coordinator_type,
            coordinator_id=tr.coordinator_id,
            coordinator_request_id=tr.coordinator_request_id,
            details=kw,
        )]

    def accept(amount: int) -> List[Union[PreparedTransfer, PreparedTransferSignal]]:
        assert sender_account is not None
        current_ts = datetime.now(tz=timezone.utc)
        sender_account.locked_amount = min(sender_account.locked_amount + amount, MAX_INT64)
        sender_account.pending_transfers_count += 1
        if sender_account.last_transfer_id < MAX_INT64:
            sender_account.last_transfer_id += 1
        else:  # pragma: no cover
            sender_account.last_transfer_id = MIN_INT64
        return [
            PreparedTransfer(
                debtor_id=tr.debtor_id,
                sender_creditor_id=tr.sender_creditor_id,
                transfer_id=sender_account.last_transfer_id,
                coordinator_type=tr.coordinator_type,
                recipient_creditor_id=tr.recipient_creditor_id,
                sender_locked_amount=amount,
                prepared_at_ts=current_ts,
            ),
            PreparedTransferSignal(
                debtor_id=tr.debtor_id,
                sender_creditor_id=tr.sender_creditor_id,
                transfer_id=sender_account.last_transfer_id,
                coordinator_type=tr.coordinator_type,
                recipient_creditor_id=tr.recipient_creditor_id,
                sender_locked_amount=amount,
                prepared_at_ts=current_ts,
                coordinator_id=tr.coordinator_id,
                coordinator_request_id=tr.coordinator_request_id,
            ),
        ]

    if sender_account is None:
        return reject(
            error_code='ACC001',
            message='The sender account does not exist.',
        )
    assert sender_account.debtor_id == tr.debtor_id
    assert sender_account.creditor_id == tr.sender_creditor_id

    if tr.sender_creditor_id == tr.recipient_creditor_id:
        return reject(
            error_code='ACC002',
            message='Recipient and sender accounts are the same.',
        )

    recipient_account = get_account(tr.debtor_id, tr.recipient_creditor_id)
    if recipient_account is None:
        return reject(
            error_code='ACC003',
            message='The recipient account does not exist.',
        )
    if recipient_account.status & Account.STATUS_SCHEDULED_FOR_DELETION_FLAG:
        return reject(
            error_code='ACC004',
            message='The recipient account is scheduled for deletion.',
        )

    amount = min(_get_available_balance(sender_account, tr.minimum_account_balance), tr.max_amount)
    if amount < tr.min_amount:
        return reject(
            error_code='ACC005',
            message='The available balance is insufficient.',
            avl_balance=amount,
        )

    if sender_account.pending_transfers_count >= MAX_INT32:
        return reject(
            error_code='ACC006',
            message='There are too many pending transfers.',
            pending_transfers_count=sender_account.pending_transfers_count,
        )

    return accept(amount)
