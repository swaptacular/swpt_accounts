import math
from datetime import datetime, date, timezone, timedelta
from typing import TypeVar, Iterable, List, Tuple, Union, Optional, Callable
from decimal import Decimal
from sqlalchemy import func
from swpt_lib.utils import is_later_event
from .extensions import db
from .models import Account, PreparedTransfer, RejectedTransferSignal, PreparedTransferSignal, \
    AccountChangeSignal, AccountPurgeSignal, AccountCommitSignal, PendingAccountChange, TransferRequest, \
    FinalizedTransferSignal, increment_seqnum, MIN_INT32, MAX_INT32, MIN_INT64, MAX_INT64, \
    INTEREST_RATE_FLOOR, INTEREST_RATE_CEIL, BEGINNING_OF_TIME

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
def get_debtor_account_list(debtor_id: int, start_after: int = None, limit: int = None) -> List[Account]:
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
        assert pt.sender_locked_amount > 0
        current_ts = datetime.now(tz=timezone.utc)
        if committed_amount == 0:
            _insert_pending_account_change(
                debtor_id=pt.debtor_id,
                creditor_id=pt.sender_creditor_id,
                coordinator_type=pt.coordinator_type,
                other_creditor_id=pt.recipient_creditor_id,
                unlocked_amount=pt.sender_locked_amount,
            )
        elif committed_amount > 0:
            committed_amount = min(committed_amount, pt.sender_locked_amount)
            _insert_pending_account_change(
                debtor_id=pt.debtor_id,
                creditor_id=pt.sender_creditor_id,
                coordinator_type=pt.coordinator_type,
                other_creditor_id=pt.recipient_creditor_id,
                inserted_at_ts=current_ts,
                transfer_info=transfer_info,
                principal_delta=-committed_amount,
                unlocked_amount=pt.sender_locked_amount,
            )
            _insert_pending_account_change(
                debtor_id=pt.debtor_id,
                creditor_id=pt.recipient_creditor_id,
                coordinator_type=pt.coordinator_type,
                other_creditor_id=pt.sender_creditor_id,
                inserted_at_ts=current_ts,
                transfer_info=transfer_info,
                principal_delta=committed_amount,
            )
        else:
            raise ValueError('The committed amount is negative.')

        db.session.add(FinalizedTransferSignal(
            debtor_id=pt.debtor_id,
            sender_creditor_id=pt.sender_creditor_id,
            transfer_id=transfer_id,
            coordinator_type=pt.coordinator_type,
            coordinator_id=pt.coordinator_id,
            coordinator_request_id=pt.coordinator_request_id,
            recipient_creditor_id=pt.recipient_creditor_id,
            prepared_at_ts=pt.prepared_at_ts,
            finalized_at_ts=max(pt.prepared_at_ts, current_ts),
            committed_amount=committed_amount,
        ))
        db.session.delete(pt)


@atomic
def change_interest_rate(debtor_id: int, creditor_id: int, interest_rate: float) -> None:
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
    if account and not (account.interest_rate == interest_rate
                        and account.status & Account.STATUS_ESTABLISHED_INTEREST_RATE_FLAG):
        current_ts = datetime.now(tz=timezone.utc)

        # Before changing the interest rate, we must not forget to
        # calculate the interest accumulated after the last account
        # change. (For that, we must use the old interest rate).
        account.interest = float(_calc_account_accumulated_interest(account, current_ts))

        account.interest_rate = interest_rate
        account.status |= Account.STATUS_ESTABLISHED_INTEREST_RATE_FLAG
        _insert_account_change_signal(account, current_ts)


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
            _make_debtor_payment(INTEREST, account, amount, current_ts=current_ts)


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
    account = _lock_or_create_account(debtor_id, creditor_id)
    _make_debtor_payment(coordinator_type, account, amount, transfer_info, current_ts)


@atomic
def zero_out_negative_balance(debtor_id: int, creditor_id: int, last_outgoing_transfer_date: date) -> None:
    assert last_outgoing_transfer_date is not None
    account = get_account(debtor_id, creditor_id, lock=True)
    if account:
        zero_out_amount = -math.floor(_calc_account_current_balance(account))
        zero_out_amount = _contain_principal_overflow(zero_out_amount)
        if account.last_outgoing_transfer_date <= last_outgoing_transfer_date and zero_out_amount > 0:
            _make_debtor_payment(ZERO_OUT_ACCOUNT, account, zero_out_amount)


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
    assert change_ts > BEGINNING_OF_TIME
    assert MIN_INT32 <= change_seqnum <= MAX_INT32
    assert not (is_scheduled_for_deletion and creditor_id == ROOT_CREDITOR_ID)

    account = _lock_or_create_account(debtor_id, creditor_id, send_account_creation_signal=False)
    this_event = (change_ts, change_seqnum)
    prev_event = (account.last_config_change_ts, account.last_config_change_seqnum)
    if is_later_event(this_event, prev_event):
        # When a new account is created, this block is guaranteed to
        # be executed, because `account.last_config_change_ts` for
        # newly created accounts is many years ago, which means that
        # `is_later_event(this_event, prev_event)` is `True`.
        if is_scheduled_for_deletion:
            account.status |= Account.STATUS_SCHEDULED_FOR_DELETION_FLAG
        else:
            account.status &= ~Account.STATUS_SCHEDULED_FOR_DELETION_FLAG
        account.negligible_amount = max(2.0, negligible_amount)
        account.last_config_change_ts = change_ts
        account.last_config_change_seqnum = change_seqnum
        _apply_account_change(account, 0, 0, datetime.now(tz=timezone.utc))


@atomic
def try_to_delete_account(debtor_id: int, creditor_id: int) -> None:
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
                    _make_debtor_payment(DELETE_ACCOUNT, account, -account.principal, current_ts=current_ts)
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
        # creation date to differentiate `AccountCommitSignal`s from
        # different "epochs" (the `account_creation_date` column). The
        # `allow_hasty_purges` parameter is used only for testing
        # purposes.
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
        account = _lock_or_create_account(debtor_id, creditor_id)
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
                _insert_account_commit_signal(
                    account=account,
                    coordinator_type=change.coordinator_type,
                    other_creditor_id=change.other_creditor_id,
                    committed_at_ts=change.inserted_at_ts,
                    committed_amount=change.principal_delta,
                    transfer_info=change.transfer_info,
                    account_new_principal=_contain_principal_overflow(account.principal + principal_delta),
                )
            db.session.delete(change)

        if nonzero_deltas:
            _apply_account_change(
                account=account,
                principal_delta=principal_delta,
                interest_delta=interest_delta,
                current_ts=current_ts,
            )


def _contain_principal_overflow(value: int) -> int:
    if value <= MIN_INT64:
        return -MAX_INT64
    if value > MAX_INT64:
        return MAX_INT64
    return value


def _insert_account_change_signal(account: Account, current_ts: datetime = None) -> None:
    # NOTE: Callers of this function should be very careful, because
    #       it updates `account.last_change_ts` without updating
    #       `account.interest`. This will result in an incorrect value
    #       for the interest, unless the current balance is zero, or
    #       `account.interest` is updated "manually" before this
    #       function is called.

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
        last_config_change_ts=account.last_config_change_ts,
        last_config_change_seqnum=account.last_config_change_seqnum,
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


def _lock_or_create_account(debtor_id: int, creditor_id: int, send_account_creation_signal: bool = True) -> Account:
    account = _get_account_instance(debtor_id, creditor_id, lock=True)
    if account is None:
        account = _create_account(debtor_id, creditor_id)
        if send_account_creation_signal:
            _insert_account_change_signal(account)
    if account.status & Account.STATUS_DELETED_FLAG:
        account.status &= ~Account.STATUS_DELETED_FLAG
        account.status &= ~Account.STATUS_ESTABLISHED_INTEREST_RATE_FLAG
        _insert_account_change_signal(account)
    return account


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

    current_balance = math.floor(_calc_account_current_balance(account))
    return current_balance - minimum_account_balance - account.locked_amount


def _calc_account_accumulated_interest(account: Account, current_ts: datetime) -> Decimal:
    return _calc_account_current_balance(account, current_ts) - account.principal


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


def _insert_account_commit_signal(
        account: Account,
        coordinator_type: str,
        other_creditor_id: int,
        committed_at_ts: datetime,
        committed_amount: int,
        transfer_info: dict,
        account_new_principal: int) -> None:

    assert committed_amount != 0
    account.last_transfer_seqnum += 1

    # We do not send notifications for transfers from/to the debtor's
    # account, because the debtor's account does not have a real
    # owning creditor.
    if account.creditor_id != ROOT_CREDITOR_ID:
        db.session.add(AccountCommitSignal(
            debtor_id=account.debtor_id,
            creditor_id=account.creditor_id,
            transfer_seqnum=account.last_transfer_seqnum,
            coordinator_type=coordinator_type,
            other_creditor_id=other_creditor_id,
            committed_at_ts=committed_at_ts,
            committed_amount=committed_amount,
            transfer_info=transfer_info,
            account_creation_date=account.creation_date,
            account_new_principal=account_new_principal,
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


def _make_debtor_payment(
        coordinator_type: str,
        account: Account,
        amount: int,
        transfer_info: dict = {},
        current_ts: datetime = None) -> None:

    assert -MAX_INT64 <= amount <= MAX_INT64
    if amount != 0 and account.creditor_id != ROOT_CREDITOR_ID:
        current_ts = current_ts or datetime.now(tz=timezone.utc)
        _insert_pending_account_change(
            debtor_id=account.debtor_id,
            creditor_id=ROOT_CREDITOR_ID,
            coordinator_type=coordinator_type,
            other_creditor_id=account.creditor_id,
            inserted_at_ts=current_ts,
            transfer_info=transfer_info,
            principal_delta=-amount,
        )
        _insert_account_commit_signal(
            account=account,
            coordinator_type=coordinator_type,
            other_creditor_id=ROOT_CREDITOR_ID,
            committed_at_ts=current_ts,
            committed_amount=amount,
            transfer_info=transfer_info,
            account_new_principal=_contain_principal_overflow(account.principal + amount),
        )
        if coordinator_type != DELETE_ACCOUNT:
            # We do not need to update the account principal and
            # interest when deleting an account, because they are
            # getting zeroed out anyway.
            _apply_account_change(
                account=account,
                principal_delta=amount,
                interest_delta=-amount if coordinator_type == INTEREST else 0,
                current_ts=current_ts,
            )


def _process_transfer_request(tr: TransferRequest, sender_account: Optional[Account]) -> list:
    # TODO: Consider verifying whether a `Prepared transfer` with the
    #       same `coordinator_type`, `coordinator_id`, and
    #       `coordinator_request_id` already exists, and if it does,
    #       do nothing. This could potentially improve the handling of
    #       multiple deliveries of `prepare_transfer` signals.

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
        sender_account.last_transfer_id += 1
        return [
            PreparedTransfer(
                debtor_id=tr.debtor_id,
                sender_creditor_id=tr.sender_creditor_id,
                transfer_id=sender_account.last_transfer_id,
                coordinator_type=tr.coordinator_type,
                coordinator_id=tr.coordinator_id,
                coordinator_request_id=tr.coordinator_request_id,
                sender_locked_amount=amount,
                recipient_creditor_id=tr.recipient_creditor_id,
                prepared_at_ts=current_ts,
            ),
            PreparedTransferSignal(
                debtor_id=tr.debtor_id,
                sender_creditor_id=tr.sender_creditor_id,
                transfer_id=sender_account.last_transfer_id,
                coordinator_type=tr.coordinator_type,
                coordinator_id=tr.coordinator_id,
                coordinator_request_id=tr.coordinator_request_id,
                sender_locked_amount=amount,
                recipient_creditor_id=tr.recipient_creditor_id,
                prepared_at_ts=current_ts,
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
