import math
from datetime import datetime, date, timezone
from typing import TypeVar, Iterable, List, Tuple, Union, Optional, Callable, Set
from decimal import Decimal
from flask import current_app
from sqlalchemy.sql.expression import tuple_
from swpt_lib.utils import is_later_event, increment_seqnum
from .extensions import db
from .models import Account, PreparedTransfer, RejectedTransferSignal, PreparedTransferSignal, \
    AccountChangeSignal, AccountCommitSignal, PendingAccountChange, TransferRequest, \
    FinalizedTransferSignal, AccountMaintenanceSignal, MIN_INT32, MAX_INT32, \
    MIN_INT64, MAX_INT64, INTEREST_RATE_FLOOR, INTEREST_RATE_CEIL, BEGINNING_OF_TIME

T = TypeVar('T')
atomic: Callable[[T], T] = db.atomic

PRISTINE_ACCOUNT_STATUS = 0
SECONDS_IN_DAY = 24 * 60 * 60
SECONDS_IN_YEAR = 365.25 * SECONDS_IN_DAY
DELETE_ACCOUNT = 'delete_account'
INTEREST = 'interest'
ZERO_OUT_ACCOUNT = 'zero_out_account'
ACCOUNT_PK = tuple_(Account.debtor_id, Account.creditor_id)

# The account `(debtor_id, ROOT_CREDITOR_ID)` is special. This is the
# debtor's account. It issuers all the money. Also, all interest and
# demurrage payments come from/to this account.
ROOT_CREDITOR_ID = 0


@atomic
def get_account(debtor_id: int, creditor_id: int, lock: bool = False) -> Optional[Account]:
    account = _get_account_instance(debtor_id, creditor_id, lock=lock)
    if account and not account.status & Account.STATUS_DELETED_FLAG:
        return account
    return None


@atomic
def get_available_amount(debtor_id: int, creditor_id: int) -> Optional[int]:
    current_ts = datetime.now(tz=timezone.utc)
    account = get_account(debtor_id, creditor_id)
    if account:
        return _get_available_amount(account, current_ts)
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
        signal_ts: datetime,
        minimum_account_balance: int = 0) -> None:

    assert MIN_INT64 <= coordinator_id <= MAX_INT64
    assert MIN_INT64 <= coordinator_request_id <= MAX_INT64
    assert 0 < min_amount <= max_amount <= MAX_INT64
    assert MIN_INT64 <= debtor_id <= MAX_INT64
    assert MIN_INT64 <= sender_creditor_id <= MAX_INT64
    assert MIN_INT64 <= recipient_creditor_id <= MAX_INT64
    assert MIN_INT64 <= minimum_account_balance <= MAX_INT64

    if sender_creditor_id != ROOT_CREDITOR_ID:
        # Only the debtor's account is allowed to go deliberately
        # negative. This is because only the debtor's account is
        # allowed to issue money.
        minimum_account_balance = max(0, minimum_account_balance)

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
        transfer_info: str = '') -> None:

    current_ts = datetime.now(tz=timezone.utc)
    pt = PreparedTransfer.lock_instance((debtor_id, sender_creditor_id, transfer_id))
    if pt:
        assert pt.sender_locked_amount > 0
        if committed_amount == 0:
            _insert_pending_account_change(
                debtor_id=pt.debtor_id,
                creditor_id=pt.sender_creditor_id,
                coordinator_type=pt.coordinator_type,
                other_creditor_id=pt.recipient_creditor_id,
                inserted_at_ts=current_ts,
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
def change_interest_rate(debtor_id: int, creditor_id: int, interest_rate: float, request_ts: datetime) -> None:
    assert MIN_INT64 <= debtor_id <= MAX_INT64
    assert MIN_INT64 <= creditor_id <= MAX_INT64
    assert interest_rate is not None
    assert request_ts is not None

    current_ts = datetime.now(tz=timezone.utc)
    account = get_account(debtor_id, creditor_id, lock=True)
    if account:
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

        signalbus_max_delay_seconds = current_app.config['APP_SIGNALBUS_MAX_DELAY_DAYS'] * SECONDS_IN_DAY
        has_established_interest_rate = account.status & Account.STATUS_ESTABLISHED_INTEREST_RATE_FLAG
        has_correct_interest_rate = has_established_interest_rate and account.interest_rate == interest_rate

        # `change_interest_rate` requests can come out-of-order. This
        # works fine, because sooner or later the announced interest
        # rate will be set. Nevertheless, we must not set an interest
        # rate that is excessively outdated.
        is_valid_request = (current_ts - request_ts).total_seconds() <= signalbus_max_delay_seconds

        if is_valid_request and not has_correct_interest_rate:
            # Before changing the interest rate, we must not forget to
            # calculate the interest accumulated after the last account
            # change. (For that, we must use the old interest rate).
            account.interest = float(_calc_account_accumulated_interest(account, current_ts))

            account.interest_rate = interest_rate
            account.status |= Account.STATUS_ESTABLISHED_INTEREST_RATE_FLAG
            _insert_account_change_signal(account, current_ts)

    _insert_account_maintenance_signal(debtor_id, creditor_id, request_ts, current_ts)


@atomic
def capitalize_interest(
        debtor_id: int,
        creditor_id: int,
        accumulated_interest_threshold: int,
        request_ts: datetime) -> None:

    assert MIN_INT64 <= debtor_id <= MAX_INT64
    assert MIN_INT64 <= creditor_id <= MAX_INT64

    current_ts = datetime.now(tz=timezone.utc)
    account = get_account(debtor_id, creditor_id, lock=True)
    if account:
        positive_threshold = max(1, abs(accumulated_interest_threshold))
        accumulated_interest = math.floor(_calc_account_accumulated_interest(account, current_ts))
        accumulated_interest = _contain_principal_overflow(accumulated_interest)
        if abs(accumulated_interest) >= positive_threshold:
            _make_debtor_payment(INTEREST, account, accumulated_interest, current_ts)

    _insert_account_maintenance_signal(debtor_id, creditor_id, request_ts, current_ts)


@atomic
def make_debtor_payment(
        coordinator_type: str,
        debtor_id: int,
        creditor_id: int,
        amount: int,
        transfer_info: str = '') -> None:

    assert MIN_INT64 <= debtor_id <= MAX_INT64
    assert MIN_INT64 <= creditor_id <= MAX_INT64
    assert -MAX_INT64 <= amount <= MAX_INT64

    current_ts = datetime.now(tz=timezone.utc)
    account = _lock_or_create_account(debtor_id, creditor_id, current_ts)
    _make_debtor_payment(coordinator_type, account, amount, current_ts, transfer_info)


@atomic
def zero_out_negative_balance(
        debtor_id: int,
        creditor_id: int,
        last_outgoing_transfer_date: date,
        request_ts: datetime) -> None:

    assert MIN_INT64 <= debtor_id <= MAX_INT64
    assert MIN_INT64 <= creditor_id <= MAX_INT64
    assert last_outgoing_transfer_date is not None

    current_ts = datetime.now(tz=timezone.utc)
    account = get_account(debtor_id, creditor_id, lock=True)
    if account:
        zero_out_amount = -math.floor(_calc_account_current_balance(account, current_ts))
        zero_out_amount = _contain_principal_overflow(zero_out_amount)
        if account.last_outgoing_transfer_date <= last_outgoing_transfer_date and zero_out_amount > 0:
            _make_debtor_payment(ZERO_OUT_ACCOUNT, account, zero_out_amount, current_ts)

    _insert_account_maintenance_signal(debtor_id, creditor_id, request_ts, current_ts)


@atomic
def configure_account(
        debtor_id: int,
        creditor_id: int,
        signal_ts: datetime,
        signal_seqnum: int,
        is_scheduled_for_deletion: bool = False,
        negligible_amount: float = 0.0) -> None:

    assert MIN_INT64 <= debtor_id <= MAX_INT64
    assert MIN_INT64 <= creditor_id <= MAX_INT64
    assert signal_ts > BEGINNING_OF_TIME
    assert MIN_INT32 <= signal_seqnum <= MAX_INT32
    assert not (is_scheduled_for_deletion and creditor_id == ROOT_CREDITOR_ID)
    assert negligible_amount >= 0.0

    current_ts = datetime.now(tz=timezone.utc)
    signalbus_max_delay_seconds = current_app.config['APP_SIGNALBUS_MAX_DELAY_DAYS'] * SECONDS_IN_DAY
    if (current_ts - signal_ts).total_seconds() > signalbus_max_delay_seconds:
        # Too old `configure_account` signals should be ignored,
        # otherwise deleted/purged accounts could be needlessly
        # resurrected.
        return

    account = _lock_or_create_account(debtor_id, creditor_id, current_ts, send_account_creation_signal=False)
    this_event = (signal_ts, signal_seqnum)
    prev_event = (account.last_config_signal_ts, account.last_config_signal_seqnum)
    if is_later_event(this_event, prev_event):
        # When a new account has been created, this block is guaranteed
        # to be executed, because `account.last_config_signal_ts` for
        # newly created accounts is many years ago, which means that
        # `is_later_event(this_event, prev_event)` is `True`.
        if is_scheduled_for_deletion:
            account.status |= Account.STATUS_SCHEDULED_FOR_DELETION_FLAG
        else:
            account.status &= ~Account.STATUS_SCHEDULED_FOR_DELETION_FLAG
        account.negligible_amount = negligible_amount
        account.last_config_signal_ts = signal_ts
        account.last_config_signal_seqnum = signal_seqnum
        _apply_account_change(account, 0, 0, current_ts)


@atomic
def try_to_delete_account(debtor_id: int, creditor_id: int, request_ts: datetime) -> None:
    assert MIN_INT64 <= debtor_id <= MAX_INT64
    assert MIN_INT64 <= creditor_id <= MAX_INT64

    current_ts = datetime.now(tz=timezone.utc)
    account = get_account(debtor_id, creditor_id, lock=True)
    if account and account.pending_transfers_count == 0:
        if creditor_id == ROOT_CREDITOR_ID:
            if account.principal == 0:
                _mark_account_as_deleted(account, current_ts)
        else:
            current_balance = _calc_account_current_balance(account, current_ts)
            has_negligible_balance = 0 <= current_balance <= max(2.0, account.negligible_amount)
            is_scheduled_for_deletion = account.status & Account.STATUS_SCHEDULED_FOR_DELETION_FLAG
            if has_negligible_balance and is_scheduled_for_deletion:
                if account.principal != 0:
                    _make_debtor_payment(DELETE_ACCOUNT, account, -account.principal, current_ts)
                _mark_account_as_deleted(account, current_ts)

    _insert_account_maintenance_signal(debtor_id, creditor_id, request_ts, current_ts)


@atomic
def get_accounts_with_transfer_requests() -> Iterable[Tuple[int, int]]:
    return set(db.session.query(TransferRequest.debtor_id, TransferRequest.sender_creditor_id).all())


@atomic
def get_accounts_with_pending_changes() -> Iterable[Tuple[int, int]]:
    return set(db.session.query(PendingAccountChange.debtor_id, PendingAccountChange.creditor_id).all())


@atomic
def process_transfer_requests(debtor_id: int, creditor_id: int) -> None:
    current_ts = datetime.now(tz=timezone.utc)
    transfer_requests = TransferRequest.query.\
        filter_by(debtor_id=debtor_id, sender_creditor_id=creditor_id).\
        with_for_update(skip_locked=True).\
        all()

    if transfer_requests:
        sender_account = get_account(debtor_id, creditor_id, lock=True)
        accessible_recipient_account_pks = _get_accessible_recipient_account_pks(transfer_requests)
        new_objects = []

        # TODO: Consider using bulk-inserts and bulk-deletes when we
        #       decide to disable auto-flushing. This would probably be
        #       slightly faster.
        for tr in transfer_requests:
            is_recipient_accessible = (debtor_id, tr.recipient_creditor_id) in accessible_recipient_account_pks
            new_objects.extend(_process_transfer_request(tr, sender_account, current_ts, is_recipient_accessible))
            db.session.delete(tr)
        db.session.add_all(new_objects)


@atomic
def process_pending_account_changes(debtor_id: int, creditor_id: int) -> None:
    current_ts = datetime.now(tz=timezone.utc)
    changes = PendingAccountChange.query.\
        filter_by(debtor_id=debtor_id, creditor_id=creditor_id).\
        with_for_update(skip_locked=True).\
        all()

    if changes:
        current_date = current_ts.date()
        nonzero_deltas = False
        principal_delta = 0
        interest_delta = 0
        account = _lock_or_create_account(debtor_id, creditor_id, current_ts)

        # TODO: Consider using bulk-inserts and bulk-deletes when we
        #       decide to disable auto-flushing. This would probably be
        #       slightly faster.
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
            _apply_account_change(account, principal_delta, interest_delta, current_ts)


def _contain_principal_overflow(value: int) -> int:
    if value <= MIN_INT64:
        return -MAX_INT64
    if value > MAX_INT64:
        return MAX_INT64
    return value


def _insert_account_change_signal(account: Account, current_ts: datetime) -> None:
    # NOTE: Callers of this function should be very careful, because
    #       it updates `account.last_change_ts` without updating
    #       `account.interest`. This will result in an incorrect value
    #       for the interest, unless the current balance is zero, or
    #       `account.interest` is updated "manually" before this
    #       function is called.

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
        last_config_signal_ts=account.last_config_signal_ts,
        last_config_signal_seqnum=account.last_config_signal_seqnum,
        creation_date=account.creation_date,
        negligible_amount=account.negligible_amount,
        status=account.status,
        inserted_at_ts=account.last_change_ts,
    ))


def _create_account(debtor_id: int, creditor_id: int, current_ts: datetime) -> Account:
    account = Account(
        debtor_id=debtor_id,
        creditor_id=creditor_id,
        status=PRISTINE_ACCOUNT_STATUS,
        creation_date=current_ts.date(),
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


def _lock_or_create_account(
        debtor_id: int,
        creditor_id: int,
        current_ts: datetime,
        send_account_creation_signal: bool = True) -> Account:

    account = _get_account_instance(debtor_id, creditor_id, lock=True)
    if account is None:
        account = _create_account(debtor_id, creditor_id, current_ts)
        if send_account_creation_signal:
            _insert_account_change_signal(account, current_ts)
    if account.status & Account.STATUS_DELETED_FLAG:
        account.status &= ~Account.STATUS_DELETED_FLAG
        account.status &= ~Account.STATUS_ESTABLISHED_INTEREST_RATE_FLAG
        _insert_account_change_signal(account, current_ts)
    return account


def _calc_account_current_balance(account: Account, current_ts: datetime) -> Decimal:
    if account.creditor_id == ROOT_CREDITOR_ID:
        # Any interest accumulated on the debtor's account will not be
        # included in the current balance. Thus, accumulating interest
        # on the debtor's account is has no real effect.
        return Decimal(account.principal)

    current_balance = account.principal + Decimal.from_float(account.interest)
    if current_balance > 0:
        k = math.log(1.0 + account.interest_rate / 100.0) / SECONDS_IN_YEAR
        passed_seconds = max(0.0, (current_ts - account.last_change_ts).total_seconds())
        current_balance *= Decimal.from_float(math.exp(k * passed_seconds))
    return current_balance


def _get_available_amount(account: Account, current_ts: datetime) -> int:
    current_balance = math.floor(_calc_account_current_balance(account, current_ts))
    return _contain_principal_overflow(current_balance - account.locked_amount)


def _calc_account_accumulated_interest(account: Account, current_ts: datetime) -> Decimal:
    return _calc_account_current_balance(account, current_ts) - account.principal


def _insert_pending_account_change(
        debtor_id: int,
        creditor_id: int,
        coordinator_type: str,
        other_creditor_id: int,
        inserted_at_ts: datetime,
        transfer_info: str = None,
        principal_delta: int = 0,
        interest_delta: int = 0,
        unlocked_amount: int = None) -> None:

    # TODO: To achieve better scalability, consider emitting a
    #       `PendingAccountChangeSignal` instead (with a globally unique
    #       ID), then implement an actor that reads those signals and
    #       inserts `PendingAccountChange` records for them (correctly
    #       handling possible multiple deliveries).

    if principal_delta != 0 or interest_delta != 0 or unlocked_amount is not None:
        db.session.add(PendingAccountChange(
            debtor_id=debtor_id,
            creditor_id=creditor_id,
            coordinator_type=coordinator_type,
            other_creditor_id=other_creditor_id,
            inserted_at_ts=inserted_at_ts,
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
        transfer_info: str,
        account_new_principal: int) -> None:

    assert committed_amount != 0
    previous_transfer_seqnum = account.last_transfer_seqnum
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
            is_insignificant=0 <= committed_amount <= account.negligible_amount,
            previous_transfer_seqnum=previous_transfer_seqnum,
        ))


def _mark_account_as_deleted(account: Account, current_ts: datetime):
    account.principal = 0
    account.interest = 0.0
    account.locked_amount = 0
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
        current_ts: datetime,
        transfer_info: str = '') -> None:

    assert -MAX_INT64 <= amount <= MAX_INT64
    if amount != 0 and account.creditor_id != ROOT_CREDITOR_ID:
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

        # We do not need to update the account principal and interest
        # when deleting an account because they are getting zeroed out
        # anyway.
        if coordinator_type != DELETE_ACCOUNT:
            principal_delta = amount
            interest_delta = -amount if coordinator_type == INTEREST else 0
            _apply_account_change(account, principal_delta, interest_delta, current_ts)


def _process_transfer_request(
        tr: TransferRequest,
        sender_account: Optional[Account],
        current_ts: datetime,
        is_recipient_accessible: bool) -> list:

    def reject(rejection_code: str, available_amount: int) -> List[RejectedTransferSignal]:
        return [RejectedTransferSignal(
            debtor_id=tr.debtor_id,
            coordinator_type=tr.coordinator_type,
            coordinator_id=tr.coordinator_id,
            coordinator_request_id=tr.coordinator_request_id,
            rejection_code=rejection_code,
            available_amount=available_amount,
            sender_creditor_id=tr.sender_creditor_id,
        )]

    def accept(amount: int) -> List[Union[PreparedTransfer, PreparedTransferSignal]]:
        assert sender_account is not None
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
                inserted_at_ts=current_ts,
            ),
        ]

    if sender_account is None:
        return reject('SENDER_DOES_NOT_EXIST', 0)

    assert sender_account.debtor_id == tr.debtor_id
    assert sender_account.creditor_id == tr.sender_creditor_id
    available_amount = _get_available_amount(sender_account, current_ts)
    expendable_amount = min(available_amount - tr.minimum_account_balance, tr.max_amount)

    if sender_account.pending_transfers_count >= MAX_INT32:
        return reject('TOO_MANY_TRANSFERS', available_amount)

    if expendable_amount < tr.min_amount:
        return reject('INSUFFICIENT_AVAILABLE_AMOUNT', available_amount)

    if tr.sender_creditor_id == tr.recipient_creditor_id:
        return reject('RECIPIENT_SAME_AS_SENDER', available_amount)

    if not (is_recipient_accessible or tr.recipient_creditor_id == ROOT_CREDITOR_ID):
        return reject('RECIPIENT_NOT_ACCESSIBLE', available_amount)

    # Note that transfers to the debtor's account are allowed even when the
    # debtor's account does not exist. In this case, it will be created
    # when the transfer is committed.
    return accept(expendable_amount)


def _insert_account_maintenance_signal(
        debtor_id: int,
        creditor_id: int,
        request_ts: datetime,
        current_ts: datetime) -> None:

    db.session.add(AccountMaintenanceSignal(
        debtor_id=debtor_id,
        creditor_id=creditor_id,
        request_ts=request_ts,
        inserted_at_ts=current_ts,
    ))


def _get_accessible_recipient_account_pks(transfer_requests: List[TransferRequest]) -> Set[Tuple[int, int]]:
    # TODO: To achieve better scalability, consider using some fast
    #       distributed key-store (Redis?) containing the (debtor_id,
    #       creditor_id) tuples for all accessible accounts.

    account_pks = [(tr.debtor_id, tr.recipient_creditor_id) for tr in transfer_requests]
    account_pks = db.session.\
        query(Account.debtor_id, Account.creditor_id).\
        filter(ACCOUNT_PK.in_(account_pks)).\
        filter(Account.status.op('&')(Account.STATUS_DELETED_FLAG) == 0).\
        filter(Account.status.op('&')(Account.STATUS_SCHEDULED_FOR_DELETION_FLAG) == 0).\
        all()
    return set(account_pks)
