import math
from datetime import datetime, date, timezone
from typing import TypeVar, Iterable, List, Tuple, Union, Optional, Callable, Set
from decimal import Decimal
from flask import current_app
from sqlalchemy.sql.expression import tuple_
from swpt_lib.utils import is_later_event, increment_seqnum
from .extensions import db
from .models import (
    Account, TransferRequest, PreparedTransfer, PendingAccountChange,
    RejectedTransferSignal, PreparedTransferSignal, FinalizedTransferSignal,
    AccountChangeSignal, AccountCommitSignal, AccountMaintenanceSignal,
    ROOT_CREDITOR_ID, INTEREST_RATE_FLOOR, INTEREST_RATE_CEIL,
    MIN_INT16, MAX_INT16, MIN_INT32, MAX_INT32, MIN_INT64, MAX_INT64,
    BEGINNING_OF_TIME, SECONDS_IN_DAY,
    CT_INTEREST, CT_NULLIFY, CT_DELETE, CT_DIRECT
)

T = TypeVar('T')
atomic: Callable[[T], T] = db.atomic

ACCOUNT_PK = tuple_(Account.debtor_id, Account.creditor_id)


@atomic
def configure_account(
        debtor_id: int,
        creditor_id: int,
        signal_ts: datetime,
        signal_seqnum: int,
        status_flags: int = 0,
        negligible_amount: float = 0.0) -> None:

    assert MIN_INT64 <= debtor_id <= MAX_INT64
    assert MIN_INT64 <= creditor_id <= MAX_INT64
    assert signal_ts > BEGINNING_OF_TIME
    assert MIN_INT32 <= signal_seqnum <= MAX_INT32
    assert MIN_INT16 <= status_flags <= MAX_INT16

    current_ts = datetime.now(tz=timezone.utc)
    signalbus_max_delay_seconds = current_app.config['APP_SIGNALBUS_MAX_DELAY_DAYS'] * SECONDS_IN_DAY
    is_signal_outdated = (current_ts - signal_ts).total_seconds() > signalbus_max_delay_seconds
    if not is_signal_outdated:
        account = _lock_or_create_account(debtor_id, creditor_id, current_ts, send_account_creation_signal=False)
        this_event = (signal_ts, signal_seqnum)
        prev_event = (account.last_config_signal_ts, account.last_config_signal_seqnum)
        if is_later_event(this_event, prev_event):
            # Note that when a new account is created this block will
            # always be executed, because `last_config_signal_ts` for
            # newly created accounts is many years ago.
            account.set_user_flags(status_flags)
            account.negligible_amount = max(0.0, negligible_amount)
            account.last_config_signal_ts = signal_ts
            account.last_config_signal_seqnum = signal_seqnum
            _apply_account_change(account, 0, 0, current_ts)


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

    assert len(coordinator_type) <= 30 and coordinator_type.encode('ascii')
    assert MIN_INT64 <= coordinator_id <= MAX_INT64
    assert MIN_INT64 <= coordinator_request_id <= MAX_INT64
    assert 0 < min_amount <= max_amount <= MAX_INT64
    assert MIN_INT64 <= debtor_id <= MAX_INT64
    assert MIN_INT64 <= sender_creditor_id <= MAX_INT64
    assert MIN_INT64 <= recipient_creditor_id <= MAX_INT64
    assert signal_ts > BEGINNING_OF_TIME
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
        transfer_message: str = '',
        transfer_flags: int = 0) -> None:

    assert MIN_INT64 <= debtor_id <= MAX_INT64
    assert MIN_INT64 <= sender_creditor_id <= MAX_INT64
    assert MIN_INT64 <= transfer_id <= MAX_INT64
    assert committed_amount >= 0
    assert MIN_INT32 <= transfer_flags <= MAX_INT32

    current_ts = datetime.now(tz=timezone.utc)
    pt = PreparedTransfer.lock_instance((debtor_id, sender_creditor_id, transfer_id))
    if pt:
        committed_amount = pt.correct_committed_amount(committed_amount, current_ts)

        _insert_pending_account_change(
            debtor_id=pt.debtor_id,
            creditor_id=pt.sender_creditor_id,
            coordinator_type=pt.coordinator_type,
            other_creditor_id=pt.recipient_creditor_id,
            inserted_at_ts=current_ts,
            transfer_id=transfer_id,
            transfer_message=transfer_message,
            transfer_flags=transfer_flags,
            principal_delta=-committed_amount,
            unlocked_amount=pt.sender_locked_amount,
        )
        _insert_pending_account_change(
            debtor_id=pt.debtor_id,
            creditor_id=pt.recipient_creditor_id,
            coordinator_type=pt.coordinator_type,
            other_creditor_id=pt.sender_creditor_id,
            inserted_at_ts=current_ts,
            transfer_id=transfer_id,
            transfer_message=transfer_message,
            transfer_flags=transfer_flags,
            principal_delta=committed_amount,
        )
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
    assert not math.isnan(interest_rate)

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

        has_established_interest_rate = account.status & Account.STATUS_ESTABLISHED_INTEREST_RATE_FLAG
        has_incorrect_interest_rate = not has_established_interest_rate or account.interest_rate != interest_rate
        signalbus_max_delay_seconds = current_app.config['APP_SIGNALBUS_MAX_DELAY_DAYS'] * SECONDS_IN_DAY
        is_request_outdated = (current_ts - request_ts).total_seconds() > signalbus_max_delay_seconds
        if not is_request_outdated and has_incorrect_interest_rate:
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
    assert MIN_INT64 <= accumulated_interest_threshold <= MAX_INT64

    current_ts = datetime.now(tz=timezone.utc)
    account = get_account(debtor_id, creditor_id, lock=True)
    if account:
        positive_threshold = max(1, abs(accumulated_interest_threshold))
        accumulated_interest = math.floor(_calc_account_accumulated_interest(account, current_ts))
        accumulated_interest = _contain_principal_overflow(accumulated_interest)
        if abs(accumulated_interest) >= positive_threshold:
            _make_debtor_payment(CT_INTEREST, account, accumulated_interest, current_ts)

    _insert_account_maintenance_signal(debtor_id, creditor_id, request_ts, current_ts)


@atomic
def zero_out_negative_balance(
        debtor_id: int,
        creditor_id: int,
        last_outgoing_transfer_date: date,
        request_ts: datetime) -> None:

    assert MIN_INT64 <= debtor_id <= MAX_INT64
    assert MIN_INT64 <= creditor_id <= MAX_INT64

    current_ts = datetime.now(tz=timezone.utc)
    account = get_account(debtor_id, creditor_id, lock=True)
    if account:
        zero_out_amount = -math.floor(account.calc_current_balance(current_ts))
        zero_out_amount = _contain_principal_overflow(zero_out_amount)
        if account.last_outgoing_transfer_date <= last_outgoing_transfer_date and zero_out_amount > 0:
            _make_debtor_payment(CT_NULLIFY, account, zero_out_amount, current_ts)

    _insert_account_maintenance_signal(debtor_id, creditor_id, request_ts, current_ts)


@atomic
def try_to_delete_account(debtor_id: int, creditor_id: int, request_ts: datetime) -> None:
    assert MIN_INT64 <= debtor_id <= MAX_INT64
    assert MIN_INT64 <= creditor_id <= MAX_INT64

    current_ts = datetime.now(tz=timezone.utc)
    account = get_account(debtor_id, creditor_id, lock=True)
    if account and account.pending_transfers_count == 0:
        if creditor_id == ROOT_CREDITOR_ID:
            can_be_deleted = account.principal == 0
        else:
            current_balance = account.calc_current_balance(current_ts)
            has_negligible_balance = 0 <= current_balance <= max(2.0, account.negligible_amount)
            is_scheduled_for_deletion = account.status & Account.STATUS_SCHEDULED_FOR_DELETION_FLAG
            can_be_deleted = has_negligible_balance and is_scheduled_for_deletion

        if can_be_deleted:
            if account.principal != 0:
                _make_debtor_payment(CT_DELETE, account, -account.principal, current_ts)
            _mark_account_as_deleted(account, current_ts)

    _insert_account_maintenance_signal(debtor_id, creditor_id, request_ts, current_ts)


@atomic
def get_accounts_with_transfer_requests() -> Iterable[Tuple[int, int]]:
    return set(db.session.query(TransferRequest.debtor_id, TransferRequest.sender_creditor_id).all())


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
        rejected_transfer_signals = []
        prepared_transfer_signals = []

        for tr in transfer_requests:
            is_recipient_accessible = (debtor_id, tr.recipient_creditor_id) in accessible_recipient_account_pks
            signal = _process_transfer_request(tr, sender_account, is_recipient_accessible, current_ts)
            if isinstance(signal, RejectedTransferSignal):
                rejected_transfer_signals.append(signal)
            else:
                assert isinstance(signal, PreparedTransferSignal)
                prepared_transfer_signals.append(signal)

        # TODO: Use bulk-inserts when we decide to disable
        #       auto-flushing. This will be faster, because the useless
        #       auto-generated `signal_id`s would not be fetched separately
        #       for each inserted row.
        db.session.add_all(rejected_transfer_signals)
        db.session.add_all(prepared_transfer_signals)


@atomic
def get_accounts_with_pending_changes() -> Iterable[Tuple[int, int]]:
    return set(db.session.query(PendingAccountChange.debtor_id, PendingAccountChange.creditor_id).all())


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
                    transfer_id=change.transfer_id or 0,
                    transfer_message=change.transfer_message,
                    transfer_flags=change.transfer_flags,
                    account_new_principal=_contain_principal_overflow(account.principal + principal_delta),
                )
            db.session.delete(change)

        if nonzero_deltas:
            _apply_account_change(account, principal_delta, interest_delta, current_ts)


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
def make_debtor_payment(
        coordinator_type: str,
        debtor_id: int,
        creditor_id: int,
        amount: int,
        transfer_message: str = '',
        transfer_flags: int = 0) -> None:

    current_ts = datetime.now(tz=timezone.utc)
    account = _lock_or_create_account(debtor_id, creditor_id, current_ts)
    _make_debtor_payment(coordinator_type, account, amount, current_ts, transfer_message, transfer_flags)


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


def _get_available_amount(account: Account, current_ts: datetime) -> int:
    current_balance = math.floor(account.calc_current_balance(current_ts))
    return _contain_principal_overflow(current_balance - account.locked_amount)


def _calc_account_accumulated_interest(account: Account, current_ts: datetime) -> Decimal:
    return account.calc_current_balance(current_ts) - account.principal


def _insert_pending_account_change(
        debtor_id: int,
        creditor_id: int,
        coordinator_type: str,
        other_creditor_id: int,
        inserted_at_ts: datetime,
        transfer_id: int = None,
        transfer_message: str = None,
        transfer_flags=0,
        principal_delta: int = 0,
        interest_delta: int = 0,
        unlocked_amount: int = None) -> None:

    # TODO: To achieve better scalability, consider emitting a
    #       `PendingAccountChangeSignal` instead (with a globally unique
    #       ID), then implement an actor that reads those signals and
    #       inserts `PendingAccountChange` records for them (correctly
    #       handling possible multiple deliveries).

    if principal_delta != 0 or interest_delta != 0 or unlocked_amount is not None:
        if not (principal_delta < 0 and coordinator_type == CT_DIRECT):
            transfer_id = None
        if principal_delta == 0:
            transfer_message = None
            transfer_flags = None
        db.session.add(PendingAccountChange(
            debtor_id=debtor_id,
            creditor_id=creditor_id,
            coordinator_type=coordinator_type,
            other_creditor_id=other_creditor_id,
            inserted_at_ts=inserted_at_ts,
            transfer_id=transfer_id,
            transfer_message=transfer_message,
            transfer_flags=transfer_flags,
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
        transfer_id: int,
        transfer_message: str,
        transfer_flags: int,
        account_new_principal: int) -> None:

    assert committed_amount != 0
    previous_transfer_seqnum = account.last_transfer_seqnum
    account.last_transfer_seqnum += 1

    # Note that we do not send notifications for transfers from/to the
    # debtor's account, because the debtor's account does not have a
    # real owning creditor.
    if account.creditor_id != ROOT_CREDITOR_ID:
        system_flags = 0

        if abs(committed_amount) <= account.negligible_amount:
            system_flags |= AccountCommitSignal.SYSTEM_FLAG_IS_NEGLIGIBLE

        db.session.add(AccountCommitSignal(
            debtor_id=account.debtor_id,
            creditor_id=account.creditor_id,
            transfer_seqnum=account.last_transfer_seqnum,
            coordinator_type=coordinator_type,
            other_creditor_id=other_creditor_id,
            committed_at_ts=committed_at_ts,
            committed_amount=committed_amount,
            transfer_message=transfer_message,
            transfer_flags=transfer_flags,
            account_creation_date=account.creation_date,
            account_new_principal=account_new_principal,
            previous_transfer_seqnum=previous_transfer_seqnum,
            system_flags=system_flags,
            transfer_id=transfer_id,
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
        transfer_message: str = '',
        transfer_flags=0) -> None:

    assert coordinator_type != CT_DIRECT
    assert -MAX_INT64 <= amount <= MAX_INT64

    if amount != 0 and account.creditor_id != ROOT_CREDITOR_ID:
        _insert_pending_account_change(
            debtor_id=account.debtor_id,
            creditor_id=ROOT_CREDITOR_ID,
            coordinator_type=coordinator_type,
            other_creditor_id=account.creditor_id,
            inserted_at_ts=current_ts,
            transfer_message=transfer_message,
            transfer_flags=transfer_flags,
            principal_delta=-amount,
        )
        _insert_account_commit_signal(
            account=account,
            coordinator_type=coordinator_type,
            other_creditor_id=ROOT_CREDITOR_ID,
            committed_at_ts=current_ts,
            committed_amount=amount,
            transfer_id=0,
            transfer_message=transfer_message,
            transfer_flags=transfer_flags,
            account_new_principal=_contain_principal_overflow(account.principal + amount),
        )

        # Note that we do not need to update the principal and the
        # interest when the account is getting deleted, because they
        # will be immediately zeroed out anyway.
        if coordinator_type != CT_DELETE:
            principal_delta = amount
            interest_delta = -amount if coordinator_type == CT_INTEREST else 0
            _apply_account_change(account, principal_delta, interest_delta, current_ts)


def _process_transfer_request(
        tr: TransferRequest,
        sender_account: Optional[Account],
        is_recipient_accessible: bool,
        current_ts: datetime) -> Union[RejectedTransferSignal, PreparedTransferSignal]:

    def reject(rejection_code: str, available_amount: int) -> RejectedTransferSignal:
        return RejectedTransferSignal(
            debtor_id=tr.debtor_id,
            coordinator_type=tr.coordinator_type,
            coordinator_id=tr.coordinator_id,
            coordinator_request_id=tr.coordinator_request_id,
            rejection_code=rejection_code,
            available_amount=available_amount,
            sender_creditor_id=tr.sender_creditor_id,
        )

    def prepare(amount: int) -> PreparedTransferSignal:
        assert sender_account is not None
        sender_account.locked_amount = min(sender_account.locked_amount + amount, MAX_INT64)
        sender_account.pending_transfers_count += 1
        sender_account.last_transfer_id += 1
        db.session.add(PreparedTransfer(
            debtor_id=tr.debtor_id,
            sender_creditor_id=tr.sender_creditor_id,
            transfer_id=sender_account.last_transfer_id,
            coordinator_type=tr.coordinator_type,
            coordinator_id=tr.coordinator_id,
            coordinator_request_id=tr.coordinator_request_id,
            sender_locked_amount=amount,
            recipient_creditor_id=tr.recipient_creditor_id,
            prepared_at_ts=current_ts,
        ))
        return PreparedTransferSignal(
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
        )

    db.session.delete(tr)

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

    # Note that transfers to the debtor's account should be allowed
    # even when the debtor's account does not exist. In this case, it
    # will be created when the transfer is committed.
    if tr.recipient_creditor_id != ROOT_CREDITOR_ID and not is_recipient_accessible:
        return reject('RECIPIENT_NOT_ACCESSIBLE', available_amount)

    return prepare(expendable_amount)


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
