import math
from datetime import datetime, timezone, timedelta
from typing import TypeVar, Iterable, Tuple, Union, Optional, Callable
from decimal import Decimal
from sqlalchemy.sql.expression import tuple_, and_
from sqlalchemy.exc import IntegrityError
from swpt_lib.utils import Seqnum, increment_seqnum
from swpt_accounts.extensions import db
from swpt_accounts.schemas import parse_root_config_data
from swpt_accounts.models import Account, TransferRequest, PreparedTransfer, PendingBalanceChange, \
    RegisteredBalanceChange, PendingBalanceChangeSignal, RejectedConfigSignal, RejectedTransferSignal, \
    PreparedTransferSignal, FinalizedTransferSignal, AccountUpdateSignal, AccountTransferSignal, \
    FinalizationRequest, ROOT_CREDITOR_ID, INTEREST_RATE_FLOOR, INTEREST_RATE_CEIL, MAX_INT32, MIN_INT64, \
    MAX_INT64, SECONDS_IN_DAY, CT_INTEREST, CT_DELETE, CT_DIRECT, SC_OK, SC_SENDER_IS_UNREACHABLE, \
    SC_RECIPIENT_IS_UNREACHABLE, SC_INSUFFICIENT_AVAILABLE_AMOUNT, SC_RECIPIENT_SAME_AS_SENDER, \
    SC_TOO_MANY_TRANSFERS, SC_TOO_LOW_INTEREST_RATE, T0, is_negligible_balance, contain_principal_overflow

T = TypeVar('T')
atomic: Callable[[T], T] = db.atomic

ACCOUNT_PK = tuple_(
    Account.debtor_id,
    Account.creditor_id,
)
REGISTERED_BALANCE_CHANGE_PK = tuple_(
    RegisteredBalanceChange.debtor_id,
    RegisteredBalanceChange.other_creditor_id,
    RegisteredBalanceChange.change_id,
)
RC_INVALID_CONFIGURATION = 'INVALID_CONFIGURATION'
PREPARED_TRANSFER_JOIN_CLAUSE = and_(
    FinalizationRequest.debtor_id == PreparedTransfer.debtor_id,
    FinalizationRequest.sender_creditor_id == PreparedTransfer.sender_creditor_id,
    FinalizationRequest.transfer_id == PreparedTransfer.transfer_id,
    FinalizationRequest.coordinator_type == PreparedTransfer.coordinator_type,
    FinalizationRequest.coordinator_id == PreparedTransfer.coordinator_id,
    FinalizationRequest.coordinator_request_id == PreparedTransfer.coordinator_request_id,
)


@atomic
def configure_account(
        debtor_id: int,
        creditor_id: int,
        ts: datetime,
        seqnum: int,
        negligible_amount: float = 0.0,
        config_flags: int = 0,
        config_data: str = '',
        signalbus_max_delay_seconds: float = 1e30) -> bool:

    current_ts = datetime.now(tz=timezone.utc)
    should_change_interest_rate = False

    def clear_deleted_flag(account):
        nonlocal should_change_interest_rate

        if account.status_flags & Account.STATUS_DELETED_FLAG:
            account.status_flags &= ~Account.STATUS_DELETED_FLAG
            should_change_interest_rate = True

    def is_valid_config():
        if not negligible_amount >= 0.0:
            return False

        if config_data == '':
            return True

        if creditor_id == ROOT_CREDITOR_ID:
            try:
                parse_root_config_data(config_data)
            except ValueError:
                return False

        return True

    def try_to_configure(account):
        nonlocal should_change_interest_rate

        if is_valid_config():
            if account is None:
                account = _create_account(debtor_id, creditor_id, current_ts)
                should_change_interest_rate = True
            else:
                clear_deleted_flag(account)

            account.config_flags = config_flags
            account.config_data = config_data
            account.negligible_amount = negligible_amount
            account.last_config_ts = ts
            account.last_config_seqnum = seqnum
            _apply_account_change(account, 0, 0.0, current_ts)
            _insert_account_update_signal(account, current_ts)

        else:
            db.session.add(RejectedConfigSignal(
                debtor_id=debtor_id,
                creditor_id=creditor_id,
                config_ts=ts,
                config_seqnum=seqnum,
                config_flags=config_flags,
                negligible_amount=negligible_amount,
                config_data=config_data,
                rejection_code=RC_INVALID_CONFIGURATION,
            ))

    account = _get_account_instance(debtor_id, creditor_id, lock=True)
    if account:
        this_event = (ts, Seqnum(seqnum))
        last_event = (account.last_config_ts, Seqnum(account.last_config_seqnum))
        if this_event > last_event:
            try_to_configure(account)
    else:
        signal_age_seconds = (current_ts - ts).total_seconds()
        if signal_age_seconds <= signalbus_max_delay_seconds:
            try_to_configure(account)

    return should_change_interest_rate


@atomic
def prepare_transfer(
        coordinator_type: str,
        coordinator_id: int,
        coordinator_request_id: int,
        min_locked_amount: int,
        max_locked_amount: int,
        debtor_id: int,
        creditor_id: int,
        recipient_creditor_id: Optional[int],
        ts: datetime,
        max_commit_delay: int = MAX_INT32,
        min_interest_rate: float = -100.0) -> None:

    if recipient_creditor_id is None:
        db.session.add(RejectedTransferSignal(
            debtor_id=debtor_id,
            coordinator_type=coordinator_type,
            coordinator_id=coordinator_id,
            coordinator_request_id=coordinator_request_id,
            status_code=SC_RECIPIENT_IS_UNREACHABLE,
            total_locked_amount=0,
            sender_creditor_id=creditor_id,
        ))

    else:
        db.session.add(TransferRequest(
            debtor_id=debtor_id,
            coordinator_type=coordinator_type,
            coordinator_id=coordinator_id,
            coordinator_request_id=coordinator_request_id,
            min_locked_amount=min_locked_amount,
            max_locked_amount=max_locked_amount,
            sender_creditor_id=creditor_id,
            recipient_creditor_id=recipient_creditor_id,
            deadline=ts + timedelta(seconds=max_commit_delay),
            min_interest_rate=min_interest_rate,
        ))


@atomic
def finalize_transfer(
        debtor_id: int,
        creditor_id: int,
        transfer_id: int,
        coordinator_type: str,
        coordinator_id: int,
        coordinator_request_id: int,
        committed_amount: int,
        transfer_note_format: str = '',
        transfer_note: str = '',
        ts: datetime = None) -> None:

    db.session.add(FinalizationRequest(
        debtor_id=debtor_id,
        sender_creditor_id=creditor_id,
        transfer_id=transfer_id,
        coordinator_type=coordinator_type,
        coordinator_id=coordinator_id,
        coordinator_request_id=coordinator_request_id,
        committed_amount=committed_amount,
        transfer_note_format=transfer_note_format,
        transfer_note=transfer_note,
        ts=ts or datetime.now(tz=timezone.utc),
    ))

    try:
        db.session.flush()
    except IntegrityError:
        db.session.rollback()


@atomic
def is_reachable_account(debtor_id: int, creditor_id: int) -> bool:
    if creditor_id == ROOT_CREDITOR_ID:
        return True

    account_query = Account.query.\
        filter_by(debtor_id=debtor_id, creditor_id=creditor_id).\
        filter(Account.status_flags.op('&')(Account.STATUS_DELETED_FLAG) == 0).\
        filter(Account.config_flags.op('&')(Account.CONFIG_SCHEDULED_FOR_DELETION_FLAG) == 0)

    return db.session.query(account_query.exists()).scalar()


@atomic
def get_account_config_data(debtor_id: int, creditor_id: int) -> Optional[str]:
    return db.session.\
        query(Account.config_data).\
        filter_by(debtor_id=debtor_id, creditor_id=creditor_id).\
        scalar()


@atomic
def change_interest_rate(
        debtor_id: int,
        creditor_id: int,
        interest_rate: float,
        ts: datetime = None,
        signalbus_max_delay_seconds: float = 0.0) -> None:

    if creditor_id == ROOT_CREDITOR_ID:  # pragma: nocover
        return

    current_ts = datetime.now(tz=timezone.utc)
    ts = ts or current_ts
    change_min_interval_seconds = signalbus_max_delay_seconds + SECONDS_IN_DAY

    # If the scheduled "chores" have not been processed for a long
    # time, an old interest rate change request can arrive. In such
    # cases, the request will be ignored, avoiding setting a
    # potentially outdated interest rate.
    is_old_request = (current_ts - ts).total_seconds() > change_min_interval_seconds
    if is_old_request:
        return

    account = get_account(debtor_id, creditor_id, lock=True)
    if account:
        if interest_rate > INTEREST_RATE_CEIL:
            interest_rate = INTEREST_RATE_CEIL

        if interest_rate < INTEREST_RATE_FLOOR:
            interest_rate = INTEREST_RATE_FLOOR

        if math.isnan(interest_rate):  # pragma: nocover
            return

        old_interest_rate = account.interest_rate
        seconds_since_last_change = (current_ts - account.last_interest_rate_change_ts).total_seconds()

        if old_interest_rate != interest_rate and seconds_since_last_change >= change_min_interval_seconds:
            assert current_ts >= account.last_interest_rate_change_ts

            account.interest = float(_calc_account_accumulated_interest(account, current_ts))
            account.previous_interest_rate = old_interest_rate
            account.interest_rate = interest_rate
            account.last_interest_rate_change_ts = current_ts
            account.last_change_seqnum = increment_seqnum(account.last_change_seqnum)
            account.last_change_ts = max(account.last_change_ts, current_ts)

            _insert_account_update_signal(account, current_ts)


@atomic
def capitalize_interest(debtor_id: int, creditor_id: int, min_capitalization_interval: timedelta = timedelta()) -> None:
    current_ts = datetime.now(tz=timezone.utc)
    capitalization_cutoff_ts = current_ts - min_capitalization_interval
    account = get_account(debtor_id, creditor_id, lock=True)

    if account and account.last_interest_capitalization_ts <= capitalization_cutoff_ts:
        accumulated_interest = math.floor(_calc_account_accumulated_interest(account, current_ts))
        accumulated_interest = contain_principal_overflow(accumulated_interest)

        if accumulated_interest != 0:
            account.last_interest_capitalization_ts = current_ts
            _make_debtor_payment(CT_INTEREST, account, accumulated_interest, current_ts)


@atomic
def try_to_delete_account(debtor_id: int, creditor_id: int) -> None:
    if creditor_id == ROOT_CREDITOR_ID:
        # TODO: Allow the deletion of the debtor's account, but only
        #       when there are no other accounts with the given debtor
        #       in the whole system.
        return

    current_ts = datetime.now(tz=timezone.utc)

    account = get_account(debtor_id, creditor_id, lock=True)
    if account:
        account.last_deletion_attempt_ts = current_ts

        can_be_deleted = (
            account.config_flags & Account.CONFIG_SCHEDULED_FOR_DELETION_FLAG
            and account.pending_transfers_count == 0
            and is_negligible_balance(account.calc_current_balance(current_ts), account.negligible_amount)
        )
        if can_be_deleted:
            if account.principal != 0:
                _make_debtor_payment(CT_DELETE, account, -account.principal, current_ts)

            _mark_account_as_deleted(account, current_ts)


@atomic
def get_accounts_with_transfer_requests(max_count: int = None) -> Iterable[Tuple[int, int]]:
    query = db.session.query(TransferRequest.debtor_id, TransferRequest.sender_creditor_id).distinct()
    if max_count is not None:
        query = query.limit(max_count)

    return query.all()


@atomic
def process_transfer_requests(debtor_id: int, creditor_id: int, commit_period: int = MAX_INT32) -> None:
    current_ts = datetime.now(tz=timezone.utc)

    transfer_requests = TransferRequest.query.\
        filter_by(debtor_id=debtor_id, sender_creditor_id=creditor_id).\
        with_for_update(skip_locked=True).\
        all()

    if transfer_requests:
        sender_account = get_account(debtor_id, creditor_id, lock=True)
        rejected_transfer_signals = []
        prepared_transfer_signals = []

        for tr in transfer_requests:
            signal = _process_transfer_request(tr, sender_account, current_ts, commit_period)

            if isinstance(signal, RejectedTransferSignal):
                rejected_transfer_signals.append(signal)
            elif isinstance(signal, PreparedTransferSignal):
                prepared_transfer_signals.append(signal)
            else:  # pragma: nocover
                raise RuntimeError('unexpected return type')

        db.session.bulk_save_objects(rejected_transfer_signals, preserve_order=False)
        db.session.bulk_save_objects(prepared_transfer_signals, preserve_order=False)


@atomic
def get_accounts_with_finalization_requests(max_count: int = None) -> Iterable[Tuple[int, int]]:
    query = db.session.query(FinalizationRequest.debtor_id, FinalizationRequest.sender_creditor_id).distinct()
    if max_count is not None:
        query = query.limit(max_count)

    return query.all()


@atomic
def process_finalization_requests(debtor_id: int, sender_creditor_id: int) -> None:
    current_ts = datetime.now(tz=timezone.utc)

    requests = db.session.query(FinalizationRequest, PreparedTransfer).\
        outerjoin(PreparedTransfer, PREPARED_TRANSFER_JOIN_CLAUSE).\
        filter(
            FinalizationRequest.debtor_id == debtor_id,
            FinalizationRequest.sender_creditor_id == sender_creditor_id).\
        with_for_update(skip_locked=True, of=FinalizationRequest).\
        all()

    if requests:
        principal_delta = 0
        pending_balance_change_signals = []

        sender_account = get_account(debtor_id, sender_creditor_id, lock=True)
        if sender_account:
            starting_balance = math.floor(sender_account.calc_current_balance(current_ts))
            min_account_balance = _get_min_account_balance(sender_creditor_id)

        for finalization_request, prepared_transfer in requests:
            if sender_account and prepared_transfer:
                expendable_amount = (
                    + starting_balance
                    + principal_delta
                    - sender_account.total_locked_amount
                    - min_account_balance
                )
                signal = _finalize_prepared_transfer(
                    prepared_transfer,
                    finalization_request,
                    sender_account,
                    expendable_amount,
                    current_ts,
                )
                if signal:
                    committed_amount = signal.principal_delta
                    assert committed_amount > 0
                    pending_balance_change_signals.append(signal)
                else:
                    committed_amount = 0

                principal_delta -= committed_amount
                db.session.delete(prepared_transfer)

            db.session.delete(finalization_request)

        if principal_delta != 0:
            assert sender_account
            _apply_account_change(sender_account, principal_delta, 0.0, current_ts)

        db.session.bulk_save_objects(pending_balance_change_signals, preserve_order=False)


@atomic
def get_accounts_with_pending_balance_changes(max_count: int = None) -> Iterable[Tuple[int, int]]:
    query = db.session.query(PendingBalanceChange.debtor_id, PendingBalanceChange.creditor_id).distinct()
    if max_count is not None:
        query = query.limit(max_count)

    return query.all()


@atomic
def process_pending_balance_changes(debtor_id: int, creditor_id: int) -> None:
    current_ts = datetime.now(tz=timezone.utc)

    changes = PendingBalanceChange.query.\
        filter_by(debtor_id=debtor_id, creditor_id=creditor_id).\
        with_for_update(skip_locked=True).\
        all()

    if changes:
        applied_change_pks = []
        principal_delta = 0
        interest_delta = 0.0
        account = _lock_or_create_account(debtor_id, creditor_id, current_ts)

        for change in changes:
            principal_delta += change.principal_delta

            # We should compensate for the fact that the transfer was
            # committed at `change.committed_at`, but the transferred
            # amount is being added to the account's principal just
            # now (`current_ts`).
            interest_delta += account.calc_due_interest(change.principal_delta, change.committed_at, current_ts)

            _insert_account_transfer_signal(
                account=account,
                coordinator_type=change.coordinator_type,
                other_creditor_id=change.other_creditor_id,
                committed_at=change.committed_at,
                acquired_amount=change.principal_delta,
                transfer_note_format=change.transfer_note_format,
                transfer_note=change.transfer_note,
                principal=contain_principal_overflow(account.principal + principal_delta),
            )

            applied_change_pks.append((change.debtor_id, change.other_creditor_id, change.change_id))
            db.session.delete(change)

        _apply_account_change(account, principal_delta, interest_delta, current_ts)

        RegisteredBalanceChange.query.\
            filter(REGISTERED_BALANCE_CHANGE_PK.in_(applied_change_pks)).\
            update({RegisteredBalanceChange.is_applied: True}, synchronize_session=False)


@atomic
def get_account(debtor_id: int, creditor_id: int, lock: bool = False) -> Optional[Account]:
    account = _get_account_instance(debtor_id, creditor_id, lock=lock)
    if account and not account.status_flags & Account.STATUS_DELETED_FLAG:
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
        transfer_note_format: str = '',
        transfer_note: str = '') -> None:

    assert coordinator_type != CT_DIRECT

    current_ts = datetime.now(tz=timezone.utc)
    account = _lock_or_create_account(debtor_id, creditor_id, current_ts)
    _make_debtor_payment(coordinator_type, account, amount, current_ts, transfer_note_format, transfer_note)


@atomic
def insert_pending_balance_change(
        debtor_id: int,
        other_creditor_id: int,
        change_id: int,
        creditor_id: int,
        coordinator_type: str,
        transfer_note_format: str,
        transfer_note: str,
        committed_at: datetime,
        principal_delta: int,
        cutoff_ts: datetime = T0) -> None:

    assert MIN_INT64 <= debtor_id <= MAX_INT64
    assert MIN_INT64 <= creditor_id <= MAX_INT64
    assert MIN_INT64 <= change_id <= MAX_INT64

    if committed_at < cutoff_ts:
        return  # pragma: nocover

    registered_balance_change_query = RegisteredBalanceChange.query.filter_by(
        debtor_id=debtor_id,
        other_creditor_id=other_creditor_id,
        change_id=change_id,
    )
    if not db.session.query(registered_balance_change_query.exists()).scalar():
        with db.retry_on_integrity_error():
            db.session.add(RegisteredBalanceChange(
                debtor_id=debtor_id,
                other_creditor_id=other_creditor_id,
                change_id=change_id,
                committed_at=committed_at,
            ))

        db.session.add(PendingBalanceChange(
            debtor_id=debtor_id,
            other_creditor_id=other_creditor_id,
            change_id=change_id,
            creditor_id=creditor_id,
            coordinator_type=coordinator_type,
            transfer_note_format=transfer_note_format,
            transfer_note=transfer_note,
            committed_at=committed_at,
            principal_delta=principal_delta,
        ))


def _insert_account_update_signal(account: Account, current_ts: datetime) -> None:
    account.last_heartbeat_ts = current_ts
    account.pending_account_update = False

    db.session.add(AccountUpdateSignal(
        debtor_id=account.debtor_id,
        creditor_id=account.creditor_id,
        last_change_seqnum=account.last_change_seqnum,
        last_change_ts=account.last_change_ts,
        principal=account.principal,
        interest=account.interest,
        interest_rate=account.interest_rate,
        last_interest_rate_change_ts=account.last_interest_rate_change_ts,
        last_transfer_number=account.last_transfer_number,
        last_transfer_committed_at=account.last_transfer_committed_at,
        last_config_ts=account.last_config_ts,
        last_config_seqnum=account.last_config_seqnum,
        creation_date=account.creation_date,
        negligible_amount=account.negligible_amount,
        config_data=account.config_data,
        config_flags=account.config_flags,
        debtor_info_iri=account.debtor_info_iri,
        debtor_info_content_type=account.debtor_info_content_type,
        debtor_info_sha256=account.debtor_info_sha256,
        inserted_at=account.last_change_ts,
    ))


def _create_account(debtor_id: int, creditor_id: int, current_ts: datetime) -> Account:
    assert MIN_INT64 <= debtor_id <= MAX_INT64
    assert MIN_INT64 <= creditor_id <= MAX_INT64

    account = Account(
        debtor_id=debtor_id,
        creditor_id=creditor_id,
        creation_date=current_ts.date(),
    )
    with db.retry_on_integrity_error():
        db.session.add(account)

    return account


def _get_account_instance(debtor_id: int, creditor_id: int, lock: bool = False) -> Optional[Account]:
    query = Account.query.filter_by(debtor_id=debtor_id, creditor_id=creditor_id)
    if lock:
        query = query.with_for_update()

    return query.one_or_none()


def _lock_or_create_account(debtor_id: int, creditor_id: int, current_ts: datetime) -> Account:
    account = _get_account_instance(debtor_id, creditor_id, lock=True)
    if account is None:
        account = _create_account(debtor_id, creditor_id, current_ts)
        _insert_account_update_signal(account, current_ts)

    if account.status_flags & Account.STATUS_DELETED_FLAG:
        account.status_flags &= ~Account.STATUS_DELETED_FLAG
        account.last_change_seqnum = increment_seqnum(account.last_change_seqnum)
        account.last_change_ts = max(account.last_change_ts, current_ts)
        _insert_account_update_signal(account, current_ts)

    return account


def _get_available_amount(account: Account, current_ts: datetime) -> int:
    current_balance = math.floor(account.calc_current_balance(current_ts))

    return contain_principal_overflow(current_balance - account.total_locked_amount)


def _calc_account_accumulated_interest(account: Account, current_ts: datetime) -> Decimal:
    return account.calc_current_balance(current_ts) - account.principal


def _insert_account_transfer_signal(
        account: Account,
        coordinator_type: str,
        other_creditor_id: int,
        committed_at: datetime,
        acquired_amount: int,
        transfer_note_format: str,
        transfer_note: str,
        principal: int) -> None:

    assert acquired_amount != 0

    is_negligible = 0 < acquired_amount <= account.negligible_amount

    # We do not send notifications for transfers from/to the debtor's
    # account, because the debtor's account does not have a real
    # owning creditor. Sending these notifications would consume a lot
    # of resources for no good reason.
    if account.creditor_id != ROOT_CREDITOR_ID and not is_negligible:
        previous_transfer_number = account.last_transfer_number
        account.last_transfer_number += 1
        account.last_transfer_committed_at = committed_at

        db.session.add(AccountTransferSignal(
            debtor_id=account.debtor_id,
            creditor_id=account.creditor_id,
            transfer_number=account.last_transfer_number,
            coordinator_type=coordinator_type,
            other_creditor_id=other_creditor_id,
            committed_at=committed_at,
            acquired_amount=acquired_amount,
            transfer_note_format=transfer_note_format,
            transfer_note=transfer_note,
            creation_date=account.creation_date,
            principal=principal,
            previous_transfer_number=previous_transfer_number,
        ))


def _mark_account_as_deleted(account: Account, current_ts: datetime):
    account.principal = 0
    account.interest = 0.0
    account.total_locked_amount = 0
    account.status_flags |= Account.STATUS_DELETED_FLAG
    account.last_change_seqnum = increment_seqnum(account.last_change_seqnum)
    account.last_change_ts = max(account.last_change_ts, current_ts)

    _insert_account_update_signal(account, current_ts)


def _apply_account_change(account: Account, principal_delta: int, interest_delta: float, current_ts: datetime) -> None:
    account.interest = float(_calc_account_accumulated_interest(account, current_ts)) + interest_delta
    principal_possibly_overflown = account.principal + principal_delta
    principal = contain_principal_overflow(principal_possibly_overflown)

    if principal != principal_possibly_overflown:
        account.status_flags |= Account.STATUS_OVERFLOWN_FLAG

    account.principal = principal
    account.last_change_seqnum = increment_seqnum(account.last_change_seqnum)
    account.last_change_ts = max(account.last_change_ts, current_ts)
    account.pending_account_update = True


def _make_debtor_payment(
        coordinator_type: str,
        account: Account,
        amount: int,
        current_ts: datetime,
        transfer_note_format: str = '',
        transfer_note: str = '') -> None:

    assert -MAX_INT64 <= amount <= MAX_INT64

    if amount != 0 and account.creditor_id != ROOT_CREDITOR_ID:
        db.session.add(PendingBalanceChangeSignal(
            debtor_id=account.debtor_id,
            other_creditor_id=account.creditor_id,
            creditor_id=ROOT_CREDITOR_ID,
            committed_at=current_ts,
            coordinator_type=coordinator_type,
            transfer_note_format=transfer_note_format,
            transfer_note=transfer_note,
            principal_delta=-amount,
        ))
        _insert_account_transfer_signal(
            account=account,
            coordinator_type=coordinator_type,
            other_creditor_id=ROOT_CREDITOR_ID,
            committed_at=current_ts,
            acquired_amount=amount,
            transfer_note_format=transfer_note_format,
            transfer_note=transfer_note,
            principal=contain_principal_overflow(account.principal + amount),
        )

        principal_delta = amount
        interest_delta = float(-amount if coordinator_type == CT_INTEREST else 0)
        _apply_account_change(account, principal_delta, interest_delta, current_ts)


def _process_transfer_request(
        tr: TransferRequest,
        sender_account: Optional[Account],
        current_ts: datetime,
        commit_period: int) -> Union[RejectedTransferSignal, PreparedTransferSignal]:

    def reject(status_code: str, total_locked_amount: int) -> RejectedTransferSignal:
        assert total_locked_amount >= 0

        return RejectedTransferSignal(
            debtor_id=tr.debtor_id,
            coordinator_type=tr.coordinator_type,
            coordinator_id=tr.coordinator_id,
            coordinator_request_id=tr.coordinator_request_id,
            status_code=status_code,
            total_locked_amount=total_locked_amount,
            sender_creditor_id=tr.sender_creditor_id,
        )

    def prepare(amount: int) -> PreparedTransferSignal:
        assert sender_account is not None

        sender_account.total_locked_amount = min(sender_account.total_locked_amount + amount, MAX_INT64)
        sender_account.pending_transfers_count += 1
        sender_account.last_transfer_id += 1
        min_interest_rate = tr.min_interest_rate

        # When a real interest rate constraint is set, we put an upper
        # limit of one day on the deadline, to ensure that no more
        # than one change in the interest rate will happen before the
        # transfer gets finalized. This should be OK, because distant
        # deadlines are not needed in this case.
        if min_interest_rate > INTEREST_RATE_FLOOR:
            transfer_commit_period = min(commit_period, SECONDS_IN_DAY)  # pragma: no cover
        else:
            transfer_commit_period = commit_period

        deadline = min(current_ts + timedelta(seconds=transfer_commit_period), tr.deadline)

        db.session.add(PreparedTransfer(
            debtor_id=tr.debtor_id,
            sender_creditor_id=tr.sender_creditor_id,
            transfer_id=sender_account.last_transfer_id,
            coordinator_type=tr.coordinator_type,
            coordinator_id=tr.coordinator_id,
            coordinator_request_id=tr.coordinator_request_id,
            locked_amount=amount,
            recipient_creditor_id=tr.recipient_creditor_id,
            min_interest_rate=min_interest_rate,
            demurrage_rate=INTEREST_RATE_FLOOR,
            deadline=deadline,
            prepared_at=current_ts,
        ))

        return PreparedTransferSignal(
            debtor_id=tr.debtor_id,
            sender_creditor_id=tr.sender_creditor_id,
            transfer_id=sender_account.last_transfer_id,
            coordinator_type=tr.coordinator_type,
            coordinator_id=tr.coordinator_id,
            coordinator_request_id=tr.coordinator_request_id,
            locked_amount=amount,
            recipient_creditor_id=tr.recipient_creditor_id,
            prepared_at=current_ts,
            demurrage_rate=INTEREST_RATE_FLOOR,
            deadline=deadline,
            min_interest_rate=min_interest_rate,
            inserted_at=current_ts,
        )

    db.session.delete(tr)

    if sender_account is None:
        return reject(SC_SENDER_IS_UNREACHABLE, 0)

    assert sender_account.debtor_id == tr.debtor_id
    assert sender_account.creditor_id == tr.sender_creditor_id

    if sender_account.pending_transfers_count >= MAX_INT32:
        return reject(SC_TOO_MANY_TRANSFERS, sender_account.total_locked_amount)

    if tr.sender_creditor_id == tr.recipient_creditor_id:
        return reject(SC_RECIPIENT_SAME_AS_SENDER, sender_account.total_locked_amount)

    if sender_account.interest_rate < tr.min_interest_rate:
        return reject(SC_TOO_LOW_INTEREST_RATE, sender_account.total_locked_amount)

    available_amount = _get_available_amount(sender_account, current_ts)
    expendable_amount = available_amount - _get_min_account_balance(tr.sender_creditor_id)
    expendable_amount = min(expendable_amount, tr.max_locked_amount)
    expendable_amount = max(0, expendable_amount)

    # The available amount should be checked last, because if the
    # transfer request is rejected due to insufficient available
    # amount, and the same transfer request is made again, but for
    # small enough amount, we want it to succeed, and not fail for
    # some of the other possible reasons.
    if expendable_amount < tr.min_locked_amount:
        return reject(SC_INSUFFICIENT_AVAILABLE_AMOUNT, sender_account.total_locked_amount)

    return prepare(expendable_amount)


def _finalize_prepared_transfer(
        pt: PreparedTransfer,
        fr: FinalizationRequest,
        sender_account: Account,
        expendable_amount: int,
        current_ts: datetime) -> Optional[PendingBalanceChangeSignal]:

    sender_account.total_locked_amount = max(0, sender_account.total_locked_amount - pt.locked_amount)
    sender_account.pending_transfers_count = max(0, sender_account.pending_transfers_count - 1)
    interest_rate = sender_account.interest_rate
    status_code = pt.calc_status_code(fr.committed_amount, expendable_amount, interest_rate, current_ts)
    committed_amount = fr.committed_amount if status_code == SC_OK else 0

    db.session.add(FinalizedTransferSignal(
        debtor_id=pt.debtor_id,
        sender_creditor_id=pt.sender_creditor_id,
        transfer_id=pt.transfer_id,
        coordinator_type=pt.coordinator_type,
        coordinator_id=pt.coordinator_id,
        coordinator_request_id=pt.coordinator_request_id,
        prepared_at=pt.prepared_at,
        finalized_at=current_ts,
        committed_amount=committed_amount,
        total_locked_amount=sender_account.total_locked_amount,
        status_code=status_code,
    ))

    if committed_amount > 0:
        _insert_account_transfer_signal(
            account=sender_account,
            coordinator_type=pt.coordinator_type,
            other_creditor_id=pt.recipient_creditor_id,
            committed_at=current_ts,
            acquired_amount=-committed_amount,
            transfer_note_format=fr.transfer_note_format,
            transfer_note=fr.transfer_note,
            principal=contain_principal_overflow(sender_account.principal - committed_amount),
        )

        return PendingBalanceChangeSignal(
            debtor_id=pt.debtor_id,
            other_creditor_id=pt.sender_creditor_id,
            creditor_id=pt.recipient_creditor_id,
            committed_at=current_ts,
            coordinator_type=pt.coordinator_type,
            transfer_note_format=fr.transfer_note_format,
            transfer_note=fr.transfer_note,
            principal_delta=committed_amount,
        )

    assert committed_amount == 0
    return None


def _get_min_account_balance(creditor_id: int) -> int:
    return 0 if creditor_id != ROOT_CREDITOR_ID else -MAX_INT64
