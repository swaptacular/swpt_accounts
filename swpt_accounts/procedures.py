import datetime
import math
from .extensions import db
from .models import Account, PreparedTransfer, RejectedTransferSignal, PreparedTransferSignal, MAX_INT64, \
    AccountChangeSignal, CommittedTransferSignal, DebtorPolicy

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
    assert min_amount >= 0
    assert max_amount >= 0
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
def execute_prepared_transfer(pt, committed_amount, transfer_info):
    assert committed_amount >= 0
    pt = PreparedTransfer.get_instance(pt, db.joinedload('sender_account', innerjoin=True))
    if pt:
        if committed_amount == 0:
            _delete_prepared_transfer(pt)
        else:
            committed_at_ts = datetime.datetime.now(tz=datetime.timezone.utc)
            _commit_prepared_transfer(pt, committed_amount, committed_at_ts, transfer_info)


@db.atomic
def update_account_interest_rate(account, concession_interest_rate=None):
    account = _get_account(account, db.joinedload('debtor_policy', innerjoin=True))
    current_ts = datetime.datetime.now(tz=datetime.timezone.utc)
    _change_account_balance(account, 0, current_ts)  # triggers interest recalculation
    if concession_interest_rate is not None:
        account.concession_interest_rate = concession_interest_rate
    account.standard_interest_rate = account.debtor_policy.interest_rate
    _insert_account_change_signal(account, current_ts)


@db.atomic
def set_debtor_policy_interest_rate(debtor_policy, interest_rate, change_seqnum):
    debtor_policy = _get_debtor_policy(debtor_policy)
    last_change_seqnum = debtor_policy.last_interest_rate_change_seqnum
    if last_change_seqnum is None or last_change_seqnum < change_seqnum:
        # TODO: implement sign flip?
        debtor_policy.interest_rate = interest_rate
        debtor_policy.last_interest_rate_change_seqnum = change_seqnum
        return Account.query(Account.creditor_id).filter_by(debtor_id=debtor_policy.debtor_id).all()
    return []


def _get_debtor_policy(debtor_policy):
    instance = DebtorPolicy.get_instance(debtor_policy)
    if instance is None:
        debtor_id, = DebtorPolicy.get_pk_values(debtor_policy)
        instance = DebtorPolicy(debtor_id=debtor_id)
        with db.retry_on_integrity_error():
            db.session.add(instance)
    return instance


def _get_account(account):
    instance = Account.get_instance(account)
    if instance is None:
        debtor_id, creditor_id = Account.get_pk_values(account)
        debtor_policy = _get_debtor_policy(debtor_id)
        instance = Account(
            debtor_policy=debtor_policy,
            creditor_id=creditor_id,
            standard_interest_rate=debtor_policy.interest_rate,
        )
        with db.retry_on_integrity_error():
            db.session.add(instance)
    return instance


def _recalc_account_current_principal(account, current_ts):
    passed_seconds = max(0.0, (current_ts - account.last_change_ts).total_seconds())
    interest_rate = max(account.concession_interest_rate, account.standard_interest_rate)
    try:
        k = math.log(1 + interest_rate / 100) / SECONDS_IN_YEAR
    except ValueError:
        k = -math.inf  # the interest rate is -100
    old_principal = max(0, account.balance + account.interest)
    return math.floor(old_principal * math.exp(k * passed_seconds))


def _get_account_current_avl_balance(account, current_ts, ignore_interest=False):
    if ignore_interest:
        return account.balance - account.locked_amount
    return _recalc_account_current_principal(account, current_ts) - account.locked_amount


def _change_account_balance(account, delta, current_ts):
    current_principal = _recalc_account_current_principal(account, current_ts)
    account.interest = current_principal - account.balance
    account.balance += delta
    if delta != 0:
        _insert_account_change_signal(account, current_ts)


def _insert_account_change_signal(account, last_change_ts):
    account.last_change_seqnum += 1
    account.last_change_ts = last_change_ts
    db.session.add(AccountChangeSignal(
        debtor_id=account.debtor_id,
        creditor_id=account.creditor_id,
        change_seqnum=account.last_change_seqnum,
        change_ts=account.last_change_ts,
        balance=account.ballance,
        interest=account.interest,
        concession_interest_rate=account.concession_interest_rate,
        standard_interest_rate=account.standard_interest_rate
    ))


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


def _create_prepared_transfer(account, coordinator_type, recipient_creditor_id, amount, sender_locked_amount):
    account.locked_amount += sender_locked_amount
    pt = PreparedTransfer(
        sender_account=account,
        coordinator_type=coordinator_type,
        recipient_creditor_id=recipient_creditor_id,
        amount=amount,
        sender_locked_amount=sender_locked_amount,
    )
    db.session.add(pt)
    return pt


def _delete_prepared_transfer(pt):
    sender_account = pt.sender_account
    sender_account.locked_amount -= pt.sender_locked_amount
    db.session.delete(pt)


def _commit_prepared_transfer(pt, committed_amount, committed_at_ts, transfer_info):
    sender_account = pt.sender_account
    recipient_account = _get_account((pt.debtor_id, pt.recipient_creditor_id))
    _change_account_balance(sender_account, -committed_amount, committed_at_ts)
    _change_account_balance(recipient_account, committed_amount, committed_at_ts)
    if pt.coordinator_type != 'interest':
        sender_account.last_activity_ts = committed_at_ts
        recipient_account.last_activity_ts = committed_at_ts
    _insert_committed_transfer_signal(pt, committed_amount, committed_at_ts, transfer_info)
    _delete_prepared_transfer(pt)
