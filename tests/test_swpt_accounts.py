import pytest
from datetime import datetime, timezone, timedelta
from swpt_accounts.extensions import db
from swpt_accounts import __version__
from swpt_accounts import procedures as p
from swpt_accounts.models import MAX_INT64, Account, PendingChange, RejectedTransferSignal, \
    PreparedTransfer, PreparedTransferSignal, AccountChangeSignal, CommittedTransferSignal


def test_version(db_session):
    assert __version__


@pytest.fixture(scope='function')
def current_ts():
    return datetime.now(tz=timezone.utc)


D_ID = -1
C_ID = 1


def account():
    return p.get_or_create_account(D_ID, C_ID)


def test_get_or_create_account(db_session):
    a = p.get_or_create_account(D_ID, C_ID)
    assert isinstance(a, Account)
    assert a.status == 0
    assert a.principal == 0
    assert a.interest == 0.0
    assert a.locked_amount == 0
    assert a.prepared_transfers_count == 0
    assert a.interest_rate == 0.0
    assert a.interest_rate_last_change_seqnum is None
    assert a.interest_rate_last_change_ts is None
    assert a.last_outgoing_transfer_date is None
    acs = AccountChangeSignal.query.filter_by(debtor_id=D_ID, creditor_id=C_ID).one()
    assert acs.last_outgoing_transfer_date is None
    assert acs.status == a.status
    assert acs.principal == a.principal
    assert acs.interest == a.interest
    assert acs.interest_rate == a.interest_rate


def test_set_interest_rate(db_session, current_ts):
    # The account does not exist.
    p.set_interest_rate(D_ID, C_ID, 7.0, 665, current_ts)
    assert p.get_account(D_ID, C_ID) is None
    assert len(AccountChangeSignal.query.all()) == 0

    # The account does exist.
    p.get_or_create_account(D_ID, C_ID)
    p.set_interest_rate(D_ID, C_ID, 7.0, 666, current_ts)
    a = p.get_account(D_ID, C_ID)
    assert a.interest_rate == 7.0
    assert a.status & Account.STATUS_ESTABLISHED_INTEREST_RATE_FLAG
    assert len(AccountChangeSignal.query.all()) == 2

    # Older event
    p.set_interest_rate(D_ID, C_ID, 8.0, 665, current_ts)
    assert p.get_account(D_ID, C_ID).interest_rate == 7.0
    assert len(AccountChangeSignal.query.all()) == 2


def test_set_interest_rate_on_self(db_session, current_ts):
    p.get_or_create_account(D_ID, p.ROOT_CREDITOR_ID)
    p.set_interest_rate(D_ID, p.ROOT_CREDITOR_ID, 7.0, 666, current_ts)
    a = p.get_account(D_ID, p.ROOT_CREDITOR_ID)
    assert a.interest_rate == 0.0
    assert a.status & Account.STATUS_ESTABLISHED_INTEREST_RATE_FLAG
    assert len(AccountChangeSignal.query.all()) == 2


AMOUNT = 50000


@pytest.fixture(params=['positive', 'negative'])
def amount(request):
    if request.param == 'positive':
        return AMOUNT
    elif request.param == 'negative':
        return -AMOUNT
    raise Exception()


def test_make_debtor_payment(db_session, amount):
    p.get_or_create_account(D_ID, C_ID)
    p.make_debtor_payment('test', D_ID, C_ID, amount)
    cts = CommittedTransferSignal.query.filter_by(debtor_id=D_ID).one()
    assert cts.coordinator_type == 'test'
    assert cts.sender_creditor_id == p.ROOT_CREDITOR_ID if amount > 0 else C_ID
    assert cts.recipient_creditor_id == C_ID if amount > 0 else p.ROOT_CREDITOR_ID
    assert cts.committed_amount == abs(amount)
    assert cts.committed_transfer_id is None
    root_change = PendingChange.query.filter_by(debtor_id=D_ID, creditor_id=p.ROOT_CREDITOR_ID).one()
    assert root_change.principal_delta == -amount
    assert root_change.interest_delta == 0
    change = PendingChange.query.filter_by(debtor_id=D_ID, creditor_id=C_ID).one()
    assert change.principal_delta == amount
    assert change.interest_delta == 0


def test_make_debtor_interest_payment(db_session, amount):
    p.get_or_create_account(D_ID, C_ID)
    p.make_debtor_payment('interest', D_ID, C_ID, amount)
    root_change = PendingChange.query.filter_by(debtor_id=D_ID, creditor_id=p.ROOT_CREDITOR_ID).one()
    assert root_change.principal_delta == -amount
    assert root_change.interest_delta == 0
    change = PendingChange.query.filter_by(debtor_id=D_ID, creditor_id=C_ID).one()
    assert change.principal_delta == amount
    assert change.interest_delta == -amount


def test_process_pending_changes(db_session):
    p.get_or_create_account(D_ID, C_ID)
    assert len(p.get_accounts_with_pending_changes()) == 0
    p.make_debtor_payment('test', D_ID, C_ID, 10000)
    assert len(p.get_accounts_with_pending_changes()) == 2
    p.process_pending_changes(D_ID, C_ID)
    p.process_pending_changes(D_ID, p.ROOT_CREDITOR_ID)
    assert AccountChangeSignal.query.filter_by(
        debtor_id=D_ID,
        creditor_id=C_ID,
        principal=10000,
    ).one_or_none()
    assert AccountChangeSignal.query.filter_by(
        debtor_id=D_ID,
        creditor_id=p.ROOT_CREDITOR_ID,
        principal=-10000,
    ).one_or_none()
    assert len(p.get_accounts_with_pending_changes()) == 0
    assert p.get_account(D_ID, C_ID).principal == 10000
    assert p.get_account(D_ID, p.ROOT_CREDITOR_ID).principal == -10000


def test_positive_overflow(db_session):
    p.get_or_create_account(D_ID, C_ID)

    p.make_debtor_payment('test', D_ID, C_ID, MAX_INT64)
    p.process_pending_changes(D_ID, C_ID)
    assert not p.get_account(D_ID, C_ID).status & Account.STATUS_OVERFLOWN_FLAG

    p.make_debtor_payment('test', D_ID, C_ID, 1)
    p.process_pending_changes(D_ID, C_ID)
    assert p.get_account(D_ID, C_ID).status & Account.STATUS_OVERFLOWN_FLAG


def test_negative_overflow(db_session):
    p.get_or_create_account(D_ID, C_ID)

    p.make_debtor_payment('test', D_ID, C_ID, -MAX_INT64)
    p.process_pending_changes(D_ID, C_ID)
    assert not p.get_account(D_ID, C_ID).status & Account.STATUS_OVERFLOWN_FLAG

    p.make_debtor_payment('test', D_ID, C_ID, -2)
    p.process_pending_changes(D_ID, C_ID)
    assert p.get_account(D_ID, C_ID).status & Account.STATUS_OVERFLOWN_FLAG


@pytest.fixture(scope='function')
def myaccount(request, amount):
    p.get_or_create_account(D_ID, C_ID)
    p.make_debtor_payment('test', D_ID, C_ID, amount)
    p.process_pending_changes(D_ID, C_ID)
    return p.get_account(D_ID, C_ID)


# TODO
def test_capitalize_interest(db_session, myaccount, current_ts):
    amt = myaccount.principal
    calc_cb = db.atomic(p._calc_account_current_balance)
    assert calc_cb(myaccount, current_ts) == amt
    assert calc_cb(myaccount, current_ts + timedelta(days=50000)) == amt
    p.set_interest_rate(D_ID, C_ID, 10.0, 666, current_ts)
    new_amt = calc_cb(account(), current_ts + timedelta(days=365))
    if amt > 0:
        assert 1.09 * amt < new_amt < 1.11 * amt
    else:
        assert new_amt == amt
    p.capitalize_interest(D_ID, C_ID, 0, current_ts + timedelta(days=365))
    p.process_pending_changes(D_ID, C_ID)
    assert abs(account().principal - new_amt) <= p.TINY_POSITIVE_AMOUNT


def test_discard_interest_on_self(db_session, current_ts):
    p.get_or_create_account(D_ID, p.ROOT_CREDITOR_ID)
    q = Account.query.filter_by(debtor_id=D_ID, creditor_id=p.ROOT_CREDITOR_ID)
    q.update({Account.interest: 100.0, Account.principal: 50})
    p.capitalize_interest(D_ID, p.ROOT_CREDITOR_ID, 0, current_ts)
    p.process_pending_changes(D_ID, p.ROOT_CREDITOR_ID)
    a = p.get_account(D_ID, p.ROOT_CREDITOR_ID)
    assert a.principal == 50
    assert a.interest == 0.0


def test_delete_account(db_session, current_ts):
    assert p.get_account(D_ID, C_ID) is None
    p.get_or_create_account(D_ID, C_ID)
    assert p.get_account(D_ID, C_ID)
    p.delete_account_if_zeroed(D_ID, C_ID)
    assert p.get_account(D_ID, C_ID) is None
    assert AccountChangeSignal.query.\
        filter(AccountChangeSignal.debtor_id == D_ID).\
        filter(AccountChangeSignal.creditor_id == C_ID).\
        filter(AccountChangeSignal.status.op('&')(Account.STATUS_DELETED_FLAG) == Account.STATUS_DELETED_FLAG).\
        one_or_none()
    q = Account.query.filter_by(debtor_id=D_ID, creditor_id=C_ID)
    assert q.one().status & Account.STATUS_DELETED_FLAG
    p.purge_deleted_account(D_ID, C_ID, current_ts - timedelta(days=1000))
    assert q.one().status & Account.STATUS_DELETED_FLAG
    p.purge_deleted_account(D_ID, C_ID, current_ts + timedelta(days=1000))
    assert not q.one_or_none()


def test_delete_account_negative_balance(db_session):
    p.get_or_create_account(D_ID, C_ID)
    q = Account.query.filter_by(debtor_id=D_ID, creditor_id=C_ID)
    q.update({Account.principal: -1})
    p.delete_account_if_zeroed(D_ID, C_ID)
    assert p.get_account(D_ID, C_ID)


def test_delete_account_tiny_positive_balance(db_session, current_ts):
    p.get_or_create_account(D_ID, C_ID)
    q = Account.query.filter_by(debtor_id=D_ID, creditor_id=C_ID)
    q.update({Account.principal: 1})
    p.delete_account_if_zeroed(D_ID, C_ID)
    assert p.get_account(D_ID, C_ID) is None
    p.process_pending_changes(D_ID, p.ROOT_CREDITOR_ID)
    assert p.get_account(D_ID, p.ROOT_CREDITOR_ID).principal == 1


def test_resurect_deleted_account(db_session, current_ts):
    p.get_or_create_account(D_ID, C_ID)
    q = Account.query.filter_by(debtor_id=D_ID, creditor_id=C_ID)
    q.update({Account.interest_rate: 10.0})
    p.delete_account_if_zeroed(D_ID, C_ID)
    assert p.get_or_create_account(D_ID, C_ID).interest_rate == 0.0


def test_prepare_transfer_fail(db_session):
    assert 1234 != D_ID
    p.get_or_create_account(D_ID, 1234)
    p.get_or_create_account(D_ID, C_ID)
    assert len(AccountChangeSignal.query.all()) == 2
    p.prepare_transfer(
        coordinator_type='test',
        coordinator_id=1,
        coordinator_request_id=2,
        min_amount=1,
        max_amount=200,
        debtor_id=D_ID,
        sender_creditor_id=C_ID,
        recipient_creditor_id=1234,
        ignore_interest=False,
    )
    a = p.get_account(D_ID, C_ID)
    assert a.locked_amount == 0
    assert a.prepared_transfers_count == 0
    p.process_pending_changes(D_ID, 1234)
    p.process_pending_changes(D_ID, C_ID)
    assert len(AccountChangeSignal.query.all()) == 2
    assert len(PreparedTransfer.query.all()) == 0
    assert len(PreparedTransferSignal.query.all()) == 0
    assert len(CommittedTransferSignal.query.all()) == 0
    rts = RejectedTransferSignal.query.one()
    assert rts.debtor_id == D_ID
    assert rts.coordinator_type == 'test'
    assert rts.coordinator_id == 1
    assert rts.coordinator_request_id == 2


def test_prepare_transfer_success(db_session):
    assert 1234 != D_ID
    p.get_or_create_account(D_ID, 1234)
    p.get_or_create_account(D_ID, C_ID)
    assert len(AccountChangeSignal.query.all()) == 2
    q = Account.query.filter_by(debtor_id=D_ID, creditor_id=C_ID)
    q.update({Account.principal: 100})
    p.prepare_transfer(
        coordinator_type='test',
        coordinator_id=1,
        coordinator_request_id=2,
        min_amount=1,
        max_amount=200,
        debtor_id=D_ID,
        sender_creditor_id=C_ID,
        recipient_creditor_id=1234,
        ignore_interest=False,
    )
    a = p.get_account(D_ID, C_ID)
    assert a.locked_amount == 100
    assert a.prepared_transfers_count == 1
    p.process_pending_changes(D_ID, 1234)
    p.process_pending_changes(D_ID, C_ID)
    assert len(AccountChangeSignal.query.all()) == 2
    assert len(RejectedTransferSignal.query.all()) == 0
    pts = PreparedTransferSignal.query.one()
    assert pts.debtor_id == D_ID
    assert pts.coordinator_type == 'test'
    assert pts.coordinator_id == 1
    assert pts.coordinator_request_id == 2
    assert pts.sender_creditor_id == C_ID
    assert pts.recipient_creditor_id == 1234
    assert pts.amount == 100
    assert pts.sender_locked_amount == pts.amount
    pt = PreparedTransfer.query.filter_by(debtor_id=D_ID, sender_creditor_id=C_ID).one()
    assert pt.transfer_id == pts.transfer_id
    assert pt.coordinator_type == 'test'
    assert pt.recipient_creditor_id == 1234
    assert pt.amount == pts.amount
    assert pt.sender_locked_amount == pts.amount

    # Discard the transfer.
    p.finalize_prepared_transfer(D_ID, C_ID, pt.transfer_id, 0)
    p.process_pending_changes(D_ID, 1234)
    p.process_pending_changes(D_ID, C_ID)
    a = p.get_account(D_ID, C_ID)
    assert a.locked_amount == 0
    assert a.prepared_transfers_count == 0
    assert a.principal == 100
    assert a.interest == 0.0
    assert not PreparedTransfer.query.one_or_none()
    assert len(AccountChangeSignal.query.all()) == 2
    assert len(RejectedTransferSignal.query.all()) == 0
    assert len(CommittedTransferSignal.query.all()) == 0


def test_commit_prepared_transfer(db_session):
    p.get_or_create_account(D_ID, 1234)
    p.get_or_create_account(D_ID, C_ID)
    q = Account.query.filter_by(debtor_id=D_ID, creditor_id=C_ID)
    q.update({Account.principal: 100})
    p.prepare_transfer(
        coordinator_type='test',
        coordinator_id=1,
        coordinator_request_id=2,
        min_amount=1,
        max_amount=200,
        debtor_id=D_ID,
        sender_creditor_id=C_ID,
        recipient_creditor_id=1234,
        ignore_interest=False,
    )
    pt = PreparedTransfer.query.filter_by(debtor_id=D_ID, sender_creditor_id=C_ID).one()
    p.finalize_prepared_transfer(D_ID, C_ID, pt.transfer_id, 40)
    p.process_pending_changes(D_ID, 1234)
    p.process_pending_changes(D_ID, C_ID)
    a1 = p.get_account(D_ID, 1234)
    assert a1.locked_amount == 0
    assert a1.prepared_transfers_count == 0
    assert a1.principal == 40
    assert a1.interest == 0.0
    a2 = p.get_account(D_ID, C_ID)
    assert a2.locked_amount == 0
    assert a2.prepared_transfers_count == 0
    assert a2.principal == 60
    assert a2.interest == 0.0
    assert not PreparedTransfer.query.one_or_none()
    assert len(AccountChangeSignal.query.all()) == 4
    assert len(RejectedTransferSignal.query.all()) == 0
    cts = CommittedTransferSignal.query.filter_by(debtor_id=D_ID).one()
    assert cts.coordinator_type == 'test'
    assert cts.sender_creditor_id == C_ID
    assert cts.recipient_creditor_id == 1234
    assert cts.committed_amount == 40
    assert cts.committed_transfer_id == pt.transfer_id
