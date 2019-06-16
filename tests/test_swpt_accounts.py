import pytest
from datetime import datetime, timezone, timedelta
from swpt_accounts.extensions import db
from swpt_accounts import __version__
from swpt_accounts import procedures as p
from swpt_accounts.models import MAX_INT64, Account, PendingChange, RejectedTransferSignal, \
    PreparedTransfer, PreparedTransferSignal


def test_version(db_session):
    assert __version__


@pytest.fixture(scope='function')
def current_ts():
    return datetime.now(tz=timezone.utc)


D_ID = -1
C_ID = -1


def account():
    return p.get_or_create_account(D_ID, C_ID)


def test_get_or_create_account(db_session):
    a = account()
    assert isinstance(a, Account)
    assert a.principal == 0
    assert a.interest == 0.0
    assert a.interest_rate == 0.0


def test_set_interest_rate(db_session, current_ts):
    account()

    # The account does not exist.
    p.set_interest_rate(-1234, 1234, 7.0, 666, current_ts)
    assert not Account.query.filter_by(debtor_id=-1234, creditor_id=1234).one_or_none()

    # The account does exist.
    p.set_interest_rate(D_ID, C_ID, 7.0, 666, current_ts)
    assert account().interest_rate == 7.0

    # Older event
    p.set_interest_rate(D_ID, C_ID, 8.0, 665, current_ts)
    assert account().interest_rate == 7.0


AMOUNT = 50000


@pytest.fixture(params=['positive', 'negative'])
def amount(request):
    if request.param == 'positive':
        return AMOUNT
    elif request.param == 'negative':
        return -AMOUNT
    raise Exception()


def test_make_debtor_payment(db_session, amount):
    account()
    p.make_debtor_payment('test', D_ID, C_ID, amount)
    root_change = PendingChange.query.filter_by(debtor_id=D_ID, creditor_id=p.ROOT_CREDITOR_ID).one_or_none()
    assert root_change
    assert root_change.principal_delta == -amount
    assert root_change.interest_delta == 0
    change = PendingChange.query.filter_by(debtor_id=D_ID, creditor_id=C_ID).one_or_none()
    assert change
    assert change.principal_delta == amount
    assert change.interest_delta == 0


def test_process_pending_changes(db_session):
    account()
    p.make_debtor_payment('test', D_ID, C_ID, 10000)
    assert len(p.get_accounts_with_pending_changes()) == 2
    p.process_pending_changes(D_ID, C_ID)
    p.process_pending_changes(D_ID, p.ROOT_CREDITOR_ID)
    assert len(p.get_accounts_with_pending_changes()) == 0
    assert account().principal == 10000
    assert p.get_or_create_account(D_ID, p.ROOT_CREDITOR_ID).principal == -10000


def test_positive_overflow(db_session):
    account()
    p.make_debtor_payment('test', D_ID, C_ID, MAX_INT64)
    p.process_pending_changes(D_ID, C_ID)
    assert not account().status & Account.STATUS_OVERFLOWN_FLAG
    p.make_debtor_payment('test', D_ID, C_ID, 1)
    p.process_pending_changes(D_ID, C_ID)
    assert account().status & Account.STATUS_OVERFLOWN_FLAG


def test_negative_overflow(db_session):
    account()
    p.make_debtor_payment('test', D_ID, C_ID, -MAX_INT64)
    p.process_pending_changes(D_ID, C_ID)
    assert not account().status & Account.STATUS_OVERFLOWN_FLAG
    p.make_debtor_payment('test', D_ID, C_ID, -2)
    p.process_pending_changes(D_ID, C_ID)
    assert account().status & Account.STATUS_OVERFLOWN_FLAG


@pytest.fixture(scope='function')
def myaccount(request, amount):
    account()
    p.make_debtor_payment('test', D_ID, C_ID, amount)
    p.process_pending_changes(D_ID, C_ID)
    return account()


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


def test_prepare_transfer(db_session, myaccount):
    assert account().locked_amount == 0
    assert account().prepared_transfers_count == 0
    p.get_or_create_account(D_ID, 1234)
    amt = myaccount.principal
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
    if amt > 0:
        assert account().locked_amount > 0
        assert account().prepared_transfers_count == 1
        pts = PreparedTransferSignal.query.one_or_none()
        assert pts
        assert pts.debtor_id == D_ID
        assert pts.coordinator_type == 'test'
        assert pts.coordinator_id == 1
        assert pts.coordinator_request_id == 2
        assert pts.sender_creditor_id == C_ID
        assert pts.recipient_creditor_id == 1234
        assert 1 <= pts.amount <= 200
        assert pts.sender_locked_amount == pts.amount
        pt = PreparedTransfer.query.filter_by(
            debtor_id=D_ID,
            sender_creditor_id=C_ID,
            transfer_id=pts.transfer_id,
        ).one_or_none()
        assert pt
        assert pt.coordinator_type == 'test'
        assert pt.recipient_creditor_id == 1234
        assert pt.amount == pts.amount
        assert pt.sender_locked_amount == pts.amount

        # Discard the transfer.
        p.finalize_prepared_transfer(D_ID, C_ID, pt.transfer_id, 0)
        assert not PreparedTransfer.query.one_or_none()
        assert account().locked_amount == 0
        assert account().prepared_transfers_count == 0
    else:
        rts = RejectedTransferSignal.query.one_or_none()
        assert rts
        assert rts.debtor_id == D_ID
        assert rts.coordinator_type == 'test'
        assert rts.coordinator_id == 1
        assert rts.coordinator_request_id == 2
