import pytest
from datetime import datetime, timezone, timedelta
from swpt_accounts.extensions import db
from swpt_accounts import __version__
from swpt_accounts import procedures as p
from swpt_accounts.models import Account, PendingChange


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


@pytest.fixture(scope='function')
def myaccount(request):
    account()
    p.make_debtor_payment('test', D_ID, C_ID, 10000)
    p.process_pending_changes(D_ID, C_ID)
    return account()


def test_calc_account_current_balance(db_session, myaccount, current_ts):
    calc_cb = db.atomic(p._calc_account_current_balance)
    assert calc_cb(myaccount, current_ts) == 10000
    assert calc_cb(myaccount, current_ts + timedelta(days=50000)) == 10000
    p.set_interest_rate(D_ID, C_ID, 10.0, 666, current_ts)
    assert 10950 < calc_cb(account(), current_ts + timedelta(days=365)) < 11050
