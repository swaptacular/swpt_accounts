import pytest
from datetime import datetime, timezone, timedelta
from swpt_lib.utils import date_to_int24
from swpt_accounts import __version__
from swpt_accounts import procedures as p
from swpt_accounts.models import MAX_INT32, MAX_INT64, INTEREST_RATE_FLOOR, INTEREST_RATE_CEIL, \
    Account, PendingAccountChange, RejectedTransferSignal, PreparedTransfer, PreparedTransferSignal, \
    AccountChangeSignal, AccountPurgeSignal, CommittedTransferSignal


def test_version(db_session):
    assert __version__


@pytest.fixture(scope='function')
def current_ts():
    return datetime.now(tz=timezone.utc)


D_ID = -1
C_ID = 1


def test_configure_account(db_session, current_ts):
    p.configure_account(D_ID, C_ID, current_ts, 0)
    a = p.get_account(D_ID, C_ID)
    assert isinstance(a, Account)
    assert a.status == 0
    assert a.principal == 0
    assert a.interest == 0.0
    assert a.locked_amount == 0
    assert a.pending_transfers_count == 0
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
    a = p.configure_account(D_ID, C_ID, current_ts, 0)
    assert len(AccountChangeSignal.query.filter_by(debtor_id=D_ID, creditor_id=C_ID).all()) == 1
    a = p.configure_account(D_ID, C_ID, current_ts, 1)
    assert len(AccountChangeSignal.query.filter_by(debtor_id=D_ID, creditor_id=C_ID).all()) == 2


def test_set_interest_rate(db_session, current_ts):
    # The account does not exist.
    p.change_interest_rate(D_ID, C_ID, 665, current_ts, 7.0)
    assert p.get_account(D_ID, C_ID) is None
    assert len(AccountChangeSignal.query.all()) == 0

    # The account does exist.
    p.configure_account(D_ID, C_ID, current_ts, 0)
    p.change_interest_rate(D_ID, C_ID, 666, current_ts, 7.0)
    a = p.get_account(D_ID, C_ID)
    assert a.interest_rate == 7.0
    assert a.status & Account.STATUS_ESTABLISHED_INTEREST_RATE_FLAG
    assert len(AccountChangeSignal.query.all()) == 2

    # Older event
    p.change_interest_rate(D_ID, C_ID, 665, current_ts, 8.0)
    assert p.get_account(D_ID, C_ID).interest_rate == 7.0
    assert len(AccountChangeSignal.query.all()) == 2

    # Too big positive interest rate.
    p.change_interest_rate(D_ID, C_ID, 667, current_ts, 1e9)
    assert p.get_account(D_ID, C_ID).interest_rate == INTEREST_RATE_CEIL

    # Too big negative interest rate.
    p.change_interest_rate(D_ID, C_ID, 668, current_ts, -99.9999999999)
    assert p.get_account(D_ID, C_ID).interest_rate == INTEREST_RATE_FLOOR


AMOUNT = 50000


@pytest.fixture(params=['positive', 'negative'])
def amount(request):
    if request.param == 'positive':
        return AMOUNT
    elif request.param == 'negative':
        return -AMOUNT
    raise Exception()


def test_make_debtor_payment(db_session, current_ts, amount):
    TRANSFER_INFO = {'transer_data': 123}
    p.configure_account(D_ID, C_ID, current_ts, 0)
    p.make_debtor_payment('test', D_ID, C_ID, amount, TRANSFER_INFO)

    root_change = PendingAccountChange.query.filter_by(debtor_id=D_ID, creditor_id=p.ROOT_CREDITOR_ID).one()
    assert root_change.principal_delta == -amount
    assert root_change.interest_delta == 0
    change = PendingAccountChange.query.filter_by(debtor_id=D_ID, creditor_id=C_ID).one()
    assert change.principal_delta == amount
    assert change.interest_delta == 0

    p.process_pending_account_changes(D_ID, C_ID)
    p.process_pending_account_changes(D_ID, p.ROOT_CREDITOR_ID)
    assert len(CommittedTransferSignal.query.filter_by(debtor_id=D_ID).all()) == 1
    cts1 = CommittedTransferSignal.query.filter_by(debtor_id=D_ID, creditor_id=C_ID).one()
    transfer_seqnum1 = (date_to_int24(cts1.transfer_epoch) << 40) + 1
    assert cts1.coordinator_type == 'test'
    assert cts1.creditor_id == C_ID
    assert cts1.other_creditor_id == p.ROOT_CREDITOR_ID
    assert cts1.committed_amount == amount
    assert cts1.transfer_info == TRANSFER_INFO
    assert cts1.transfer_seqnum == transfer_seqnum1
    assert cts1.new_account_principal == amount
    assert len(CommittedTransferSignal.query.filter_by(debtor_id=D_ID, creditor_id=p.ROOT_CREDITOR_ID).all()) == 0

    p.make_debtor_payment('test', D_ID, C_ID, 2 * amount, TRANSFER_INFO)
    p.process_pending_account_changes(D_ID, C_ID)
    cts = CommittedTransferSignal.query.filter_by(
        debtor_id=D_ID, creditor_id=C_ID, transfer_seqnum=transfer_seqnum1 + 1).one()
    assert cts.committed_amount == 2 * amount
    assert cts.new_account_principal == 3 * amount
    assert p.get_account(D_ID, C_ID).last_outgoing_transfer_date is None


def test_make_debtor_zero_payment(db_session, current_ts):
    p.configure_account(D_ID, C_ID, current_ts, 0)
    p.make_debtor_payment('interest', D_ID, C_ID, 0)
    assert not PendingAccountChange.query.all()
    p.process_pending_account_changes(D_ID, C_ID)
    p.process_pending_account_changes(D_ID, p.ROOT_CREDITOR_ID)
    assert not CommittedTransferSignal.query.all()


def test_make_debtor_creditor_account_deletion(db_session, current_ts, amount):
    p.configure_account(D_ID, C_ID, current_ts, 0)
    p.make_debtor_payment('delete_account', D_ID, C_ID, amount)
    changes = PendingAccountChange.query.all()
    assert len(changes) == 1
    root_change = changes[0]
    assert root_change.creditor_id == p.ROOT_CREDITOR_ID
    assert root_change.principal_delta == -amount
    assert root_change.interest_delta == 0
    p.process_pending_account_changes(D_ID, C_ID)
    p.process_pending_account_changes(D_ID, p.ROOT_CREDITOR_ID)
    assert len(CommittedTransferSignal.query.filter_by(debtor_id=D_ID).all()) == 0


def test_make_debtor_interest_payment(db_session, current_ts, amount):
    p.configure_account(D_ID, C_ID, current_ts, 0)
    p.make_debtor_payment('interest', D_ID, C_ID, amount)
    root_change = PendingAccountChange.query.filter_by(debtor_id=D_ID, creditor_id=p.ROOT_CREDITOR_ID).one()
    assert root_change.principal_delta == -amount
    assert root_change.interest_delta == 0
    change = PendingAccountChange.query.filter_by(debtor_id=D_ID, creditor_id=C_ID).one()
    assert change.principal_delta == amount
    assert change.interest_delta == -amount


def test_process_pending_account_changes(db_session, current_ts):
    p.configure_account(D_ID, C_ID, current_ts, 0)
    assert len(p.get_accounts_with_pending_changes()) == 0
    p.make_debtor_payment('test', D_ID, C_ID, 10000)
    assert len(p.get_accounts_with_pending_changes()) == 2
    p.process_pending_account_changes(D_ID, C_ID)
    p.process_pending_account_changes(D_ID, p.ROOT_CREDITOR_ID)
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


def test_positive_overflow(db_session, current_ts):
    p.configure_account(D_ID, C_ID, current_ts, 0)

    p.make_debtor_payment('test', D_ID, C_ID, MAX_INT64)
    p.process_pending_account_changes(D_ID, C_ID)
    assert not p.get_account(D_ID, C_ID).status & Account.STATUS_OVERFLOWN_FLAG

    p.make_debtor_payment('test', D_ID, C_ID, 1)
    p.process_pending_account_changes(D_ID, C_ID)
    assert p.get_account(D_ID, C_ID).status & Account.STATUS_OVERFLOWN_FLAG


def test_negative_overflow(db_session, current_ts):
    p.configure_account(D_ID, C_ID, current_ts, 0)

    p.make_debtor_payment('test', D_ID, C_ID, -MAX_INT64)
    p.process_pending_account_changes(D_ID, C_ID)
    assert not p.get_account(D_ID, C_ID).status & Account.STATUS_OVERFLOWN_FLAG

    p.make_debtor_payment('test', D_ID, C_ID, -2)
    p.process_pending_account_changes(D_ID, C_ID)
    assert p.get_account(D_ID, C_ID).status & Account.STATUS_OVERFLOWN_FLAG


def test_get_available_balance(db_session, current_ts):
    q = Account.query.filter_by(debtor_id=D_ID, creditor_id=C_ID)
    q_root = Account.query.filter_by(debtor_id=D_ID, creditor_id=p.ROOT_CREDITOR_ID)

    assert p.get_available_balance(D_ID, p.ROOT_CREDITOR_ID) == 0
    assert p.get_available_balance(D_ID, p.ROOT_CREDITOR_ID, -1000) == 1000
    p.configure_account(D_ID, p.ROOT_CREDITOR_ID, current_ts, 0)
    assert p.get_available_balance(D_ID, p.ROOT_CREDITOR_ID) == 0
    assert p.get_available_balance(D_ID, p.ROOT_CREDITOR_ID, -1000) == 1000
    q_root.update({
        Account.interest: 100.0,
        Account.principal: 500,
    })
    assert p.get_available_balance(D_ID, p.ROOT_CREDITOR_ID) == 500
    assert p.get_available_balance(D_ID, p.ROOT_CREDITOR_ID, -1000) == 1500

    assert p.get_available_balance(D_ID, C_ID, -1000) == 0
    assert p.get_available_balance(D_ID, C_ID) == 0
    p.configure_account(D_ID, C_ID, current_ts, 0)
    assert p.get_available_balance(D_ID, C_ID) == 0
    q.update({
        Account.interest: 100.0,
        Account.principal: 5000,
    })
    assert p.get_available_balance(D_ID, C_ID) == 5100
    q.update({
        Account.locked_amount: 1000,
    })
    assert p.get_available_balance(D_ID, C_ID) == 4100
    q.update({
        Account.interest_rate: 10.00,
        Account.last_change_ts: current_ts - timedelta(days=365),
        Account.last_change_seqnum: 666,
    })
    assert 4608 <= p.get_available_balance(D_ID, C_ID) <= 4610
    q.update({
        Account.interest_rate: -10.00,
        Account.last_change_ts: current_ts - timedelta(days=365),
        Account.last_change_seqnum: 666,
    })
    assert 3590 <= p.get_available_balance(D_ID, C_ID) <= 3592
    q.update({
        Account.interest: -5100.0,
    })
    assert p.get_available_balance(D_ID, C_ID) == -1100


def test_capitalize_positive_interest(db_session, current_ts):
    q = Account.query.filter_by(debtor_id=D_ID, creditor_id=C_ID)

    p.configure_account(D_ID, C_ID, current_ts, 0)
    q.update({
        Account.interest: 100.0,
        Account.principal: 5000,
        Account.interest_rate: 10.00,
        Account.last_change_ts: current_ts - timedelta(days=365),
        Account.last_change_seqnum: 666,
    })
    p.capitalize_interest(D_ID, C_ID, 10000000)
    p.process_pending_account_changes(D_ID, C_ID)
    assert p.get_account(D_ID, C_ID).interest == 100.0
    p.capitalize_interest(D_ID, C_ID, 0)
    p.process_pending_account_changes(D_ID, C_ID)
    a = p.get_account(D_ID, C_ID)
    assert abs(a.interest) <= 1.0
    assert 5608 <= a.principal <= 5612


def test_capitalize_negative_interest(db_session, current_ts):
    q = Account.query.filter_by(debtor_id=D_ID, creditor_id=C_ID)

    p.configure_account(D_ID, C_ID, current_ts, 0)
    q.update({
        Account.interest: -100.0,
        Account.principal: 5000,
        Account.interest_rate: -10.00,
        Account.last_change_ts: current_ts - timedelta(days=365),
        Account.last_change_seqnum: 666,
    })
    p.capitalize_interest(D_ID, C_ID, 10000000)
    p.process_pending_account_changes(D_ID, C_ID)
    assert p.get_account(D_ID, C_ID).interest == -100.0
    p.capitalize_interest(D_ID, C_ID, 0)
    p.process_pending_account_changes(D_ID, C_ID)
    a = p.get_account(D_ID, C_ID)
    assert abs(a.interest) <= 1.0
    assert 4408 <= a.principal <= 4412


def test_debtor_account_capitalization(db_session, current_ts):
    p.configure_account(D_ID, p.ROOT_CREDITOR_ID, current_ts, 0)
    q = Account.query.filter_by(debtor_id=D_ID, creditor_id=p.ROOT_CREDITOR_ID)
    q.update({Account.interest: 100.0, Account.principal: 50})
    p.capitalize_interest(D_ID, p.ROOT_CREDITOR_ID, 0, current_ts)
    p.process_pending_account_changes(D_ID, p.ROOT_CREDITOR_ID)
    a = p.get_account(D_ID, p.ROOT_CREDITOR_ID)
    assert a.principal == 50
    assert a.interest == 100.0


def test_delete_account(db_session, current_ts):
    assert p.get_account(D_ID, C_ID) is None
    p.configure_account(D_ID, C_ID, current_ts, 0)
    a = p.get_account(D_ID, C_ID)
    creation_date = a.creation_date
    assert a is not None
    assert not a.status & Account.STATUS_DELETED_FLAG
    assert not a.status & Account.STATUS_SCHEDULED_FOR_DELETION_FLAG
    p.configure_account(D_ID, C_ID, current_ts, 1, is_scheduled_for_deletion=True)
    p.try_to_delete_account(D_ID, C_ID)
    assert p.get_account(D_ID, C_ID) is None
    q = Account.query.filter_by(debtor_id=D_ID, creditor_id=C_ID)
    assert q.one().status & Account.STATUS_DELETED_FLAG
    assert q.one().status & Account.STATUS_SCHEDULED_FOR_DELETION_FLAG
    assert AccountChangeSignal.query.\
        filter(AccountChangeSignal.debtor_id == D_ID).\
        filter(AccountChangeSignal.creditor_id == C_ID).\
        filter(AccountChangeSignal.status.op('&')(Account.STATUS_DELETED_FLAG) == Account.STATUS_DELETED_FLAG).\
        one_or_none()
    p.purge_deleted_account(D_ID, C_ID, current_ts - timedelta(days=1000), allow_hasty_purges=True)
    assert q.one().status & Account.STATUS_DELETED_FLAG
    assert q.one().status & Account.STATUS_SCHEDULED_FOR_DELETION_FLAG
    p.purge_deleted_account(D_ID, C_ID, current_ts + timedelta(days=1000), allow_hasty_purges=False)
    assert q.one().status & Account.STATUS_DELETED_FLAG
    assert q.one().status & Account.STATUS_SCHEDULED_FOR_DELETION_FLAG
    p.purge_deleted_account(D_ID, C_ID, current_ts + timedelta(days=1000), allow_hasty_purges=True)
    assert not q.one_or_none()
    aps = AccountPurgeSignal.query.one()
    assert aps.debtor_id == D_ID
    assert aps.creditor_id == C_ID
    assert aps.creation_date == creation_date


def test_delete_account_negative_balance(db_session, current_ts):
    p.configure_account(D_ID, C_ID, current_ts, 0)
    q = Account.query.filter_by(debtor_id=D_ID, creditor_id=C_ID)
    q.update({Account.principal: -1})
    p.configure_account(D_ID, C_ID, current_ts, 1, is_scheduled_for_deletion=True, negligible_amount=MAX_INT64)
    p.try_to_delete_account(D_ID, C_ID)
    a = p.get_account(D_ID, C_ID)
    assert a is not None
    assert not a.status & Account.STATUS_DELETED_FLAG
    assert a.status & Account.STATUS_SCHEDULED_FOR_DELETION_FLAG

    # Verify that incoming transfers are not allowed:
    p.configure_account(D_ID, 1234, current_ts, 0)
    q = Account.query.filter_by(debtor_id=D_ID, creditor_id=C_ID)
    q.update({Account.principal: 200})
    p.prepare_transfer(
        coordinator_type='test',
        coordinator_id=1,
        coordinator_request_id=2,
        min_amount=1,
        max_amount=200,
        debtor_id=D_ID,
        sender_creditor_id=1234,
        recipient_creditor_id=C_ID,
    )
    p.process_transfer_requests(D_ID, 1234)
    rts = RejectedTransferSignal.query.one()
    assert rts.debtor_id == D_ID
    assert rts.coordinator_type == 'test'
    assert rts.coordinator_id == 1
    assert rts.coordinator_request_id == 2
    assert rts.details['error_code'] == 'ACC004'

    # Verify that re-creating the account clears STATUS_SCHEDULED_FOR_DELETION_FLAG:
    p.configure_account(D_ID, C_ID, current_ts + timedelta(days=1000), 0)
    assert not q.one().status & Account.STATUS_DELETED_FLAG
    assert not q.one().status & Account.STATUS_SCHEDULED_FOR_DELETION_FLAG


def test_delete_account_tiny_positive_balance(db_session, current_ts):
    p.configure_account(D_ID, C_ID, current_ts, 0)
    q = Account.query.filter_by(debtor_id=D_ID, creditor_id=C_ID)
    q.update({Account.principal: 2, Account.interest: -1.0})
    p.configure_account(D_ID, C_ID, current_ts, 1, is_scheduled_for_deletion=True, negligible_amount=2.0)
    p.try_to_delete_account(D_ID, C_ID)
    assert p.get_account(D_ID, C_ID) is None
    a = q.one()
    assert a.status & Account.STATUS_SCHEDULED_FOR_DELETION_FLAG
    assert a.status & Account.STATUS_DELETED_FLAG
    assert a.principal == 0
    assert a.interest == 0
    changes = PendingAccountChange.query.all()
    assert len(changes) == 1
    assert changes[0].creditor_id == p.ROOT_CREDITOR_ID
    p.process_pending_account_changes(D_ID, C_ID)
    p.process_pending_account_changes(D_ID, p.ROOT_CREDITOR_ID)

    assert len(CommittedTransferSignal.query.all()) == 1
    cts1 = CommittedTransferSignal.query.filter_by(creditor_id=C_ID).one()
    assert cts1.committed_amount == -2
    assert cts1.new_account_principal == 0
    a = q.one()
    assert a.status & Account.STATUS_SCHEDULED_FOR_DELETION_FLAG
    assert a.status & Account.STATUS_DELETED_FLAG
    assert a.principal == 0
    assert a.interest == 0


def test_delete_debtor_account(db_session, current_ts):
    future_ts = current_ts + timedelta(days=1000)
    q = Account.query.filter_by(debtor_id=D_ID, creditor_id=p.ROOT_CREDITOR_ID)
    p.configure_account(D_ID, p.ROOT_CREDITOR_ID, current_ts, 0)
    p.configure_account(D_ID, C_ID, current_ts, 0)

    # There is another existing account.
    p.try_to_delete_account(D_ID, p.ROOT_CREDITOR_ID)
    a = p.get_account(D_ID, p.ROOT_CREDITOR_ID)
    assert not a.status & Account.STATUS_DELETED_FLAG
    assert not a.status & Account.STATUS_SCHEDULED_FOR_DELETION_FLAG

    # Delete the other account.
    p.configure_account(D_ID, C_ID, current_ts, 1, is_scheduled_for_deletion=True)
    p.try_to_delete_account(D_ID, C_ID)
    assert p.get_account(D_ID, C_ID) is None
    p.purge_deleted_account(D_ID, C_ID, if_deleted_before=future_ts, allow_hasty_purges=True)

    # There are no other accounts.
    p.try_to_delete_account(D_ID, p.ROOT_CREDITOR_ID)
    assert q.one().status & Account.STATUS_DELETED_FLAG
    assert not q.one().status & Account.STATUS_SCHEDULED_FOR_DELETION_FLAG
    p.purge_deleted_account(D_ID, p.ROOT_CREDITOR_ID, if_deleted_before=future_ts, allow_hasty_purges=True)
    assert q.one_or_none() is None


def test_resurect_deleted_account_create(db_session, current_ts):
    p.configure_account(D_ID, C_ID, current_ts, 0)
    q = Account.query.filter_by(debtor_id=D_ID, creditor_id=C_ID)
    q.update({Account.interest_rate: 10.0})
    p.configure_account(D_ID, C_ID, current_ts, 1, is_scheduled_for_deletion=True, negligible_amount=10.0)
    p.try_to_delete_account(D_ID, C_ID)
    p.configure_account(D_ID, C_ID, current_ts + timedelta(days=1000), 0)
    assert q.one().interest_rate == 0.0
    assert not q.one().status & Account.STATUS_SCHEDULED_FOR_DELETION_FLAG


def test_resurect_deleted_account_transfer(db_session, current_ts):
    p.configure_account(D_ID, C_ID, current_ts, 0)
    q = Account.query.filter_by(debtor_id=D_ID, creditor_id=C_ID)
    q.update({Account.interest_rate: 10.0})
    p.configure_account(D_ID, C_ID, current_ts, 1, is_scheduled_for_deletion=True, negligible_amount=10.0)
    p.try_to_delete_account(D_ID, C_ID)
    assert not p.get_account(D_ID, C_ID)
    p.make_debtor_payment('test', D_ID, C_ID, 1)
    p.process_pending_account_changes(D_ID, C_ID)
    a = p.get_account(D_ID, C_ID)
    assert a is not None
    assert a.interest_rate == 0.0
    assert a.status & Account.STATUS_SCHEDULED_FOR_DELETION_FLAG


def test_prepare_transfer_insufficient_funds(db_session, current_ts):
    p.configure_account(D_ID, 1234, current_ts, 0)
    p.configure_account(D_ID, C_ID, current_ts, 0)
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
    )
    p.process_transfer_requests(D_ID, C_ID)
    a = p.get_account(D_ID, C_ID)
    assert a.locked_amount == 0
    assert a.pending_transfers_count == 0
    p.process_pending_account_changes(D_ID, 1234)
    p.process_pending_account_changes(D_ID, C_ID)
    assert len(AccountChangeSignal.query.all()) == 2
    assert len(PreparedTransfer.query.all()) == 0
    assert len(PreparedTransferSignal.query.all()) == 0
    assert len(CommittedTransferSignal.query.all()) == 0
    rts = RejectedTransferSignal.query.one()
    assert rts.debtor_id == D_ID
    assert rts.coordinator_type == 'test'
    assert rts.coordinator_id == 1
    assert rts.coordinator_request_id == 2


def test_prepare_transfer_account_does_not_exist(db_session, current_ts):
    p.configure_account(D_ID, C_ID, current_ts, 0)
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
    )
    p.process_transfer_requests(D_ID, C_ID)
    rts = RejectedTransferSignal.query.one()
    assert rts.debtor_id == D_ID
    assert rts.coordinator_type == 'test'
    assert rts.coordinator_id == 1
    assert rts.coordinator_request_id == 2
    assert rts.details['error_code'] == 'ACC003'


def test_prepare_transfer_to_self(db_session, current_ts):
    p.configure_account(D_ID, C_ID, current_ts, 0)
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
        recipient_creditor_id=C_ID,
    )
    p.process_transfer_requests(D_ID, C_ID)
    rts = RejectedTransferSignal.query.one()
    assert rts.debtor_id == D_ID
    assert rts.coordinator_type == 'test'
    assert rts.coordinator_id == 1
    assert rts.coordinator_request_id == 2
    assert rts.details['error_code'] == 'ACC002'


def test_prepare_transfer_too_many_prepared_transfers(db_session, current_ts):
    p.configure_account(D_ID, C_ID, current_ts, 0)
    p.configure_account(D_ID, 1234, current_ts, 0)
    q = Account.query.filter_by(debtor_id=D_ID, creditor_id=C_ID)
    q.update({Account.principal: 100, Account.pending_transfers_count: MAX_INT32})
    p.prepare_transfer(
        coordinator_type='test',
        coordinator_id=1,
        coordinator_request_id=2,
        min_amount=1,
        max_amount=200,
        debtor_id=D_ID,
        sender_creditor_id=C_ID,
        recipient_creditor_id=1234,
    )
    p.process_transfer_requests(D_ID, C_ID)
    rts = RejectedTransferSignal.query.one()
    assert rts.debtor_id == D_ID
    assert rts.coordinator_type == 'test'
    assert rts.coordinator_id == 1
    assert rts.coordinator_request_id == 2
    assert rts.details['error_code'] == 'ACC006'


def test_prepare_transfer_success(db_session, current_ts):
    assert 1234 != C_ID
    p.configure_account(D_ID, C_ID, current_ts, 0)
    p.configure_account(D_ID, 1234, current_ts, 0)
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
    )
    p.process_transfer_requests(D_ID, C_ID)
    a = p.get_account(D_ID, C_ID)
    assert a.locked_amount == 100
    assert a.pending_transfers_count == 1
    p.process_pending_account_changes(D_ID, 1234)
    p.process_pending_account_changes(D_ID, C_ID)
    assert len(AccountChangeSignal.query.all()) == 2
    assert len(RejectedTransferSignal.query.all()) == 0
    pts = PreparedTransferSignal.query.one()
    assert pts.debtor_id == D_ID
    assert pts.coordinator_type == 'test'
    assert pts.coordinator_id == 1
    assert pts.coordinator_request_id == 2
    assert pts.sender_creditor_id == C_ID
    assert pts.recipient_creditor_id == 1234
    assert pts.sender_locked_amount == 100
    pt = PreparedTransfer.query.filter_by(debtor_id=D_ID, sender_creditor_id=C_ID).one()
    assert pt.transfer_id == pts.transfer_id
    assert pt.coordinator_type == 'test'
    assert pt.recipient_creditor_id == 1234
    assert pt.sender_locked_amount == pts.sender_locked_amount

    # Discard the transfer.
    with pytest.raises(ValueError):
        p.finalize_prepared_transfer(D_ID, C_ID, pt.transfer_id, -1)
    p.finalize_prepared_transfer(D_ID, C_ID, pt.transfer_id, 0)
    p.process_pending_account_changes(D_ID, 1234)
    p.process_pending_account_changes(D_ID, C_ID)
    a = p.get_account(D_ID, C_ID)
    assert a.locked_amount == 0
    assert a.pending_transfers_count == 0
    assert a.principal == 100
    assert a.interest == 0.0
    assert a.last_outgoing_transfer_date is None
    assert not PreparedTransfer.query.one_or_none()
    assert len(AccountChangeSignal.query.all()) == 2
    assert len(RejectedTransferSignal.query.all()) == 0
    assert len(CommittedTransferSignal.query.all()) == 0


def test_commit_prepared_transfer(db_session, current_ts):
    p.configure_account(D_ID, C_ID, current_ts, 0)
    p.configure_account(D_ID, 1234, current_ts, 0)
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
    )
    p.process_transfer_requests(D_ID, C_ID)
    pt = PreparedTransfer.query.filter_by(debtor_id=D_ID, sender_creditor_id=C_ID).one()
    p.finalize_prepared_transfer(D_ID, C_ID, pt.transfer_id, 40)
    p.process_pending_account_changes(D_ID, 1234)
    p.process_pending_account_changes(D_ID, C_ID)
    a1 = p.get_account(D_ID, 1234)
    assert a1.locked_amount == 0
    assert a1.pending_transfers_count == 0
    assert a1.principal == 40
    assert a1.interest == 0.0
    assert a1.last_outgoing_transfer_date is None
    a2 = p.get_account(D_ID, C_ID)
    assert a2.locked_amount == 0
    assert a2.pending_transfers_count == 0
    assert a2.principal == 60
    assert a2.interest == 0.0
    assert a2.last_outgoing_transfer_date is not None
    assert not PreparedTransfer.query.one_or_none()
    assert len(AccountChangeSignal.query.all()) == 4
    assert len(RejectedTransferSignal.query.all()) == 0

    assert len(CommittedTransferSignal.query.filter_by(debtor_id=D_ID).all()) == 2
    cts1 = CommittedTransferSignal.query.filter_by(debtor_id=D_ID, creditor_id=C_ID).one()
    assert cts1.coordinator_type == 'test'
    assert cts1.creditor_id == C_ID
    assert cts1.other_creditor_id == 1234
    assert cts1.committed_amount == -40
    cts2 = CommittedTransferSignal.query.filter_by(debtor_id=D_ID, creditor_id=1234).one()
    assert cts2.coordinator_type == 'test'
    assert cts2.creditor_id == 1234
    assert cts2.other_creditor_id == C_ID
    assert cts2.committed_amount == 40


def test_commit_to_debtor_account(db_session, current_ts):
    p.configure_account(D_ID, p.ROOT_CREDITOR_ID, current_ts, 0)
    p.configure_account(D_ID, C_ID, current_ts, 0)
    q = Account.query.filter_by(debtor_id=D_ID, creditor_id=C_ID)
    q.update({Account.principal: 200, Account.interest: -150.0})
    p.prepare_transfer(
        coordinator_type='test',
        coordinator_id=1,
        coordinator_request_id=2,
        min_amount=1,
        max_amount=200,
        debtor_id=D_ID,
        sender_creditor_id=C_ID,
        recipient_creditor_id=p.ROOT_CREDITOR_ID,
    )
    p.process_transfer_requests(D_ID, C_ID)
    pt = PreparedTransfer.query.filter_by(debtor_id=D_ID, sender_creditor_id=C_ID).one()
    assert pt.sender_locked_amount == 50
    p.finalize_prepared_transfer(pt.debtor_id, pt.sender_creditor_id, pt.transfer_id, 40)

    p.process_pending_account_changes(D_ID, p.ROOT_CREDITOR_ID)
    p.process_pending_account_changes(D_ID, C_ID)
    assert len(CommittedTransferSignal.query.filter_by(debtor_id=D_ID).all()) == 1
    cts1 = CommittedTransferSignal.query.filter_by(debtor_id=D_ID, creditor_id=C_ID).one()
    assert cts1.committed_amount == -40


def test_get_dead_transfers(db_session):
    assert p.get_dead_transfers() == []


def test_get_debtor_account_list(db_session, current_ts):
    p.configure_account(D_ID, C_ID, current_ts, 0)
    p.configure_account(D_ID, C_ID + 1, current_ts, 0)
    accounts = p.get_debtor_account_list(D_ID, start_after=None, limit=None)
    assert len(accounts) == 2
    assert accounts[0].creditor_id == C_ID
    assert accounts[1].creditor_id == C_ID + 1
    assert len(p.get_debtor_account_list(1234, start_after=None, limit=None)) == 0
    assert len(p.get_debtor_account_list(D_ID, start_after=None, limit=1)) == 1
    assert len(p.get_debtor_account_list(D_ID, start_after=C_ID, limit=100)) == 1
    assert len(p.get_debtor_account_list(D_ID, start_after=C_ID + 1, limit=100)) == 0
    assert len(p.get_debtor_account_list(D_ID, start_after=None, limit=-1)) == 0


def test_marshmallow_auto_generated_classes(db_session):
    RejectedTransferSignal.query.all()
    assert hasattr(RejectedTransferSignal, '__marshmallow__')
    assert hasattr(RejectedTransferSignal, '__marshmallow_schema__')
    assert hasattr(CommittedTransferSignal, '__marshmallow__')
    assert hasattr(CommittedTransferSignal, '__marshmallow_schema__')


def test_zero_out_negative_balance(db_session, current_ts):
    p.configure_account(D_ID, C_ID, current_ts, 0)
    q = Account.query.filter_by(debtor_id=D_ID, creditor_id=C_ID)
    q.update({
        Account.interest: 100.99,
        Account.principal: -5000,
    })
    assert p.get_available_balance(D_ID, C_ID) == -4900
    p.zero_out_negative_balance(D_ID, C_ID, current_ts.date())
    p.process_pending_account_changes(D_ID, C_ID)
    assert p.get_available_balance(D_ID, C_ID) == 0
    p.configure_account(D_ID, C_ID, current_ts, 1, is_scheduled_for_deletion=True)
    p.try_to_delete_account(D_ID, C_ID)
    q = Account.query.filter_by(debtor_id=D_ID, creditor_id=C_ID)
    assert q.one().status & Account.STATUS_DELETED_FLAG
    assert q.one().status & Account.STATUS_SCHEDULED_FOR_DELETION_FLAG
