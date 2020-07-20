import pytest
from datetime import datetime, timezone, timedelta
from swpt_lib.utils import i64_to_u64
from swpt_accounts import __version__
from swpt_accounts import procedures as p
from swpt_accounts.models import MAX_INT32, MAX_INT64, INTEREST_RATE_FLOOR, INTEREST_RATE_CEIL, \
    Account, PendingAccountChange, RejectedTransferSignal, PreparedTransfer, PreparedTransferSignal, \
    AccountUpdateSignal, AccountTransferSignal, FinalizedTransferSignal, RejectedConfigSignal, \
    AccountPurgeSignal, FinalizationRequest, \
    CT_DIRECT, SC_OK, SC_TIMEOUT, SC_INSUFFICIENT_AVAILABLE_AMOUNT


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
    assert a.status_flags == 0
    assert a.principal == 0
    assert a.interest == 0.0
    assert a.total_locked_amount == 0
    assert a.pending_transfers_count == 0
    assert a.interest_rate == 0.0
    assert not a.status_flags & Account.STATUS_UNREACHABLE_FLAG
    acs = AccountUpdateSignal.query.filter_by(debtor_id=D_ID, creditor_id=C_ID).one()
    assert acs.status_flags == a.status_flags
    assert acs.principal == a.principal
    assert acs.interest == a.interest
    assert acs.interest_rate == a.interest_rate
    acs_obj = acs.__marshmallow_schema__.dump(acs)
    assert acs_obj['debtor_id'] == D_ID
    assert acs_obj['creditor_id'] == C_ID
    assert acs_obj['creation_date'] == a.creation_date.isoformat()
    assert acs_obj['last_change_ts'] == a.last_change_ts.isoformat()
    assert acs_obj['last_change_seqnum'] == a.last_change_seqnum
    assert acs_obj['principal'] == a.principal
    assert acs_obj['interest'] == a.interest
    assert acs_obj['interest_rate'] == a.interest_rate
    assert acs_obj['demurrage_rate'] == -50.0
    assert acs_obj['commit_period'] == 30 * 24 * 60 * 60
    assert acs_obj['last_config_ts'] == a.last_config_ts.isoformat()
    assert acs_obj['last_config_seqnum'] == a.last_config_seqnum
    assert acs_obj['negligible_amount'] == a.negligible_amount
    assert acs_obj['config'] == ''
    assert acs_obj['config_flags'] == a.config_flags
    assert acs_obj['status_flags'] == a.status_flags
    assert acs_obj['account_identity'] == str(C_ID)
    assert acs_obj['last_transfer_number'] == 0
    assert acs_obj['last_transfer_committed_at'] == a.last_transfer_committed_at_ts.isoformat()
    assert acs_obj['debtor_info_url'] == 'https://example.com/debtors/{}/'.format(i64_to_u64(D_ID))
    assert isinstance(acs_obj['ts'], str)
    assert acs_obj['ttl'] == 7 * 24 * 60 * 60

    a = p.configure_account(D_ID, C_ID, current_ts, 0)
    assert len(AccountUpdateSignal.query.filter_by(debtor_id=D_ID, creditor_id=C_ID).all()) == 1
    a = p.configure_account(D_ID, C_ID, current_ts, 1)
    assert len(AccountUpdateSignal.query.filter_by(debtor_id=D_ID, creditor_id=C_ID).all()) == 2


def test_ignored_config(db_session, current_ts):
    p.configure_account(D_ID, C_ID, current_ts, 0)
    assert len(AccountUpdateSignal.query.all()) == 1
    assert len(RejectedConfigSignal.query.all()) == 0

    p.configure_account(D_ID, C_ID, current_ts, 0)
    p.configure_account(D_ID, C_ID, current_ts, -1)
    p.configure_account(D_ID, C_ID, current_ts - timedelta(microseconds=1), 1)
    assert len(AccountUpdateSignal.query.all()) == 1
    assert len(RejectedConfigSignal.query.all()) == 0


def test_invalid_config(db_session, current_ts):
    p.configure_account(D_ID, C_ID, current_ts, 123, -10.0, config_flags=0x1fff, config='xxx')
    assert p.get_account(D_ID, C_ID) is None
    assert len(AccountUpdateSignal.query.all()) == 0
    rcs = RejectedConfigSignal.query.one()
    assert rcs.debtor_id == D_ID
    assert rcs.creditor_id == C_ID
    assert rcs.rejection_code == p.RC_INVALID_CONFIGURATION
    assert rcs.config_ts == current_ts
    assert rcs.config_seqnum == 123
    assert rcs.config_flags == 0x1fff
    assert rcs.negligible_amount == -10.0
    assert rcs.config == 'xxx'
    rcs_obj = rcs.__marshmallow_schema__.dump(rcs)
    assert rcs_obj['debtor_id'] == D_ID
    assert rcs_obj['creditor_id'] == C_ID
    assert rcs_obj['config_ts'] == current_ts.isoformat()
    assert rcs_obj['config_seqnum'] == 123
    assert rcs_obj['config_flags'] == rcs.config_flags
    assert rcs_obj['negligible_amount'] == rcs.negligible_amount
    assert rcs_obj['config'] == 'xxx'
    assert rcs_obj['rejection_code'] == p.RC_INVALID_CONFIGURATION
    assert isinstance(rcs_obj['ts'], str)

    p.configure_account(D_ID, C_ID, current_ts - timedelta(days=1000), 123)
    assert p.get_account(D_ID, C_ID) is None
    assert len(AccountUpdateSignal.query.all()) == 0
    assert len(RejectedConfigSignal.query.all()) == 1


def test_set_interest_rate(db_session, current_ts):
    # The account does not exist.
    p.try_to_change_interest_rate(D_ID, C_ID, 7.0, current_ts)
    assert p.get_account(D_ID, C_ID) is None
    assert len(AccountUpdateSignal.query.all()) == 0

    # The account does exist.
    p.configure_account(D_ID, C_ID, current_ts, 0)
    p.try_to_change_interest_rate(D_ID, C_ID, 7.0, current_ts)
    a = p.get_account(D_ID, C_ID)
    assert a.interest_rate == 7.0
    assert a.status_flags & Account.STATUS_ESTABLISHED_INTEREST_RATE_FLAG
    assert len(AccountUpdateSignal.query.all()) == 2

    # Changing the interest rate too often.
    p.try_to_change_interest_rate(D_ID, C_ID, 1.0, current_ts)
    a = p.get_account(D_ID, C_ID)
    assert a.interest_rate == 7.0


def test_too_big_interest_rate(db_session, current_ts):
    p.configure_account(D_ID, C_ID, current_ts, 0)
    p.try_to_change_interest_rate(D_ID, C_ID, 1e9, current_ts)
    assert p.get_account(D_ID, C_ID).interest_rate == INTEREST_RATE_CEIL


def test_too_small_interest_rate(db_session, current_ts):
    p.configure_account(D_ID, C_ID, current_ts, 0)
    p.try_to_change_interest_rate(D_ID, C_ID, -99.9999999999, current_ts)
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
    TRANSFER_NOTE = '{"transer_data": 123}'
    p.configure_account(D_ID, C_ID, current_ts, 0,
                        config_flags=Account.CONFIG_SCHEDULED_FOR_DELETION_FLAG, negligible_amount=abs(amount))
    p.make_debtor_payment('test', D_ID, C_ID, amount, TRANSFER_NOTE)

    root_change = PendingAccountChange.query.filter_by(debtor_id=D_ID, creditor_id=p.ROOT_CREDITOR_ID).one()
    assert root_change.principal_delta == -amount
    assert root_change.interest_delta == 0
    assert len(PendingAccountChange.query.filter_by(debtor_id=D_ID, creditor_id=C_ID).all()) == 0
    a = p.get_account(D_ID, C_ID)
    assert a.principal == amount
    assert a.interest == 0

    p.process_pending_account_changes(D_ID, C_ID)
    p.process_pending_account_changes(D_ID, p.ROOT_CREDITOR_ID)
    assert len(AccountTransferSignal.query.filter_by(debtor_id=D_ID).all()) == 1
    cts1 = AccountTransferSignal.query.filter_by(debtor_id=D_ID, creditor_id=C_ID).one()
    transfer_number1 = 1
    assert cts1.coordinator_type == 'test'
    assert cts1.creditor_id == C_ID
    assert cts1.other_creditor_id == p.ROOT_CREDITOR_ID
    assert cts1.acquired_amount == amount
    assert cts1.transfer_note == TRANSFER_NOTE
    assert cts1.transfer_number == transfer_number1
    assert cts1.principal == amount
    assert len(AccountTransferSignal.query.filter_by(debtor_id=D_ID, creditor_id=p.ROOT_CREDITOR_ID).all()) == 0
    assert cts1.transfer_flags & AccountTransferSignal.SYSTEM_FLAG_IS_NEGLIGIBLE
    cts1_obj = cts1.__marshmallow_schema__.dump(cts1)
    assert cts1_obj['debtor_id'] == D_ID
    assert cts1_obj['creditor_id'] == C_ID
    assert cts1_obj['creation_date'] == cts1.creation_date.isoformat()
    assert cts1_obj['transfer_number'] == 1
    assert cts1_obj['coordinator_type'] == 'test'
    assert cts1_obj['sender'] in [str(p.ROOT_CREDITOR_ID), str(C_ID)]
    assert cts1_obj['recipient'] in [str(p.ROOT_CREDITOR_ID), str(C_ID)]
    assert cts1_obj['sender'] != cts1_obj['recipient']
    assert cts1_obj['acquired_amount'] == amount
    assert cts1_obj['committed_at'] == cts1.committed_at_ts.isoformat()
    assert cts1_obj['transfer_note'] == TRANSFER_NOTE
    assert cts1_obj['transfer_flags'] & AccountTransferSignal.SYSTEM_FLAG_IS_NEGLIGIBLE
    assert isinstance(cts1_obj['ts'], str)
    assert cts1_obj['previous_transfer_number'] == 0
    assert cts1_obj['principal'] == amount

    p.make_debtor_payment('test', D_ID, C_ID, 2 * amount, TRANSFER_NOTE)
    p.process_pending_account_changes(D_ID, C_ID)
    cts = AccountTransferSignal.query.filter_by(
        debtor_id=D_ID, creditor_id=C_ID, transfer_number=transfer_number1 + 1).one()
    assert cts.acquired_amount == 2 * amount
    assert cts.principal == 3 * amount


def test_make_debtor_zero_payment(db_session, current_ts):
    p.configure_account(D_ID, C_ID, current_ts, 0)
    p.make_debtor_payment('interest', D_ID, C_ID, 0)
    assert not PendingAccountChange.query.all()
    p.process_pending_account_changes(D_ID, C_ID)
    p.process_pending_account_changes(D_ID, p.ROOT_CREDITOR_ID)
    assert not AccountTransferSignal.query.all()


def test_make_debtor_creditor_account_deletion(db_session, current_ts, amount):
    p.configure_account(D_ID, C_ID, current_ts, 0)
    p.make_debtor_payment('delete_account', D_ID, C_ID, amount)
    cts = AccountTransferSignal.query.filter_by(debtor_id=D_ID).one()
    cts.debtor_id == D_ID
    cts.creditor_id == C_ID
    cts.coordinator_type == 'delete_account'
    cts.other_creditor_id == p.ROOT_CREDITOR_ID
    cts.acquired_amount == amount
    cts.principal == 0
    changes = PendingAccountChange.query.all()
    assert len(changes) == 1
    root_change = changes[0]
    assert root_change.creditor_id == p.ROOT_CREDITOR_ID
    assert root_change.principal_delta == -amount
    assert root_change.interest_delta == 0
    p.process_pending_account_changes(D_ID, C_ID)
    p.process_pending_account_changes(D_ID, p.ROOT_CREDITOR_ID)


def test_make_debtor_interest_payment(db_session, current_ts, amount):
    p.configure_account(D_ID, C_ID, current_ts, 0)
    p.make_debtor_payment('interest', D_ID, C_ID, amount)
    root_change = PendingAccountChange.query.filter_by(debtor_id=D_ID, creditor_id=p.ROOT_CREDITOR_ID).one()
    assert root_change.principal_delta == -amount
    assert root_change.interest_delta == 0
    assert not PendingAccountChange.query.filter_by(debtor_id=D_ID, creditor_id=C_ID).all()
    assert p.get_account(D_ID, C_ID).principal == amount
    assert p.get_account(D_ID, C_ID).interest == -amount
    cts = AccountTransferSignal.query.filter_by(debtor_id=D_ID).one()
    cts.debtor_id == D_ID
    cts.creditor_id == C_ID
    cts.coordinator_type == 'interest'
    cts.other_creditor_id == p.ROOT_CREDITOR_ID
    cts.acquired_amount == amount
    cts.principal == amount


def test_process_pending_account_changes(db_session, current_ts):
    p.configure_account(D_ID, C_ID, current_ts, 0)
    assert len(p.get_accounts_with_pending_changes()) == 0
    p.make_debtor_payment('test', D_ID, C_ID, 10000)
    assert len(p.get_accounts_with_pending_changes()) == 1
    assert p.get_account(D_ID, p.ROOT_CREDITOR_ID) is None
    p.process_pending_account_changes(D_ID, p.ROOT_CREDITOR_ID)
    assert AccountUpdateSignal.query.filter_by(
        debtor_id=D_ID,
        creditor_id=p.ROOT_CREDITOR_ID,
        principal=-10000,
    ).one_or_none()
    assert len(p.get_accounts_with_pending_changes()) == 0
    assert p.get_account(D_ID, p.ROOT_CREDITOR_ID).principal == -10000


def test_positive_overflow(db_session, current_ts):
    p.configure_account(D_ID, C_ID, current_ts, 0)

    p.make_debtor_payment('test', D_ID, C_ID, MAX_INT64)
    p.process_pending_account_changes(D_ID, C_ID)
    assert not p.get_account(D_ID, C_ID).status_flags & Account.STATUS_OVERFLOWN_FLAG

    p.make_debtor_payment('test', D_ID, C_ID, 1)
    p.process_pending_account_changes(D_ID, C_ID)
    assert p.get_account(D_ID, C_ID).status_flags & Account.STATUS_OVERFLOWN_FLAG


def test_negative_overflow(db_session, current_ts):
    p.configure_account(D_ID, C_ID, current_ts, 0)

    p.make_debtor_payment('test', D_ID, C_ID, -MAX_INT64)
    p.process_pending_account_changes(D_ID, C_ID)
    assert not p.get_account(D_ID, C_ID).status_flags & Account.STATUS_OVERFLOWN_FLAG

    p.make_debtor_payment('test', D_ID, C_ID, -2)
    p.process_pending_account_changes(D_ID, C_ID)
    assert p.get_account(D_ID, C_ID).status_flags & Account.STATUS_OVERFLOWN_FLAG


def test_get_available_amount(db_session, current_ts):
    q = Account.query.filter_by(debtor_id=D_ID, creditor_id=C_ID)
    q_root = Account.query.filter_by(debtor_id=D_ID, creditor_id=p.ROOT_CREDITOR_ID)

    assert p.get_available_amount(D_ID, p.ROOT_CREDITOR_ID) is None
    p.configure_account(D_ID, p.ROOT_CREDITOR_ID, current_ts, 0)
    assert p.get_available_amount(D_ID, p.ROOT_CREDITOR_ID) == 0
    q_root.update({
        Account.interest: 100.0,
        Account.principal: 500,
    })
    assert p.get_available_amount(D_ID, p.ROOT_CREDITOR_ID) == 500

    assert p.get_available_amount(D_ID, C_ID) is None
    p.configure_account(D_ID, C_ID, current_ts, 0)
    assert p.get_available_amount(D_ID, C_ID) == 0
    q.update({
        Account.interest: 100.0,
        Account.principal: 5000,
    })
    assert p.get_available_amount(D_ID, C_ID) == 5100
    q.update({
        Account.total_locked_amount: 1000,
    })
    assert p.get_available_amount(D_ID, C_ID) == 4100
    q.update({
        Account.interest_rate: 10.00,
        Account.last_change_ts: current_ts - timedelta(days=365),
        Account.last_change_seqnum: 666,
    })
    assert 4608 <= p.get_available_amount(D_ID, C_ID) <= 4610
    q.update({
        Account.interest_rate: -10.00,
        Account.last_change_ts: current_ts - timedelta(days=365),
        Account.last_change_seqnum: 666,
    })
    assert 3590 <= p.get_available_amount(D_ID, C_ID) <= 3592
    q.update({
        Account.interest: -5100.0,
    })
    assert p.get_available_amount(D_ID, C_ID) == -1100


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
    p.capitalize_interest(D_ID, C_ID, 10000000, current_ts)
    p.process_pending_account_changes(D_ID, C_ID)
    assert p.get_account(D_ID, C_ID).interest == 100.0
    p.capitalize_interest(D_ID, C_ID, 0, current_ts)
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
    p.capitalize_interest(D_ID, C_ID, 10000000, current_ts)
    p.process_pending_account_changes(D_ID, C_ID)
    assert p.get_account(D_ID, C_ID).interest == -100.0
    p.capitalize_interest(D_ID, C_ID, 0, current_ts)
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
    assert a is not None
    assert not a.status_flags & Account.STATUS_DELETED_FLAG
    assert not a.config_flags & Account.CONFIG_SCHEDULED_FOR_DELETION_FLAG
    p.configure_account(D_ID, C_ID, current_ts, 1, config_flags=Account.CONFIG_SCHEDULED_FOR_DELETION_FLAG)
    p.try_to_delete_account(D_ID, C_ID, current_ts)
    assert p.get_account(D_ID, C_ID) is None
    q = Account.query.filter_by(debtor_id=D_ID, creditor_id=C_ID)
    assert q.one().status_flags & Account.STATUS_DELETED_FLAG
    assert q.one().config_flags & Account.CONFIG_SCHEDULED_FOR_DELETION_FLAG
    assert AccountUpdateSignal.query.\
        filter(AccountUpdateSignal.debtor_id == D_ID).\
        filter(AccountUpdateSignal.creditor_id == C_ID).\
        filter(AccountUpdateSignal.status_flags.op('&')(Account.STATUS_DELETED_FLAG) == Account.STATUS_DELETED_FLAG).\
        one_or_none()


def test_delete_account_negative_balance(db_session, current_ts):
    p.configure_account(D_ID, C_ID, current_ts, 0)
    q = Account.query.filter_by(debtor_id=D_ID, creditor_id=C_ID)
    q.update({Account.principal: -5})
    p.configure_account(D_ID, C_ID, current_ts, 1,
                        config_flags=Account.CONFIG_SCHEDULED_FOR_DELETION_FLAG, negligible_amount=MAX_INT64)

    # Verify that incoming transfers are not allowed:
    p.configure_account(D_ID, 1234, current_ts, 0)
    Account.query.filter_by(debtor_id=D_ID, creditor_id=1234).update({Account.principal: 10})
    p.prepare_transfer(
        coordinator_type='test',
        coordinator_id=1,
        coordinator_request_id=2,
        min_locked_amount=1,
        max_locked_amount=200,
        debtor_id=D_ID,
        creditor_id=1234,
        recipient=str(C_ID),
        ts=current_ts,
    )
    p.process_transfer_requests(D_ID, 1234)
    rts = RejectedTransferSignal.query.one()
    assert rts.debtor_id == D_ID
    assert rts.coordinator_type == 'test'
    assert rts.coordinator_id == 1
    assert rts.coordinator_request_id == 2
    assert rts.status_code == p.SC_RECIPIENT_IS_UNREACHABLE

    # Verify that re-creating the account clears CONFIG_SCHEDULED_FOR_DELETION_FLAG:
    p.configure_account(D_ID, C_ID, current_ts, 2)
    assert not q.one().status_flags & Account.STATUS_DELETED_FLAG
    assert not q.one().config_flags & Account.CONFIG_SCHEDULED_FOR_DELETION_FLAG

    # Try to delete the account.
    p.configure_account(D_ID, C_ID, current_ts, 3, config_flags=Account.CONFIG_SCHEDULED_FOR_DELETION_FLAG)
    p.try_to_delete_account(D_ID, C_ID, current_ts)
    a = p.get_account(D_ID, C_ID)
    assert a is None


def test_delete_account_tiny_positive_balance(db_session, current_ts):
    p.configure_account(D_ID, C_ID, current_ts, 0)
    q = Account.query.filter_by(debtor_id=D_ID, creditor_id=C_ID)
    q.update({Account.principal: 2, Account.interest: -1.0})
    p.configure_account(D_ID, C_ID, current_ts, 1,
                        config_flags=Account.CONFIG_SCHEDULED_FOR_DELETION_FLAG, negligible_amount=2.0)
    p.try_to_delete_account(D_ID, C_ID, current_ts)
    assert p.get_account(D_ID, C_ID) is None
    a = q.one()
    assert a.config_flags & Account.CONFIG_SCHEDULED_FOR_DELETION_FLAG
    assert a.status_flags & Account.STATUS_DELETED_FLAG
    assert a.principal == 0
    assert a.interest == 0
    changes = PendingAccountChange.query.all()
    assert len(changes) == 1
    assert changes[0].creditor_id == p.ROOT_CREDITOR_ID
    p.process_pending_account_changes(D_ID, C_ID)
    p.process_pending_account_changes(D_ID, p.ROOT_CREDITOR_ID)

    assert len(AccountTransferSignal.query.all()) == 1
    cts1 = AccountTransferSignal.query.filter_by(creditor_id=C_ID).one()
    assert cts1.acquired_amount == -2
    assert cts1.principal == 0
    a = q.one()
    assert a.config_flags & Account.CONFIG_SCHEDULED_FOR_DELETION_FLAG
    assert a.status_flags & Account.STATUS_DELETED_FLAG
    assert a.principal == 0
    assert a.interest == 0


def test_delete_debtor_account(db_session, current_ts):
    q = Account.query.filter_by(debtor_id=D_ID, creditor_id=p.ROOT_CREDITOR_ID)
    p.configure_account(D_ID, p.ROOT_CREDITOR_ID, current_ts, 0)
    p.configure_account(D_ID, C_ID, current_ts, 0)

    # The principal is not zero.
    p.make_debtor_payment('test', D_ID, C_ID, 1)
    p.process_pending_account_changes(D_ID, p.ROOT_CREDITOR_ID)
    p.try_to_delete_account(D_ID, p.ROOT_CREDITOR_ID, current_ts)
    a = p.get_account(D_ID, p.ROOT_CREDITOR_ID)
    assert not a.status_flags & Account.STATUS_DELETED_FLAG
    assert not a.config_flags & Account.CONFIG_SCHEDULED_FOR_DELETION_FLAG

    # The principal is zero.
    p.make_debtor_payment('test', D_ID, C_ID, -1)
    p.process_pending_account_changes(D_ID, p.ROOT_CREDITOR_ID)
    p.try_to_delete_account(D_ID, p.ROOT_CREDITOR_ID, current_ts)
    assert q.one().status_flags & Account.STATUS_DELETED_FLAG
    assert not q.one().config_flags & Account.CONFIG_SCHEDULED_FOR_DELETION_FLAG


def test_resurrect_deleted_account_create(db_session, current_ts):
    p.configure_account(D_ID, C_ID, current_ts, 0)
    q = Account.query.filter_by(debtor_id=D_ID, creditor_id=C_ID)
    q.update({Account.interest_rate: 10.0})
    p.configure_account(D_ID, C_ID, current_ts, 1,
                        config_flags=Account.CONFIG_SCHEDULED_FOR_DELETION_FLAG, negligible_amount=10.0)
    p.try_to_delete_account(D_ID, C_ID, current_ts)
    p.configure_account(D_ID, C_ID, current_ts + timedelta(days=1000), 0)
    assert q.one().interest_rate == 10.0
    assert not q.one().status_flags & Account.STATUS_ESTABLISHED_INTEREST_RATE_FLAG
    assert not q.one().status_flags & Account.STATUS_DELETED_FLAG
    assert not q.one().config_flags & Account.CONFIG_SCHEDULED_FOR_DELETION_FLAG


def test_resurrect_deleted_account_transfer(db_session, current_ts):
    p.configure_account(D_ID, C_ID, current_ts, 0)
    q = Account.query.filter_by(debtor_id=D_ID, creditor_id=C_ID)
    q.update({Account.interest_rate: 10.0})
    p.configure_account(D_ID, C_ID, current_ts, 1,
                        config_flags=Account.CONFIG_SCHEDULED_FOR_DELETION_FLAG, negligible_amount=10.0)
    p.try_to_delete_account(D_ID, C_ID, current_ts)
    assert not p.get_account(D_ID, C_ID)
    p.make_debtor_payment('test', D_ID, C_ID, 1)
    p.process_pending_account_changes(D_ID, C_ID)
    a = p.get_account(D_ID, C_ID)
    assert a is not None
    assert a.interest_rate == 10.0
    assert not a.status_flags & Account.STATUS_ESTABLISHED_INTEREST_RATE_FLAG
    assert not a.status_flags & Account.STATUS_DELETED_FLAG
    assert a.config_flags & Account.CONFIG_SCHEDULED_FOR_DELETION_FLAG


def test_prepare_transfer_insufficient_funds(db_session, current_ts):
    p.configure_account(D_ID, 1234, current_ts, 0)
    p.configure_account(D_ID, C_ID, current_ts, 0)
    assert len(AccountUpdateSignal.query.all()) == 2
    p.prepare_transfer(
        coordinator_type='test',
        coordinator_id=1,
        coordinator_request_id=2,
        min_locked_amount=1,
        max_locked_amount=200,
        debtor_id=D_ID,
        creditor_id=C_ID,
        recipient='1234',
        ts=current_ts,
    )
    p.process_transfer_requests(D_ID, C_ID)
    a = p.get_account(D_ID, C_ID)
    assert a.total_locked_amount == 0
    assert a.pending_transfers_count == 0
    p.process_pending_account_changes(D_ID, 1234)
    p.process_pending_account_changes(D_ID, C_ID)
    assert len(AccountUpdateSignal.query.all()) == 2
    assert len(PreparedTransfer.query.all()) == 0
    assert len(PreparedTransferSignal.query.all()) == 0
    assert len(AccountTransferSignal.query.all()) == 0
    assert len(FinalizedTransferSignal.query.all()) == 0
    rts = RejectedTransferSignal.query.one()
    assert rts.debtor_id == D_ID
    assert rts.coordinator_type == 'test'
    assert rts.coordinator_id == 1
    assert rts.coordinator_request_id == 2
    rts_obj = rts.__marshmallow_schema__.dump(rts)
    assert rts_obj['debtor_id'] == D_ID
    assert rts_obj['creditor_id'] == C_ID
    assert isinstance(rts_obj['status_code'], str)
    assert rts_obj['coordinator_type'] == 'test'
    assert rts_obj['coordinator_id'] == 1
    assert rts_obj['coordinator_request_id'] == 2
    assert rts_obj['total_locked_amount'] == 0
    assert rts_obj['recipient'] == '1234'
    assert isinstance(rts_obj['ts'], str)


def test_prepare_transfer_account_does_not_exist(db_session, current_ts):
    p.configure_account(D_ID, C_ID, current_ts, 0)
    q = Account.query.filter_by(debtor_id=D_ID, creditor_id=C_ID)
    q.update({Account.principal: 100})
    p.prepare_transfer(
        coordinator_type='test',
        coordinator_id=1,
        coordinator_request_id=2,
        min_locked_amount=1,
        max_locked_amount=200,
        debtor_id=D_ID,
        creditor_id=C_ID,
        recipient='1234',
        ts=current_ts,
    )
    p.process_transfer_requests(D_ID, C_ID)
    rts = RejectedTransferSignal.query.one()
    assert rts.debtor_id == D_ID
    assert rts.coordinator_type == 'test'
    assert rts.coordinator_id == 1
    assert rts.coordinator_request_id == 2
    assert rts.status_code == p.SC_RECIPIENT_IS_UNREACHABLE


def test_prepare_transfer_to_self(db_session, current_ts):
    p.configure_account(D_ID, C_ID, current_ts, 0)
    q = Account.query.filter_by(debtor_id=D_ID, creditor_id=C_ID)
    q.update({Account.principal: 100})
    p.prepare_transfer(
        coordinator_type='test',
        coordinator_id=1,
        coordinator_request_id=2,
        min_locked_amount=1,
        max_locked_amount=200,
        debtor_id=D_ID,
        creditor_id=C_ID,
        recipient=str(C_ID),
        ts=current_ts,
    )
    p.process_transfer_requests(D_ID, C_ID)
    rts = RejectedTransferSignal.query.one()
    assert rts.debtor_id == D_ID
    assert rts.coordinator_type == 'test'
    assert rts.coordinator_id == 1
    assert rts.coordinator_request_id == 2
    assert rts.status_code == p.SC_RECIPIENT_SAME_AS_SENDER


def test_prepare_transfer_too_many_prepared_transfers(db_session, current_ts):
    p.configure_account(D_ID, C_ID, current_ts, 0)
    p.configure_account(D_ID, 1234, current_ts, 0)
    q = Account.query.filter_by(debtor_id=D_ID, creditor_id=C_ID)
    q.update({Account.principal: 100, Account.pending_transfers_count: MAX_INT32})
    p.prepare_transfer(
        coordinator_type='test',
        coordinator_id=1,
        coordinator_request_id=2,
        min_locked_amount=1,
        max_locked_amount=200,
        debtor_id=D_ID,
        creditor_id=C_ID,
        recipient='1234',
        ts=current_ts,
    )
    p.process_transfer_requests(D_ID, C_ID)
    rts = RejectedTransferSignal.query.one()
    assert rts.debtor_id == D_ID
    assert rts.coordinator_type == 'test'
    assert rts.coordinator_id == 1
    assert rts.coordinator_request_id == 2
    assert rts.status_code == p.SC_TOO_MANY_TRANSFERS


def test_prepare_transfer_invalid_recipient(db_session, current_ts):
    p.configure_account(D_ID, C_ID, current_ts, 0)
    q = Account.query.filter_by(debtor_id=D_ID, creditor_id=C_ID)
    q.update({Account.principal: 100, Account.pending_transfers_count: MAX_INT32})
    p.prepare_transfer(
        coordinator_type='test',
        coordinator_id=1,
        coordinator_request_id=2,
        min_locked_amount=1,
        max_locked_amount=200,
        debtor_id=D_ID,
        creditor_id=C_ID,
        recipient='invalid',
        ts=current_ts,
    )
    p.process_transfer_requests(D_ID, C_ID)
    rts = RejectedTransferSignal.query.one()
    assert rts.debtor_id == D_ID
    assert rts.coordinator_type == 'test'
    assert rts.coordinator_id == 1
    assert rts.coordinator_request_id == 2
    assert rts.status_code == p.SC_RECIPIENT_IS_UNREACHABLE


def test_prepare_transfer_interest_rate_too_low(db_session, current_ts):
    p.configure_account(D_ID, C_ID, current_ts, 0)
    p.configure_account(D_ID, 1234, current_ts, 0)
    q = Account.query.filter_by(debtor_id=D_ID, creditor_id=C_ID)
    q.update({Account.principal: 100, Account.interest_rate: -10.0})
    p.prepare_transfer(
        coordinator_type='test',
        coordinator_id=1,
        coordinator_request_id=2,
        min_locked_amount=1,
        max_locked_amount=200,
        debtor_id=D_ID,
        creditor_id=C_ID,
        recipient='1234',
        min_account_balance=-1,
        min_interest_rate=-9.99999,
        ts=current_ts,
    )
    p.process_transfer_requests(D_ID, C_ID)
    rts = RejectedTransferSignal.query.one()
    assert rts.status_code == p.SC_TOO_LOW_INTEREST_RATE


def test_prepare_transfer_success(db_session, current_ts):
    assert 1234 != C_ID
    p.configure_account(D_ID, C_ID, current_ts, 0)
    p.configure_account(D_ID, 1234, current_ts, 0)
    assert len(AccountUpdateSignal.query.all()) == 2
    q = Account.query.filter_by(debtor_id=D_ID, creditor_id=C_ID)
    q.update({Account.principal: 100})
    p.prepare_transfer(
        coordinator_type='test',
        coordinator_id=1,
        coordinator_request_id=2,
        min_locked_amount=1,
        max_locked_amount=200,
        debtor_id=D_ID,
        creditor_id=C_ID,
        recipient='1234',
        min_account_balance=-1,
        ts=current_ts,
    )
    p.process_transfer_requests(D_ID, C_ID)
    a = p.get_account(D_ID, C_ID)
    assert a.total_locked_amount == 100
    assert a.pending_transfers_count == 1
    p.process_pending_account_changes(D_ID, 1234)
    p.process_pending_account_changes(D_ID, C_ID)
    assert len(AccountUpdateSignal.query.all()) == 2
    assert len(RejectedTransferSignal.query.all()) == 0
    assert len(FinalizedTransferSignal.query.all()) == 0
    pts = PreparedTransferSignal.query.one()
    assert pts.debtor_id == D_ID
    assert pts.coordinator_type == 'test'
    assert pts.coordinator_id == 1
    assert pts.coordinator_request_id == 2
    assert pts.sender_creditor_id == C_ID
    assert pts.recipient_creditor_id == 1234
    assert pts.locked_amount == 100
    pts_obj = pts.__marshmallow_schema__.dump(pts)
    assert pts_obj['recipient'] == '1234'
    pt = PreparedTransfer.query.filter_by(debtor_id=D_ID, sender_creditor_id=C_ID).one()
    assert pt.transfer_id == pts.transfer_id
    assert pt.coordinator_type == 'test'
    assert pt.recipient_creditor_id == 1234
    assert pt.locked_amount == pts.locked_amount
    assert pt.min_account_balance == 0

    pts_obj = pts.__marshmallow_schema__.dump(pts)
    assert pts_obj['debtor_id'] == D_ID
    assert pts_obj['creditor_id'] == C_ID
    assert pts_obj['transfer_id'] == pts.transfer_id
    assert pts_obj['coordinator_type'] == 'test'
    assert pts_obj['coordinator_id'] == 1
    assert pts_obj['coordinator_request_id'] == 2
    assert pts_obj['locked_amount'] == pts.locked_amount
    assert pts_obj['recipient'] == '1234'
    assert pts_obj['prepared_at'] == pts_obj['ts']
    assert pts_obj['deadline'] == pts.deadline.isoformat()
    assert pts_obj['demurrage_rate'] == -50.0
    assert isinstance(pts_obj['ts'], str)

    # Discard the transfer.
    with pytest.raises(AssertionError):
        p.finalize_transfer(D_ID, C_ID, pt.transfer_id, 'test', 1, 2, -1)
    p.finalize_transfer(D_ID, C_ID, pt.transfer_id, 'test', 1, 2, 0)
    p.process_finalization_requests(D_ID, C_ID)
    p.process_pending_account_changes(D_ID, 1234)
    p.process_pending_account_changes(D_ID, C_ID)
    a = p.get_account(D_ID, C_ID)
    assert a.total_locked_amount == 0
    assert a.pending_transfers_count == 0
    assert a.principal == 100
    assert a.interest == 0.0
    assert not PreparedTransfer.query.one_or_none()
    assert len(AccountUpdateSignal.query.all()) == 2
    assert len(RejectedTransferSignal.query.all()) == 0
    assert len(AccountTransferSignal.query.all()) == 0
    fpt = FinalizedTransferSignal.query.one()
    fpt_obj = fpt.__marshmallow_schema__.dump(fpt)
    assert fpt_obj['debtor_id'] == D_ID
    assert fpt_obj['creditor_id'] == C_ID
    assert fpt_obj['transfer_id'] == pt.transfer_id
    assert fpt_obj['coordinator_type'] == 'test'
    assert fpt_obj['coordinator_id'] == 1
    assert fpt_obj['coordinator_request_id'] == 2
    assert fpt_obj['committed_amount'] == 0
    assert fpt_obj['total_locked_amount'] == 0
    assert fpt_obj['recipient'] == '1234'
    assert fpt_obj['status_code'] == 'OK'
    assert isinstance(fpt_obj['ts'], str)
    assert fpt_obj['prepared_at'] == fpt.prepared_at_ts.isoformat()


def test_commit_prepared_transfer(db_session, current_ts):
    p.configure_account(D_ID, C_ID, current_ts, 0)
    p.configure_account(D_ID, 1234, current_ts, 0)
    q = Account.query.filter_by(debtor_id=D_ID, creditor_id=C_ID)
    q.update({Account.principal: 100})
    p.prepare_transfer(
        coordinator_type='direct',
        coordinator_id=1,
        coordinator_request_id=2,
        min_locked_amount=1,
        max_locked_amount=200,
        debtor_id=D_ID,
        creditor_id=C_ID,
        recipient='1234',
        ts=current_ts,
    )
    p.process_transfer_requests(D_ID, C_ID)
    pt = PreparedTransfer.query.filter_by(debtor_id=D_ID, sender_creditor_id=C_ID).one()
    p.finalize_transfer(D_ID, C_ID, pt.transfer_id, 'direct', 1, 2, 40)
    p.process_finalization_requests(D_ID, C_ID)
    p.process_pending_account_changes(D_ID, 1234)
    p.process_pending_account_changes(D_ID, C_ID)
    a1 = p.get_account(D_ID, 1234)
    assert a1.total_locked_amount == 0
    assert a1.pending_transfers_count == 0
    assert a1.principal == 40
    assert a1.interest == 0.0
    a2 = p.get_account(D_ID, C_ID)
    assert a2.total_locked_amount == 0
    assert a2.pending_transfers_count == 0
    assert a2.principal == 60
    assert a2.interest == 0.0
    assert not PreparedTransfer.query.one_or_none()
    assert len(AccountUpdateSignal.query.all()) == 4
    assert len(RejectedTransferSignal.query.all()) == 0
    assert len(FinalizedTransferSignal.query.all()) == 1

    assert len(AccountTransferSignal.query.filter_by(debtor_id=D_ID).all()) == 2
    cts1 = AccountTransferSignal.query.filter_by(debtor_id=D_ID, creditor_id=C_ID).one()
    assert cts1.coordinator_type == 'direct'
    assert cts1.creditor_id == C_ID
    assert cts1.other_creditor_id == 1234
    assert cts1.acquired_amount == -40
    cts2 = AccountTransferSignal.query.filter_by(debtor_id=D_ID, creditor_id=1234).one()
    assert cts2.coordinator_type == 'direct'
    assert cts2.creditor_id == 1234
    assert cts2.other_creditor_id == C_ID
    assert cts2.acquired_amount == 40


def test_zero_locked_amount_unsuccessful_commit(db_session, current_ts):
    p.configure_account(D_ID, C_ID, current_ts, 0)
    p.configure_account(D_ID, 1234, current_ts, 0)
    p.prepare_transfer(
        coordinator_type='direct',
        coordinator_id=1,
        coordinator_request_id=2,
        min_locked_amount=0,
        max_locked_amount=0,
        debtor_id=D_ID,
        creditor_id=C_ID,
        recipient='1234',
        ts=current_ts,
    )
    p.process_transfer_requests(D_ID, C_ID)
    pt = PreparedTransfer.query.filter_by(debtor_id=D_ID, sender_creditor_id=C_ID).one()
    assert pt.locked_amount == 0

    p.finalize_transfer(D_ID, C_ID, pt.transfer_id, 'direct', 1, 2, 40)
    p.process_finalization_requests(D_ID, C_ID)
    fts = FinalizedTransferSignal.query.one()
    assert fts.status_code == SC_INSUFFICIENT_AVAILABLE_AMOUNT
    assert fts.committed_amount == 0

    p.process_pending_account_changes(D_ID, 1234)
    p.process_pending_account_changes(D_ID, C_ID)
    a1 = p.get_account(D_ID, 1234)
    assert a1.principal == 0
    a2 = p.get_account(D_ID, C_ID)
    assert a2.principal == 0


def test_zero_locked_amount_successful_commit(db_session, current_ts):
    p.configure_account(D_ID, C_ID, current_ts, 0)
    p.configure_account(D_ID, 1234, current_ts, 0)
    p.prepare_transfer(
        coordinator_type='direct',
        coordinator_id=1,
        coordinator_request_id=2,
        min_locked_amount=0,
        max_locked_amount=0,
        debtor_id=D_ID,
        creditor_id=C_ID,
        recipient='1234',
        ts=current_ts,
    )
    p.process_transfer_requests(D_ID, C_ID)
    pt = PreparedTransfer.query.filter_by(debtor_id=D_ID, sender_creditor_id=C_ID).one()
    assert pt.locked_amount == 0

    q = Account.query.filter_by(debtor_id=D_ID, creditor_id=C_ID)
    q.update({Account.principal: 100})
    p.finalize_transfer(D_ID, C_ID, pt.transfer_id, 'direct', 1, 2, 40)
    p.process_finalization_requests(D_ID, C_ID)
    fts = FinalizedTransferSignal.query.one()
    assert fts.status_code == SC_OK
    assert fts.committed_amount == 40

    p.process_pending_account_changes(D_ID, 1234)
    p.process_pending_account_changes(D_ID, C_ID)
    a1 = p.get_account(D_ID, 1234)
    assert a1.principal == 40
    a2 = p.get_account(D_ID, C_ID)
    assert a2.principal == 60


def test_prepared_transfer_commit_timeout(db_session, current_ts):
    p.configure_account(D_ID, C_ID, current_ts, 0)
    p.configure_account(D_ID, 1234, current_ts, 0)
    q = Account.query.filter_by(debtor_id=D_ID, creditor_id=C_ID)
    q.update({Account.principal: 100})
    p.prepare_transfer(
        coordinator_type='direct',
        coordinator_id=1,
        coordinator_request_id=2,
        min_locked_amount=1,
        max_locked_amount=200,
        debtor_id=D_ID,
        creditor_id=C_ID,
        recipient='1234',
        min_account_balance=3,
        ts=current_ts,
    )
    p.process_transfer_requests(D_ID, C_ID)
    pt = PreparedTransfer.query.filter_by(debtor_id=D_ID, sender_creditor_id=C_ID).one()
    assert pt.min_account_balance == 3
    pt.prepared_at_ts = pt.prepared_at_ts - timedelta(days=100)
    pt.deadline = pt.prepared_at_ts + timedelta(days=30)
    db_session.commit()
    p.finalize_transfer(D_ID, C_ID, pt.transfer_id, 'direct', 1, 2, 40)
    p.process_finalization_requests(D_ID, C_ID)
    fts = FinalizedTransferSignal.query.one()
    assert fts.status_code == SC_TIMEOUT
    assert fts.committed_amount == 0


def test_prepared_transfer_too_big_committed_amount(db_session, current_ts):
    p.configure_account(D_ID, C_ID, current_ts, 0)
    p.configure_account(D_ID, 1234, current_ts, 0)
    q = Account.query.filter_by(debtor_id=D_ID, creditor_id=C_ID)
    q.update({Account.principal: 100})
    p.prepare_transfer(
        coordinator_type='direct',
        coordinator_id=1,
        coordinator_request_id=2,
        min_locked_amount=1,
        max_locked_amount=200,
        debtor_id=D_ID,
        creditor_id=C_ID,
        recipient='1234',
        ts=current_ts,
    )
    p.process_transfer_requests(D_ID, C_ID)
    pt = PreparedTransfer.query.filter_by(debtor_id=D_ID, sender_creditor_id=C_ID).one()
    p.finalize_transfer(D_ID, C_ID, pt.transfer_id, 'direct', 1, 2, 40000)
    p.process_finalization_requests(D_ID, C_ID)
    fts = FinalizedTransferSignal.query.one()
    assert fts.status_code == SC_INSUFFICIENT_AVAILABLE_AMOUNT
    assert fts.committed_amount == 0


def test_commit_to_debtor_account(db_session, current_ts):
    p.configure_account(D_ID, p.ROOT_CREDITOR_ID, current_ts, 0)
    p.configure_account(D_ID, C_ID, current_ts, 0)
    q = Account.query.filter_by(debtor_id=D_ID, creditor_id=C_ID)
    q.update({Account.principal: 200, Account.interest: -150.0})
    p.prepare_transfer(
        coordinator_type='test',
        coordinator_id=1,
        coordinator_request_id=2,
        min_locked_amount=1,
        max_locked_amount=200,
        debtor_id=D_ID,
        creditor_id=C_ID,
        recipient=str(p.ROOT_CREDITOR_ID),
        ts=current_ts,
    )
    p.process_transfer_requests(D_ID, C_ID)
    pt = PreparedTransfer.query.filter_by(debtor_id=D_ID, sender_creditor_id=C_ID).one()
    assert pt.locked_amount == 50
    p.finalize_transfer(pt.debtor_id, pt.sender_creditor_id, pt.transfer_id, 'test', 1, 2, 40)
    p.process_finalization_requests(D_ID, C_ID)
    p.process_pending_account_changes(D_ID, p.ROOT_CREDITOR_ID)
    p.process_pending_account_changes(D_ID, C_ID)
    assert len(AccountTransferSignal.query.filter_by(debtor_id=D_ID).all()) == 1
    assert len(FinalizedTransferSignal.query.all()) == 1
    cts1 = AccountTransferSignal.query.filter_by(debtor_id=D_ID, creditor_id=C_ID).one()
    assert cts1.acquired_amount == -40


def test_marshmallow_auto_generated_classes(db_session):
    RejectedTransferSignal.query.all()
    assert hasattr(RejectedTransferSignal, '__marshmallow__')
    assert hasattr(RejectedTransferSignal, '__marshmallow_schema__')
    assert hasattr(AccountTransferSignal, '__marshmallow__')
    assert hasattr(AccountTransferSignal, '__marshmallow_schema__')


def test_delayed_direct_transfer(db_session, current_ts):
    p.configure_account(D_ID, C_ID, current_ts, 0)
    p.configure_account(D_ID, 1234, current_ts, 0)
    q = Account.query.filter_by(debtor_id=D_ID, creditor_id=C_ID)
    q.update({Account.principal: 1000})
    p.prepare_transfer(
        coordinator_type=CT_DIRECT,
        coordinator_id=1,
        coordinator_request_id=2,
        min_locked_amount=1000,
        max_locked_amount=1000,
        debtor_id=D_ID,
        creditor_id=C_ID,
        recipient='1234',
        ts=current_ts,
    )
    p.process_transfer_requests(D_ID, C_ID)
    pt = PreparedTransfer.query.one()
    assert pt.calc_status_code(1000, 0, -100.0, current_ts) == SC_OK
    assert pt.calc_status_code(1000, 0, -100.0, current_ts + timedelta(days=31)) != SC_OK
    p.finalize_transfer(D_ID, C_ID, pt.transfer_id, CT_DIRECT, 1, 2, 9999999)
    p.process_finalization_requests(D_ID, C_ID)
    fts = FinalizedTransferSignal.query.one()
    assert fts.status_code != SC_OK
    assert fts.committed_amount == 0


def test_calc_status_code(db_session, current_ts):
    pt = PreparedTransfer(
        debtor_id=D_ID,
        sender_creditor_id=C_ID,
        transfer_id=1,
        coordinator_type='test',
        coordinator_id=11,
        coordinator_request_id=22,
        recipient_creditor_id=1,
        prepared_at_ts=current_ts,
        min_account_balance=10,
        min_interest_rate=-10.0,
        demurrage_rate=-50,
        deadline=current_ts + timedelta(days=10000),
        locked_amount=1000,
    )
    assert pt.calc_status_code(1000, 0, -10.0001, current_ts) != SC_OK
    assert pt.calc_status_code(1000, 0, -10.0, current_ts) == SC_OK
    assert pt.calc_status_code(1000, 0, -10.0, current_ts - timedelta(days=10)) == SC_OK
    assert pt.calc_status_code(1000, 0, -10.0, current_ts + timedelta(days=10)) == SC_OK
    assert pt.calc_status_code(1000, -1, -10.0, current_ts) == SC_OK
    assert pt.calc_status_code(1000, -1, -10.0, current_ts + timedelta(seconds=1)) != SC_OK
    assert pt.calc_status_code(1000, -1, -10.0, current_ts - timedelta(days=10)) == SC_OK
    assert pt.calc_status_code(999, -5, -10.0, current_ts + timedelta(days=10)) != SC_OK
    assert pt.calc_status_code(995, -5, -10.0, current_ts + timedelta(days=10)) == SC_OK
    assert pt.calc_status_code(995, -50000, -10.0, current_ts + timedelta(days=10)) != SC_OK
    assert pt.calc_status_code(980, -50000, -10.0, current_ts + timedelta(days=10)) == SC_OK
    pt.recipient_creditor_id = 0
    assert pt.calc_status_code(1000, -50000, -10.0, current_ts + timedelta(days=10)) == SC_OK


def test_finalize_transfer_twice(db_session):
    p.finalize_transfer(D_ID, C_ID, 1, 'test', 1, 2, 0)
    p.finalize_transfer(D_ID, C_ID, 1, 'test', 1, 2, 0)
    assert len(FinalizationRequest.query.all()) == 1


def test_account_purge_signal(db_session, current_ts):
    db_session.add(AccountPurgeSignal(
        debtor_id=D_ID,
        creditor_id=C_ID,
        creation_date=current_ts.date(),
    ))
    db_session.commit()
    aps = AccountPurgeSignal.query.one()
    aps_obj = aps.__marshmallow_schema__.dump(aps)
    assert aps_obj['debtor_id'] == D_ID
    assert aps_obj['creditor_id'] == C_ID
    assert aps_obj['creation_date'] == current_ts.date().isoformat()
    assert isinstance(aps_obj['ts'], str)
