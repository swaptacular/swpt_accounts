from datetime import datetime, timezone
from swpt_accounts import actors as a

D_ID = -1
C_ID = 1


def test_prepare_transfer(db_session):
    a.prepare_transfer(
        coordinator_type='test',
        coordinator_id=1,
        coordinator_request_id=2,
        min_amount=1,
        max_amount=200,
        debtor_id=D_ID,
        sender_creditor_id=C_ID,
        recipient_creditor_id=1234,
        signal_ts=datetime.now(tz=timezone.utc).isoformat(),
    )


def test_finalize_prepared_transfer(db_session):
    a.finalize_prepared_transfer(
        debtor_id=D_ID,
        sender_creditor_id=C_ID,
        transfer_id=666,
        committed_amount=100,
        transfer_message='',
        transfer_flags=0,
    )


def test_set_interest_rate(db_session):
    a.change_interest_rate(
        debtor_id=D_ID,
        creditor_id=C_ID,
        interest_rate=10.0,
        request_ts='2019-12-31T00:00:00Z',
    )


def test_capitalize_interest(db_session):
    a.capitalize_interest(
        debtor_id=D_ID,
        creditor_id=C_ID,
        accumulated_interest_threshold=0,
        request_ts='2019-12-31T00:00:00Z',
    )


def test_configure_account(db_session):
    a.configure_account(
        debtor_id=D_ID,
        creditor_id=C_ID,
        signal_ts='2099-12-31T00:00:00Z',
        signal_seqnum=0,
        status_flags=0,
        negligible_amount=500.0,
        config='',
    )


def test_zero_out_negative_balance(db_session):
    a.zero_out_negative_balance(
        debtor_id=D_ID,
        creditor_id=C_ID,
        last_outgoing_transfer_date='2019-07-01',
        request_ts='2019-12-31T00:00:00Z',
    )


def test_try_to_delete_account(db_session):
    a.try_to_delete_account(
        debtor_id=D_ID,
        creditor_id=C_ID,
        request_ts='2019-12-31T00:00:00Z',
    )
