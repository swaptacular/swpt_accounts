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
    )


def test_finalize_prepared_transfer(db_session):
    a.finalize_prepared_transfer(
        debtor_id=D_ID,
        sender_creditor_id=C_ID,
        transfer_id=666,
        committed_amount=100,
    )


def test_set_interest_rate(db_session):
    a.change_account_attributes(
        debtor_id=D_ID,
        creditor_id=C_ID,
        change_seqnum=777,
        change_ts='2019-07-01T00:00:00Z',
        interest_rate=10.0,
    )


def test_capitalize_interest(db_session):
    a.capitalize_interest(
        debtor_id=D_ID,
        creditor_id=C_ID,
    )


def test_create_account(db_session):
    a.create_account(
        debtor_id=D_ID,
        creditor_id=C_ID,
    )


def test_zero_out_negative_balance(db_session):
    a.zero_out_negative_balance(
        debtor_id=D_ID,
        creditor_id=C_ID,
        last_outgoing_transfer_date='2019-07-01',
    )


def test_purge_deleted_account(db_session):
    a.purge_deleted_account(
        debtor_id=D_ID,
        creditor_id=C_ID,
        if_deleted_before='2019-07-01T00:00:00Z',
    )


def test_delete_account_if_negligible(db_session):
    a.delete_account_if_negligible(
        debtor_id=D_ID,
        creditor_id=C_ID,
        negligible_amount=100,
        ignore_after_ts='2000-01-01T00:00:00Z',
    )
