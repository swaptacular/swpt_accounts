from swpt_accounts import chores

D_ID = -1
C_ID = 1


def test_set_interest_rate(db_session):
    chores.change_interest_rate(
        debtor_id=D_ID,
        creditor_id=C_ID,
        interest_rate=10.0,
        ts='2019-12-31T00:00:00+00:00',
    )


def test_capitalize_interest(db_session):
    chores.capitalize_interest(
        debtor_id=D_ID,
        creditor_id=C_ID,
    )


def test_try_to_delete_account(db_session):
    chores.try_to_delete_account(
        debtor_id=D_ID,
        creditor_id=C_ID,
    )
