from swpt_accounts import procedures as p

D_ID = -1
C_ID = 1


def test_process_pending_changes(app, db_session):
    p.make_debtor_payment('test', D_ID, C_ID, 1000)
    assert p.get_available_balance(D_ID, C_ID, ignore_interest=True) == 0
    assert p.get_available_balance(D_ID, p.ROOT_CREDITOR_ID, ignore_interest=True) == 0
    runner = app.test_cli_runner()
    result = runner.invoke(args=['swpt_accounts', 'process-pending-changes'])
    assert not result.output
    assert p.get_available_balance(D_ID, C_ID, ignore_interest=True) == 1000
    assert p.get_available_balance(D_ID, p.ROOT_CREDITOR_ID, ignore_interest=True) == -1000
