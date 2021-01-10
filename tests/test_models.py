from datetime import datetime, date, timezone, timedelta
from swpt_accounts.models import Account

D_ID = -1
C_ID = 1


def test_configure_account():
    one_year = timedelta(days=365.25)
    current_ts = datetime.now(tz=timezone.utc)
    committed_at = current_ts - 2 * one_year
    account = Account(
        debtor_id=D_ID,
        creditor_id=C_ID,
        creation_date=date(1970, 1, 1),
        principal=1000,
        total_locked_amount=0,
        pending_transfers_count=0,
        last_transfer_id=0,
        status_flags=0,
        last_change_ts=current_ts,
        previous_interest_rate=0.0,
        last_interest_rate_change_ts=current_ts - one_year,
        interest_rate=10.0,
    )
    i = account.calc_due_interest(1000, committed_at, current_ts)
    assert abs(i - 100) < 1e-12

    i = account.calc_due_interest(-1000, committed_at, current_ts)
    assert abs(i + 100) < 1e-12

    assert account.calc_due_interest(1000, committed_at, committed_at) == 0
    assert account.calc_due_interest(1000, current_ts, current_ts) == 0
    assert account.calc_due_interest(1000, current_ts, committed_at) == 0

    i = account.calc_due_interest(1000, current_ts - timedelta(days=1), current_ts)
    assert abs(i - 0.26098) < 1e-3

    i = account.calc_due_interest(1000, committed_at, committed_at + timedelta(days=1))
    assert abs(i) == 0
