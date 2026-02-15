import pytest
from datetime import datetime, timezone
from swpt_accounts.extensions import db
from sqlalchemy import delete
from sqlalchemy.sql.expression import tuple_
from swpt_accounts.models import RegisteredBalanceChange


@pytest.fixture(scope="function")
def current_ts():
    return datetime.now(tz=timezone.utc)


@pytest.mark.skip
def test_registered_balance_change_mass_delete(db_session, current_ts):
    n = 7500
    pk = tuple_(
        RegisteredBalanceChange.debtor_id,
        RegisteredBalanceChange.other_creditor_id,
        RegisteredBalanceChange.change_id,
    )
    pks_to_delete = [
        (i, -i, 2 * i) for i in range(n)
    ]
    for t in pks_to_delete:
        db.session.add(
            RegisteredBalanceChange(
                debtor_id=t[0],
                other_creditor_id=t[1],
                change_id=t[2],
                committed_at=current_ts,
            )
        )
    db.session.commit()
    pks_to_delete.pop()
    chosen = RegisteredBalanceChange.choose_rows(pks_to_delete)
    db.session.execute(
        delete(RegisteredBalanceChange)
        .execution_options(synchronize_session=False)
        .where(pk == tuple_(*chosen.c))
    )
    db.session.commit()
    assert len(RegisteredBalanceChange.query.all()) == 1
