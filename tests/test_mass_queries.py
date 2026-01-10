import pytest
from datetime import datetime, timezone, timedelta
from swpt_accounts.extensions import db
from sqlalchemy.sql.expression import tuple_, null, true, or_
from swpt_accounts.models import (
    Account,
    PreparedTransfer,
    RegisteredBalanceChange,
)


@pytest.fixture(scope="function")
def current_ts():
    return datetime.now(tz=timezone.utc)


@pytest.mark.skip
def test_registered_balance_change_mass_delete(db_session, current_ts):
    n = 7500
    table = RegisteredBalanceChange.__table__
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
    db.session.execute(
        table.delete().where(pk.in_(pks_to_delete))
    )
    db.session.commit()
    assert len(RegisteredBalanceChange.query.all()) == 0


@pytest.mark.skip
def test_prepared_transfer_mass_update(db_session, current_ts):
    n = 7500
    pk = tuple_(
        PreparedTransfer.debtor_id,
        PreparedTransfer.sender_creditor_id,
        PreparedTransfer.transfer_id,
    )
    db.session.add(
        Account(
            debtor_id=1,
            creditor_id=2,
            creation_date=current_ts.date(),
        )
    )
    db.session.flush()

    pks_to_remind = [
        (1, 2, i) for i in range(1, n + 1)
    ]
    for t in pks_to_remind:
        db.session.add(
            PreparedTransfer(
                debtor_id=t[0],
                sender_creditor_id=t[1],
                transfer_id=t[2],
                coordinator_type='direct',
                coordinator_id=1234,
                coordinator_request_id=t[2],
                recipient_creditor_id=t[2],
                final_interest_rate_ts=current_ts,
                demurrage_rate=0.0,
                deadline=current_ts,
                locked_amount=0,
                last_reminder_ts=None,
            )
        )
    db.session.commit()
    to_update = (
        db.session.query(
            PreparedTransfer.debtor_id,
            PreparedTransfer.sender_creditor_id,
            PreparedTransfer.transfer_id,
        )
        .filter(pk.in_(pks_to_remind))
        .with_for_update(skip_locked=True, key_share=True)
        .all()
    )
    pks_to_update = {pk for pk in to_update}
    PreparedTransfer.query.filter(
        pk.in_(pks_to_update)
    ).update(
        {
            PreparedTransfer.last_reminder_ts: current_ts,
        },
        synchronize_session=False,
    )
    db.session.commit()
    assert len(
        PreparedTransfer.query
        .filter(PreparedTransfer.last_reminder_ts == null())
        .all()
    ) == 0


@pytest.mark.skip
def test_account_mass_update(db_session, current_ts):
    n = 7500
    pk = tuple_(Account.debtor_id, Account.creditor_id)
    pks_to_heartbeat = [
        (i, -i) for i in range(n)
    ]
    for t in pks_to_heartbeat:
        db.session.add(
            Account(
                debtor_id=t[0],
                creditor_id=t[1],
                creation_date=current_ts.date(),
                pending_account_update=True,
            )
        )
    db.session.commit()
    heartbeat_cutoff_ts = current_ts + timedelta(days=100)
    to_heartbeat = (
        Account.query
        .filter(
            pk.in_(pks_to_heartbeat),
            Account.status_flags.op("&")(Account.STATUS_DELETED_FLAG) == 0,
            or_(
                Account.last_heartbeat_ts < heartbeat_cutoff_ts,
                Account.pending_account_update == true(),
            ),
        )
        .with_for_update(skip_locked=True, key_share=True)
        .all()
    )
    pks_to_remind = [
        (account.debtor_id, account.creditor_id)
        for account in to_heartbeat
    ]
    Account.query.filter(pk.in_(pks_to_remind)).update(
        {
            Account.last_heartbeat_ts: current_ts,
            Account.pending_account_update: False,
        },
        synchronize_session=False,
    )
    db.session.commit()
    assert len(
        Account.query
        .filter(Account.pending_account_update == true())
        .all()
    ) == 0
