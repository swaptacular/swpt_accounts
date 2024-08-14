import pytest
from datetime import datetime, timedelta, timezone, date
from sqlalchemy import text
from swpt_pythonlib.utils import date_to_int24
from swpt_accounts import models
from swpt_accounts import procedures as p


@pytest.fixture(scope="function")
def current_ts():
    return datetime.now(tz=timezone.utc)


D_ID = -1
C_ID = 1

ACCOUNT_DATA_FIELDS = """
    creditor_id,
    debtor_id,
    creation_date,
    last_change_seqnum,
    last_change_ts,
    principal,
    interest,
    interest_rate,
    previous_interest_rate,
    last_transfer_number,
    last_transfer_committed_at,
    status_flags,
    total_locked_amount,
    pending_transfers_count,
    last_transfer_id,
    last_heartbeat_ts,
    negligible_amount,
    config_data,
    last_interest_rate_change_ts,
    pending_account_update
"""


def test_calc_k(db_session):
    for rate in [0.0, 10.0, 100.0, -5.0, -99.9]:
        calc_k = (
            db_session.execute(
                text("SELECT calc_k(:interest_rate)"), {"interest_rate": rate}
            )
        ).scalar()
        assert calc_k == models.calc_k(rate)


def test_contain_principal_overflow(db_session):
    for n in [
            0,
            10,
            9223372036854775807,
            9223372036854775808,
            99999999999999999999999,
            -10,
            -9223372036854775807,
            -9223372036854775808,
            -99999999999999999999999,
    ]:
        contained_principal = (
            db_session.execute(
                text("SELECT contain_principal_overflow(:n)"), {"n": n}
            )
        ).scalar()
        assert contained_principal == models.contain_principal_overflow(n)


def test_calc_current_balance(db_session):
    ts = datetime.now(tz=timezone.utc)
    for (
            creditor_id,
            principal,
            interest,
            interest_rate,
            last_change_ts,
            current_ts,
    ) in [
            (C_ID, 0, 0.0, 0.0, ts, ts),
            (C_ID, 1_000_000, 0.0, 0.0, ts, ts),
            (C_ID, 1_000_000, 0.0, 0.0, ts - timedelta(days=365), ts),
            (C_ID, 1_000_000, 0.0, 10.0, ts - timedelta(days=365), ts),
            (C_ID, 1_000_000, 0.0, -10.0, ts - timedelta(days=365), ts),
            (C_ID, 1_000_000, 0.0, -10.0, ts, ts - timedelta(days=365)),
            (C_ID, 1_000_000, 0.0, 10.0, ts, ts - timedelta(days=365)),
            (0, 0, 1_000_000, 10.0, ts - timedelta(days=365), ts),
            (0, 0, 1_000_000, -10.0, ts - timedelta(days=365), ts),
            (C_ID, 1_000_000, 1e6, 10.0, ts - timedelta(days=365), ts),
            (C_ID, 1_000_000, 1e6, -10.0, ts - timedelta(days=365), ts),
            (C_ID, 1_000_000, -1e6, 10.0, ts - timedelta(days=365), ts),
            (C_ID, 1_000_000, -1e6, -10.0, ts - timedelta(days=365), ts),
            (C_ID, 1_000_000, -1e6, 10.0, ts - timedelta(days=365000), ts),
            (C_ID, 1_000_000, -1e6, -10.0, ts - timedelta(days=365000), ts),
    ]:
        calc_current_balance = (
            db_session.execute(
                text(
                    "SELECT calc_current_balance(:creditor_id, :principal,"
                    " :interest, :interest_rate, :last_change_ts, :current_ts)"
                ),
                {
                    "creditor_id": creditor_id,
                    "principal": principal,
                    "interest": interest,
                    "interest_rate": interest_rate,
                    "last_change_ts": last_change_ts,
                    "current_ts": current_ts
                },
            )
        ).scalar()
        assert abs(
            calc_current_balance - models.calc_current_balance(
                creditor_id=creditor_id,
                principal=principal,
                interest=interest,
                interest_rate=interest_rate,
                last_change_ts=last_change_ts,
                current_ts=current_ts,
            )
        ) < 5e-8


def test_lock_account(db_session, current_ts):
    p.configure_account(D_ID, C_ID, current_ts, 0)

    account = (
        db_session.execute(
            text("SELECT * FROM lock_account(:did, :cid)"),
            {"did": D_ID, "cid": C_ID},
        )
        .mappings()
        .one_or_none()
    )
    assert account
    assert account["creditor_id"] == C_ID
    assert account["debtor_id"] == D_ID

    account = (
        db_session.execute(
            text("SELECT * FROM lock_account(:did, :cid)"),
            {"did": 1234, "cid": 5678},
        )
        .mappings()
        .one_or_none()
    )
    assert account
    for k, v in account.items():
        assert v is None

    # Mark the account as "deleted".
    models.Account.query.update({"status_flags": 0b10001})
    account = (
        db_session.execute(
            text("SELECT * FROM lock_account(:did, :cid)"),
            {"did": D_ID, "cid": C_ID},
        )
        .mappings()
        .one_or_none()
    )
    assert account
    for k, v in account.items():
        assert v is None


def test_lock_or_create_account(db_session, current_ts):
    account = (
        db_session.execute(
            text(
                "SELECT * FROM lock_or_create_account("
                ":did, :cid, :current_ts)"
            ),
            {"did": D_ID, "cid": C_ID, "current_ts": current_ts},
        )
        .mappings()
        .one_or_none()
    )
    assert account
    assert account["creditor_id"] == C_ID
    assert account["debtor_id"] == D_ID
    assert account["creation_date"] == current_ts.date()
    assert account["last_change_ts"] == current_ts
    assert account["last_interest_rate_change_ts"] == models.T0
    assert account["last_transfer_id"] == (
        date_to_int24(account["creation_date"]) << 40
    )
    last_change_seqnum = account["last_change_seqnum"]
    assert isinstance(last_change_seqnum, int)

    aus = models.AccountUpdateSignal.query.one()
    assert aus.debtor_id == account.debtor_id
    assert aus.creditor_id == account.creditor_id
    assert aus.last_change_seqnum == account.last_change_seqnum
    assert aus.last_change_ts == account.last_change_ts
    assert aus.principal == account.principal
    assert aus.interest == account.interest
    assert aus.interest_rate == account.interest_rate
    assert (
        aus.last_interest_rate_change_ts
        == account.last_interest_rate_change_ts
    )
    assert aus.last_transfer_number == account.last_transfer_number
    assert aus.last_transfer_committed_at == account.last_transfer_committed_at
    assert aus.creation_date == account.creation_date
    assert aus.negligible_amount == account.negligible_amount
    assert aus.config_data == account.config_data
    assert aus.inserted_at == account.last_change_ts

    account = (
        db_session.execute(
            text(
                "SELECT * FROM lock_or_create_account("
                ":did, :cid, :current_ts)"
            ),
            {"did": D_ID, "cid": C_ID, "current_ts": current_ts},
        )
        .mappings()
        .one_or_none()
    )
    assert account
    assert account["creditor_id"] == C_ID
    assert account["debtor_id"] == D_ID
    assert account["creation_date"] == current_ts.date()
    assert account["last_change_ts"] == current_ts
    assert account["last_interest_rate_change_ts"] == models.T0
    assert account["last_transfer_id"] == (
        date_to_int24(account["creation_date"]) << 40
    )
    assert len(models.AccountUpdateSignal.query.all()) == 1

    models.Account.query.update(
        {
            "status_flags": 0b1,
            "pending_account_update": True,
        }
    )
    account = (
        db_session.execute(
            text(
                "SELECT * FROM lock_or_create_account("
                ":did, :cid, :current_ts)"
            ),
            {
                "did": D_ID,
                "cid": C_ID,
                "current_ts": current_ts + timedelta(days=1),
            },
        )
        .mappings()
        .one_or_none()
    )
    assert len(models.AccountUpdateSignal.query.all()) == 2
    assert account
    assert account["creditor_id"] == C_ID
    assert account["debtor_id"] == D_ID
    assert account["pending_account_update"] is False
    assert account["status_flags"] == 0
    assert account["last_change_ts"] == current_ts + timedelta(days=1)
    assert account["last_change_seqnum"] == last_change_seqnum + 1


@pytest.mark.parametrize("overflow", [True, False])
def test_apply_account_change(db_session, current_ts, overflow):
    p.configure_account(D_ID, C_ID, current_ts, 0)
    acc = (
        models.Account.query
        .filter_by(debtor_id=D_ID, creditor_id=C_ID)
        .one()
    )
    assert acc
    last_change_seqnum = acc.last_change_seqnum
    last_change_ts = acc.last_change_ts
    flags = acc.status_flags

    if overflow:
        acc.principal = models.MAX_INT64 - 999
        acc.last_change_seqnum = 0x7fffffff
    db_session.commit()

    account = (
        db_session.execute(
            text(
                "SELECT * FROM apply_account_change("
                "("
                "  SELECT a::account_data "
                f" FROM (SELECT {ACCOUNT_DATA_FIELDS} FROM account) a"
                "),"
                " :principal_delta,"
                " :interest_delta,"
                " :current_ts"
                ")"
            ),
            {
                "principal_delta": 1000,
                "interest_delta": 100.0,
                "current_ts": current_ts,
            },
        )
        .mappings()
        .one_or_none()
    )
    db_session.commit()
    acc = (
        models.Account.query
        .filter_by(debtor_id=D_ID, creditor_id=C_ID)
        .one()
    )
    assert acc

    assert account
    assert account["creditor_id"] == C_ID == acc.creditor_id
    assert account["debtor_id"] == D_ID == acc.debtor_id

    if overflow:
        ovrf = models.Account.STATUS_OVERFLOWN_FLAG
        assert account["principal"] == 0x7fffffffffffffff == acc.principal
        assert account["status_flags"] == flags | ovrf == acc.status_flags
        assert (
            account["last_change_seqnum"]
            == -0x80000000
            == acc.last_change_seqnum
        )
    else:
        assert account["principal"] == 1000 == acc.principal
        assert account["status_flags"] == flags == acc.status_flags
        assert (
            account["last_change_seqnum"]
            == last_change_seqnum + 1
            == acc.last_change_seqnum
        )

    assert account["interest"] == 100.0 == acc.interest
    assert (
        account["pending_account_update"]
        == bool(True)
        == acc.pending_account_update
    )
    assert (
        account["last_change_ts"]
        == max(last_change_ts, current_ts)
        == acc.last_change_ts
    )


def test_calc_status_code_sp(db_session, current_ts):
    from swpt_accounts.models import SC_OK

    def calc_status_code(
        committed_amount: int,
        expendable_amount: int,
        last_interest_rate_change_ts: datetime,
        current_ts: datetime,
    ) -> str:
        return (
            db_session.execute(
                text(
                    "SELECT calc_status_code("
                    " (SELECT p FROM prepared_transfer p),"
                    " :committed_amount,"
                    " :expendable_amount,"
                    " :last_interest_rate_change_ts,"
                    " :current_ts"
                    ")"
                ),
                {
                    "committed_amount": committed_amount,
                    "expendable_amount": expendable_amount,
                    "last_interest_rate_change_ts": (
                        last_interest_rate_change_ts
                    ),
                    "current_ts": current_ts,
                },
            )
            .scalar()
        )

    p.configure_account(D_ID, C_ID, current_ts, 0)
    p.configure_account(D_ID, 0, current_ts, 0)
    pt = models.PreparedTransfer(
        debtor_id=D_ID,
        sender_creditor_id=C_ID,
        transfer_id=1,
        coordinator_type="test",
        coordinator_id=11,
        coordinator_request_id=22,
        recipient_creditor_id=1,
        prepared_at=current_ts,
        final_interest_rate_ts=current_ts,
        demurrage_rate=-50,
        deadline=current_ts + timedelta(days=10000),
        locked_amount=1000,
    )
    db_session.add(pt)
    db_session.commit()

    assert (
        calc_status_code(
            1000, 0, current_ts + timedelta(minutes=1), current_ts
        ) != SC_OK
    )
    assert calc_status_code(1000, 0, current_ts, current_ts) == SC_OK
    assert (
        calc_status_code(
            1000, 0, current_ts, current_ts + timedelta(days=20000)
        )
    ) != SC_OK
    assert (
        calc_status_code(
            1000, 0, current_ts, current_ts - timedelta(days=10)
        ) == SC_OK
    )
    assert (
        calc_status_code(
            1000, 0, current_ts, current_ts + timedelta(days=10)
        )
        == SC_OK
    )
    assert (
        calc_status_code(1000, -1, current_ts, current_ts) == SC_OK
    )
    assert (
        calc_status_code(
            1000, -1, current_ts, current_ts + timedelta(seconds=1)
        ) != SC_OK
    )
    assert (
        calc_status_code(
            1000, -1, current_ts, current_ts - timedelta(days=10)
        ) == SC_OK
    )
    assert (
        calc_status_code(
            999, -5, current_ts, current_ts + timedelta(days=10)
        ) != SC_OK
    )
    assert (
        calc_status_code(
            995, -5, current_ts, current_ts + timedelta(days=10)
        ) == SC_OK
    )
    assert (
        calc_status_code(
            995, -50000, current_ts, current_ts + timedelta(days=10)
        ) != SC_OK
    )
    assert (
        calc_status_code(
            980, -50000, current_ts, current_ts + timedelta(days=10)
        ) == SC_OK
    )
    pt.recipient_creditor_id = 0
    db_session.commit()
    assert (
        calc_status_code(
            1000, -50000, current_ts, current_ts + timedelta(days=10)
        ) != SC_OK
    )
    pt.recipient_creditor_id = 1
    pt.sender_creditor_id = 0
    db_session.commit()
    assert (
        calc_status_code(
            1000, -50000, current_ts, current_ts + timedelta(days=10)
        ) == SC_OK
    )


def test_process_finalization_requests(db_session, current_ts):
    p.configure_account(D_ID, C_ID, current_ts, 0)
    p.configure_account(D_ID, 1234, current_ts, 0)
    q = models.Account.query.filter_by(debtor_id=D_ID, creditor_id=C_ID)
    last_transfer_number = q.one().last_transfer_number
    q.update({models.Account.principal: 100})
    p.prepare_transfer(
        coordinator_type="direct",
        coordinator_id=1,
        coordinator_request_id=2,
        min_locked_amount=1,
        max_locked_amount=200,
        debtor_id=D_ID,
        creditor_id=C_ID,
        recipient_creditor_id=1234,
        ts=current_ts,
    )
    p.process_transfer_requests(D_ID, C_ID)
    pt = models.PreparedTransfer.query.filter_by(
        debtor_id=D_ID, sender_creditor_id=C_ID
    ).one()
    p.finalize_transfer(D_ID, C_ID, pt.transfer_id, "direct", 1, 2, 40)

    db_session.execute(
        text(
            "SELECT process_finalization_requests("
            " :debtor_id, :sender_creditor_id, :ignore_all)"
        ),
        {
            "debtor_id": D_ID,
            "sender_creditor_id": C_ID,
            "ignore_all": False,
        },
    )

    assert len(models.PreparedTransfer.query.all()) == 0
    assert len(models.FinalizationRequest.query.all()) == 0
    assert len(models.PendingBalanceChangeSignal.query.all()) == 1
    assert len(models.AccountTransferSignal.query.all()) == 1

    account = (
        models.Account.query
        .filter_by(debtor_id=D_ID, creditor_id=C_ID)
        .one()
    )
    assert account.last_transfer_number == last_transfer_number + 1


def test_calc_due_interest(db_session, current_ts):
    one_year = timedelta(days=365.25)
    committed_at = current_ts - 2 * one_year
    account = models.Account(
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
    db_session.add(account)
    db_session.commit()

    def calc_due_interest(amount, due_ts, curr_ts):
        return (
            db_session.execute(
                text(
                    "SELECT * FROM calc_due_interest("
                    "("
                    "  SELECT a::account_data "
                    f" FROM (SELECT {ACCOUNT_DATA_FIELDS} FROM account) a"
                    "),"
                    " :amount,"
                    " :due_ts,"
                    " :current_ts"
                    ")"
                ),
                {
                    "amount": amount,
                    "due_ts": due_ts,
                    "current_ts": curr_ts,
                },
            )
            .scalar()
        )

    i = calc_due_interest(1000, committed_at, current_ts)
    assert abs(i - 100) < 1e-12

    i = calc_due_interest(-1000, committed_at, current_ts)
    assert abs(i + 100) < 1e-12

    assert calc_due_interest(1000, committed_at, committed_at) == 0
    assert calc_due_interest(1000, current_ts, current_ts) == 0
    assert calc_due_interest(1000, current_ts, committed_at) == 0

    i = calc_due_interest(
        1000, current_ts - timedelta(days=1), current_ts
    )
    assert abs(i - 0.26098) < 1e-3

    i = calc_due_interest(
        1000, committed_at, committed_at + timedelta(days=1)
    )
    assert abs(i) == 0
