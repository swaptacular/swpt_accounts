import pytest
from datetime import datetime, timedelta, timezone
from sqlalchemy import text
from swpt_pythonlib.utils import date_to_int24
from swpt_accounts import models
from swpt_accounts import procedures as p


@pytest.fixture(scope="function")
def current_ts():
    return datetime.now(tz=timezone.utc)


D_ID = -1
C_ID = 1


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
    assert aus.last_config_ts == account.last_config_ts
    assert aus.last_config_seqnum == account.last_config_seqnum
    assert aus.creation_date == account.creation_date
    assert aus.negligible_amount == account.negligible_amount
    assert aus.config_data == account.config_data
    assert aus.config_flags == account.config_flags
    assert aus.debtor_info_iri == account.debtor_info_iri
    assert aus.debtor_info_content_type == account.debtor_info_content_type
    assert aus.debtor_info_sha256 == account.debtor_info_sha256
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