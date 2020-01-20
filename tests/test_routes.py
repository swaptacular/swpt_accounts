import json
import pytest
from datetime import datetime, timezone
from swpt_accounts.procedures import configure_account, get_account


@pytest.fixture(scope='function')
def client(app, db_session):
    return app.test_client()


@pytest.fixture(scope='function')
def account():
    configure_account(1, 1, datetime.now(tz=timezone.utc), 0)
    return get_account(1, 1)


def test_get_accounts(client, account):
    r = client.get('/api/accounts/1/')
    assert r.status_code == 200
    assert r.content_type == 'application/json'
    accounts = json.loads(r.data)
    assert len(accounts) == 1
    assert accounts[0]['creditor_id'] == 1

    r = client.get('/api/accounts/1/?start_after=1&limit=100')
    assert r.status_code == 200
    assert r.content_type == 'application/json'
    assert len(json.loads(r.data)) == 0


def test_get_account(client, account):
    r = client.get('/api/accounts/1/666/')
    assert r.status_code == 404

    r = client.get('/api/accounts/1/1/')
    assert r.status_code == 200
    assert r.content_type == 'application/json'
    assert json.loads(r.data)['principal'] == account.principal
