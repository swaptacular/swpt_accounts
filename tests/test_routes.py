import json
import pytest
from swpt_accounts.procedures import get_or_create_account


@pytest.fixture(scope='function')
def client(app, db_session):
    return app.test_client()


@pytest.fixture(scope='function')
def account():
    return get_or_create_account(1, 1)


def test_get_account(client, account):
    r = client.get('/api/accounts/1/666/')
    assert r.status_code == 404

    r = client.get('/api/accounts/1/1/')
    assert r.status_code == 200
    assert r.content_type == 'application/json'
    assert json.loads(r.data)['principal'] == account.principal


def test_delete_account(client, account):
    r = client.delete('/api/accounts/1/1/')
    assert r.status_code == 202
    r = client.get('/api/accounts/1/1/')
    assert r.status_code == 404
