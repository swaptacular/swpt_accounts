import json
import pytest
from swpt_accounts.procedures import get_or_create_account


@pytest.fixture(scope='function')
def client(app, db_session):
    return app.test_client()


@pytest.fixture(scope='function')
def account():
    return get_or_create_account(1, 1)


def test_get_accounts(client, account):
    r = client.get('/v1/borrowers/1/accounts?limit=100')
    assert r.status_code == 200
    assert r.content_type == 'application/json'
    accounts = json.loads(r.data)
    assert 'self' in accounts
    assert accounts['self'].endswith('/v1/borrowers/1/accounts?limit=100')
    assert 'contents' in accounts
    contents = accounts['contents']
    assert len(contents) == 1
    assert contents[0]['creditor_id'] == 1
    assert 'self' in contents[0]
    assert contents[0]['self'] == '1'

    r = client.get('/v1/borrowers/1/accounts?start_after=1&limit=100')
    assert r.status_code == 200
    assert r.content_type == 'application/json'
    assert len(json.loads(r.data)['contents']) == 0


def test_get_account(client, account):
    r = client.get('/v1/borrowers/1/accounts/666')
    assert r.status_code == 404

    r = client.get('/v1/borrowers/1/accounts/1')
    assert r.status_code == 200
    assert r.content_type == 'application/json'
    a = json.loads(r.data)
    assert 'self' in a
    assert a['self'].endswith('/v1/borrowers/1/accounts/1')
    assert a['principal'] == account.principal
