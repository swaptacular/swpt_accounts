from datetime import datetime, timezone
import pytest
from swpt_accounts import procedures as p
from swpt_accounts import models as m

D_ID = -1
C_ID = 1


@pytest.fixture(scope='function')
def client(app, db_session):
    return app.test_client()


@pytest.fixture(scope='function')
def current_ts():
    return datetime.now(tz=timezone.utc)


@pytest.fixture(scope='function')
def account(app, db_session, current_ts):
    return p.configure_account(D_ID, C_ID, current_ts, 0)


def test_get_reachable(client, account, current_ts):
    r = client.post('/accounts/1/18446744073709551615/reachable', json={})
    assert r.status_code == 405
    r = client.get('/accounts/1/18446744073709551615/reachable')
    assert r.status_code == 204
    assert r.get_data() == b''
    r = client.get('/accounts/1/18446744073709551614/reachable')
    assert r.status_code == 404
    assert r.get_data() == b''
    r = client.get('/accounts/0/18446744073709551614/reachable')
    assert r.status_code == 204
    assert r.get_data() == b''

    p.configure_account(D_ID, C_ID, current_ts, 1, config_flags=m.Account.CONFIG_SCHEDULED_FOR_DELETION_FLAG)
    r = client.get('/accounts/1/18446744073709551615/reachable')
    assert r.status_code == 404
    assert r.get_data() == b''


def test_get_config(client, account, current_ts):
    r = client.post('/accounts/1/18446744073709551615/config', json={})
    assert r.status_code == 405

    r = client.get('/accounts/1/18446744073709551615/config')
    assert r.status_code == 200
    assert r.mimetype == 'text/plain'
    assert r.charset == 'utf-8'
    assert r.cache_control.max_age > 10000
    assert r.get_data() == b''

    r = client.get('/accounts/1/18446744073709551614/reachable')
    assert r.status_code == 404
    assert r.get_data() == b''
