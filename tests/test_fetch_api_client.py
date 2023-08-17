import json
import pytest
import logging
from datetime import datetime, timezone
from flask import current_app
from swpt_accounts.models import RootConfigData
from swpt_accounts.fetch_api_client import (
    parse_root_config_data,
    get_root_config_data_dict,
    get_if_account_is_reachable,
)

D_ID = -1
C_ID = 1


def test_parse_root_config_data():
    default = RootConfigData()
    assert default.interest_rate_target == 0.0
    assert default.info_content_type is None
    assert default.info_iri is None
    assert default.info_sha256 is None
    assert default.issuing_limit == 9223372036854775807

    assert RootConfigData().interest_rate_target == 0.0
    assert parse_root_config_data("") == RootConfigData()
    assert parse_root_config_data("{}") == RootConfigData()
    assert parse_root_config_data('{"rate": 99.5}') == RootConfigData(99.5)
    assert parse_root_config_data('{"rate": -49.0}') == RootConfigData(-49.0)
    assert parse_root_config_data(
        '{"type": "RootConfigData", "rate": 0.0}'
    ) == RootConfigData(0.0)

    with pytest.raises(
        ValueError, match="invalid root config data: 'NOT JSON'"
    ):
        parse_root_config_data("NOT JSON")

    with pytest.raises(
        ValueError, match="invalid root config data: '{\"rate\": NaN}'"
    ):
        parse_root_config_data('{"rate": NaN}')

    with pytest.raises(
        ValueError, match="invalid root config data: '{\"rate\": -51}'"
    ):
        parse_root_config_data('{"rate": -51}')

    with pytest.raises(ValueError):
        parse_root_config_data('{"rate": 101}')

    with pytest.raises(ValueError):
        parse_root_config_data('{"type": "INVALID_TYPE", "rate": 0.0}')

    with pytest.raises(ValueError):
        parse_root_config_data('{"info": {"iri": "%s"}}' % (201 * "x"))

    with pytest.raises(ValueError):
        parse_root_config_data(
            '{"info": {"iri": "x", "contentType": "%s"}}' % (101 * "x")
        )

    with pytest.raises(ValueError):
        parse_root_config_data('{"info": {"iri": "x", "contentType": "Щ"}}')

    with pytest.raises(ValueError):
        parse_root_config_data('{"limit": -1}')

    with pytest.raises(ValueError):
        parse_root_config_data('{"limit": 9223372036854775808}')

    assert parse_root_config_data(
        '{"info": {"iri": "http://example.com"}}'
    ) == RootConfigData(0.0, "http://example.com")

    data = parse_root_config_data(
        json.dumps(
            {
                "rate": 1.0,
                "info": {
                    "iri": "http://example.com",
                    "sha256": 32 * "20",
                    "contentType": "text/plain",
                },
                "limit": 1000,
            }
        )
    )
    assert data == RootConfigData(
        1.0, "http://example.com", 32 * b" ", "text/plain", 1000
    )


def test_get_root_config_data_dict(app):
    assert get_root_config_data_dict(range(1, 12)) == {
        i: None for i in range(1, 12)
    }
    assert get_root_config_data_dict(range(1, 12), cache_seconds=-1e6) == {
        i: None for i in range(1, 12)
    }


def test_get_if_account_is_reachable(app, db_session, caplog):
    from swpt_accounts import procedures as p

    app_fetch_api_url = current_app.config["FETCH_API_URL"]
    current_ts = datetime.now(tz=timezone.utc)
    p.configure_account(D_ID, C_ID, current_ts, 0)
    assert get_if_account_is_reachable(D_ID, C_ID)
    assert not get_if_account_is_reachable(666, C_ID)

    current_app.config["FETCH_API_URL"] = "localhost:1111"
    with caplog.at_level(logging.ERROR):
        assert not get_if_account_is_reachable(D_ID, C_ID)
        assert ["Caught error while making a fetch request."] == [
            rec.message for rec in caplog.records
        ]
    current_app.config["FETCH_API_URL"] = app_fetch_api_url


def test_get_root_account_config_data(app, db_session, caplog):
    from swpt_accounts import procedures as p
    from swpt_accounts.fetch_api_client import _clear_root_config_data

    app_fetch_api_url = current_app.config["FETCH_API_URL"]

    current_ts = datetime.now(tz=timezone.utc)
    p.configure_account(
        D_ID, p.ROOT_CREDITOR_ID, current_ts, 0, config_data='{"rate": 2.0}'
    )
    assert get_root_config_data_dict([D_ID, 666]) == {
        D_ID: RootConfigData(2.0),
        666: None,
    }

    current_app.config["FETCH_API_URL"] = "localhost:1111"
    with caplog.at_level(logging.ERROR):
        assert get_root_config_data_dict([777]) == {777: None}
        assert ["Caught error while making a fetch request."] == [
            rec.message for rec in caplog.records
        ]

    current_app.config["FETCH_API_URL"] = app_fetch_api_url
    caplog.clear()
    with caplog.at_level(logging.ERROR):
        assert get_root_config_data_dict([D_ID, 666, 777]) == {
            D_ID: RootConfigData(2.0),
            666: None,
            777: None,
        }
        assert len(caplog.records) == 0

    _clear_root_config_data()
