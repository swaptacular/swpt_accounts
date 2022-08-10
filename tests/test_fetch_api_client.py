import json
import pytest
from swpt_accounts.models import RootConfigData
from swpt_accounts.fetch_api_client import parse_root_config_data, get_root_config_data_dict


def test_parse_root_config_data():
    assert RootConfigData().interest_rate_target == 0.0
    assert parse_root_config_data('') == RootConfigData()
    assert parse_root_config_data('{}') == RootConfigData()
    assert parse_root_config_data('{"rate": 99.5}') == RootConfigData(99.5)
    assert parse_root_config_data('{"rate": -49.0}') == RootConfigData(-49.0)
    assert parse_root_config_data('{"type": "RootConfigData", "rate": 0.0}') == RootConfigData(0.0)

    with pytest.raises(ValueError, match="invalid root config data: 'NOT JSON'"):
        parse_root_config_data('NOT JSON')

    with pytest.raises(ValueError, match="invalid root config data: '{\"rate\": NaN}'"):
        parse_root_config_data('{"rate": NaN}')

    with pytest.raises(ValueError, match="invalid root config data: '{\"rate\": -51}'"):
        parse_root_config_data('{"rate": -51}')

    with pytest.raises(ValueError):
        parse_root_config_data('{"rate": 101}')

    with pytest.raises(ValueError):
        parse_root_config_data('{"type": "INVALID_TYPE", "rate": 0.0}')

    with pytest.raises(ValueError):
        parse_root_config_data('{"info": {"iri": "%s"}}' % (201 * 'x'))

    with pytest.raises(ValueError):
        parse_root_config_data('{"info": {"iri": "x", "contentType": "%s"}}' % (101 * 'x'))

    with pytest.raises(ValueError):
        parse_root_config_data('{"info": {"iri": "x", "contentType": "Щ"}}')

    assert parse_root_config_data('{"info": {"iri": "http://example.com"}}') == RootConfigData(
        0.0, 'http://example.com')

    data = parse_root_config_data(json.dumps({
        'rate': 1.0,
        'info': {
            'iri': 'http://example.com',
            'sha256': 32 * '20',
            'contentType': 'text/plain',
        },
    }))
    assert data == RootConfigData(1.0, 'http://example.com', 32 * b' ', 'text/plain')


def test_get_root_config_data_dict(app):
    assert get_root_config_data_dict(range(1, 12)) == {i: None for i in range(1, 12)}
    assert get_root_config_data_dict(range(1, 12), cache_seconds=-1e6) == {i: None for i in range(1, 12)}
