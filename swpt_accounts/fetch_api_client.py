import logging
import time
import asyncio
from collections import OrderedDict
from functools import partial
from urllib.parse import urljoin
from typing import Optional, Iterable, Dict, Tuple
import typing
import requests
from flask import current_app, url_for
from swpt_accounts.extensions import requests_session, aiohttp_session, asyncio_loop
from swpt_accounts.models import ROOT_CREDITOR_ID, RootConfigData
from swpt_accounts.schemas import parse_root_config_data

_fetch_conifg_path = partial(url_for, 'fetch.config', _external=False, creditorId=ROOT_CREDITOR_ID)
_root_config_data_lru_cache: typing.OrderedDict[int, Tuple[Optional[RootConfigData], float]] = OrderedDict()


def get_if_account_is_reachable(debtor_id: int, creditor_id: int) -> bool:
    with current_app.test_request_context():
        path = url_for('fetch.reachable', _external=False, debtorId=debtor_id, creditorId=creditor_id)

    url = urljoin(current_app.config['APP_FETCH_API_URL'], path)

    try:
        response = requests_session.get(url)
        status_code = response.status_code
        if status_code == 204:
            return True
        if status_code != 404:
            response.raise_for_status()  # pragma: no cover

    except requests.RequestException as e:
        _log_error(e)

    return False


def get_root_config_data_dict(
        debtor_ids: Iterable[int],
        cache_seconds: float = 7200.0) -> Dict[int, Optional[RootConfigData]]:

    cutoff_ts = time.time() - cache_seconds
    result_dict: Dict[int, Optional[RootConfigData]] = {debtor_id: None for debtor_id in debtor_ids}
    results = asyncio_loop.run_until_complete(_fetch_root_config_data_list(debtor_ids, cutoff_ts))

    for debtor_id, result in zip(debtor_ids, results):
        if isinstance(result, Exception):
            _log_error(result)
        else:
            result_dict[debtor_id] = result

    return result_dict


def _log_error(e):
    try:
        raise e
    except Exception:
        logger = logging.getLogger(__name__)
        logger.exception('Caught error while making a fetch request.')


async def _make_root_config_data_request(debtor_id: int) -> Optional[RootConfigData]:
    fetch_api_url = current_app.config['APP_FETCH_API_URL']
    url = urljoin(fetch_api_url, _fetch_conifg_path(debtorId=debtor_id))

    async with aiohttp_session.get(url) as response:
        status_code = response.status
        if status_code == 200:
            return parse_root_config_data(await response.text())
        if status_code == 404:
            return None

        raise RuntimeError(
            f'Got an unexpected status code ({status_code}) from fetch request.') from None  # pragma: no cover


def _clear_root_config_data() -> None:
    _root_config_data_lru_cache.clear()


def _lookup_root_config_data(debtor_id: int, cutoff_ts: float) -> Optional[RootConfigData]:
    config_data, ts = _root_config_data_lru_cache[debtor_id]
    if ts < cutoff_ts:
        raise KeyError

    return config_data


def _register_root_config_data(debtor_id: int, config_data: Optional[RootConfigData]) -> None:
    max_size = current_app.config['APP_FETCH_DATA_CACHE_SIZE']

    while len(_root_config_data_lru_cache) >= max_size:
        try:
            _root_config_data_lru_cache.popitem(last=False)
        except KeyError:  # pragma: nocover
            break

    _root_config_data_lru_cache[debtor_id] = (config_data, time.time())


async def _fetch_root_config_data(debtor_id: int, cutoff_ts: float) -> Optional[RootConfigData]:
    try:
        config_data = _lookup_root_config_data(debtor_id, cutoff_ts)
    except KeyError:
        config_data = await _make_root_config_data_request(debtor_id)
        _register_root_config_data(debtor_id, config_data)

    return config_data


async def _fetch_root_config_data_list(
        debtor_ids: Iterable[int],
        cutoff_ts: float) -> Iterable:

    with current_app.test_request_context():
        return await asyncio.gather(
            *(_fetch_root_config_data(debtor_id, cutoff_ts) for debtor_id in debtor_ids),
            return_exceptions=True,
        )
