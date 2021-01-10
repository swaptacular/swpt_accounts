import logging
import time
import asyncio
from functools import partial
from urllib.parse import urljoin
from typing import Optional, Iterable, Dict, List, Union
import requests
from async_lru import alru_cache
from flask import current_app, url_for
from .extensions import requests_session, aiohttp_session, asyncio_loop
from .models import ROOT_CREDITOR_ID
from .schemas import RootConfigData, parse_root_config_data

_fetch_conifg_path = partial(url_for, 'fetch.config', _external=False, creditorId=ROOT_CREDITOR_ID)


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


def get_root_config_data_dict(debtor_ids: Iterable[int]) -> Dict[int, Optional[RootConfigData]]:
    result_dict: Dict[int, Optional[RootConfigData]] = {debtor_id: None for debtor_id in debtor_ids}
    results = asyncio_loop.run_until_complete(_fetch_root_config_data_list(debtor_ids))

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


@alru_cache(maxsize=50000, cache_exceptions=False)
async def _fetch_root_config_data(debtor_id: int, ttl_hash: int) -> Optional[RootConfigData]:
    fetch_api_url = current_app.config['APP_FETCH_API_URL']
    url = urljoin(fetch_api_url, _fetch_conifg_path(debtorId=debtor_id))

    async with aiohttp_session.get(url) as response:
        status_code = response.status
        if status_code == 200:
            return parse_root_config_data(await response.text())
        if status_code == 404:
            return None

        raise RuntimeError(f'Got an unexpected status code ({status_code}) from fetch request.')  # pragma: no cover


def _get_ttl_hash(debtor_id: int) -> int:
    # For each `debtor_id`, every two hours, we produce a different
    # "ttl_hash"` value, which invalidates the entry for the given
    # debtor in `_fetch_root_config_data()`'s LRU cache. Note that the
    # cache entries for different debtors are invalidated at different
    # times, which prevents a momentary total invalidation of the
    # cache.

    max_retention_seconds = 7200
    variation = (debtor_id * 2654435761) % max_retention_seconds  # a pseudo-random number
    ttl_hash = int((time.time() + variation) / max_retention_seconds)
    return ttl_hash


async def _fetch_root_config_data_list(debtor_ids: Iterable[int]) -> List[Union[RootConfigData, Exception]]:
    with current_app.test_request_context():
        return await asyncio.gather(
            *(_fetch_root_config_data(debtor_id, _get_ttl_hash(debtor_id)) for debtor_id in debtor_ids),
            return_exceptions=True,
        )
