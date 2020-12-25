import logging
import asyncio
from functools import partial
from urllib.parse import urljoin
from base64 import b16decode
from typing import NamedTuple, Optional, Iterable, Dict, List, Union
import requests
from marshmallow import Schema, fields, validate, validates, ValidationError
from flask import current_app, url_for
from .extensions import requests_session, aiohttp_session, asyncio_loop
from .models import INTEREST_RATE_FLOOR, INTEREST_RATE_CEIL, ROOT_CREDITOR_ID


class ValidateTypeMixin:
    @validates('type')
    def validate_type(self, value):
        if f'{value}Schema' != type(self).__name__:
            raise ValidationError('Invalid type.')


class DebtorInfoSchema(ValidateTypeMixin, Schema):
    type = fields.String(
        missing='DebtorInfo',
        default='DebtorInfo',
        description='The type of this object.',
        example='DebtorInfo',
    )
    iri = fields.String(
        required=True,
        validate=validate.Length(max=200),
        format='iri',
        description='A link (Internationalized Resource Identifier) referring to a document '
                    'containing information about the debtor.',
        example='https://example.com/debtors/1/',
    )
    optional_content_type = fields.String(
        validate=validate.Length(max=100),
        data_key='contentType',
        description='Optional MIME type of the document that the `iri` field refers to.',
        example='text/html',
    )
    optional_sha256 = fields.String(
        validate=validate.Regexp('^[0-9A-F]{64}$'),
        data_key='sha256',
        description='Optional SHA-256 cryptographic hash (Base16 encoded) of the content of '
                    'the document that the `iri` field refers to.',
        example='E3B0C44298FC1C149AFBF4C8996FB92427AE41E4649B934CA495991B7852B855',
    )

    @validates('optional_content_type')
    def validate_content_type(self, value):
        if not value.isascii():
            raise ValidationError('Non-ASCII symbols are not allowed.')


class RootConfigDataSchema(ValidateTypeMixin, Schema):
    type = fields.String(
        missing='RootConfigData',
        default='RootConfigData',
        description='The type of this object.',
        example='RootConfigData',
    )
    interest_rate_target = fields.Float(
        missing=0.0,
        validate=validate.Range(min=INTEREST_RATE_FLOOR, max=INTEREST_RATE_CEIL),
        data_key='rate',
        description='The annual rate (in percents) at which the debtor wants the interest '
                    'to accumulate on creditors\' accounts. The actual current interest rate may '
                    'be different if interest rate limits are being enforced.',
        example=0.0,
    )
    optional_info = fields.Nested(
        DebtorInfoSchema,
        data_key='info',
        description='Optional `DebtorInfo`.',
    )


class RootConfigData(NamedTuple):
    interest_rate_target: float = 0.0
    info_iri: Optional[str] = None
    info_sha256: Optional[bytes] = None
    info_content_type: Optional[str] = None

    @property
    def interest_rate(self):
        # NOTE: Interest rate limits might be implemented in the future.

        return self.interest_rate_target


_root_config_data_schema = RootConfigDataSchema()
_fetch_conifg_path = partial(url_for, 'fetch.config', _external=False, creditorId=ROOT_CREDITOR_ID)


def parse_root_config_data(config_data: str) -> RootConfigData:
    if config_data == '':
        return RootConfigData()

    try:
        data = _root_config_data_schema.loads(config_data)
    except ValidationError:
        raise ValueError from None

    interest_rate_target = data['interest_rate_target']
    info = data.get('optional_info')
    if info:
        optional_sha256 = info.get('optional_sha256')
        info_iri = info['iri']
        info_sha256 = optional_sha256 and b16decode(optional_sha256)
        info_content_type = info.get('optional_content_type')
    else:
        info_iri = None
        info_sha256 = None
        info_content_type = None

    return RootConfigData(interest_rate_target, info_iri, info_sha256, info_content_type)


def get_if_account_is_reachable(debtor_id: int, creditor_id: int) -> bool:
    with current_app.test_request_context():
        path = url_for('fetch.reachable', _external=False, debtorId=debtor_id, creditorId=creditor_id)

    url = urljoin(current_app.config['APP_FETCH_API_URL'], path)

    try:
        response = requests_session.get(url)
        status_code = response.status_code
        if status_code == 204:
            return True
        if status_code != 404:  # pragma: no cover
            response.raise_for_status()

    except requests.RequestException:  # pragma: no cover
        logger = logging.getLogger(__name__)
        logger.exception('Caught error while making a fetch request.')

    return False


def get_root_config_data_dict(debtor_ids: Iterable[int]) -> Dict[int, Optional[str]]:
    result_dict = {debtor_id: None for debtor_id in debtor_ids}
    results = asyncio_loop.run_until_complete(_fetch_root_config_data_list(debtor_ids))

    for debtor_id, result in zip(debtor_ids, results):
        if isinstance(result, Exception):  # pragma: no cover
            _log_error(result)
        else:
            result_dict[debtor_id] = result

    return result_dict


def _log_error(e):  # pragma: no cover
    try:
        raise e
    except Exception:
        logger = logging.getLogger(__name__)
        logger.exception('Caught error while making a fetch request.')


async def _fetch_root_config_data_list(debtor_ids: Iterable[int]) -> List[Union[str, Exception]]:
    fetch_api_url = current_app.config['APP_FETCH_API_URL']
    with current_app.test_request_context():
        paths = {debtor_id: _fetch_conifg_path(debtorId=debtor_id) for debtor_id in debtor_ids}

    async def fetch(debtor_id: int) -> str:
        url = urljoin(fetch_api_url, paths[debtor_id])

        async with aiohttp_session.get(url) as response:
            status_code = response.status
            if status_code == 200:
                return await response.text()
            if status_code == 404:
                return None

            raise RuntimeError(f'Got an unexpected status code ({status_code}) from fetch request.')  # pragma: no cover

    return await asyncio.gather(
        *(fetch(debtor_id) for debtor_id in debtor_ids),
        return_exceptions=True,
    )
