from base64 import b16decode
from typing import NamedTuple, Optional
from marshmallow import Schema, fields, validate, validates, ValidationError
from .models import INTEREST_RATE_FLOOR, INTEREST_RATE_CEIL, CONFIG_DATA_MAX_BYTES


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
