from base64 import b16decode
from marshmallow import (
    Schema,
    fields,
    validate,
    validates,
    ValidationError,
    EXCLUDE,
)
from swpt_accounts.models import (
    INTEREST_RATE_FLOOR,
    INTEREST_RATE_CEIL,
    MIN_INT64,
    MAX_INT64,
    IRI_MAX_LENGTH,
    CONTENT_TYPE_MAX_BYTES,
    DEBTOR_INFO_SHA256_REGEX,
    RootConfigData,
)


class ValidateTypeMixin:
    @validates("type")
    def validate_type(self, value):
        if f"{value}Schema" != type(self).__name__:
            raise ValidationError("Invalid type.")


class DebtorInfoSchema(ValidateTypeMixin, Schema):
    type = fields.String(
        load_default="DebtorInfo",
        dump_default="DebtorInfo",
        metadata=dict(
            description="The type of this object.",
            example="DebtorInfo",
        ),
    )
    iri = fields.String(
        required=True,
        validate=validate.Length(max=200),
        metadata=dict(
            format="iri",
            description=(
                "A link (Internationalized Resource Identifier) referring to a"
                " document containing information about the debtor."
            ),
            example="https://example.com/debtors/1/",
        ),
    )
    optional_content_type = fields.String(
        validate=validate.Length(max=100),
        data_key="contentType",
        metadata=dict(
            description=(
                "Optional MIME type of the document that the `iri` field"
                " refers to."
            ),
            example="text/html",
        ),
    )
    optional_sha256 = fields.String(
        validate=validate.Regexp("^[0-9A-F]{64}$"),
        data_key="sha256",
        metadata=dict(
            description=(
                "Optional SHA-256 cryptographic hash (Base16 encoded) of the"
                " content of the document that the `iri` field refers to."
            ),
            example=(
                "E3B0C44298FC1C149AFBF4C8996FB924"
                "27AE41E4649B934CA495991B7852B855"
            ),
        ),
    )

    @validates("optional_content_type")
    def validate_content_type(self, value):
        if not value.isascii():
            raise ValidationError("Non-ASCII symbols are not allowed.")


class RootConfigDataSchema(ValidateTypeMixin, Schema):
    type = fields.String(
        load_default="RootConfigData",
        dump_default="RootConfigData",
        metadata=dict(
            description="The type of this object.",
            example="RootConfigData",
        ),
    )
    interest_rate_target = fields.Float(
        load_default=0.0,
        validate=validate.Range(
            min=INTEREST_RATE_FLOOR, max=INTEREST_RATE_CEIL
        ),
        data_key="rate",
        metadata=dict(
            description=(
                "The annual rate (in percents) at which the debtor wants the"
                " interest to accumulate on creditors' accounts. The actual"
                " current interest rate may be different if interest rate"
                " limits are being enforced."
            ),
            example=0.0,
        ),
    )
    optional_info = fields.Nested(
        DebtorInfoSchema,
        data_key="info",
        metadata=dict(
            description="Optional `DebtorInfo`.",
        ),
    )


class ValidateChoreMessageMixin:
    class Meta:
        unknown = EXCLUDE

    type = fields.String(required=True)
    debtor_id = fields.Integer(
        required=True, validate=validate.Range(min=MIN_INT64, max=MAX_INT64)
    )
    creditor_id = fields.Integer(
        required=True, validate=validate.Range(min=MIN_INT64, max=MAX_INT64)
    )

    @validates("type")
    def validate_type(self, value):
        if f"{value}MessageSchema" != type(self).__name__:
            raise ValidationError("Invalid type.")


class ChangeInterestRateMessageSchema(ValidateChoreMessageMixin, Schema):
    """``ChangeInterestRate`` message schema."""

    interest_rate = fields.Float(required=True)
    ts = fields.DateTime(required=True)


class UpdateDebtorInfoMessageSchema(ValidateChoreMessageMixin, Schema):
    """``UpdateDebtorInfo`` message schema."""

    debtor_info_iri = fields.String(
        required=True, validate=validate.Length(max=IRI_MAX_LENGTH)
    )
    debtor_info_content_type = fields.String(
        required=True, validate=validate.Length(max=CONTENT_TYPE_MAX_BYTES)
    )
    debtor_info_sha256 = fields.String(
        required=True, validate=validate.Regexp(DEBTOR_INFO_SHA256_REGEX)
    )
    ts = fields.DateTime(required=True)

    @validates("debtor_info_content_type")
    def validate_debtor_info_content_type(self, value):
        if not value.isascii():
            raise ValidationError(
                "The debtor_info_content_type field contains non-ASCII"
                " characters."
            )


class CapitalizeInterestMessageSchema(ValidateChoreMessageMixin, Schema):
    """``CapitalizeInterest`` message schema."""


class TryToDeleteAccountMessageSchema(ValidateChoreMessageMixin, Schema):
    """``TryToDeleteAccount`` message schema."""


_ROOT_CONFIG_DATA_SCHEMA = RootConfigDataSchema()


def parse_root_config_data(config_data: str) -> RootConfigData:
    if config_data == "":
        return RootConfigData()

    try:
        data = _ROOT_CONFIG_DATA_SCHEMA.loads(config_data)
    except (ValueError, ValidationError):
        raise ValueError(f"invalid root config data: '{config_data}'")

    interest_rate_target = data["interest_rate_target"]
    info = data.get("optional_info")
    if info:
        optional_sha256 = info.get("optional_sha256")
        info_iri = info["iri"]
        info_sha256 = optional_sha256 and b16decode(optional_sha256)
        info_content_type = info.get("optional_content_type")
    else:
        info_iri = None
        info_sha256 = None
        info_content_type = None

    return RootConfigData(
        interest_rate_target, info_iri, info_sha256, info_content_type
    )
