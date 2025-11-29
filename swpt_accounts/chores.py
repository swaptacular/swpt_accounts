import logging
import json
from base64 import b16decode
from datetime import datetime, timedelta
from marshmallow import ValidationError
from flask import current_app
from swpt_pythonlib import rabbitmq
from swpt_accounts.extensions import db
from swpt_accounts.models import SECONDS_IN_DAY, is_valid_account
from swpt_accounts import procedures
from swpt_accounts import schemas


def _on_change_interest_rate(
    debtor_id: int,
    creditor_id: int,
    interest_rate: float,
    ts: datetime,
    *args,
    **kwargs
) -> None:
    """Try to change the interest rate on the account.

    The interest rate will not be changed if the request is too old,
    or not enough time has passed since the previous change in the
    interest rate.

    """

    procedures.change_interest_rate(
        debtor_id=debtor_id,
        creditor_id=creditor_id,
        interest_rate=interest_rate,
        ts=ts,
        message_max_delay_seconds=current_app.config[
            "APP_MESSAGE_MAX_DELAY_DAYS"
        ]
        * SECONDS_IN_DAY,
    )


def _on_update_debtor_info(
    debtor_id: int,
    creditor_id: int,
    debtor_info_iri: str,
    debtor_info_content_type: str,
    debtor_info_sha256: str,
    ts: datetime,
    *args,
    **kwargs
) -> None:
    """Update the information about the debtor on a given the account.

    The information about the debtor will not be updated if the
    request is too old.

    """

    procedures.update_debtor_info(
        debtor_id=debtor_id,
        creditor_id=creditor_id,
        debtor_info_iri=debtor_info_iri or None,
        debtor_info_content_type=debtor_info_content_type or None,
        debtor_info_sha256=(
            b16decode(debtor_info_sha256) if debtor_info_sha256 else None
        ),
        ts=ts,
    )


def _on_capitalize_interest(
    debtor_id: int, creditor_id: int, *args, **kwargs
) -> None:
    """Add the interest accumulated on the account to the principal.

    Does nothing if not enough time has passed since the previous
    interest capitalization.

    """

    procedures.capitalize_interest(
        debtor_id=debtor_id,
        creditor_id=creditor_id,
        min_capitalization_interval=timedelta(
            days=current_app.config["APP_MIN_INTEREST_CAPITALIZATION_DAYS"]
        ),
    )


def _on_try_to_delete_account(
    debtor_id: int, creditor_id: int, *args, **kwargs
) -> None:
    """Mark the account as deleted, if possible.

    If it is a "normal" account, it will be marked as deleted if it
    has been scheduled for deletion, there are no prepared transfers,
    and the current balance is not bigger than `max(2.0,
    account.negligible_amount)`.

    The debtor's account will be marked as deleted if it has been scheduled
    for deletion, there are no prepared transfers, and the remaining
    principal is exactly zero.

    Note that when a "normal" account has been successfully marked as
    deleted, it could be "resurrected" (with "scheduled for deletion"
    configuration flag) by a delayed incoming transfer. Therefore,
    this function does not guarantee that the account will be marked
    as deleted successfully, or that it will "stay" deleted for
    long. To achieve a reliable deletion, this function may need to be
    called repeatedly, until the account has been purged from the
    database.

    """

    procedures.try_to_delete_account(debtor_id, creditor_id)


_LOGGER = logging.getLogger(__name__)

_MESSAGE_TYPES = {
    "ChangeInterestRate": (
        schemas.ChangeInterestRateMessageSchema(),
        _on_change_interest_rate,
    ),
    "UpdateDebtorInfo": (
        schemas.UpdateDebtorInfoMessageSchema(),
        _on_update_debtor_info,
    ),
    "CapitalizeInterest": (
        schemas.CapitalizeInterestMessageSchema(),
        _on_capitalize_interest,
    ),
    "TryToDeleteAccount": (
        schemas.TryToDeleteAccountMessageSchema(),
        _on_try_to_delete_account,
    ),
}


TerminatedConsumtion = rabbitmq.TerminatedConsumtion


class ChoresConsumer(rabbitmq.Consumer):
    """Passes messages to proper handlers."""

    def process_message(self, body, properties):
        content_type = getattr(properties, "content_type", None)
        if content_type != "application/json":
            _LOGGER.error('Unknown message content type: "%s"', content_type)
            return False

        massage_type = getattr(properties, "type", None)
        try:
            schema, actor = _MESSAGE_TYPES[massage_type]
        except KeyError:
            _LOGGER.error('Unknown message type: "%s"', massage_type)
            return False

        try:
            obj = json.loads(body.decode("utf8"))
        except (UnicodeError, json.JSONDecodeError):
            _LOGGER.error(
                "The message does not contain a valid JSON document."
            )
            return False

        try:
            message_content = schema.load(obj)
        except ValidationError as e:
            _LOGGER.error("Message validation error: %s", str(e))
            return False

        if is_valid_account(
            message_content["debtor_id"], message_content["creditor_id"]
        ):
            # NOTE: Simply ignore chores for accounts this shard is not
            #       responsible for. This is important because otherwise,
            #       for example, an interest payment could be performed
            #       twice, on both children shards.
            actor(**message_content)
            db.session.close()

        return True


def create_chore_message(data):
    message_type = data["type"]
    properties = rabbitmq.MessageProperties(
        delivery_mode=2,
        app_id="swpt_accounts",
        content_type="application/json",
        type=message_type,
    )
    schema, actor = _MESSAGE_TYPES[message_type]
    body = schema.dumps(data).encode("utf8")

    return rabbitmq.Message(
        exchange="",
        routing_key=current_app.config["CHORES_BROKER_QUEUE"],
        body=body,
        properties=properties,
        mandatory=True,
    )
