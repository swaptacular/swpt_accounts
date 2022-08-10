import logging
import json
from datetime import datetime
from flask import current_app
from marshmallow import ValidationError
from swpt_pythonlib import rabbitmq
from swpt_pythonlib.utils import u64_to_i64
import swpt_pythonlib.protocol_schemas as ps
from swpt_accounts.models import SECONDS_IN_DAY
from swpt_accounts.fetch_api_client import get_if_account_is_reachable, get_root_config_data_dict
from swpt_accounts import procedures


def _on_configure_account_signal(
        debtor_id: int,
        creditor_id: int,
        ts: datetime,
        seqnum: int,
        negligible_amount: float = 0.0,
        config_flags: int = 0,
        config_data: str = '',
        *args, **kwargs) -> None:

    """Make sure the account exists, and update its configuration settings."""

    _configure_and_initialize_account(
        debtor_id=debtor_id,
        creditor_id=creditor_id,
        ts=ts,
        seqnum=seqnum,
        negligible_amount=negligible_amount,
        config_flags=config_flags,
        config_data=config_data,
    )


def _on_prepare_transfer_signal(
        coordinator_type: str,
        coordinator_id: int,
        coordinator_request_id: int,
        min_locked_amount: int,
        max_locked_amount: int,
        debtor_id: int,
        creditor_id: int,
        recipient: str,
        ts: datetime,
        max_commit_delay: int,
        min_interest_rate: float = -100.0,
        *args, **kwargs) -> None:

    """Try to secure some amount, to eventually transfer it to another account."""

    try:
        recipient_creditor_id = u64_to_i64(int(recipient))
    except ValueError:
        is_reachable = False
    else:
        is_reachable = get_if_account_is_reachable(debtor_id, recipient_creditor_id)

    procedures.prepare_transfer(
        coordinator_type=coordinator_type,
        coordinator_id=coordinator_id,
        coordinator_request_id=coordinator_request_id,
        min_locked_amount=min_locked_amount,
        max_locked_amount=max_locked_amount,
        debtor_id=debtor_id,
        creditor_id=creditor_id,
        recipient_creditor_id=recipient_creditor_id if is_reachable else None,
        ts=ts,
        max_commit_delay=max_commit_delay,
        min_interest_rate=min_interest_rate,
    )


def _on_finalize_transfer_signal(
        debtor_id: int,
        creditor_id: int,
        transfer_id: int,
        coordinator_type: str,
        coordinator_id: int,
        coordinator_request_id: int,
        committed_amount: int,
        transfer_note_format: str,
        transfer_note: str,
        ts: str,
        *args, **kwargs) -> None:

    """Finalize a prepared transfer."""

    procedures.finalize_transfer(
        debtor_id=debtor_id,
        creditor_id=creditor_id,
        transfer_id=transfer_id,
        coordinator_type=coordinator_type,
        coordinator_id=coordinator_id,
        coordinator_request_id=coordinator_request_id,
        committed_amount=committed_amount,
        transfer_note_format=transfer_note_format,
        transfer_note=transfer_note,
        ts=ts,
    )


def _on_pending_balance_change_signal(
        debtor_id: int,
        creditor_id: int,
        change_id: int,
        coordinator_type: str,
        transfer_note_format: str,
        transfer_note: str,
        committed_at: datetime,
        principal_delta: int,
        other_creditor_id: int,
        *args, **kwargs) -> None:

    """Queue a pendding balance change."""

    procedures.insert_pending_balance_change(
        debtor_id=debtor_id,
        creditor_id=creditor_id,
        change_id=change_id,
        coordinator_type=coordinator_type,
        transfer_note_format=transfer_note_format,
        transfer_note=transfer_note,
        committed_at=committed_at,
        principal_delta=principal_delta,
        other_creditor_id=other_creditor_id,
        cutoff_ts=current_app.config['APP_REGISTERED_BALANCE_CHANGES_RETENTION_DATETIME'],
    )


def _configure_and_initialize_account(
        *,
        debtor_id: int,
        creditor_id: int,
        ts: datetime,
        seqnum: int,
        negligible_amount: float = 0.0,
        config_flags: int = 0,
        config_data: str = '') -> None:

    signalbus_max_delay_seconds = current_app.config['APP_SIGNALBUS_MAX_DELAY_DAYS'] * SECONDS_IN_DAY
    should_be_initialized = procedures.configure_account(
        debtor_id=debtor_id,
        creditor_id=creditor_id,
        ts=ts,
        seqnum=seqnum,
        negligible_amount=negligible_amount,
        config_flags=config_flags,
        config_data=config_data,
        signalbus_max_delay_seconds=signalbus_max_delay_seconds,
    )
    if should_be_initialized:
        root_config_data = get_root_config_data_dict([debtor_id]).get(debtor_id)

        if root_config_data:
            procedures.change_interest_rate(
                debtor_id=debtor_id,
                creditor_id=creditor_id,
                interest_rate=root_config_data.interest_rate_target,
                signalbus_max_delay_seconds=signalbus_max_delay_seconds,
            )
            procedures.update_debtor_info(
                debtor_id=debtor_id,
                creditor_id=creditor_id,
                debtor_info_iri=root_config_data.info_iri,
                debtor_info_content_type=root_config_data.info_content_type,
                debtor_info_sha256=root_config_data.info_sha256,
            )


_MESSAGE_TYPES = {
    'ConfigureAccount': (ps.ConfigureAccountMessageSchema(), _on_configure_account_signal),
    'PrepareTransfer': (ps.PrepareTransferMessageSchema(), _on_prepare_transfer_signal),
    'FinalizeTransfer': (ps.FinalizeTransferMessageSchema(), _on_finalize_transfer_signal),
    'PendingBalanceChange': (ps.PendingBalanceChangeMessageSchema(), _on_pending_balance_change_signal),
}

_LOGGER = logging.getLogger(__name__)


TerminatedConsumtion = rabbitmq.TerminatedConsumtion


class SmpConsumer(rabbitmq.Consumer):
    """Passes messages to proper handlers (actors)."""

    def process_message(self, body, properties):
        content_type = getattr(properties, 'content_type', '')
        if content_type != 'application/json':
            _LOGGER.error('Unknown message content type: "%s"', content_type)
            return False

        massage_type = getattr(properties, 'type', '')
        try:
            schema, actor = _MESSAGE_TYPES[massage_type]
        except KeyError:
            _LOGGER.error('Unknown message type: "%s"', massage_type)
            return False

        try:
            obj = json.loads(body.decode('utf8'))
        except (UnicodeError, json.JSONDecodeError):
            _LOGGER.error('The message does not contain a valid JSON document.')
            return False

        try:
            message_content = schema.load(obj)
        except ValidationError as e:
            _LOGGER.error('Message validation error: %s', str(e))
            return False

        actor(**message_content)
        return True
