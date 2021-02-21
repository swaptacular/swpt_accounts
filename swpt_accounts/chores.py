import math
from base64 import b16decode
from typing import Optional
from datetime import datetime, timedelta
from flask import current_app
from .extensions import chores_broker
from swpt_accounts.models import MIN_INT64, MAX_INT64, SECONDS_IN_DAY
from swpt_accounts import procedures


@chores_broker.actor(queue_name='change_interest_rate', max_retries=0)
def change_interest_rate(debtor_id: int, creditor_id: int, interest_rate: float, ts: str) -> None:
    """Try to change the interest rate on the account.

    The interest rate will not be changed if the request is too old,
    or not enough time has passed since the previous change in the
    interest rate.

    """

    assert MIN_INT64 <= debtor_id <= MAX_INT64
    assert MIN_INT64 <= creditor_id <= MAX_INT64
    assert not math.isnan(interest_rate)

    procedures.change_interest_rate(
        debtor_id=debtor_id,
        creditor_id=creditor_id,
        interest_rate=interest_rate,
        ts=datetime.fromisoformat(ts),
        signalbus_max_delay_seconds=current_app.config['APP_SIGNALBUS_MAX_DELAY_DAYS'] * SECONDS_IN_DAY,
    )


@chores_broker.actor(queue_name='update_debtor_info', max_retries=0)
def update_debtor_info(
        debtor_id: int,
        creditor_id: int,
        debtor_info_iri: Optional[str],
        debtor_info_content_type: Optional[str],
        debtor_info_sha256: Optional[str],
        ts: str) -> None:

    """Update the information about the debtor of on a given the account."""

    debtor_info_sha256 = debtor_info_sha256 and b16decode(debtor_info_sha256)

    assert MIN_INT64 <= debtor_id <= MAX_INT64
    assert MIN_INT64 <= creditor_id <= MAX_INT64
    assert debtor_info_iri is None or len(debtor_info_iri) <= 200
    assert debtor_info_content_type is None or (
        len(debtor_info_content_type) <= 100 and debtor_info_content_type.isascii())
    assert debtor_info_sha256 is None or len(debtor_info_sha256) == 32

    procedures.update_debtor_info(
        debtor_id=debtor_id,
        creditor_id=creditor_id,
        debtor_info_iri=debtor_info_iri,
        debtor_info_content_type=debtor_info_content_type,
        debtor_info_sha256=debtor_info_sha256,
        ts=datetime.fromisoformat(ts),
    )


@chores_broker.actor(queue_name='capitalize_interest', max_retries=0)
def capitalize_interest(debtor_id: int, creditor_id: int) -> None:
    """Add the interest accumulated on the account to the principal."""

    assert MIN_INT64 <= debtor_id <= MAX_INT64
    assert MIN_INT64 <= creditor_id <= MAX_INT64

    procedures.capitalize_interest(
        debtor_id=debtor_id,
        creditor_id=creditor_id,
        min_capitalization_interval=timedelta(days=current_app.config['APP_MIN_INTEREST_CAPITALIZATION_DAYS']),
    )


@chores_broker.actor(queue_name='delete_account', max_retries=0)
def try_to_delete_account(debtor_id: int, creditor_id: int) -> None:
    """Mark the account as deleted, if possible.

    If it is a "normal" account, it will be marked as deleted if it
    has been scheduled for deletion, there are no prepared transfers,
    and the current balance is not bigger than `max(2.0,
    account.negligible_amount)`.

    If it is a debtor's account, noting will be done. (Deleting
    debtors' accounts is not implemented yet.)

    Note that when a "normal" account has been successfully marked as
    deleted, it could be "resurrected" (with "scheduled for deletion"
    configuration flag) by a delayed incoming transfer. Therefore,
    this function does not guarantee that the account will be marked
    as deleted successfully, or that it will "stay" deleted for
    long. To achieve a reliable deletion, this function may need to be
    called repeatedly, until the account has been purged from the
    database.

    """

    assert MIN_INT64 <= debtor_id <= MAX_INT64
    assert MIN_INT64 <= creditor_id <= MAX_INT64

    procedures.try_to_delete_account(debtor_id, creditor_id)
