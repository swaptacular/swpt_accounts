from flask import Blueprint
from . import procedures


web_api = Blueprint('web_api', __name__)


@web_api.route('/account/<debtor_id>/<creditor_id>', methods=['POST'])
def create_account(debtor_id, creditor_id):
    """Make sure the account `(debtor_id, creditor_id)` exists."""

    # TODO: Add a real implementation.
    procedures.get_or_create_account(debtor_id, creditor_id)


@web_api.route('/delete-account/<debtor_id>/<creditor_id>', methods=['POST'])
def delete_account(debtor_id, creditor_id):
    """Mark the account `(debtor_id, creditor_id)` as deleted if there are
    no prepared transfers, the principal is zero, and the available
    balance is non-negative and very close to zero.

    Even if the account has been marked as deleted, it could be
    "resurrected" by an incoming transfer. Therefore, this method does
    not guarantee that the account will be marked as deleted
    successfully, nor that it will "stay" deleted.

    """

    # TODO: Add a real implementation.
    procedures.delete_account_if_zeroed(debtor_id, creditor_id)


@web_api.route('/old-prepared-transfers/<debtor_id>/', methods=['GET'])
def get_debtor_old_prepared_transfers(debtor_id):
    """Set stale prepared transfers for a given debtor."""

    # TODO: Add a real implementation.
    pass
