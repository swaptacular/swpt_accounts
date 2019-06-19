import json
from marshmallow_sqlalchemy import ModelSchema
from flask import Blueprint, abort
from flask.views import MethodView
from . import procedures
from .models import Account


class AccountSchema(ModelSchema):
    class Meta:
        model = Account
        exclude = ['prepared_transfers']


account_schema = AccountSchema()
web_api = Blueprint('web_api', __name__)


class AccountsAPI(MethodView):
    def get(self, debtor_id, creditor_id):
        account = procedures.get_account(debtor_id, creditor_id) or abort(404)
        account_json = json.dumps(account_schema.dump(account))
        return account_json, 200, {'Content-Type': 'application/json'}

    def delete(self, debtor_id, creditor_id):
        """Mark the account as deleted if there are no prepared transfers, and
        the current balance is zero or positive and very close to zero.

        Even if the account has been marked as deleted, it could be
        "resurrected" by an incoming transfer. Therefore, this
        function does not guarantee that the account will be marked as
        deleted successfully, nor that it will "stay" deleted.

        """

        procedures.delete_account_if_zeroed(debtor_id, creditor_id)
        return '', 202, {'Content-Type': 'application/json'}


web_api.add_url_rule('/accounts/<int:debtor_id>/<int:creditor_id>/', view_func=AccountsAPI.as_view('show_account'))
