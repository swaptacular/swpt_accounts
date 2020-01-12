import json
from marshmallow_sqlalchemy import ModelSchema
from flask import Blueprint, abort, request
from flask.views import MethodView
from . import procedures
from .models import Account


class AccountSchema(ModelSchema):
    class Meta:
        model = Account


account_schema = AccountSchema()
web_api = Blueprint('web_api', __name__)


class AccountsAPI(MethodView):
    def get(self, debtor_id):
        start_after = None
        limit = None
        if 'start_after' in request.args:
            start_after = int(request.args['start_after'])
        if 'limit' in request.args:
            limit = int(request.args['limit'])
        debtor_account_list = procedures.get_debtor_account_list(debtor_id, start_after, limit)
        debtor_account_list_json = json.dumps([account_schema.dump(a) for a in debtor_account_list])
        return debtor_account_list_json, 200, {'Content-Type': 'application/json'}


class AccountAPI(MethodView):
    def get(self, debtor_id, creditor_id):
        account = procedures.get_account(debtor_id, creditor_id) or abort(404)
        account_json = json.dumps(account_schema.dump(account))
        return account_json, 200, {'Content-Type': 'application/json'}


# TODO: This API should be improved. For example, currently there is
#       no way to find the debtor IDs of all existing debtors. Also,
#       there is no way to find the creditor IDs of all creditors to a
#       given debtor, without obtaining the account details.
web_api.add_url_rule('/accounts/<int:debtor_id>/', view_func=AccountsAPI.as_view('show_accounts'))
web_api.add_url_rule('/accounts/<int:debtor_id>/<int:creditor_id>/', view_func=AccountAPI.as_view('show_account'))
