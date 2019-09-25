import json
from marshmallow_sqlalchemy import ModelSchema
from flask import Blueprint, abort, request
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
    def get(self, debtor_id):
        start_after = None
        limit = None
        if 'start_after' in request.args:
            start_after = int(request.args['start_after'])
        if 'limit' in request.args:
            limit = int(request.args['limit'])
        debtor_account_list = procedures.get_debtor_account_list(debtor_id, start_after, limit)
        debtor_account_list_json = json.dumps({
            'self': request.url,
            'contents': [self._dump_account(a) for a in debtor_account_list],
        })
        return debtor_account_list_json, 200, {'Content-Type': 'application/json'}

    @staticmethod
    def _dump_account(account):
        d = account_schema.dump(account)
        d['self'] = str(account.creditor_id)  # the relative URL
        return d


class AccountAPI(MethodView):
    def get(self, debtor_id, creditor_id):
        account = procedures.get_account(debtor_id, creditor_id) or abort(404)
        account_dict = account_schema.dump(account)
        account_dict['self'] = request.base_url
        return json.dumps(account_dict), 200, {'Content-Type': 'application/json'}


web_api.add_url_rule(
    '/debtors/<int:debtor_id>/accounts',
    view_func=AccountsAPI.as_view('show_accounts'),
)
web_api.add_url_rule(
    '/debtors/<int:debtor_id>/accounts/<int:creditor_id>',
    view_func=AccountAPI.as_view('show_account'),
)
