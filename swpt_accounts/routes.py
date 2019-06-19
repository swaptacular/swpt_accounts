import json
from datetime import datetime, timezone, timedelta
from marshmallow_sqlalchemy import ModelSchema
from flask import Blueprint, abort, request
from flask.views import MethodView
from . import procedures
from .models import Account, PreparedTransfer


class AccountSchema(ModelSchema):
    class Meta:
        model = Account
        exclude = ['prepared_transfers']


class PreparedTransferSchema(ModelSchema):
    class Meta:
        model = PreparedTransfer


account_schema = AccountSchema()
prepared_transfer_schema = PreparedTransferSchema()
web_api = Blueprint('web_api', __name__)


class AccountsAPI(MethodView):
    def get(self, debtor_id, creditor_id):
        account = procedures.get_account(debtor_id, creditor_id) or abort(404)
        account_json = json.dumps(account_schema.dump(account))
        return account_json, 200, {'Content-Type': 'application/json'}

    def delete(self, debtor_id, creditor_id):
        procedures.delete_account_if_zeroed(debtor_id, creditor_id)
        return '', 202, {'Content-Type': 'application/json'}


web_api.add_url_rule('/accounts/<int:debtor_id>/<int:creditor_id>/', view_func=AccountsAPI.as_view('show_account'))


@web_api.route('/staled-transfers/<int:debtor_id>/', methods=['GET'])
def get_staled_transfers(debtor_id):
    days = int(request.args.get('days', '30'))
    current_ts = datetime.now(tz=timezone.utc)
    staled_transfers = procedures.get_staled_transfers(debtor_id, current_ts + timedelta(days=days))
    staled_transfers_json = json.dumps([prepared_transfer_schema.dump(pt) for pt in staled_transfers])
    return staled_transfers_json, 200, {'Content-Type': 'application/json'}
