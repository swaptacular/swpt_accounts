from flask import Blueprint
from swpt_accounts import procedures

HTTP_HEADERS = {
    'Content-Type': 'text/plain; charset=utf-8',
    'Cache-Control': 'max-age=86400',
}

fetch_api = Blueprint('fetch', __name__, url_prefix='/accounts')


@fetch_api.route('/<i64:debtorId>/<i64:creditorId>/reachable')
def reachable(debtorId, creditorId):
    is_rachable_account = procedures.is_reachable_account(debtorId, creditorId)
    status_code = 204 if is_rachable_account else 404
    return '', status_code, HTTP_HEADERS


@fetch_api.route('/<i64:debtorId>/<i64:creditorId>/config')
def config(debtorId, creditorId):
    config_data = procedures.get_account_config_data(debtorId, creditorId)
    status_code = 404 if config_data is None else 200
    return config_data or '', status_code, HTTP_HEADERS
