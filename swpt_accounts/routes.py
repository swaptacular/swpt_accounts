from flask import Blueprint

web_api = Blueprint('web_api', __name__)


@web_api.route('/create-account', methods=['POST'])
def create_account():
    return 'TODO'
