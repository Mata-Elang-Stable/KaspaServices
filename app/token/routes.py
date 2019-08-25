from flask import Blueprint, jsonify, g, current_app as app

mod = Blueprint('token', __name__)


@mod.route('/api/token/v1.0/getauthtoken', methods=['POST'])
@app.auth.login_required
def getauthtoken():
    token = g.user.generate_auth_token()
    return jsonify({'token': token.decode('ascii')})
