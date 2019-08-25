from flask import Blueprint, jsonify, current_app as app

mod = Blueprint('index', __name__)


@mod.route('/')
@mod.route('/index')
@app.auth.login_required
def idx():
    return jsonify({"page": "index"})
