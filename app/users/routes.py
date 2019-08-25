from flask import Blueprint, request, abort, jsonify, current_app as app

from app.models import User

mod = Blueprint('users', __name__)


@mod.route('/api/users/v1.0/getuserdetail/<username>', methods=['POST'])
@app.auth.login_required
def getuserdetail(username):
    q = User.objects.filter(username=username).first()
    if User.objects.filter(username=username).first() is None:
        abort(400)
    username = q['username']
    first_name = q['first_name']
    last_name = q['last_name']
    email = q['email']
    company = q['company']

    return jsonify({
        'username': username,
        'first_name': first_name,
        'last_name': last_name,
        'email': email,
        'company': company
    })


@mod.route('/api/users/v1.0/createuser', methods=['POST'])
def createuser():
    username = request.json.get('username')
    password = request.json.get('password')
    first_name = request.json.get('first_name')
    last_name = request.json.get('last_name')
    email = request.json.get('email')
    company = request.json.get('company')
    if username is None or password is None:
        abort(400)
    if User.objects.filter(username=username).first() is not None:
        abort(400)
    user = User(username=username, first_name=first_name, last_name=last_name, email=email, company=company)
    user.hash_password(password)
    user.set_admin()

    User.create(username=user['username'],
                first_name=user['first_name'],
                last_name=user['last_name'],
                password_hash=user['password_hash'],
                email=user['email'],
                company=user['company'],
                group=user['group'],
                time_joined=user['time_joined']
                )

    return jsonify({'username': user['username']}), 201
