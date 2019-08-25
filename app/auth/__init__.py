from flask_httpauth import HTTPBasicAuth
from flask import g

from app.models import User

auth = HTTPBasicAuth()


@auth.verify_password
def verify_password(username_or_token, password):
    user = User.verify_auth_token(username_or_token)
    if not user:
        user = User.objects.filter(username=username_or_token).first()
        if not user or not user.verify_password(password):
            return False
    g.user = user
    return True
