from datetime import datetime

from cassandra.cqlengine import columns, models
from flask import current_app as app
from itsdangerous import TimedJSONWebSignatureSerializer as Serializer, BadSignature, SignatureExpired
from passlib.apps import custom_app_context as pwd_context


class User(models.Model):
    username = columns.Text(primary_key=True)
    first_name = columns.Text()
    last_name = columns.Text()
    email = columns.Text()
    password_hash = columns.Text()
    group = columns.Text(default="client")
    company = columns.Text()
    time_joined = columns.DateTime(default=datetime.now())

    def hash_password(self, password):
        self.password_hash = pwd_context.encrypt(password)

    def verify_password(self, password):
        return pwd_context.verify(password, self.password_hash)

    def set_admin(self):
        self.group = "admin"

    def generate_auth_token(self):
        EXPIRES_IN_A_YEAR = 365 * 24 * 60 * 60
        s = Serializer(app.config['SECRET_KEY'], expires_in=EXPIRES_IN_A_YEAR)
        return s.dumps({'username': self.username})

    @staticmethod
    def verify_auth_token(token):
        s = Serializer(app.config['SECRET_KEY'])
        try:
            data = s.loads(token)
        except SignatureExpired:
            return None
        except BadSignature:
            return None
        user = User.objects.filter(username=data['username']).first()
        return user
