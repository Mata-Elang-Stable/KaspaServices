from app.auth import auth
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.cqlengine import connection, models
from flask import Flask

import app.index.routes as index
import app.sensors.routes as sensors
import app.statistic.routes as statistic
import app.token.routes as token
import app.users.routes as users
from config import Config

# Setup app
app = Flask(__name__)
app.auth = auth

app.config.from_object(Config)

app.cassandra_auth_provider = PlainTextAuthProvider(username=app.config['CASSANDRA_USERNAME'],
                                                    password=app.config['CASSANDRA_PASSWORD'])
app.cluster = Cluster([app.config['CASSANDRA_CLUSTER_HOST']], auth_provider=app.cassandra_auth_provider)
app.session = app.cluster.connect()
app.session.set_keyspace('kaspa')

connection.register_connection('clusterKaspa', session=app.session)

connection.set_default_connection('clusterKaspa')
models.DEFAULT_KEYSPACE = 'kaspa'

app.register_blueprint(auth.mod)
app.register_blueprint(index.mod)
app.register_blueprint(sensors.mod)
app.register_blueprint(statistic.mod)
app.register_blueprint(token.mod)
app.register_blueprint(users.mod)
