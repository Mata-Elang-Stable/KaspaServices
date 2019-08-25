import uuid
from datetime import datetime

from cassandra.cqlengine import columns, models
from flask import current_app as app


class Sensor(models.Model):
    company = columns.Text(primary_key=True)
    device_id = columns.Text(primary_key=True, clustering_order="ASC")
    device_name = columns.Text()
    hostname = columns.Text()
    ip_address = columns.Text()
    location = columns.Text()
    protected_subnet = columns.Text()
    external_subnet = columns.Text(default=app.config['DEFAULT_EXTERNAL_SUBNET'])
    oinkcode = columns.Text(default=app.config['DEFAULT_OINKCODE'])
    topic_global = columns.Text(default=app.config['DEFAULT_GLOBAL_TOPIC'])
    topic_cmd = columns.Text()
    topic_resp = columns.Text()
    sensor_key = columns.UUID(default=uuid.uuid4())
    time_created = columns.DateTime(default=datetime.now())

    def create_dev_id(self, device_name):
        self.device_id = "{}-{}".format(device_name, uuid.uuid4())

    def create_topic_cmd(self):
        self.topic_cmd = "cmd-{}".format(uuid.uuid4())

    def create_topic_resp(self):
        self.topic_resp = "resp-{}".format(uuid.uuid4())

    def set_external_subnet(self, external_subnet):
        self.external_subnet = external_subnet

    def set_oinkcode(self, oinkcode):
        self.external_subnet = oinkcode
