import os
import tarfile
from datetime import datetime

from cassandra.query import SimpleStatement
from flask import Blueprint, request, abort, jsonify, g, send_from_directory, current_app as app

from app.models import Sensor

mod = Blueprint('sensors', __name__)


@mod.route('/api/sensors/v1.0/verifysensorkey', methods=['POST'])
@app.auth.login_required
def verifysensorkey():
    device_id = request.json.get('device_id')
    sensor_key = request.json.get('sensor_key')
    netint = request.json.get('netint')
    print(device_id)
    print(sensor_key)
    print(netint)
    if device_id is None or sensor_key is None or netint is None:
        abort(400)
    q = Sensor.objects.filter(company=g.user['company'])
    q = q.filter(device_id=device_id)
    sensor = q.first()

    if sensor is None:
        abort(400)

    # create tarball

    buildfile = 'build_snoqtt.sh'
    conffile = 'env-conf.conf'
    removefile = 'remove_snoqtt.sh'
    startfile = 'start_snoqtt.sh'
    stopfile = 'stop_snoqtt.sh'

    filedirtemplate = app.config['BASEDIR'] + '/app/static/template/'

    if not os.path.exists(app.config['BASEDIR'] + '/app/static/generated/{}/'.format(sensor_key)):
        os.makedirs(app.config['BASEDIR'] + '/app/static/generated/{}/'.format(sensor_key))

    filediroutput = app.config['BASEDIR'] + '/app/static/generated/{}/'.format(sensor_key)

    with open(filedirtemplate + buildfile) as build_template:
        templatebuild = build_template.read()
    with open(filediroutput + buildfile, "w") as current:
        current.write(templatebuild.format(protected_subnet=sensor['protected_subnet'],
                                           external_subnet="'{}'".format(sensor['external_subnet']),
                                           oinkcode=sensor['oinkcode']))

    with open(filedirtemplate + conffile) as conf_template:
        templateconf = conf_template.read()
    with open(filediroutput + conffile, "w") as current:
        current.write(templateconf.format(global_topic=sensor['topic_global'],
                                          global_server='103.24.56.244',
                                          global_port='1883',
                                          device_id=sensor['device_id'],
                                          oinkcode=sensor['oinkcode'],
                                          protected_subnet=sensor['protected_subnet'],
                                          external_subnet=sensor['external_subnet'],
                                          netint=netint,
                                          company=g.user['company']))

    with open(filedirtemplate + removefile) as remove_template:
        templateremove = remove_template.read()
    with open(filediroutput + removefile, "w") as current:
        current.write(templateremove)

    with open(filedirtemplate + startfile) as start_template:
        templatestart = start_template.read()
    with open(filediroutput + startfile, "w") as current:
        current.write(templatestart)

    with open(filedirtemplate + stopfile) as stop_template:
        templatestop = stop_template.read()
    with open(filediroutput + stopfile, "w") as current:
        current.write(templatestop)

    filetarname = 'snoqtt-{}.tar.gz'.format(sensor_key)
    if os.path.exists(filediroutput + filetarname):
        os.remove(filediroutput + filetarname)

    tar = tarfile.open((filediroutput + filetarname), "w:gz")
    tar.add(filediroutput + buildfile, arcname=buildfile)
    tar.add(filediroutput + conffile, arcname=conffile)
    tar.add(filediroutput + removefile, arcname=removefile)
    tar.add(filediroutput + startfile, arcname=startfile)
    tar.add(filediroutput + stopfile, arcname=stopfile)
    tar.close()

    os.remove(filediroutput + buildfile)
    os.remove(filediroutput + conffile)
    os.remove(filediroutput + removefile)
    os.remove(filediroutput + startfile)
    os.remove(filediroutput + stopfile)

    return send_from_directory(filediroutput, filetarname, as_attachment=True)


@mod.route('/api/sensors/v1.0/listsensors', methods=['POST'])
@app.auth.login_required
def listsensors():
    company = g.user['company']
    if company is None:
        abort(400)

    obj = {
        "company": g.user['company'],
        "count": 0,
        "sensors": []
    }
    for sensor in Sensor.objects.filter(company=company):
        sensor_obj = {
            "device_id": sensor['device_id'],
            "device_name": sensor['device_name'],
            "hostname": sensor['hostname'],
            "ip_address": sensor['ip_address'],
            "location": sensor['location'],
            "protected_subnet": sensor['protected_subnet'],
            "external_subnet": sensor['external_subnet'],
            "oinkcode": sensor['oinkcode'],
            "topic_global": sensor['topic_global'],
            "topic_cmd": sensor['topic_cmd'],
            "topic_resp": sensor['topic_resp'],
            "sensor_key": sensor['sensor_key'],
            "time_created": sensor['time_created']
        }
        obj['sensors'].append(sensor_obj)
        obj['count'] = obj['count'] + 1

    return jsonify(obj)


@mod.route('/api/sensors/v1.0/getsensordetail', methods=['POST'])
@app.auth.login_required
def getsensordetail():
    company = g.user['company']
    device_id = request.json.get('device_id')
    if device_id is None or company is None:
        abort(400)

    q = Sensor.objects.filter(company=company)
    q = q.filter(device_id=device_id)
    sensor = q.first()

    if sensor is None:
        abort(400)

    sensor_obj = {
        "company": sensor['company'],
        "device_id": sensor['device_id'],
        "device_name": sensor['device_name'],
        "hostname": sensor['hostname'],
        "ip_address": sensor['ip_address'],
        "location": sensor['location'],
        "protected_subnet": sensor['protected_subnet'],
        "external_subnet": sensor['external_subnet'],
        "oinkcode": sensor['oinkcode'],
        "topic_global": sensor['topic_global'],
        "topic_cmd": sensor['topic_cmd'],
        "topic_resp": sensor['topic_resp'],
        "sensor_key": sensor['sensor_key'],
        "time_created": sensor['time_created']
    }

    return jsonify(sensor_obj)


@mod.route('/api/sensors/v1.0/createsensor', methods=['POST'])
@app.auth.login_required
def createsensor():
    device_name = request.json.get('device_name')
    hostname = request.json.get('hostname')
    ip_address = request.json.get('ip_address')
    location = request.json.get('location')
    protected_subnet = request.json.get('protected_subnet')
    external_subnet = request.json.get('external_subnet')
    oinkcode = request.json.get('oinkcode')
    company = g.user['company']
    sensor = Sensor(company=company, device_name=device_name,
                    hostname=hostname, ip_address=ip_address, location=location,
                    protected_subnet=protected_subnet)

    if external_subnet:
        sensor.set_oinkcode(oinkcode)
    if external_subnet:
        sensor.set_external_subnet(external_subnet)

    sensor.create_dev_id(device_name)
    sensor.create_topic_cmd()
    sensor.create_topic_resp()

    Sensor.create(company=sensor['company'],
                  device_id=sensor['device_id'],
                  device_name=sensor['device_name'],
                  hostname=sensor['hostname'],
                  ip_address=sensor['ip_address'],
                  location=sensor['location'],
                  protected_subnet=sensor['protected_subnet'],
                  external_subnet=sensor['external_subnet'],
                  oinkcode=sensor['oinkcode'],
                  topic_global=sensor['topic_global'],
                  topic_cmd=sensor['topic_cmd'],
                  topic_resp=sensor['topic_resp'],
                  sensor_key=sensor['sensor_key'],
                  time_created=sensor['time_created']
                  )

    app.session.execute(
        """
        INSERT INTO sensor_status (device_id, status, ts) 
        VALUES (%s, %s, %s)
        """,
        (sensor['device_id'], "STOPPED", datetime.now())
    )

    return jsonify({
        'device_id': sensor['device_id'],
        'device_name': sensor['device_name'],
        'sensor_key': sensor['sensor_key'],
    })


@mod.route('/api/sensor/v1.0/checkstatus/<device_id>', methods=['POST'])
@app.auth.login_required
def getSensorStatus(device_id):
    query = "SELECT * FROM sensor_status WHERE device_id='{}'".format(device_id)
    statement = SimpleStatement(query)
    obj = {
        "device_id": device_id,
        "ts": "",
        "status": "",
    }
    for status in app.session.execute(statement):
        obj['ts'] = status['ts']
        obj['status'] = status['status']

    return jsonify(obj)


@mod.route('/api/sensor/v1.0/startsensor/<device_id>', methods=['POST'])
def startSensor(device_id):
    app.session.execute(
        """
        INSERT INTO sensor_status (device_id, status, ts) 
        VALUES (%s, %s, %s)
        """,
        (device_id, "RUNNING", datetime.now())
    )


@mod.route('/api/sensor/v1.0/stopsensor/<device_id>', methods=['POST'])
def stopSensor(device_id):
    app.session.execute(
        """
        INSERT INTO sensor_status (device_id, status, ts) 
        VALUES (%s, %s, %s)
        """,
        (device_id, "STOPPED", datetime.now())
    )
