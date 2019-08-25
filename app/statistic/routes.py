from cassandra.query import SimpleStatement
from flask import Blueprint, request, jsonify, current_app as app

mod = Blueprint('statistic', __name__)


@mod.route('/api/statistic/v1.0/rawdata', methods=['POST'])
@app.auth.login_required
def getrawdata():
    # company = g.user['company']
    company = request.json.get('company')
    year = request.json.get('year')
    month = request.json.get('month')
    day = request.json.get('day')
    hour = request.json.get('hour')
    minute = request.json.get('minute')
    second = request.json.get('second')
    limit = request.json.get('limit')

    query = "SELECT * FROM kaspa.raw_data_by_company WHERE company='{}' LIMIT {}".format(
        company, limit
    )
    if year is not None:
        query = "SELECT * FROM kaspa.raw_data_by_company WHERE company='{}' and year={} LIMIT {}".format(
            company, year, limit
        )
        if month is not None:
            query = "SELECT * FROM kaspa.raw_data_by_company WHERE company='{}' and year={} and month={} LIMIT {}".format(
                company, year, month, limit
            )
            if day is not None:
                query = "SELECT * FROM kaspa.raw_data_by_company WHERE company='{}' and year={} and month={} and day={} LIMIT {}".format(
                    company, year, month, day, limit
                )
                if hour is not None:
                    query = "SELECT * FROM kaspa.raw_data_by_company WHERE company='{}' and year={} and month={} and day={} and hour={} LIMIT {}".format(
                        company, year, month, day, hour, limit
                    )
                    if minute is not None:
                        query = "SELECT * FROM kaspa.raw_data_by_company WHERE company='{}' and year={} and month={} and day={} and hour={} and minute={} LIMIT {}".format(
                            company, year, month, day, hour, minute, limit
                        )
                        if second is not None:
                            query = "SELECT * FROM kaspa.raw_data_by_company WHERE company='{}' and year={} and month={} and day={} and hour={} and minute={} and second={} LIMIT {}".format(
                                company, year, month, day, hour, minute, second, limit
                            )

    statement = SimpleStatement(query)
    obj = {
        "company": company,
        "count": 0,
        "data": []
    }
    for raw_data in app.session.execute(statement):
        obj['data'].append(raw_data)
        obj['count'] = obj['count'] + 1

    return jsonify(obj)


@mod.route('/api/statistic/v1.0/rawdata/<device_id>', methods=['POST'])
@app.auth.login_required
def getrawdatadev(device_id):
    year = request.json.get('year')
    month = request.json.get('month')
    day = request.json.get('day')
    hour = request.json.get('hour')
    minute = request.json.get('minute')
    second = request.json.get('second')
    limit = request.json.get('limit')

    query = "SELECT * FROM kaspa.raw_data_by_device_id WHERE device_id='{}' LIMIT {}".format(
        device_id, limit
    )
    if year is not None:
        query = "SELECT * FROM kaspa.raw_data_by_device_id WHERE device_id='{}' and year={} LIMIT {}".format(
            device_id, year, limit
        )
        if month is not None:
            query = "SELECT * FROM kaspa.raw_data_by_device_id WHERE device_id='{}' and year={} and month={} LIMIT {}".format(
                device_id, year, month, limit
            )
            if day is not None:
                query = "SELECT * FROM kaspa.raw_data_by_device_id WHERE device_id='{}' and year={} and month={} and day={} LIMIT {}".format(
                    device_id, year, month, day, limit
                )
                if hour is not None:
                    query = "SELECT * FROM kaspa.raw_data_by_device_id WHERE device_id='{}' and year={} and month={} and day={} and hour={} LIMIT {}".format(
                        device_id, year, month, day, hour, limit
                    )
                    if minute is not None:
                        query = "SELECT * FROM kaspa.raw_data_by_device_id WHERE device_id='{}' and year={} and month={} and day={} and hour={} and minute={} LIMIT {}".format(
                            device_id, year, month, day, hour, minute, limit
                        )
                        if second is not None:
                            query = "SELECT * FROM kaspa.raw_data_by_device_id WHERE device_id='{}' and year={} and month={} and day={} and hour={} and minute={} and second={} LIMIT {}".format(
                                device_id, year, month, day, hour, minute, second, limit
                            )

    statement = SimpleStatement(query)
    obj = {
        "device_id": device_id,
        "count": 0,
        "data": []
    }
    for raw_data in app.session.execute(statement):
        obj['data'].append(raw_data)
        obj['count'] = obj['count'] + 1

    return jsonify(obj)


@mod.route('/api/statistic/v1.0/eventhit', methods=['POST'])
@app.auth.login_required
def geteventhit():
    # company = g.user['company']
    company = request.json.get('company')
    year = request.json.get('year')
    month = request.json.get('month')
    day = request.json.get('day')
    hour = request.json.get('hour')
    minute = request.json.get('minute')
    second = request.json.get('second')
    limit = request.json.get('limit')

    query = "SELECT * FROM event_hit_on_company_year WHERE company='{}' LIMIT {}".format(
        company, limit
    )
    if year is not None:
        query = "SELECT * FROM event_hit_on_company_month WHERE company='{}' and year={} LIMIT {}".format(
            company, year, limit
        )
        if month is not None:
            query = "SELECT * FROM event_hit_on_company_day WHERE company='{}' and year={} and month={} LIMIT {}".format(
                company, year, month, limit
            )
            if day is not None:
                query = "SELECT * FROM event_hit_on_company_hour WHERE company='{}' and year={} and month={} and day={} LIMIT {}".format(
                    company, year, month, day, limit
                )
                if hour is not None:
                    query = "SELECT * FROM event_hit_on_company_minute WHERE company='{}' and year={} and month={} and day={} and hour={} LIMIT {}".format(
                        company, year, month, day, hour, limit
                    )
                    if minute is not None:
                        query = "SELECT * FROM event_hit_on_company_sec WHERE company='{}' and year={} and month={} and day={} and hour={} and minute={} LIMIT {}".format(
                            company, year, month, day, hour, minute, limit
                        )

    statement = SimpleStatement(query)
    obj = {
        "company": company,
        "count": 0,
        "data": []
    }
    for eventhit in app.session.execute(statement):
        obj['data'].append(eventhit)
        obj['count'] = obj['count'] + 1

    return jsonify(obj)


@mod.route('/api/statistic/v1.0/eventhit/<device_id>', methods=['POST'])
@app.auth.login_required
def geteventhitdev(device_id):
    year = request.json.get('year')
    month = request.json.get('month')
    day = request.json.get('day')
    hour = request.json.get('hour')
    minute = request.json.get('minute')
    second = request.json.get('second')
    limit = request.json.get('limit')

    query = "SELECT * FROM event_hit_on_device_id_year WHERE device_id='{}' LIMIT {}".format(
        device_id, limit
    )
    if year is not None:
        query = "SELECT * FROM event_hit_on_device_id_month WHERE device_id='{}' and year={} LIMIT {}".format(
            device_id, year, limit
        )
        if month is not None:
            query = "SELECT * FROM event_hit_on_device_id_day WHERE device_id='{}' and year={} and month={} LIMIT {}".format(
                device_id, year, month, limit
            )
            if day is not None:
                query = "SELECT * FROM event_hit_on_device_id_hour WHERE device_id='{}' and year={} and month={} and day={} LIMIT {}".format(
                    device_id, year, month, day, limit
                )
                if hour is not None:
                    query = "SELECT * FROM event_hit_on_device_id_min WHERE device_id='{}' and year={} and month={} and day={} and hour={} LIMIT {}".format(
                        device_id, year, month, day, hour, limit
                    )
                    if minute is not None:
                        query = "SELECT * FROM event_hit_on_device_id_sec WHERE device_id='{}' and year={} and month={} and day={} and hour={} and minute={} LIMIT {}".format(
                            device_id, year, month, day, hour, minute, limit
                        )

    statement = SimpleStatement(query)
    obj = {
        "device_id": device_id,
        "count": 0,
        "data": []
    }
    for eventhit in app.session.execute(statement):
        obj['data'].append(eventhit)
        obj['count'] = obj['count'] + 1

    return jsonify(obj)


@mod.route('/api/statistic/v1.0/signaturehit', methods=['POST'])
@app.auth.login_required
def getsignaturehit():
    # company = g.user['company']
    company = request.json.get('company')
    year = request.json.get('year')
    month = request.json.get('month')
    day = request.json.get('day')
    hour = request.json.get('hour')
    minute = request.json.get('minute')
    second = request.json.get('second')
    limit = request.json.get('limit')

    if year is not None:
        query = "SELECT * FROM signature_hit_on_company_year WHERE company='{}' and year={} LIMIT {}".format(
            company, year, limit
        )
        if month is not None:
            query = "SELECT * FROM signature_hit_on_company_month WHERE company='{}' and year={} and month={} LIMIT {}".format(
                company, year, month, limit
            )
            if day is not None:
                query = "SELECT * FROM signature_hit_on_company_day WHERE company='{}' and year={} and month={} and day={} LIMIT {}".format(
                    company, year, month, day, limit
                )
                if hour is not None:
                    query = "SELECT * FROM signature_hit_on_company_hour WHERE company='{}' and year={} and month={} and day={} and hour={} LIMIT {}".format(
                        company, year, month, day, hour, limit
                    )
                    if minute is not None:
                        query = "SELECT * FROM signature_hit_on_company_minute WHERE company='{}' and year={} and month={} and day={} and hour={} and minute={} LIMIT {}".format(
                            company, year, month, day, hour, minute, limit
                        )
                        if second is not None:
                            query = "SELECT * FROM signature_hit_on_company_sec WHERE company='{}' and year={} and month={} and day={} and hour={} and minute={} and second={} LIMIT {}".format(
                                company, year, month, day, hour, minute, second, limit
                            )

    statement = SimpleStatement(query)

    obj = {
        "company": company,
        "count": 0,
        "data": []
    }
    for signaturehit in app.session.execute(statement):
        obj['data'].append(signaturehit)
        obj['count'] = obj['count'] + 1

    return jsonify(obj)


@mod.route('/api/statistic/v1.0/signaturehit/<device_id>', methods=['POST'])
@app.auth.login_required
def getsignaturehitdev(device_id):
    year = request.json.get('year')
    month = request.json.get('month')
    day = request.json.get('day')
    hour = request.json.get('hour')
    minute = request.json.get('minute')
    second = request.json.get('second')
    limit = request.json.get('limit')

    if year is not None:
        query = "SELECT * FROM signature_hit_on_device_id_year WHERE device_id='{}' and year={} LIMIT {}".format(
            device_id, year, limit
        )
        if month is not None:
            query = "SELECT * FROM signature_hit_on_device_id_month WHERE device_id='{}' and year={} and month={} LIMIT {}".format(
                device_id, year, month, limit
            )
            if day is not None:
                query = "SELECT * FROM signature_hit_on_device_id_day WHERE device_id='{}' and year={} and month={} and day={} LIMIT {}".format(
                    device_id, year, month, day, limit
                )
                if hour is not None:
                    query = "SELECT * FROM signature_hit_on_device_id_hour WHERE device_id='{}' and year={} and month={} and day={} and hour={} LIMIT {}".format(
                        device_id, year, month, day, hour, limit
                    )
                    if minute is not None:
                        query = "SELECT * FROM signature_hit_on_device_id_minute WHERE device_id='{}' and year={} and month={} and day={} and hour={} and minute={} LIMIT {}".format(
                            device_id, year, month, day, hour, minute, limit
                        )
                        if second is not None:
                            query = "SELECT * FROM signature_hit_on_device_id_sec WHERE device_id='{}' and year={} and month={} and day={} and hour={} and minute={} and second={} LIMIT {}".format(
                                device_id, year, month, day, hour, minute, second, limit
                            )

    statement = SimpleStatement(query)
    obj = {
        "device_id": device_id,
        "count": 0,
        "data": []
    }
    for signaturehit in app.session.execute(statement):
        obj['data'].append(signaturehit)
        obj['count'] = obj['count'] + 1

    return jsonify(obj)


@mod.route('/api/statistic/v1.0/protocolhit', methods=['POST'])
@app.auth.login_required
def getprotocolhit():
    # company = g.user['company']
    company = request.json.get('company')
    year = request.json.get('year')
    month = request.json.get('month')
    day = request.json.get('day')
    hour = request.json.get('hour')
    minute = request.json.get('minute')
    second = request.json.get('second')
    limit = request.json.get('limit')

    if year is not None:
        query = "SELECT * FROM protocol_hit_on_company_year WHERE company='{}' and year={} LIMIT {}".format(
            company, year, limit
        )
        if month is not None:
            query = "SELECT * FROM protocol_hit_on_company_month WHERE company='{}' and year={} and month={} LIMIT {}".format(
                company, year, month, limit
            )
            if day is not None:
                query = "SELECT * FROM protocol_hit_on_company_day WHERE company='{}' and year={} and month={} and day={} LIMIT {}".format(
                    company, year, month, day, limit
                )
                if hour is not None:
                    query = "SELECT * FROM protocol_hit_on_company_hour WHERE company='{}' and year={} and month={} and day={} and hour={} LIMIT {}".format(
                        company, year, month, day, hour, limit
                    )
                    if minute is not None:
                        query = "SELECT * FROM protocol_hit_on_company_minute WHERE company='{}' and year={} and month={} and day={} and hour={} and minute={} LIMIT {}".format(
                            company, year, month, day, hour, minute, limit
                        )
                        if second is not None:
                            query = "SELECT * FROM protocol_hit_on_company_sec WHERE company='{}' and year={} and month={} and day={} and hour={} and minute={} and second={} LIMIT {}".format(
                                company, year, month, day, hour, minute, second, limit
                            )

    statement = SimpleStatement(query)
    obj = {
        "company": company,
        "count": 0,
        "data": []
    }
    for protocolhit in app.session.execute(statement):
        obj['data'].append(protocolhit)
        obj['count'] = obj['count'] + 1

    return jsonify(obj)


@mod.route('/api/statistic/v1.0/protocolhit/<device_id>', methods=['POST'])
@app.auth.login_required
def getprotocolhitdev(device_id):
    year = request.json.get('year')
    month = request.json.get('month')
    day = request.json.get('day')
    hour = request.json.get('hour')
    minute = request.json.get('minute')
    second = request.json.get('second')
    limit = request.json.get('limit')

    if year is not None:
        query = "SELECT * FROM protocol_hit_on_device_id_year WHERE device_id='{}' and year={} LIMIT {}".format(
            device_id, year, limit
        )
        if month is not None:
            query = "SELECT * FROM protocol_hit_on_device_id_month WHERE device_id='{}' and year={} and month={} LIMIT {}".format(
                device_id, year, month, limit
            )
            if day is not None:
                query = "SELECT * FROM protocol_hit_on_device_id_day WHERE device_id='{}' and year={} and month={} and day={} LIMIT {}".format(
                    device_id, year, month, day, limit
                )
                if hour is not None:
                    query = "SELECT * FROM protocol_hit_on_device_id_hour WHERE device_id='{}' and year={} and month={} and day={} and hour={} LIMIT {}".format(
                        device_id, year, month, day, hour, limit
                    )
                    if minute is not None:
                        query = "SELECT * FROM protocol_hit_on_device_id_minute WHERE device_id='{}' and year={} and month={} and day={} and hour={} and minute={} LIMIT {}".format(
                            device_id, year, month, day, hour, minute, limit
                        )
                        if second is not None:
                            query = "SELECT * FROM protocol_hit_on_device_id_sec WHERE device_id='{}' and year={} and month={} and day={} and hour={} and minute={} and second={} LIMIT {}".format(
                                device_id, year, month, day, hour, minute, second, limit
                            )

    statement = SimpleStatement(query)
    obj = {
        "device_id": device_id,
        "count": 0,
        "data": []
    }
    for protocolhit in app.session.execute(statement):
        obj['data'].append(protocolhit)
        obj['count'] = obj['count'] + 1

    return jsonify(obj)


@mod.route('/api/statistic/v1.0/protocolbysporthit/<protocol>', methods=['POST'])
@app.auth.login_required
def getprotocolbysporthit(protocol):
    # company = g.user['company']
    company = request.json.get('company')
    year = request.json.get('year')
    month = request.json.get('month')
    day = request.json.get('day')
    hour = request.json.get('hour')
    minute = request.json.get('minute')
    second = request.json.get('second')
    limit = request.json.get('limit')

    if year is not None:
        query = "SELECT * FROM protocol_by_sport_hit_on_company_year WHERE company='{}' and protocol='{}' and year={} LIMIT {}".format(
            company, protocol, year, limit
        )
        if month is not None:
            query = "SELECT * FROM protocol_by_sport_hit_on_company_month WHERE company='{}' and protocol='{}' and year={} and month={} LIMIT {}".format(
                company, protocol, year, month, limit
            )
            if day is not None:
                query = "SELECT * FROM protocol_by_sport_hit_on_company_day WHERE company='{}' and protocol='{}' and year={} and month={} and day={} LIMIT {}".format(
                    company, protocol, year, month, day, limit
                )
                if hour is not None:
                    query = "SELECT * FROM protocol_by_sport_hit_on_company_hour WHERE company='{}' and protocol='{}' and year={} and month={} and day={} and hour={} LIMIT {}".format(
                        company, protocol, year, month, day, hour, limit
                    )
                    if minute is not None:
                        query = "SELECT * FROM protocol_by_sport_hit_on_company_minute WHERE company='{}' and protocol='{}' and year={} and month={} and day={} and hour={} and minute={} LIMIT {}".format(
                            company, protocol, year, month, day, hour, minute, limit
                        )
                        if second is not None:
                            query = "SELECT * FROM protocol_by_sport_hit_on_company_sec WHERE company='{}' and protocol='{}' and year={} and month={} and day={} and hour={} and minute={} and second={} LIMIT {}".format(
                                company, protocol, year, month, day, hour, minute, second, limit
                            )

    statement = SimpleStatement(query)
    obj = {
        "company": company,
        "protocol": protocol,
        "count": 0,
        "data": []
    }
    for protocolbysporthit in app.session.execute(statement):
        obj['data'].append(protocolbysporthit)
        obj['count'] = obj['count'] + 1

    return jsonify(obj)


@mod.route('/api/statistic/v1.0/protocolbysporthit/<protocol>/<device_id>', methods=['POST'])
@app.auth.login_required
def getprotocolbysporthitdev(protocol, device_id):
    year = request.json.get('year')
    month = request.json.get('month')
    day = request.json.get('day')
    hour = request.json.get('hour')
    minute = request.json.get('minute')
    second = request.json.get('second')
    limit = request.json.get('limit')

    if year is not None:
        query = "SELECT * FROM protocol_by_sport_hit_on_device_id_year WHERE device_id='{}' and protocol='{}' and year={} LIMIT {}".format(
            device_id, protocol, year, limit
        )
        if month is not None:
            query = "SELECT * FROM protocol_by_sport_hit_on_device_id_month WHERE device_id='{}' and protocol='{}' and year={} and month={} LIMIT {}".format(
                device_id, protocol, year, month, limit
            )
            if day is not None:
                query = "SELECT * FROM protocol_by_sport_hit_on_device_id_day WHERE device_id='{}' and protocol='{}' and year={} and month={} and day={} LIMIT {}".format(
                    device_id, protocol, year, month, day, limit
                )
                if hour is not None:
                    query = "SELECT * FROM protocol_by_sport_hit_on_device_id_hour WHERE device_id='{}' and protocol='{}' and year={} and month={} and day={} and hour={} LIMIT {}".format(
                        device_id, protocol, year, month, day, hour, limit
                    )
                    if minute is not None:
                        query = "SELECT * FROM protocol_by_sport_hit_on_device_id_minute WHERE device_id='{}' and protocol='{}' and year={} and month={} and day={} and hour={} and minute={} LIMIT {}".format(
                            device_id, protocol, year, month, day, hour, minute, limit
                        )
                        if second is not None:
                            query = "SELECT * FROM protocol_by_sport_hit_on_device_id_sec WHERE device_id='{}' and protocol='{}' and year={} and month={} and day={} and hour={} and minute={} and second={} LIMIT {}".format(
                                device_id, protocol, year, month, day, hour, minute, second, limit
                            )

    statement = SimpleStatement(query)
    obj = {
        "device_id": device_id,
        "protocol": protocol,
        "count": 0,
        "data": []
    }
    for protocolbysporthit in app.session.execute(statement):
        obj['data'].append(protocolbysporthit)
        obj['count'] = obj['count'] + 1

    return jsonify(obj)


@mod.route('/api/statistic/v1.0/protocolbydporthit/<protocol>', methods=['POST'])
@app.auth.login_required
def getprotocolbydporthit(protocol):
    # company = g.user['company']
    company = request.json.get('company')
    year = request.json.get('year')
    month = request.json.get('month')
    day = request.json.get('day')
    hour = request.json.get('hour')
    minute = request.json.get('minute')
    second = request.json.get('second')
    limit = request.json.get('limit')

    if year is not None:
        query = "SELECT * FROM protocol_by_dport_hit_on_company_year WHERE company='{}' and protocol='{}' and year={} LIMIT {}".format(
            company, protocol, year, limit
        )
        if month is not None:
            query = "SELECT * FROM protocol_by_dport_hit_on_company_month WHERE company='{}' and protocol='{}' and year={} and month={} LIMIT {}".format(
                company, year, month, day, limit
            )
            if day is not None:
                query = "SELECT * FROM protocol_by_dport_hit_on_company_day WHERE company='{}' and protocol='{}' and year={} and month={} and day={} LIMIT {}".format(
                    company, protocol, year, month, day, limit
                )
                if hour is not None:
                    query = "SELECT * FROM protocol_by_dport_hit_on_company_hour WHERE company='{}' and protocol='{}' and year={} and month={} and day={} and hour={} LIMIT {}".format(
                        company, protocol, year, month, day, hour, limit
                    )
                    if minute is not None:
                        query = "SELECT * FROM protocol_by_dport_hit_on_company_minute WHERE company='{}' and protocol='{}' and year={} and month={} and day={} and hour={} and minute={} LIMIT {}".format(
                            company, protocol, year, month, day, hour, minute, limit
                        )
                        if second is not None:
                            query = "SELECT * FROM protocol_by_dport_hit_on_company_second WHERE company='{}' and protocol='{}' and year={} and month={} and day={} and hour={} and minute={} and second={} LIMIT {}".format(
                                company, protocol, year, month, day, hour, minute, second, limit
                            )

    statement = SimpleStatement(query)
    obj = {
        "company": company,
        "protocol": protocol,
        "count": 0,
        "data": []
    }
    for protocolbydporthit in app.session.execute(statement):
        obj['data'].append(protocolbydporthit)
        obj['count'] = obj['count'] + 1

    return jsonify(obj)


@mod.route('/api/statistic/v1.0/protocolbydporthit/<protocol>/<device_id>', methods=['POST'])
@app.auth.login_required
def getprotocolbydporthitdev(protocol, device_id):
    year = request.json.get('year')
    month = request.json.get('month')
    day = request.json.get('day')
    hour = request.json.get('hour')
    minute = request.json.get('minute')
    second = request.json.get('second')
    limit = request.json.get('limit')

    if year is not None:
        query = "SELECT * FROM protocol_by_dport_hit_on_device_id_year WHERE device_id='{}' and protocol='{}' and year={} LIMIT {}".format(
            device_id, protocol, year, limit
        )
        if month is not None:
            query = "SELECT * FROM protocol_by_dport_hit_on_device_id_month WHERE device_id='{}' and protocol='{}' and year={} and month={} LIMIT {}".format(
                device_id, protocol, year, month, limit
            )
            if day is not None:
                query = "SELECT * FROM protocol_by_dport_hit_on_device_id_day WHERE device_id='{}' and protocol='{}' and year={} and month={} and day={} LIMIT {}".format(
                    device_id, protocol, year, month, day, limit
                )
                if hour is not None:
                    query = "SELECT * FROM protocol_by_dport_hit_on_device_id_hour WHERE device_id='{}' and protocol='{}' and year={} and month={} and day={} and hour={} LIMIT {}".format(
                        device_id, protocol, year, month, day, hour, limit
                    )
                    if minute is not None:
                        query = "SELECT * FROM protocol_by_dport_hit_on_device_id_minute WHERE device_id='{}' and protocol='{}' and year={} and month={} and day={} and hour={} and minute={} LIMIT {}".format(
                            device_id, protocol, year, month, day, hour, minute, limit
                        )
                        if second is not None:
                            query = "SELECT * FROM protocol_by_dport_hit_on_device_id_second WHERE device_id='{}' and protocol='{}' and year={} and month={} and day={} and hour={} and minute={} and second={} LIMIT {}".format(
                                device_id, protocol, year, month, day, hour, minute, second, limit
                            )

    statement = SimpleStatement(query)
    obj = {
        "device_id": device_id,
        "protocol": protocol,
        "count": 0,
        "data": []
    }
    for protocolbydporthit in app.session.execute(statement):
        obj['data'].append(protocolbydporthit)
        obj['count'] = obj['count'] + 1

    return jsonify(obj)


@mod.route('/api/statistic/v1.0/ipsourcehit', methods=['POST'])
@app.auth.login_required
def getipsourcehit():
    # company = g.user['company']
    company = request.json.get('company')
    year = request.json.get('year')
    month = request.json.get('month')
    day = request.json.get('day')
    hour = request.json.get('hour')
    minute = request.json.get('minute')
    second = request.json.get('second')
    limit = request.json.get('limit')

    if year is not None:
        query = "SELECT * FROM ip_source_hit_on_company_year WHERE company='{}' and year={} LIMIT {}".format(
            company, year, limit
        )
        if month is not None:
            query = "SELECT * FROM ip_source_hit_on_company_month WHERE company='{}' and year={} and month={} LIMIT {}".format(
                company, year, month, limit
            )
            if day is not None:
                query = "SELECT * FROM ip_source_hit_on_company_day WHERE company='{}' and year={} and month={} and day={} LIMIT {}".format(
                    company, year, month, day, limit
                )
                if hour is not None:
                    query = "SELECT * FROM ip_source_hit_on_company_hour WHERE company='{}' and year={} and month={} and day={} and hour={} LIMIT {}".format(
                        company, year, month, day, hour, limit
                    )
                    if minute is not None:
                        query = "SELECT * FROM ip_source_hit_on_company_minute WHERE company='{}' and year={} and month={} and day={} and hour={} and minute={} LIMIT {}".format(
                            company, year, month, day, hour, minute, limit
                        )
                        if second is not None:
                            query = "SELECT * FROM ip_source_hit_on_company_sec WHERE company='{}' and year={} and month={} and day={} and hour={} and minute={} and second={} LIMIT {}".format(
                                company, year, month, day, hour, minute, second, limit
                            )

    statement = SimpleStatement(query)
    obj = {
        "company": company,
        "count": 0,
        "data": []
    }
    for ipsourcehit in app.session.execute(statement):
        obj['data'].append(ipsourcehit)
        obj['count'] = obj['count'] + 1

    return jsonify(obj)


@mod.route('/api/statistic/v1.0/ipsourcehit/<device_id>', methods=['POST'])
@app.auth.login_required
def getipsourcehitdev(device_id):
    year = request.json.get('year')
    month = request.json.get('month')
    day = request.json.get('day')
    hour = request.json.get('hour')
    minute = request.json.get('minute')
    second = request.json.get('second')
    limit = request.json.get('limit')

    if year is not None:
        query = "SELECT * FROM ip_source_hit_on_device_id_year WHERE device_id='{}' and year={} LIMIT {}".format(
            device_id, year, limit
        )
        if month is not None:
            query = "SELECT * FROM ip_source_hit_on_device_id_month WHERE device_id='{}' and year={} and month={} LIMIT {}".format(
                device_id, year, month, limit
            )
            if day is not None:
                query = "SELECT * FROM ip_source_hit_on_device_id_day WHERE device_id='{}' and year={} and month={} and day={} LIMIT {}".format(
                    device_id, year, month, day, limit
                )
                if hour is not None:
                    query = "SELECT * FROM ip_source_hit_on_device_id_hour WHERE device_id='{}' and year={} and month={} and day={} and hour={} LIMIT {}".format(
                        device_id, year, month, day, hour, limit
                    )
                    if minute is not None:
                        query = "SELECT * FROM ip_source_hit_on_device_id_minute WHERE device_id='{}' and year={} and month={} and day={} and hour={} and minute={} LIMIT {}".format(
                            device_id, year, month, day, hour, minute, limit
                        )
                        if second is not None:
                            query = "SELECT * FROM ip_source_hit_on_device_id_sec WHERE device_id='{}' and year={} and month={} and day={} and hour={} and minute={} and second={} LIMIT {}".format(
                                device_id, year, month, day, hour, minute, second, limit
                            )

    statement = SimpleStatement(query)
    obj = {
        "device_id": device_id,
        "count": 0,
        "data": []
    }
    for ipsourcehit in app.session.execute(statement):
        obj['data'].append(ipsourcehit)
        obj['count'] = obj['count'] + 1

    return jsonify(obj)


@mod.route('/api/statistic/v1.0/ipdesthit', methods=['POST'])
@app.auth.login_required
def getipdesthit():
    # company = g.user['company']
    company = request.json.get('company')
    year = request.json.get('year')
    month = request.json.get('month')
    day = request.json.get('day')
    hour = request.json.get('hour')
    minute = request.json.get('minute')
    second = request.json.get('second')
    limit = request.json.get('limit')

    if year is not None:
        query = "SELECT * FROM ip_dest_hit_on_company_year WHERE company='{}' and year={} LIMIT {}".format(
            company, year, limit
        )
        if month is not None:
            query = "SELECT * FROM ip_dest_hit_on_company_month WHERE company='{}' and year={} and month={} LIMIT {}".format(
                company, year, month, limit
            )
            if day is not None:
                query = "SELECT * FROM ip_dest_hit_on_company_day WHERE company='{}' and year={} and month={} and day={} LIMIT {}".format(
                    company, year, month, day, limit
                )
                if hour is not None:
                    query = "SELECT * FROM ip_dest_hit_on_company_hour WHERE company='{}' and year={} and month={} and day={} and hour={} LIMIT {}".format(
                        company, year, month, day, hour, limit
                    )
                    if minute is not None:
                        query = "SELECT * FROM ip_dest_hit_on_company_minute WHERE company='{}' and year={} and month={} and day={} and hour={} and minute={} LIMIT {}".format(
                            company, year, month, day, hour, minute, limit
                        )
                        if second is not None:
                            query = "SELECT * FROM ip_dest_hit_on_company_sec WHERE company='{}' and year={} and month={} and day={} and hour={} and minute={} and second={} LIMIT {}".format(
                                company, year, month, day, hour, minute, second, limit
                            )

    statement = SimpleStatement(query)
    obj = {
        "company": company,
        "count": 0,
        "data": []
    }
    for ipdesthit in app.session.execute(statement):
        obj['data'].append(ipdesthit)
        obj['count'] = obj['count'] + 1

    return jsonify(obj)


@mod.route('/api/statistic/v1.0/ipdesthitdev/<device_id>', methods=['POST'])
@app.auth.login_required
def getipdesthitdev(device_id):
    year = request.json.get('year')
    month = request.json.get('month')
    day = request.json.get('day')
    hour = request.json.get('hour')
    minute = request.json.get('minute')
    second = request.json.get('second')
    limit = request.json.get('limit')

    if year is not None:
        query = "SELECT * FROM ip_dest_hit_on_device_id_year WHERE device_id='{}' and year={} LIMIT {}".format(
            device_id, year, limit
        )
        if month is not None:
            query = "SELECT * FROM ip_dest_hit_on_device_id_month WHERE device_id='{}' and year={} and month={} LIMIT {}".format(
                device_id, year, month, limit
            )
            if day is not None:
                query = "SELECT * FROM ip_dest_hit_on_device_id_day WHERE device_id='{}' and year={} and month={} and day={} LIMIT {}".format(
                    device_id, year, month, day, limit
                )
                if hour is not None:
                    query = "SELECT * FROM ip_dest_hit_on_device_id_hour WHERE device_id='{}' and year={} and month={} and day={} and hour={} LIMIT {}".format(
                        device_id, year, month, day, hour, limit
                    )
                    if minute is not None:
                        query = "SELECT * FROM ip_dest_hit_on_device_id_minute WHERE device_id='{}' and year={} and month={} and day={} and hour={} and minute={} LIMIT {}".format(
                            device_id, year, month, day, hour, minute, limit
                        )
                        if second is not None:
                            query = "SELECT * FROM ip_dest_hit_on_device_id_sec WHERE device_id='{}' and year={} and month={} and day={} and hour={} and minute={} and second={} LIMIT {}".format(
                                device_id, year, month, day, hour, minute, second, limit
                            )

    statement = SimpleStatement(query)
    obj = {
        "device_id": device_id,
        "count": 0,
        "data": []
    }
    for ipdesthit in app.session.execute(statement):
        obj['data'].append(ipdesthit)
        obj['count'] = obj['count'] + 1

    return jsonify(obj)


@mod.route('/api/statistic/v1.0/countrysourcehit', methods=['POST'])
@app.auth.login_required
def getcountrysourcehit():
    # company = g.user['company']
    company = request.json.get('company')
    year = request.json.get('year')
    month = request.json.get('month')
    day = request.json.get('day')
    hour = request.json.get('hour')
    minute = request.json.get('minute')
    second = request.json.get('second')
    limit = request.json.get('limit')

    if year is not None:
        query = "SELECT * FROM country_source_hit_on_company_day WHERE company='{}' and year={} LIMIT {}".format(
            company, year, limit
        )
        if month is not None:
            query = "SELECT * FROM country_source_hit_on_company_day WHERE company='{}' and year={} and month={} LIMIT {}".format(
                company, year, month, limit
            )
            if day is not None:
                query = "SELECT * FROM country_source_hit_on_company_day WHERE company='{}' and year={} and month={} and day={} LIMIT {}".format(
                    company, year, month, day, limit
                )
                if hour is not None:
                    query = "SELECT * FROM country_source_hit_on_company_hour WHERE company='{}' and year={} and month={} and day={} and hour={} LIMIT {}".format(
                        company, year, month, day, hour, limit
                    )
                    if minute is not None:
                        query = "SELECT * FROM country_source_hit_on_company_minute WHERE company='{}' and year={} and month={} and day={} and hour={} and minute={} LIMIT {}".format(
                            company, year, month, day, hour, minute, limit
                        )
                        if second is not None:
                            query = "SELECT * FROM country_source_hit_on_company_second WHERE company='{}' and year={} and month={} and day={} and hour={} and minute={} and second={} LIMIT {}".format(
                                company, year, month, day, hour, minute, second, limit
                            )

    statement = SimpleStatement(query)
    obj = {
        "company": company,
        "count": 0,
        "data": []
    }
    for countrysourcehit in app.session.execute(statement):
        obj['data'].append(countrysourcehit)
        obj['count'] = obj['count'] + 1

    return jsonify(obj)


@mod.route('/api/statistic/v1.0/countrysourcehit/<device_id>', methods=['POST'])
@app.auth.login_required
def getcountrysourcehitdev(device_id):
    year = request.json.get('year')
    month = request.json.get('month')
    day = request.json.get('day')
    hour = request.json.get('hour')
    minute = request.json.get('minute')
    second = request.json.get('second')
    limit = request.json.get('limit')

    if year is not None:
        query = "SELECT * FROM country_source_hit_on_device_id_year WHERE device_id='{}' and year={} LIMIT {}".format(
            device_id, year, limit
        )
        if month is not None:
            query = "SELECT * FROM country_source_hit_on_device_id_month WHERE device_id='{}' and year={} and month={} LIMIT {}".format(
                device_id, year, month, limit
            )
            if day is not None:
                query = "SELECT * FROM country_source_hit_on_device_id_day WHERE device_id='{}' and year={} and month={} and day={} LIMIT {}".format(
                    device_id, year, month, day, limit
                )
                if hour is not None:
                    query = "SELECT * FROM country_source_hit_on_device_id_hour WHERE device_id='{}' and year={} and month={} and day={} and hour={} LIMIT {}".format(
                        device_id, year, month, day, hour, limit
                    )
                    if minute is not None:
                        query = "SELECT * FROM country_source_hit_on_device_id_minute WHERE device_id='{}' and year={} and month={} and day={} and hour={} and minute={} LIMIT {}".format(
                            device_id, year, month, day, hour, minute, limit
                        )
                        if second is not None:
                            query = "SELECT * FROM country_source_hit_on_device_id_second WHERE device_id='{}' and year={} and month={} and day={} and hour={} and minute={} and second={} LIMIT {}".format(
                                device_id, year, month, day, hour, minute, second, limit
                            )

    statement = SimpleStatement(query)
    obj = {
        "device_id": device_id,
        "count": 0,
        "data": []
    }
    for ipdesthit in app.session.execute(statement):
        obj['data'].append(ipdesthit)
        obj['count'] = obj['count'] + 1

    return jsonify(obj)


@mod.route('/api/statistic/v1.0/countrydesthit', methods=['POST'])
@app.auth.login_required
def getcountrydesthit():
    # company = g.user['company']
    company = request.json.get('company')
    year = request.json.get('year')
    month = request.json.get('month')
    day = request.json.get('day')
    hour = request.json.get('hour')
    minute = request.json.get('minute')
    second = request.json.get('second')
    limit = request.json.get('limit')

    if year is not None:
        query = "SELECT * FROM country_dest_hit_on_company_year WHERE company='{}' and year={} LIMIT {}".format(
            company, year, limit
        )
        if month is not None:
            query = "SELECT * FROM country_dest_hit_on_company_day WHERE company='{}' and year={} and month={} LIMIT {}".format(
                company, year, month, limit
            )
            if day is not None:
                query = "SELECT * FROM country_dest_hit_on_company_day WHERE company='{}' and year={} and month={} and day={} LIMIT {}".format(
                    company, year, month, day, limit
                )
                if hour is not None:
                    query = "SELECT * FROM country_dest_hit_on_company_hour WHERE company='{}' and year={} and month={} and day={} and hour={} LIMIT {}".format(
                        company, year, month, day, hour, limit
                    )
                    if minute is not None:
                        query = "SELECT * FROM country_dest_hit_on_company_minute WHERE company='{}' and year={} and month={} and day={} and hour={} and minute={} LIMIT {}".format(
                            company, year, month, day, hour, minute, limit
                        )
                        if second is not None:
                            query = "SELECT * FROM country_dest_hit_on_company_second WHERE company='{}' and year={} and month={} and day={} and hour={} and minute={} and second={} LIMIT {}".format(
                                company, year, month, day, hour, minute, second, limit
                            )

    statement = SimpleStatement(query)
    obj = {
        "company": company,
        "count": 0,
        "data": []
    }
    for countrydesthit in app.session.execute(statement):
        obj['data'].append(countrydesthit)
        obj['count'] = obj['count'] + 1

    return jsonify(obj)


@mod.route('/api/statistic/v1.0/countrydesthit/<device_id>', methods=['POST'])
@app.auth.login_required
def getcountrydesthitdev(device_id):
    year = request.json.get('year')
    month = request.json.get('month')
    day = request.json.get('day')
    hour = request.json.get('hour')
    minute = request.json.get('minute')
    second = request.json.get('second')
    limit = request.json.get('limit')

    if year is not None:
        query = "SELECT * FROM country_dest_hit_on_device_id_year WHERE device_id='{}' and year={} LIMIT {}".format(
            device_id, year, limit
        )
        if month is not None:
            query = "SELECT * FROM country_dest_hit_on_device_id_month WHERE device_id='{}' and year={} and month={} LIMIT {}".format(
                device_id, year, month, limit
            )
            if day is not None:
                query = "SELECT * FROM country_dest_hit_on_device_id_day WHERE device_id='{}' and year={} and month={} and day={} LIMIT {}".format(
                    device_id, year, month, day, limit
                )
                if hour is not None:
                    query = "SELECT * FROM country_dest_hit_on_device_id_hour WHERE device_id='{}' and year={} and month={} and day={} and hour={} LIMIT {}".format(
                        device_id, year, month, day, hour, limit
                    )
                    if minute is not None:
                        query = "SELECT * FROM country_dest_hit_on_device_id_minute WHERE device_id='{}' and year={} and month={} and day={} and hour={} and minute={} LIMIT {}".format(
                            device_id, year, month, day, hour, minute, limit
                        )
                        if second is not None:
                            query = "SELECT * FROM country_dest_hit_on_device_id_second WHERE device_id='{}' and year={} and month={} and day={} and hour={} and minute={} and second={} LIMIT {}".format(
                                device_id, year, month, day, hour, minute, second, limit
                            )

    statement = SimpleStatement(query)
    obj = {
        "device_id": device_id,
        "count": 0,
        "data": []
    }
    for countrydesthit in app.session.execute(statement):
        obj['data'].append(countrydesthit)
        obj['count'] = obj['count'] + 1

    return jsonify(obj)
