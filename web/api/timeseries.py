from flask import Blueprint, request, jsonify
from sqlalchemy import text as sql

from web import util
from web.api import parameter as t_parameter, location as t_location, timestep as t_timestep

bp = Blueprint('timeseries', __name__)
ENGINE = util.get_engine('metadata')

""" Timeseries Structure:
    id timeseriesId
    1. moduleId
    2. valueType
    3. parameterId *
    4. locationId *
    5. timeseriesType
    6. timeStepId *
"""
@bp.route("/timeseries", methods=['POST'])
def timeseries_create():
    data = request.get_json()
    with ENGINE.begin() as conn:
        # Parameter
        assert 'parameter_id' in data or 'parameter' in data, f'`{parameter_id}` or `{parameter}` should be provided'
        parameter = data.get('parameter')
        data['parameterId'] = parameter_id = data.get('parameterId', parameter.get('parameterId'))
        exist_parameter_id = conn.execute(sql('''
            SELECT parameterId FROM parameters WHERE parameterId=:parameter_id
        '''), parameter_id=parameter_id).fetchone()
        if exist_parameter_id is None and parameter:
            t_parameter.db_parameter_create(conn, parameter)
            exist_parameter_id = parameter
        assert exist_parameter_id, f'Parameter does not exists: {parameter_id}'
        # Location
        assert 'location_id' in data or 'location' in data, f'`{location_id}` or `{location}` should be provided'
        location = data.get('location')
        data['locationId'] = location_id = data.get('locationId', location.get('locationId'))
        exist_location_id = conn.execute(sql('''
            SELECT locationId FROM locations WHERE locationId=:location_id
        '''), location_id=location_id).fetchone()
        if exist_location_id is None and location:
            t_location.db_location_create(conn, location)
            exist_location_id = location
        assert exist_location_id, f'Location does not exists: {location_id}'
        # TimeStep
        assert 'time_step_id' in data or 'timeStep' in data, f'`{time_step_id}` or `{timeStep}` should be provided'
        time_step = data.get('timeStep')
        data['timeStepId'] = time_step_id = data.get('timeStepId', time_step.get('timeStepId'))
        exist_time_step_id = conn.execute(sql('''
            SELECT timeStepId FROM time_steps WHERE timeStepId=:time_step_id
        '''), time_step_id=time_step_id).fetchone()
        if exist_time_step_id is None and time_step:
            t_timestep.db_time_step_create(conn, time_step)
            exist_time_step_id = time_step
        assert exist_time_step_id, f'TimeStep does not exists: {time_step_id}'

        # Create Timeseries
        timeseries = util.get_timeseries(**data)
        conn.execute(sql('''
            INSERT IGNORE INTO timeseries (timeseriesId, moduleId, valueType, parameterId, locationId, timeseriesType, timeStepId)
            VALUES (:timeseriesId, :moduleId, :valueType, :parameterId, :locationId, :timeseriesType, :timeStepId)
        '''), **timeseries)
        return jsonify(timeseries)


@bp.route("/timeseries/<timeseries_id>", methods=['GET'])
def timeseries_get(timeseries_id):
    return jsonify(timeseriesId=timeseries_id)


@bp.route("/timeseries", methods=['GET'])
def timeseries_list():
    data = request.get_json()
    return jsonify([])


@bp.route("/timeseries/<timeseries_id>", methods=['DELETE'])
def timeseries_delete(timeseries_id):
    return jsonify(timeseriesId=timeseries_id)
