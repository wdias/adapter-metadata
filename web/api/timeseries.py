from flask import Blueprint, request, jsonify
from sqlalchemy import text as sql
import logging

from web import util
from web.api import parameter as t_parameter, location as t_location, timestep as t_timestep
from web.cache import Cache

bp = Blueprint('timeseries', __name__)
ENGINE = util.get_engine('metadata')
CACHE = Cache()

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
    print('POST timeseries_create:', data)
    assert data and isinstance(data, dict), f'Timeseries data should be provided'
    with ENGINE.begin() as conn:
        # Parameter
        assert 'parameterId' in data or 'parameter' in data, f'`parameterId` or `parameter` should be provided'
        parameter = data.get('parameter', {})
        data['parameterId'] = parameter_id = data.get('parameterId', parameter.get('parameterId'))
        exist_parameter_id = conn.execute(sql('''
            SELECT parameterId FROM parameters WHERE parameterId=:parameter_id
        '''), parameter_id=parameter_id).fetchone()
        if exist_parameter_id is None and parameter:
            t_parameter.db_parameter_create(conn, parameter)
            exist_parameter_id = parameter
        assert exist_parameter_id, f'Parameter does not exists: parameter_id'
        # ValueType
        assert data.get('valueType') in ['Scalar', 'Vector', 'Grid'], 'ValueType does not have a valid value'
        # Location
        assert 'locationId' in data or 'location' in data, f'`locationId` or `location` should be provided'
        location = data.get('location', {})
        data['locationId'] = location_id = data.get('locationId', location.get('locationId'))
        exist_location_id = conn.execute(sql('''
            SELECT locationId FROM grids WHERE locationId=:location_id
        ''' if data['valueType'] == 'Grid' else '''
            SELECT locationId FROM locations WHERE locationId=:location_id
        '''), location_id=location_id).fetchone()
        if exist_location_id is None and location:
            t_location.db_regular_grid_create(conn, location) if data['valueType'] == 'Grid' \
                else t_location.db_location_create(conn, location)
            exist_location_id = location
        assert exist_location_id, f'Location does not exists: {location_id}'
        # TimeStep
        assert 'timeStepId' in data or 'timeStep' in data, f'`timeStepId` or `timeStep` should be provided'
        time_step = data.get('timeStep', {})
        data['timeStepId'] = time_step_id = data.get('timeStepId', time_step.get('timeStepId'))
        exist_time_step_id = conn.execute(sql('''
            SELECT timeStepId FROM time_steps WHERE timeStepId=:time_step_id
        '''), time_step_id=time_step_id).fetchone()
        if exist_time_step_id is None and time_step:
            t_timestep.db_time_step_create(conn, time_step)
            exist_time_step_id = time_step
        assert exist_time_step_id, f'TimeStep does not exists: {time_step_id}'
        # TimeseriesType
        assert data.get('timeseriesType') in ['ExternalHistorical', 'ExternalForecasting', 'SimulatedHistorical', 'SimulatedForecasting'], 'TimeseriesType does not have a valid value'

        # Create Timeseries
        timeseries = util.get_timeseries(**data)
        conn.execute(sql('''
            INSERT IGNORE INTO timeseries (timeseriesId, moduleId, valueType, parameterId, locationId, timeseriesType, timeStepId)
            VALUES (:timeseriesId, :moduleId, :valueType, :parameterId, :locationId, :timeseriesType, :timeStepId)
        '''), **timeseries)
        return jsonify(timeseries)


@bp.route("/timeseries/<timeseries_id>", methods=['GET'])
def timeseries_get(timeseries_id):
    timeseries = CACHE.get(timeseries_id)
    print("cached:", timeseries)
    if timeseries is None:
        timeseries = ENGINE.execute(sql('''
            SELECT timeseriesId, moduleId, valueType, parameterId, locationId, timeseriesType, timeStepId
            FROM timeseries WHERE timeseriesId=:timeseries_id
        '''), timeseries_id=timeseries_id).fetchone()
        CACHE.set_timeseries(timeseries_id, **timeseries)
    assert timeseries, f'Timeseries does not exists: {timeseries_id}'
    return jsonify(**timeseries)


@bp.route("/timeseries", methods=['GET'])
def timeseries_list():
    # TODO: Limiting and Skipping
    q = ''
    if len(request.query_string):
        for key in request.args.to_dict().keys():
            q += f'{" AND" if len(q) else "WHERE"} {key}=:{key}'

    timeseries = ENGINE.execute(sql('''
        SELECT timeseriesId, moduleId, valueType, parameterId, locationId, timeseriesType, timeStepId
        FROM timeseries
    ''' + q), **request.args.to_dict()).fetchall()
    return jsonify([dict(i) for i in timeseries])


@bp.route("/timeseries/<timeseries_id>", methods=['DELETE'])
def timeseries_delete(timeseries_id):
    timeseries = ENGINE.execute(sql('''
        DELETE FROM timeseries
        WHERE timeseriesId=:timeseries_id
    '''), timeseries_id=timeseries_id)
    CACHE.delete(timeseries_id)
    return jsonify(timeseries_id)
