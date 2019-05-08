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
        data['parameterId'] = data.get('parameterId', parameter.get('parameterId'))
        exist_parameter = t_parameter.db_parameter_get(data['parameterId'])
        if exist_parameter is None and parameter:
            t_parameter.db_parameter_create(conn, parameter)
            exist_parameter = parameter
        else:
            exist_parameter = dict(exist_parameter)
        assert exist_parameter, f'Parameter does not exists: {data["parameterId"]}'
        # ValueType
        assert data.get('valueType') in ['Scalar', 'Vector', 'Grid'], 'ValueType does not have a valid value'
        # Location
        assert 'locationId' in data or 'location' in data, f'`locationId` or `location` should be provided'
        location = data.get('location', {})
        data['locationId'] = data.get('locationId', location.get('locationId'))
        exist_location = t_location.db_regular_grid_get(data['locationId']) if data['valueType'] == 'Grid' else \
            t_location.db_location_point_get(data['locationId'])
        if exist_location is None and location:
            t_location.db_regular_grid_create(conn, location) if data['valueType'] == 'Grid' \
                else t_location.db_location_create(conn, location)
            exist_location = location
        else:
            exist_location = dict(exist_location)
        assert exist_location, f'Location does not exists: {data["locationId"]}'
        # TimeStep
        assert 'timeStepId' in data or 'timeStep' in data, f'`timeStepId` or `timeStep` should be provided'
        time_step = data.get('timeStep', {})
        data['timeStepId'] = data.get('timeStepId', time_step.get('timeStepId'))
        exist_time_step = t_timestep.db_time_step_get(data['timeStepId'])
        if exist_time_step is None and time_step:
            t_timestep.db_time_step_create(conn, time_step)
            exist_time_step = time_step
        else:
            exist_time_step = dict(exist_time_step)
        assert exist_time_step, f'TimeStep does not exists: {data["timeStepId"]}'
        # TimeseriesType
        assert data.get('timeseriesType') in ['ExternalHistorical', 'ExternalForecasting', 'SimulatedHistorical', 'SimulatedForecasting'], 'TimeseriesType does not have a valid value'

        # Create Timeseries
        timeseries = util.get_timeseries(**data)
        conn.execute(sql('''
            INSERT IGNORE INTO timeseries (timeseriesId, moduleId, valueType, parameterId, locationId, timeseriesType, timeStepId)
            VALUES (:timeseriesId, :moduleId, :valueType, :parameterId, :locationId, :timeseriesType, :timeStepId)
        '''), **timeseries)
        timeseries['parameter'] = exist_parameter
        timeseries['location'] = exist_location
        timeseries['timeStep'] = exist_time_step
        CACHE.set_timeseries(f'f-{timeseries["timeseriesId"]}', **timeseries)
        return jsonify(timeseries)

def db_timeseries_get_full(timeseries_id):
    timeseries = CACHE.get(f'f-{timeseries_id}')
    print("cached full:", timeseries)
    if timeseries is None:
        timeseries = ENGINE.execute(sql('''
            SELECT timeseriesId, moduleId, valueType, parameterId, locationId, timeseriesType, timeStepId
            FROM timeseries WHERE timeseriesId=:timeseries_id
        '''), timeseries_id=timeseries_id).fetchone()
        timeseries['parameter'] = t_parameter.db_parameter_get(timeseries['parameterId'])
        timeseries['location'] = t_location.db_regular_grid_get(timeseries['locationId']) if timeseries['valueType'] == 'Grid' \
                else t_location.db_location_point_get(timeseries['locationId'])
        timeseries['timeStep'] = t_timestep.db_time_step_get(timeseries['timeStepId'])
        CACHE.set_timeseries(f'f-{timeseries_id}', **timeseries)
    return timeseries

def db_timeseries_get(timeseries_id):
    timeseries = CACHE.get(timeseries_id)
    print("cached:", timeseries)
    if timeseries is None:
        timeseries = ENGINE.execute(sql('''
            SELECT timeseriesId, moduleId, valueType, parameterId, locationId, timeseriesType, timeStepId
            FROM timeseries WHERE timeseriesId=:timeseries_id
        '''), timeseries_id=timeseries_id).fetchone()
        CACHE.set_timeseries(timeseries_id, **timeseries)
    return timeseries

@bp.route("/timeseries/<timeseries_id>", methods=['GET'])
def timeseries_get(timeseries_id):
    if request.args.get('full') is not None:
        timeseries = db_timeseries_get_full(timeseries_id)
    else:
        timeseries = db_timeseries_get(timeseries_id)
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
    CACHE.delete(f'f-timeseries_id')
    return jsonify(timeseries_id)
