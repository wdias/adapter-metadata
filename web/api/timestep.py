from flask import Blueprint, request, jsonify
from sqlalchemy import text as sql
from web import util

bp = Blueprint('timestep', __name__)
ENGINE = util.get_engine('metadata')


@bp.route("/timestep", methods=['POST'])
def timestep_create():
    data = request.get_json()
    with ENGINE.begin() as conn:
        time_step_id = data.get('timeStepId')
        exists_id = conn.execute(sql('''
            SELECT timeStepId FROM time_steps WHERE timeStepId=:time_step_id
        '''), time_step_id=time_step_id).fetchone()
        assert exists_id is None, f'TimeStep already exists: {time_step_id}'
        conn.execute(sql('''
            INSERT INTO time_steps (timeStepId, unit, multiplier, divider)
            VALUES (:timeStepId, :unit, :multiplier, :divider)
        '''), **data)
        return jsonify(data)


@bp.route("/timestep/<time_step_id>", methods=['GET'])
def timestep_get(time_step_id):
    time_step = ENGINE.execute(sql('''
        SELECT timeStepId, unit, multiplier, divider FROM time_steps WHERE timeStepId=:time_step_id
    '''), time_step_id=time_step_id).fetchone()
    assert time_step, f'TimeStep does not exists: {time_step_id}'
    return jsonify(**time_step)


@bp.route("/timestep", methods=['GET'])
def timestep_list():
    time_steps = ENGINE.execute(sql('''
            SELECT timeStepId, unit, multiplier, divider FROM time_steps
        ''')).fetchall()
    return jsonify([dict(i) for i in time_steps])


@bp.route("/timestep/<time_step_id>", methods=['PUT'])
def timestep_update(time_step_id):
    data = request.get_json()
    return jsonify(data)


@bp.route("/timestep/<time_step_id>", methods=['DELETE'])
def timestep_delete(time_step_id):
    ENGINE.execute(sql('''
            DELETE FROM time_steps
            WHERE timeStepId=:time_step_id
        '''), time_step_id=time_step_id)
    return jsonify(time_step_id)
