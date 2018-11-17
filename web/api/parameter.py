from flask import Blueprint, request, jsonify
from sqlalchemy import text as sql
from web import util

bp = Blueprint('parameter', __name__)
ENGINE = util.get_engine('metadata')


@bp.route("/parameter", methods=['POST'])
def parameter_create():
    data = request.get_json()
    with ENGINE.begin() as conn:
        parameter_id = data.get('parameterId')
        exists_id = conn.execute(sql('''
            SELECT parameterId FROM parameters WHERE parameterId=:parameter_id
        '''), parameter_id=parameter_id).fetchone()
        assert exists_id is None, f'Parameter already exists: {parameter_id}'
        conn.execute(sql('''
            INSERT INTO parameters (parameterId, variable, unit, parameterType)
            VALUES (:parameterId, :variable, :unit, :parameterType)
        '''), **data)
        return jsonify(data)


@bp.route("/parameter/<parameter_id>", methods=['GET'])
def parameter_get(parameter_id):
    return jsonify(parameter_id)


@bp.route("/parameter", methods=['GET'])
def parameter_list():
    return jsonify([])


@bp.route("/parameter/<parameter_id>", methods=['PUT'])
def parameter_update(parameter_id):
    data = request.get_json()
    return jsonify(data)


@bp.route("/parameter/<parameter_id>", methods=['DELETE'])
def parameter_delete(parameter_id):
    ENGINE.execute(sql('''
            DELETE FROM parameters
            WHERE parameterId=:parameter_id
        '''), parameter_id=parameter_id)
    return jsonify(parameter_id)
