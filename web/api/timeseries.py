from flask import Blueprint, request, jsonify

bp = Blueprint('timeseries', __name__)


@bp.route("/timeseries", methods=['POST'])
def timeseries_create():
    data = request.get_json()
    return jsonify(data)


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
