from flask import Blueprint, request, jsonify

bp = Blueprint('location', __name__, url_prefix='/location')


# Points
@bp.route("/point", methods=['POST'])
def location_point_create():
    data = request.get_json()
    return jsonify(data)


# Grids
@bp.route("/regular-grid", methods=['POST'])
def location_regular_grid_create():
    data = request.get_json()
    return jsonify(data)


@bp.route("/irregular-grid", methods=['POST'])
def location_irregular_grid_create():
    data = request.get_json()
    return jsonify(data)

