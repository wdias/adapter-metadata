from flask import Blueprint, request, jsonify

bp = Blueprint('timestep', __name__)


@bp.route("/timestep", methods=['POST'])
def timestep_create():
    data = request.get_json()
    return jsonify(data)
