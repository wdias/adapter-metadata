from flask import Blueprint, request, jsonify

bp = Blueprint('parameter', __name__)


@bp.route("/parameter", methods=['POST'])
def parameter_create():
    data = request.get_json()
    return jsonify(data)
