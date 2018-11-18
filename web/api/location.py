from flask import Blueprint, request, jsonify
from sqlalchemy import text as sql
from web import util

bp = Blueprint('location', __name__, url_prefix='/location')
ENGINE = util.get_engine('metadata')


# --- Points ---
@bp.route("/point", methods=['POST'])
def location_point_create():
    data = request.get_json()
    with ENGINE.begin() as conn:
        location_id = data.get('locationId')
        exists_id = conn.execute(sql('''
            SELECT locationId FROM locations WHERE locationId=:location_id
        '''), location_id=location_id).fetchone()
        assert exists_id is None, f'Location already exists: {location_id}'
        conn.execute(sql('''
            INSERT INTO locations (locationId, name, lat, lon)
            VALUES (:locationId, :name, :lat, :lon)
        '''), **data)
        return jsonify(data)


@bp.route("/point/<location_id>", methods=['GET'])
def location_point_get(location_id):
    location = ENGINE.execute(sql('''
        SELECT locationId, name, lat, lon FROM locations WHERE locationId=:location_id
    '''), location_id=location_id).fetchone()
    assert location, f'Location does not exists: {location_id}'
    return jsonify(**location)


@bp.route("/point", methods=['GET'])
def location_point_list():
    locations = ENGINE.execute(sql('''
            SELECT locationId, name, lat, lon FROM locations
        ''')).fetchall()
    return jsonify([dict(i) for i in locations])


@bp.route("/point/<location_id>", methods=['PUT'])
def location_point_update(location_id):
    data = request.get_json()
    return jsonify(data)


@bp.route("/point/<location_id>", methods=['DELETE'])
def location_point_delete(location_id):
    ENGINE.execute(sql('''
            DELETE FROM locations
            WHERE locationId=:location_id
        '''), location_id=location_id)
    return jsonify(location_id)


# --- Grids ---
@bp.route("/regular-grid", methods=['POST'])
def location_regular_grid_create():
    data = request.get_json()
    return jsonify(data)


@bp.route("/irregular-grid", methods=['POST'])
def location_irregular_grid_create():
    data = request.get_json()
    return jsonify(data)

