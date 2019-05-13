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
        db_location_create(conn, data)
        return jsonify(data)


def db_location_create(conn, data):
    assert 'name' in data and 'lat' in data and 'lon' in data, 'name, lat and lon is required for Location.'
    conn.execute(sql('''
        INSERT INTO locations (locationId, name, lat, lon)
        VALUES (:locationId, :name, :lat, :lon)
    '''), **data)
    return data


@bp.route("/point/<location_id>", methods=['GET'])
def location_point_get(location_id):
    assert location_id, 'LocationId should provided.'
    location = db_location_point_get(location_id)
    assert location, f'Location does not exists: {location_id}'
    return jsonify(**location)


def db_location_point_get(location_id):
    location = ENGINE.execute(sql('''
        SELECT locationId, name, lat, lon 
        FROM locations WHERE locationId=:location_id
    '''), location_id=location_id).fetchone()
    return location


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


# --- Regular Grids ---
@bp.route("/regular-grid", methods=['POST'])
def location_regular_grid_create():
    data = request.get_json()
    with ENGINE.begin() as conn:
        location_id = data.get('locationId', None)
        exists_id = conn.execute(sql('''
            SELECT locationId FROM grids WHERE locationId=:location_id AND gridType='Regular'
        '''), location_id=location_id).fetchone()
        assert exists_id is None, f'Grid already exists: {location_id}'
        db_regular_grid_create(conn, data)
        del data['dataType']
        del data['data']
        return jsonify(data)


def db_regular_grid_create(conn, data):
    import json
    data_type_set = {'gridFirstCell', 'gridCorners'}
    data_type = data_type_set & set(data.keys())
    assert len(data_type) == 1, f'Should be provide one of {", ".join(list(data_type_set))}'
    data['dataType'] = data_type.pop()
    data['data'] = json.dumps(data[data['dataType']])
    data['description'] = data.get('description')

    conn.execute(sql('''
        INSERT INTO grids (locationId, gridType, rows, columns, geoDatum, dataType, data, description)
        VALUES (:locationId, :gridType, :rows, :columns, :geoDatum, :dataType, :data, :description)
    '''), gridType='Regular', **data)
    return data


@bp.route("/regular-grid/<location_id>", methods=['GET'])
def location_regular_grid_get(location_id):
    location = db_regular_grid_get(location_id)
    assert location, f'Location does not exists: {location_id}'
    return jsonify(**location)


def db_regular_grid_get(location_id):
    import json
    location = ENGINE.execute(sql('''
        SELECT locationId, gridType, rows, columns, geoDatum, dataType, data, description 
        FROM grids WHERE locationId=:location_id
    '''), location_id=location_id).fetchone()
    if location:
        location = dict(location)
        location[location['dataType']] = json.loads(location['data'])
        del location['dataType']
        del location['data']
    return location


@bp.route("/regular-grid", methods=['GET'])
def location_regular_grid_list():
    locations = ENGINE.execute(sql('''
        SELECT locationId, gridType, rows, columns, geoDatum, description FROM grids
    ''')).fetchall()
    return jsonify([dict(i) for i in locations])


@bp.route("/regular-grid/<location_id>", methods=['PUT'])
def location_regular_grid_update(location_id):
    data = request.get_json()
    return jsonify(data)


@bp.route("/regular-grid/<location_id>", methods=['DELETE'])
def location_regular_grid_delete(location_id):
    ENGINE.execute(sql('''
        DELETE FROM grids
        WHERE locationId=:location_id
    '''), location_id=location_id)
    return jsonify(location_id)


# --- Irregular Grids ---
# TODO: Implement Irregualr Grids support
@bp.route("/irregular-grid", methods=['POST'])
def location_irregular_grid_create():
    data = request.get_json()
    return "Not implemented", 200
