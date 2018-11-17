from flask import Flask, g, request, jsonify

app = Flask(__name__)


@app.route("/timeseries", methods=['POST'])
def timeseries_create():
    data = request.get_json()
    return jsonify(data)


@app.route("/timeseries/<timeseries_id>", methods=['GET'])
def timeseries_get(timeseries_id):
    return jsonify(timeseriesId=timeseries_id)


@app.route("/timeseries", methods=['GET'])
def timeseries_list():
    data = request.get_json()
    return jsonify([])


@app.route("/timeseries/<timeseries_id>", methods=['DELETE'])
def timeseries_delete(timeseries_id):
    return jsonify(timeseriesId=timeseries_id)


@app.route("/public/hc")
def public_hc():
    return "OK", 200


@app.errorhandler(AssertionError)
def handle_assertion(error):
    ret = {'code': 400, 'error': error.args[0]}
    app.logger.warn('ERR {code} {error}'.format(**ret),
        extra={'event': 'error', 'error': ret['error']})
    return jsonify(**ret), ret['code']


@app.after_request
def log_request(response):
    if not request.path == '/public/hc':
        ret = {'status': response.status_code, 'request_method': request.method, 'request_uri': request.url}
        app.logger.info("{status} {request_method} {request_uri}".format(**ret), extra=ret)
        print("{status} {request_method} {request_uri}".format(**ret))
    return response
