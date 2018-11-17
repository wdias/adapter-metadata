from flask import Flask, request, jsonify
from web import util
from web.api import timeseries, location, parameter, timestep

try:
    assert False; import sys; sys.exit('ERROR asserts disabled, exiting')
except AssertionError:
    pass

engine = util.get_engine('metadata')
app = Flask(__name__)
# Register endpoints
app.register_blueprint(timeseries.bp)
app.register_blueprint(location.bp)
app.register_blueprint(parameter.bp)
app.register_blueprint(timestep.bp)


@app.route("/public/hc")
def public_hc():
    return "OK", 200


@app.errorhandler(AssertionError)
def handle_assertion(error):
    ret = {'code': 400, 'error': error.args[0]}
    app.logger.warn('ERR {code} {error}'.format(**ret),
                    extra={'event': 'error', 'error': ret['error']})
    print('ERR {code} {error}'.format(**ret))
    return jsonify(**ret), ret['code']


@app.after_request
def log_request(response):
    if not request.path == '/public/hc':
        ret = {'status': response.status_code, 'request_method': request.method, 'request_uri': request.url}
        app.logger.info("{status} {request_method} {request_uri}".format(**ret), extra=ret)
        print("{status} {request_method} {request_uri}".format(**ret))
    return response
