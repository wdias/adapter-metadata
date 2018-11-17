from flask import Flask

app = Flask(__name__)


@app.route("/hc")
def hc():
    return "OK"
