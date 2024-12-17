from flask import Flask, jsonify
from flask_cors import CORS
from example_delta import (
    run_delta_example,
    run_delta_update_example,
    run_delta_versioning_example,
    run_delta_delete_example,
    run_delta_complex_example
)

app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*"}})

@app.route('/api/delta/simple', methods=['GET'])
def get_simple_data():
    result = run_delta_example()
    return jsonify(result)

@app.route('/api/delta/update', methods=['GET'])
def get_update_data():
    result = run_delta_update_example()
    return jsonify(result)

@app.route('/api/delta/versioning', methods=['GET'])
def get_versioning_data():
    result = run_delta_versioning_example()
    return jsonify(result)

@app.route('/api/delta/delete', methods=['GET'])
def get_delete_data():
    result = run_delta_delete_example()
    return jsonify(result)

@app.route('/api/delta/complex', methods=['GET'])
def get_complex_data():
    result = run_delta_complex_example()
    return jsonify(result)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)