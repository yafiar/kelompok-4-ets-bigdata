from flask import Flask, render_template, jsonify
import json
import os

app = Flask(__name__)

DATA_DIR = 'data'

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/spark')
def get_spark_results():
    try:
        with open(os.path.join(DATA_DIR, 'spark_results.json')) as f:
            return jsonify(json.load(f))
    except:
        return jsonify({"error": "No data yet"})

if __name__ == '__main__':
    app.run(debug=True, port=5000)
