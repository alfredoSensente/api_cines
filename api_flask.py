import trabajo_final
from flask import Flask
from flask import jsonify
import pandas as pd
import glob
import pymongo
from pymongo import MongoClient

app = Flask(__name__)


@app.route('/api/data/importcsv', methods=['GET'])
def welcome():
    return jsonify({"message": trabajo_final.data()})


if __name__ == '__main__':
    app.run(host='localhost', port=4000)
