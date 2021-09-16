import trabajo_final
from flask import Flask
from flask import jsonify
import pandas as pd
import glob
import pymongo
from pymongo import MongoClient

app = Flask(__name__)


@app.route('/api/data/importcsv', methods=['GET'])
def insertAll():
    return jsonify({"message": trabajo_final.data()})


@app.route('/api/data/getOne', methods=['GET'])
def getOne():
    dictionary = trabajo_final.getOne()
    return jsonify(dictionary)


@app.route('/api/data/getAll', methods=['GET'])
def getAll():
    lst = trabajo_final.getAll()
    return jsonify(lst)


if __name__ == '__main__':
    app.run(host='localhost', port=4000)
