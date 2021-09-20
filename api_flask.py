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


@app.route('/api/data/getOne/<string:title>', methods=['GET'])
def getOne(title):
    dictionary = trabajo_final.getOne(title)
    return jsonify(dictionary)

@app.route('/api/data/getTotalPersonaPais', methods=['GET'])
def getTotalPersonaPais():
    lst = trabajo_final.getTotalPersonasPais()
    return jsonify(lst)

@app.route('/api/data/getTotalPersona', methods=['GET'])
def getTotalPersona():
    lst = trabajo_final.getTotalPersonas()
    return jsonify(lst)

@app.route('/api/data/getTotalPersonasCadenaPais', methods=['GET'])
def getTotalPersonasCadenaPais():
    lst = trabajo_final.getTotalPersonasCadenaPais()
    return jsonify(lst)

@app.route('/api/data/getAsistenciaTotalPeliculas', methods=['GET'])
def getAsistenciaTotalPeliculas():
    lst = trabajo_final.getAsistenciaTotalPeliculas()
    return jsonify(lst)

@app.route('/api/data/getAsistenciaCadenaPeliculas', methods=['GET'])
def getAsistenciaCadenaPeliculas():
    lst = trabajo_final.getAsistenciaCadenaPeliculas()
    return jsonify(lst)

@app.route('/api/data/getAsistenciaCadenaPeliculasPorcentaje', methods=['GET'])
def getAsistenciaCadenaPeliculasPorcentaje():
    lst = trabajo_final.getAsistenciaCadenaPeliculasPorcentaje()
    return jsonify(lst)

@app.route('/api/data/getMasVistaMenosVista', methods=['GET'])
def getMasVistaMenosVista():
    lst = trabajo_final.getMasVistaMenosVista()
    return jsonify(lst)

@app.route('/api/data/getAsistenciaCinePaisFecha/<string:fechaInicio>/<string:fechaFin>/<string:anio>', methods=['GET'])
def getAsistenciaCinePaisFecha(fechaInicio, fechaFin, anio):
    lst = trabajo_final.getAsistenciaCinePaisFecha(fechaInicio, fechaFin, anio)
    return jsonify(lst)


if __name__ == '__main__':
    app.run(host='localhost', port=4000)
