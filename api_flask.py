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
    return jsonify({"Filas insertadas": trabajo_final.data()})


@app.route('/api/data/getOne', methods=['GET'])
def getOne():
    dictionary = trabajo_final.getOne()
    return jsonify(dictionary)

# Consulta 1


@app.route('/api/data/getTotalPersonaPais/<string:fechaInicio>/<string:fechaFin>/<string:anio>', methods=['GET'])
def getTotalPersonaPais(fechaInicio, fechaFin, anio):
    lst = trabajo_final.getTotalPersonasPais(fechaInicio, fechaFin, anio)
    return jsonify(lst)

# Consulta 2


@app.route('/api/data/getTotalPersona/<string:fechaInicio>/<string:fechaFin>/<string:anio>', methods=['GET'])
def getTotalPersona(fechaInicio, fechaFin, anio):
    lst = trabajo_final.getTotalPersonas(fechaInicio, fechaFin, anio)
    return jsonify(lst)

# Consulta 3


@app.route('/api/data/getTotalPersonasCadenaPais/<string:fechaInicio>/<string:fechaFin>/<string:anio>', methods=['GET'])
def getTotalPersonasCadenaPais(fechaInicio, fechaFin, anio):
    lst = trabajo_final.getTotalPersonasCadenaPais(fechaInicio, fechaFin, anio)
    return jsonify(lst)

# Consulta 4


@app.route('/api/data/getAsistenciaTotalPeliculas', methods=['GET'])
def getAsistenciaTotalPeliculas():
    lst = trabajo_final.getAsistenciaTotalPeliculas()
    return jsonify(lst)

# Consulta 5


@app.route('/api/data/getAsistenciaCadenaPeliculas', methods=['GET'])
def getAsistenciaCadenaPeliculas():
    lst = trabajo_final.getAsistenciaCadenaPeliculas()
    return jsonify(lst)

# Consulta 6


@app.route('/api/data/getAsistenciaCadenaPeliculasPorcentaje', methods=['GET'])
def getAsistenciaCadenaPeliculasPorcentaje():
    lst = trabajo_final.getAsistenciaCadenaPeliculasPorcentaje()
    return jsonify(lst)

# Consulta 7


@app.route('/api/data/getMasVistaMenosVista/<string:fechaInicio>/<string:fechaFin>/<string:anio>', methods=['GET'])
def getMasVistaMenosVista(fechaInicio, fechaFin, anio):
    lst = trabajo_final.getMasVistaMenosVista(fechaInicio, fechaFin, anio)
    return jsonify(lst)

# Consulta 8


@app.route('/api/data/getAsistenciaCinePaisFecha/<string:fechaInicio>/<string:fechaFin>/<string:anio>', methods=['GET'])
def getAsistenciaCinePaisFecha(fechaInicio, fechaFin, anio):
    lst = trabajo_final.getAsistenciaCinePaisFecha(fechaInicio, fechaFin, anio)
    return jsonify(lst)

#Consulta 9
@app.route('/api/data/getAsistenciaCinePaisPorcentaje/<string:fechaInicio>/<string:fechaFin>/<string:anio>/<string:pais>', methods=['GET'])
def getAsistenciaCinePaisPorcentaje(fechaInicio, fechaFin, anio, pais):
    lst = trabajo_final.getAsistenciaCinePaisPorcentaje(fechaInicio, fechaFin, anio, pais)
    return jsonify(lst)


if __name__ == '__main__':
    app.run(host='localhost', port=4000)
