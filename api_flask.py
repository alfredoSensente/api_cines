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

@app.route('/api/data/getTotalPersonasCadenaPais_Pais_Cadena/<string:fechaInicio>/<string:fechaFin>/<string:anio>/<string:pais>/<string:cadena>', methods=['GET'])
def getTotalPersonasCadenaPais_Pais_Cadena(fechaInicio, fechaFin, anio, pais, cadena):
    lst = trabajo_final.getTotalPersonasCadenaPais_Pais_Cadena(fechaInicio, fechaFin, anio, pais, cadena)
    return jsonify(lst)

@app.route('/api/data/getTotalPersonasCadenaPais_Pais/<string:fechaInicio>/<string:fechaFin>/<string:anio>/<string:pais>', methods=['GET'])
def getTotalPersonasCadenaPais_Pais(fechaInicio, fechaFin, anio, pais):
    lst = trabajo_final.getTotalPersonasCadenaPais_Pais(fechaInicio, fechaFin, anio, pais)
    return jsonify(lst)

@app.route('/api/data/getTotalPersonasCadenaPais_Cadena/<string:fechaInicio>/<string:fechaFin>/<string:anio>/<string:cadena>', methods=['GET'])
def getTotalPersonasCadenaPais_Cadena(fechaInicio, fechaFin, anio, cadena):
    lst = trabajo_final.getTotalPersonasCadenaPais_Cadena(fechaInicio, fechaFin, anio, cadena)
    return jsonify(lst)

# Consulta 4


@app.route('/api/data/getAsistenciaTotalPeliculas/<string:pelicula>', methods=['GET'])
def getAsistenciaTotalPeliculas(pelicula):
    lst = trabajo_final.getAsistenciaTotalPeliculas(pelicula)
    return jsonify(lst)

# Consulta 5


@app.route('/api/data/getAsistenciaCadenaPeliculas/<string:pelicula>', methods=['GET'])
def getAsistenciaCadenaPeliculas(pelicula):
    lst = trabajo_final.getAsistenciaCadenaPeliculas(pelicula)
    return jsonify(lst)

# Consulta 6


@app.route('/api/data/getAsistenciaCadenaPeliculasPorcentaje/<string:pelicula>', methods=['GET'])
def getAsistenciaCadenaPeliculasPorcentaje(pelicula):
    lst = trabajo_final.getAsistenciaCadenaPeliculasPorcentaje(pelicula)
    return jsonify(lst)

# Consulta 7


@app.route('/api/data/getMasVistaMenosVista/<string:fechaInicio>/<string:fechaFin>/<string:anio>/<string:pelicula>', methods=['GET'])
def getMasVistaMenosVista(fechaInicio, fechaFin, anio, pelicula):
    lst = trabajo_final.getMasVistaMenosVista(fechaInicio, fechaFin, anio, pelicula)
    return jsonify(lst)

# Consulta 8


@app.route('/api/data/getAsistenciaCinePaisFecha/<string:fechaInicio>/<string:fechaFin>/<string:anio>/<string:pais>/<string:pelicula>', methods=['GET'])
def getAsistenciaCinePaisFecha(fechaInicio, fechaFin, anio, pais, pelicula):
    lst = trabajo_final.getAsistenciaCinePaisFecha(fechaInicio, fechaFin, anio, pais, pelicula)
    return jsonify(lst)

#Consulta 9
@app.route('/api/data/getAsistenciaCinePaisPorcentaje/<string:fechaInicio>/<string:fechaFin>/<string:anio>/<string:pais>/<string:pelicula>', methods=['GET'])
def getAsistenciaCinePaisPorcentaje(fechaInicio, fechaFin, anio, pais, pelicula):
    lst = trabajo_final.getAsistenciaCinePaisPorcentaje(fechaInicio, fechaFin, anio, pais, pelicula)
    return jsonify(lst)

#Obtener peliculas por fecha
@app.route('/api/data/getFechaPeliculas/<string:fechaInicio>/<string:fechaFin>/<string:anio>', methods=['GET'])
def getFechaPeliculas(fechaInicio, fechaFin, anio):
    lst = trabajo_final.getFechaPeliculas(fechaInicio, fechaFin, anio)
    return jsonify(lst)

#Obtener peliculas por fecha
@app.route('/api/data/getFechaPaisPeliculas/<string:fechaInicio>/<string:fechaFin>/<string:anio>/<string:pais>', methods=['GET'])
def getFechaPaisPeliculas(fechaInicio, fechaFin, anio, pais):
    lst = trabajo_final.getFechaPaisPeliculas(fechaInicio, fechaFin, anio, pais)
    return jsonify(lst)


if __name__ == '__main__':
    app.run(host='localhost', port=4000)
