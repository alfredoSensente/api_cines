from datetime import date
import pandas as pd
import glob
import pymongo
from pymongo import MongoClient
from pymongo import cursor
import xlrd

# Variables globales para mongo
# Making a Connection with MongoClient
client = MongoClient("mongodb://localhost:27017/")
# database
db = client["Cine"]
# collection
collection = db["Data"]


def leer(ruta):
    archivo = ruta
    wb = xlrd.open_workbook(archivo)
    hoja = wb.sheet_by_index(0)
    row = hoja.cell_value(1, 0)

    arrayInfo = row.split(sep=",")
    arrayFecha = arrayInfo[2].split(sep=" ")

    pais = arrayInfo[0]
    fechaInicio = arrayFecha[3]
    fechaFin = arrayFecha[5].split("/")
    valores = []
    valores.append(pais)
    valores.append(fechaInicio)
    valores.append(fechaFin[0] + '/' + fechaFin[1])
    valores.append(fechaFin[2])
    return valores


def insertData(data):
    data.reset_index(inplace=True)
    data_dict = data.to_dict("records")

    for row in data_dict:
        collection.insert_one(row)


def getOne(titulo):
    dictionary = collection.find_one({'Title':titulo}, {'_id': False, 'index': False})
    return(dictionary)

#Consulta 1
def getTotalPersonasPais():
    """Mostrar de cada pais el total de personas que visitaron el cine en la semana"""
    cursor = collection.aggregate([{ '$group': {'_id': "$Country", 'personas': {'$sum': "$Week\nAdm"}}}])
    return(list(cursor))

#Consulta 2
def getTotalPersonas():
    """Mostrar el total de personas de los 5 paises que visitaron el cine en la semana"""
    cursor = collection.aggregate([
        {
            '$group': {
            '_id': 'null',
            'total_personas': {
                '$sum': "$Week\nAdm"
            }
            }
        },
        {
            '$project': {
            '_id': 0
            }
        }
    ])
    return(list(cursor))

#Consulta 3
def getTotalPersonasCadenaPais():
    """Mostrar el total de personas que acudieron al cine en la semana, por cadena de cine para cada pais"""
    cursor = collection.aggregate([
        {
            '$group': {
            '_id': {
                'Pais': "$Country",
                'Cadena': "$Circuit"
            },
            'personas': {
                '$sum': "$Week\nAdm"
            }
            }
        },
        {
            '$sort':{
                '_id.Pais': 1
            }
        }
    ])
    
    return (list(cursor))

#Consulta 4
def getAsistenciaTotalPeliculas():
    """El total de personas que vieron cada pelicula en centroamerica"""
    cursor = collection.aggregate([
        {   
            '$group': {
                '_id': {
                    'Titulo': "$Title"
                },
                'personas': {
                        '$sum': "$Week\nAdm"
                 }
            }
        }
    ])
    
    return (list(cursor))

#Consulta 5
def getAsistenciaCadenaPeliculas():
    """El total de personas que vieron cada pelicula en centroamerica por cadena de cine"""
    cursor = collection.aggregate([
        {   
            '$group': {
                '_id': {
                    'Titulo': "$Title",
                    'Cadena': "$Circuit"
                },
                'personas': {
                        '$sum': "$Week\nAdm"
                 }
            }
        },
        {
            '$sort':{
                '_id.Titulo': 1
            }
        }
    ])
    
    return (list(cursor))

#Consulta 6
def getAsistenciaCadenaPeliculasPorcentaje():
    """El total de personas que vieron cada pelicula en centroamerica por cadena de cine pero en porcentaje"""
    pass

#Consulta 7
def getMasVistaMenosVista():
    """Mostrar para cada pelicula el total de personas que acudieron a verla en la semana por pais y ordenarla de la más vista a la menos vista"""
    cursor = collection.aggregate([
        {
            '$group': {
            '_id': {
                'Pais': "$Country",
                'Title': "$Title"
            },
            'personas': {
                '$sum': "$Week\nAdm"
            }
            }
        },
        {
            '$sort':{
                'personas': -1
            }
        }
    ])
    
    return (list(cursor))

#Consulta 8
def getAsistenciaCinePaisFecha(startDate, endDate, year):
    """Total de personas por cadena de cine, por pais, por semana"""
    startDate = startDate.replace('-','/')
    endDate = endDate.replace('-','/')

    cursor = collection.aggregate([
        {
            '$group': {
            '_id': {
                'Pais': "$Country",
                'Cadena': "$Circuit",
                'Titulo': "$Title",
                'Fecha_Inicio': {
                    'StartDate':startDate
                },
                'Fecha_Fin': {
                    'EndDate':endDate
                },
                'Anio': {
                    'Year': year
                }
            },
            'personas': {
                '$sum': "$Week\nAdm"
            }
            }
        }
    ])

    return(list(cursor))


def data():

    file_list = glob.glob('./Reportes/*.xls')

    df_list = []
    for f in file_list:
        informacionArchivo = leer(f)
        datos = pd.read_excel(
            f, sheet_name='Engagements by Locations', skiprows=2)
        datos = datos.assign(
            Country=informacionArchivo[0], StartDate=informacionArchivo[1], EndDate=informacionArchivo[2], Year=informacionArchivo[3])
        df_list.append(datos)

    dframe = pd.concat(df_list, ignore_index=True)

    newDFrame = dframe.iloc[:, [1, 4, 5, 15, 23, 34, 42, 46, 47, 48, 49]]
    
    newDFrame = newDFrame.apply(lambda x: x.astype(str).str.upper())

    newDFrame['Weekend\nAdm'] = pd.to_numeric(newDFrame['Weekend\nAdm'])
    newDFrame['Week\nAdm'] = pd.to_numeric(newDFrame['Week\nAdm'])
    newDFrame['Weekend\nGross $'] = pd.to_numeric(newDFrame['Weekend\nGross $'], downcast="float")
    newDFrame['Week\nGross $'] = pd.to_numeric(newDFrame['Week\nGross $'], downcast="float")



    insertData(newDFrame)
    return("Success")
