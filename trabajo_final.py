import pandas as pd
import glob
import pymongo
from pymongo import MongoClient
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


def getOne():
    dictionary = collection.find_one({}, {'_id': False, 'index': False})
    return(dictionary)


def getAll():
    cursor = collection.find({}, {'_id': False, 'index': False})
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

    insertData(newDFrame)
    return("Success")
