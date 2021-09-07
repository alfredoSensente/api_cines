import pandas as pd
import glob
import pymongo
from pymongo import MongoClient


def insertData(data):
    # Making a Connection with MongoClient
    client = MongoClient("mongodb://localhost:27017/")
    # database
    db = client["Cine"]
    # collection
    company = db["Data"]

    data.reset_index(inplace=True)
    data_dict = data.to_dict("records")
    company.insert_one({"index": "Sensex", "data": data_dict})


def data():
    file_list = glob.glob('./Reportes/*.xls')

    df_list = []
    for f in file_list:
        datos = pd.read_excel(
            f, sheet_name='Engagements by Locations', skiprows=2)
        df_list.append(datos)

    dframe = pd.concat(df_list, ignore_index=True)

    newDFrame = dframe.iloc[:, [1, 4, 5, 15, 23, 34, 42]]

    insertData(newDFrame)
    return("Success")
