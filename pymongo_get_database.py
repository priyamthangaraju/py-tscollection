import os
import requests
import time
import json
from requests.auth import HTTPDigestAuth
import pprint
from pymongo import MongoClient
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

ATLAS_USER = os.getenv('ATLAS_USER')
ATLAS_USER_KEY = os.getenv('ATLAS_USER_KEY')
CONNECTION_STR = os.getenv('CONNECTION_STR')
BASE_URL = os.getenv('BASE_URL')
collname = input("Enter the Collection name to be created...")


def get_database():
    client = MongoClient(CONNECTION_STR)
    db = client.test

    # Create the database for our example (we will use the same database throughout the tutorial
    return db

    # This is added so that many files can reuse the function get_database()


def create_tscollection():
    collist = dbname.list_collection_names()
    if collname in collist:
        print("Collection already exists")
    else:
        dbname.command('create', collname,
                       timeseries={'timeField': 'timestamp', 'metaField': 'data', 'granularity': 'hours'})


def ins_tscollection_data():
    auth = HTTPDigestAuth(ATLAS_USER, ATLAS_USER_KEY)
    response = requests.get(BASE_URL, auth=auth)
    colname = dbname[collname]
    data = response.json()
    pprint.pprint(response.json())

    feelslike = data.get('main', {}).get('feels_like')  # dictionary so fetching data like this
    temperature = data.get('main', {}).get('temp')
    humidity = data.get('main', {}).get('humidity')
    # weatherId = list(data.weather[2].items())
    country = data.get('sys', {}).get('country')

    insdata = {
        "timestamp": datetime.now(),
        "feels_like": feelslike,
        "temperature": temperature,
        "humidity": humidity,
        "weatherId": 804,
        "country": country
    }

    colname.insert_one(insdata)


if __name__ == "__main__":
    # Get the database
    dbname = get_database()

    create_tscollection()
    while True:
        ins_tscollection_data()
        time.sleep(30)
