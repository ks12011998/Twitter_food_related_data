import os
import logging
import pandas as pd
from flask import Flask, request, send_file, jsonify, render_template
# from metadata import MetadataDict, Metadata
import sys
sys.path.insert(0, os.path.join(os.path.dirname(sys.path[0]),'setup', 'sqlalchemy'))
import json

import mysql.connector


logging.basicConfig(level=logging.DEBUG, format='%(process)d-%(levelname)s-%(message)s')


# database
"""
    host = "sql129.main-hosting.eu",
    user = "u291509283_cargill"
    password = "Cargill123",
    database = "u291509283_cargill"
"""

mydb = mysql.connector.connect(
    host = "sql129.main-hosting.eu",
    user = "u291509283_cargill",
    password = "Cargill123",
    database = "u291509283_cargill"
)

mycursor = mydb.cursor()

DATABASE = "sample_db"
TABLE = "Tweet_data"


# server
HOST = '127.0.0.1'
PORT = '5000'

app = Flask(__name__)


# services - to return required food data dictionary
@app.route('/')
def data_request():

    logging.info("Received Request.")

    food = {}
    food["data"] = {}

    # fetching distinct locations from table in database
    mycursor.execute("SELECT DISTINCT location FROM {}".format(TABLE))
    locations = mycursor.fetchall()

    # fetching field names from table in database
    mycursor.execute("select COLUMN_NAME from INFORMATION_SCHEMA.COLUMNS where TABLE_SCHEMA = \"{}\" and TABLE_NAME = \"{}\"".format(DATABASE, TABLE))
    fields = mycursor.fetchall()

    # storing data location wise in disctionary format, iteratively
    for x in locations:

        # filtering data according to a particular location
        mycursor.execute("SELECT * FROM {} WHERE location like \"{}\"".format(TABLE, str(x[0])))
        loc_data = mycursor.fetchall()

        loc_name = str(x[0])
        loc_details = {}
        tweet_list = []

        for rw in loc_data:

            tweet_info = {}
            for i in range(len(fields)):
                tweet_info[str(fields[i][0])] = str(rw[i])

            tweet_list.append(tweet_info)

        # setting fields in output dictionary
        loc_details["tweets"] = tweet_list
        food["data"][loc_name] = loc_details

    # creating json object to return
    resp = jsonify(food)
    # with open('data1.json','w') as f:
    #     f.dump(resp)
    # json_object = json.dumps(resp, indent = 4)
    # d = json.dumps(resp)
    # data = json.loads(d)
    # dataFrame = pd.DataFrame.from_dict(data) #convert json to dataframe
    # print(dataFrame)    
       
    resp.status_code = 200

    logging.info("Response Generated.")
    return resp


# main
if __name__ == '__main__':

    app.run(host = HOST, port = PORT)
