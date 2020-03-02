import pandas as pd
import os
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

"""
this file takes in the original data provided by aaron and loads it into a mongo database. The data should be located
in a folder called "data" that is located in the root directory of the project. 
"""


def list_inbound_outbound(directory):
    """
    This function takes the directory containing the datafiles as an ouput and separates them into inbound and outbound
    messages. The output of this function is meant to be read by import data
    :param directory: directory containing the messages
    :return: file names separated in an inbound and outbound list
    """

    list_inbound_files = []
    list_outbound_files = []

    for root, dirs, files in os.walk(directory):
        for f in files:
            if "inbound" in f:
                list_inbound_files.append(os.path.join(root, f))
            elif "bound" in f:
                list_outbound_files.append(os.path.join(root, f))

    return list_inbound_files, list_outbound_files


def mongo_connect():
    """
    This is a function that will be used through the project to connect to the mongo database. For now only the
    raw_data_collection collection is  in there because there is no processed data yet.
    """
    # Try to connect to MongoDB,  exit if not successful.

    conn = MongoClient('localhost', 27017)

    name = 'test'
    db = conn[name]
    raw = db.raw_data_collection

    return conn, db, raw


def main():

    mongo_connection, database, raw_data = mongo_connect()
    inbound_files, outbound_files = list_inbound_outbound('./data')

    print('starting to load files into mongo')
    for i in range(len(inbound_files)):

        print(f'now loading {inbound_files[i]} \n and the outbound counterpart')
        inbound_messages = pd.read_json(inbound_files[i], lines=True)
        outbound_messages = pd.read_json(outbound_files[i], lines=True)

        inbound_messages_dict = inbound_messages.to_dict(orient='records')  # need a dictionary for mongo
        outbound_messages_dict = outbound_messages.to_dict(orient='records')

        raw_data.insert_many(inbound_messages_dict)
        raw_data.insert_many(outbound_messages_dict)

        print('one_file_loaded')

if __name__ == "__main__":
    main()
