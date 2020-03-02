import pymongo
from pymongo import MongoClient
import pandas as pd
from bson.objectid import ObjectId
import pandas as pd
from datetime import datetime
from features.settings import TIME_STAMP_FORMAT
from pymongo import MongoClient
import pickle
from datetime import datetime, timedelta
from pymongo.errors import ConnectionFailure
import pickle
from concurrent.futures import ProcessPoolExecutor
import numpy as np
import re
import time
import random
import csv
from concurrent.futures import ProcessPoolExecutor


def mongo_connect():
    """
    This is again Ryan's connection function
    """
    # Try to connect to MongoDB,  exit if not successful.
    try:
        conn = MongoClient('localhost', 27017)
        # print "Connected successfully to MongoDB, instance created!"

    except ConnectionFailure:
        print("Could not connect to MongoDB")

    name = 'test'
    db = conn[name]
    conversations = db.cleaned_conversations
    raw = db.conversations_collection
    features = db.features_collection

    return conn, db, conversations, raw, features


def get_addresses():
    conn, db, conversations, raw, features = mongo_connect()
    try:
        c = conversations.distinct('poi')
    finally:
        conn.close()

    return list(c)


def get_conversation(poi):
    conn, db, conversations, raw, features = mongo_connect()
    try:
        c = conversations.find({'poi': poi})
    finally:
        conn.close()
    df = pd.DataFrame(list(c))
    header = ['poi', 'content', 'from_addr', 'to_addr', 'transport_type', 'transport_name', 'session_event', '_id',
              'helper_metadata']
    df = df.set_index(u'timestamp')
    # we drop duplicates of timestamp which is the index
    df = df[~df.index.duplicated(keep='first')]
    return df[header]


def find_transfer_convo(poi):
    conv = get_conversation(poi)
    outcoming_messages_dest = conv.to_addr.unique()
    good_channels = ['*120*7692*3#', '*120*4729#', '*120*7692*2#']
    messages_to_relevant_channels = sum(address in good_channels for address in outcoming_messages_dest)
    print(messages_to_relevant_channels)
    if messages_to_relevant_channels > 1:
        print('found match at %s' % poi)


def how_many_channels(poi):
    conv = get_conversation(poi)
    outcoming_messages_dest = conv.to_addr.unique()
    print len(outcoming_messages_dest)


def main():
    print('start')
    conn, db, conversations, raw, features = mongo_connect()

    addresses = get_addresses()
    ussd_poi = [poi for poi in addresses if str(poi).startswith('+')]

    with ProcessPoolExecutor() as executor:
        executor.map(how_many_channels, ussd_poi)
    print('finished')


if __name__ == '__main__':
    main()



