import pymongo
from pymongo import MongoClient
import pandas as pd
from bson.objectid import ObjectId

from concurrent.futures import ProcessPoolExecutor


def mongo_connect(mongo_port):
    # Try to connect to MongoDB,  exit if not successful.
    try:
        conn = MongoClient('localhost', mongo_port)
        # print "Connected successfully to MongoDB, instance created!"

    except pymongo.errors.ConnectionFailure, e:
        print "Could not connect to MongoDB: %s" % e

    name = 'test'

    # create database on instance
    if name in conn.list_database_names():
        # print "Found SA_Voiting_Data Database"
        # print "Connecting to db..."
        db = conn[name]  # Create the database

        # create one collection, called conversations
        conversations = db.conversations_collection
        raw = db.raw_collection
        # print ("Connected!")
    else:
        # print "Creating SA_Voting_Data Database"
        db = conn[name]  # create the database
        conversations = db.conversation_collection
        raw = db.raw_collection

    # collection is a collection in the mongodb instance created above
    # db is the database on the mongodb instance
    # conn is the mongodb instance
    return conn, db, conversations, raw


def clean_conversation_insert_in_mongo(address):
    """
    cleans the conversation and re-inserts it into a mongo collection. Not sure why this step is necessary yet. This is
    remade from Ryan's multi function. Meant to be mapped as part of a multiprocessed execution. See ConversationBuild
    notebook for the original code
    :param address: distinct address for one user
    :return: NA (inserts into mongo)
    """
    test_collection = db.get_collection('test_collection')
    convos = test_collection.find({"$or": [{"from_addr": address}, {"to_addr": address}]}).sort('timestamp', 1)
    print(convos)
    cc = list(convos)
    print cc

    inserted_id = clean_conversation.insert(clean_conversations_and_add_poi(cc, poi=address))
    print inserted_id

    print "finished with {}".format(address)
    return address


def clean_conversations_and_add_poi(conversations, poi):
    clean_convos = []
    for conv in conversations:
        clean_conversation = {}
        keys = conv.keys()

        for key in keys:
            if key == "from_addr" or key == "to_addr":
                clean_conversation[key] = retain_msisdn(conv[key])
            elif key == "_id":
                pass
            else:
                clean_conversation[key] = conv[key]

        clean_conversation['poi'] = poi
        clean_convos.append(clean_conversation)
    return clean_convos


def retain_msisdn(msisdn):
    if msisdn is None:
        return u"None"

    if msisdn == u"unknown":
        return u""

    # msisdn a regular phone number
    elif msisdn[0:3] == u"+27" and len(msisdn) == 12:
        return msisdn

    # msisdn a mxit phone number
    elif msisdn[0] == u'm':
        return msisdn

    elif msisdn[0:2] == u'07' or msisdn[0:2] == u'27' and len(msisdn) == 10:
        return u'27' + msisdn[1:]

    elif msisdn[0:2] == u'27' and len(msisdn) == 11:
        return msisdn

    # some other type of number we don't know about

    elif msisdn[0:2] != '27' and msisdn[0:2] != '07' and msisdn[0:3] != '+27':
        return msisdn


def clean_conversation_insert_in_mongo(address):
    """
    cleans the conversation and re-inserts it into a mongo collection. Not sure why this step is necessary yet. This is
    remade from Ryan's multi function. Meant to be mapped as part of a multiprocessed execution. See ConversationBuild
    notebook for the original code
    :param address: distinct address for one user
    :return: NA (inserts into mongo)
    """

    convos = test_collection.find({"$or": [{"from_addr": address}, {"to_addr": address}]}).sort('timestamp', 1)
    print(convos)
    cc = list(convos)
    print cc

    inserted_id = clean_conversation.insert(clean_conversations_and_add_poi(cc, poi=address))
    print inserted_id

    print "finished with {}".format(address)
    return address


def get_conversation(conversations):
    header = ['content', 'from_addr', 'to_addr', 'transport_type', 'transport_name', 'timestamp', 'session_event']
    df_conversations = pd.DataFrame()

    for c in conversations:
        data = [c['content'], c['from_addr'], c['to_addr'], c['transport_type'], c[u'transport_name'], c['timestamp'],
                c['session_event']]
        df_conversations = df_conversations.append([data])

    df_conversations.columns = header
    df_conversations = df_conversations.set_index('timestamp')
    return df_conversations


# constants for server connection
"""
BELUGA_IP = '206.12.88.101'  # this is the beluga cloud's ip address
USERNAME = 'egagnon' # your username on beluga
PASSWORD = 'a'  # your password on beluga
PATH_TO_SSH_KEY = 'C:/Users/Etienne Gagnon/.ssh/id_rsa'  #  path to the key that you use to connect to the server
REMOTE_LOCAL_IP = '172.19.0.1'
MONGO_PORT_ON_REMOTE = 34046  # can find this info with db.runCommand({whatsmyuri : 1}) in mongo on beluga


server = SSHTunnelForwarder(
    BELUGA_IP,
    ssh_username=USERNAME,
    ssh_password=PASSWORD,
    ssh_pkey=PATH_TO_SSH_KEY,
    remote_bind_address=(REMOTE_LOCAL_IP, MONGO_PORT_ON_REMOTE),
    local_bind_address=('127.0.0.1', 27017)
)

server.start()
"""

# Etienne: global variables are required for multiprocessing. There are ways to avoid the globals but it isn't worth the
# effort right now.

client = pymongo.MongoClient('localhost', 27017)  # local ip and mongo port on remote server
db = client['test']
message_collection = db.get_collection('cleaned_conversation')
clean_conversation = db.cleaned_conversations
conversation_collection = db.get_collection('conversation_collections')
conversation_collection.find_one()



import re
starts_with_digt = re.compile('^\d')


for i in clean_conversation.find({'from_addr': starts_with_digt, 'transport_type': 'mxit'}):
    print(i['content'])

for i in conversation_collection.find({'user': {'$in': ['+27728720860']}}):
    print(i)


if __name__ == "__main__":
    try:
        # if you want to make queries to the db, do it here
        pois = list(clean_conversation.distinct('poi'))
        types_of_events = clean_conversation.distinct('session_event')
    finally:
        client.close()

    try:
        # if you want to make queries to the db, do it here
        conversations = list(clean_conversation.find({'poi': {'$in': pois[0:1000]}}))
    finally:
        client.close()

    len(conversations)
    conversation_data_frame = get_conversation(conversations)
    conversation_data_frame.head()


    cleaned_coll = db.get_collection('cleaned_conversations')
    test_collection = db.get_collection('test_collection')

    for i in clean_conversation.find({'content': 'Make a choice, have a voice, vote! Your inked finger will show everyone that you have.'}):
        print(i)

i['timestamp']

import pandas as pd


cleaned_featuers = pd.read_csv('C:\Users\Etienne Gagnon\Downloads\cleaned_features_nov22th.csv')
problematic_pois = cleaned_featuers[cleaned_featuers.loc[:, ['total_time_in_system', 'average_response_time']].apply(lambda x: x.total_time_in_system < x.average_response_time, axis=1)]['poi']

cleaned_featuers.transport_types

cleaned_featuers.loc[261988, :]



for i in clean_conversation.find({'poi': '+27824291183'}):
    print(i)
    break


conv = get_conversation(u'+27725107387')

conv.index = index_to_date(conv.index)
conv.sort_index(inplace=True)
ryan_payload = make_payload(conv)

channel_var = channels(conv)

conv
conv['to_addr']

channel_list = conv[(conv['to_addr'] == '*120*7692*2#') |
                               (conv['to_addr'] == '*120*7692*3#') |
                               (conv['to_addr'] == '*120*4729#')]['to_addr'].tolist()
    previous = None
    ch1_to_ch2 = False
    ch2_to_ch1 = False
    ch2_to_ch3 = False
    ch3_to_ch2 = False
    ch1_to_ch3 = False
    ch3_to_ch1 = False
    for x in channel_list[1:]:
        next_one = x
        if next_one == previous:
            previous = next_one
            pass
        else:
            # ch1 = '*120*7692*2#' ch2 = '*120*7692*3#' ch3 = '*120*4729#'
            if (previous == '*120*7692*2#' and next_one == '*120*7692*3#'):
                ch1_to_ch2 = True
            if (next_one == '*120*7692*2#' and previous == '*120*7692*3#'):
                ch2_to_ch1 = True
            if (previous == '*120*7692*3#' and next_one == '*120*4729#'):
                ch2_to_ch3 = True
            if (next_one == '*120*7692*3#' and previous == '*120*4729#'):
                ch3_to_ch2 = True
            if (previous == '*120*7692*2#' and next_one == '*120*4729#'):
                ch3_to_ch1 = True
            if (next_one == '*120*7692*2#' and previous == '*120*4729#'):
                ch1_to_ch3 = True
            previous = next_one

    return ch1_to_ch2, ch2_to_ch1, ch2_to_ch3, ch3_to_ch2, ch1_to_ch3, ch3_to_ch1


def get_addresses():
    conn, db, conversations, raw, features = mongo_connect()
    try:
        c = conversations.distinct('poi')
    finally:
        conn.close()

    return list(c)


addresses = get_addresses()

conv = get_conversation('+27823909270')
for i in conv.helper_metadata:
    print(i)

conv.index = index_to_date(conv.index)
conv.sort_index(inplace=True)

other_conv = get_conversation('+27727680706')
for i in other_conv.transport_type:
    print(i)

for i in conversations.find({u'helper_metadata': {u'go': {u'conversation_key': u'8f15e531b32e4f859d182f8ef5c22ef5'}}}):
    print(i)

for i in conv['helper_metadata']:
    print(i)

for i in clean_conversation.find({'helper_metadata.go.conversation_key': '8f15e531b32e4f859d182f8ef5c22ef5'}):
    print(i)

for i in clean_conversation.find({'helper_metadata.go.conversation_key': '8f15e531b32e4f859d182f8ef5c22ef5'}):
    print(i)

for i in clean_conversation.find({'from_addr': '+27797272488'}):
    print(i)


conv_2_transport_type = get_conversation('+27762382046')
conv_2_transport_type.index = index_to_date(conv_2_transport_type.index)
conv_2_transport_type.sort_index(inplace=True)

for i in conv_2_transport_type.helper_metadata:
    print(i)

cleaned_featuers[cleaned_featuers.total_responses_sms > 1]

conv_more_than2_sms = get_conversation('+27797422824')
conv_more_than2_sms.index = index_to_date(conv_more_than2_sms.index)
conv_more_than2_sms.sort_index(inplace=True)

cleaned_featuers[cleaned_featuers.poi == '+27764914783']
lots_of_outgoing = cleaned_featuers[cleaned_featuers.to_address_values.apply(len) > 100]
lots_of_outgoing.poi

distinct_user = conversation_collection.distinct('user')
distinct_conversations = conversation_collection.distinct('_id')

addresses = get_addresses()
addresses = [address for address in addresses if str(address).endswith('*4729#')]


to_ch2 = []
for i in clean_conversation.find({'to_addr': u'*120*7692*2#'}):
    to_ch2.append(i[u'from_addr'])

to_ch3 = []
for i in clean_conversation.find({'to_addr': u'*120*7692*3#'}):
    to_ch3.append(i[u'from_addr'])

to_ch1 = []
for i in clean_conversation.find({'to_addr': u'*120*4729#'}):
    to_ch1.append(i[u'from_addr'])



pd.options.display.width = 0
import pandas as pd

cleaned_features = pd.read_csv('cleaned_features_jan13th.csv')
ryan_data = pd.read_csv('cleaned_features_v3.csv')

stat_with_plus = cleaned_features[cleaned_features.poi.str.startswith('+')]
stat_with_plus[stat_with_plus.transport_types.apply(lambda x: 'mxit' in x)].iloc[2, :]



current_poi_set = set(curent_poi.unique())

result = current_poi_set - set(ryan_data.poi)

reverse_result = set(ryan_data.poi) - current_poi_set



