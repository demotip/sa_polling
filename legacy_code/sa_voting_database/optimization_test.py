from BuildDB.build_db import *
from structuredata import *
import json
import pandas as pd
import os



inb, outb = list_inbound_outbound('./AllData/')

conn, db, conversations_collection = mongo_database_setup()

print('improt data is started')
conversations = {}
print('conversation generated')
if len(inb) == len(outb):
    print('loop about to start')
    counter = 0
    for ind in range(len(inb)):
        # returns dictionary
        # key = from_addr category in inbound
        # val = list of dictionaries (messages to/from system, ordered by timestamp)
        conversations.update(create_conversations(inb[ind], outb[ind]))
        print('loop is still running')
        break
        counter += 1
        if counter % 2 == 0:
            print(len(conversations))

columns = ['content',
           'from_addr',
           'from_addr_type',
           'group',
           'helper_metadata',
           'in_reply_to',
           'message_id',
           'message_type',
           'message_version',
           'provider',
           'routing_metadata',
           'session_event',
           'timestamp',
           'to_addr',
           'to_addr_type',
           'transport_metadata',
           'transport_name',
           'transport_type']

print("Creating list of data frames...\n")
# transforms the values of items in conversations dictionary to be a list of pandas dataframes


for k, v in list(conversations.items()):
    print(v)
    k = k.encode('utf-8')
    tmp = []
    for d in v:
        # label columns of messages
        df_conv = pd.DataFrame.from_dict([d])
        df_conv.columns = columns
        tmp.append(df_conv)
    conversations[k] = tmp
print(conversations)
columns = ['user', 'conversation']
# returns a list of top level conversation dataframes (user: conversations array)
# do this because cannot convert dictionaries directly to a dataframe since conversations
# are list of different length
frames = []
len_conv = len(conversations.items())
count = 0
for val in conversations.items():
    count += 1
    df = pd.DataFrame(([val]))
    df.columns = columns
    frames.append(df)
print(frames)
# concatenate
df_outer = pd.concat(frames)

inb, outb = list_inbound_outbound('./AllData/')
conn, db, conversations_collection = mongo_database_setup()
if len(inb) == len(outb):
    for ind in range(len(inb)):
        # returns dictionary
        # key = from_addr category in inbound
        # val = list of dictionaries (messages to/from system, ordered by timestamp)
        conversation = create_conversations(inb[ind], outb[ind])

        columns = ['content',
                   'from_addr',
                   'from_addr_type',
                   'group',
                   'helper_metadata',
                   'in_reply_to',
                   'message_id',
                   'message_type',
                   'message_version',
                   'provider',
                   'routing_metadata',
                   'session_event',
                   'timestamp',
                   'to_addr',
                   'to_addr_type',
                   'transport_metadata',
                   'transport_name',
                   'transport_type']

        for k, v in list(conversation.items()):
            k = k.encode('utf-8')
            tmp = []
            for d in v:
                # label columns of messages
                df_conv = pd.DataFrame.from_dict([d])

                df_conv.columns = columns
                tmp.append(df_conv)
            conversation[k] = tmp
        columns = ['user', 'conversation']
        # returns a list of top level conversation dataframes (user: conversations array)
        # do this because cannot convert dictionaries directly to a dataframe since conversations
        # are list of different length
        frames = []
        len_conv = len(conversation.items())
        count = 0
        for val in conversation.items():
            count += 1
            df = pd.DataFrame(([val]))
            df.columns = columns
            frames.append(df)
        df_outer = pd.concat(frames)

        #object_ids = mongod_insert_df(df, conversations_collection)

        len_df = len(df_outer)
        inserts = []
        for row in df_outer.iterrows():

            new_dict = {}
            conv_dict = {}

            # creates a single document for each message in a conversation array
            conv = row[1]['conversation']
            ind = 1

            for msg in conv:
                if type(msg) is dict:
                    msg = pd.DataFrame([msg])
                conv_dict['msg%d' % ind] = list(json.loads(msg.T.to_json()).values())[0]
                print('conv_dict_updated')
                ind += 1

            new_dict['user'] = row[1]['user']
            new_dict['conversation'] = conv_dict

            # ready to insert to mongodb

            # list of ids for each document
            inserts.append(conversations_collection.insert_one(new_dict).inserted_id)
            print('one insertion complete')

            #  inserts is an array of object id for documents in conversation collection
            #  print 'There was a total of {} out of {}'.format(len(inserts),length_of_df)




