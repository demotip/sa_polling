from pymongo import MongoClient
from concurrent.futures import ProcessPoolExecutor
from pymongo.errors import ConnectionFailure
from collections import MutableMapping
import traceback

"""
This script cleans the phone numbers and reloads them in the database. It also adds the poi. 

"""

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
    raw = db.raw_data_collection
    clean = db.clean_conversation

    return conn, db, raw, clean


def clean_messages_and_add_poi(messages, poi):
    """
    cleans up the phone numbers of users and adds poi as a field in every message's dictionary.
    """
    def clean_dictionary_recursively(inner_dict):
        """
        mongo adds a dictionary with a na value for some observations when a field is missing in the original object
        This dictionary cannot be reloaded in mongo so I recursively look in the dictionary and wipeout any key that
        might contain.
        :param message:
        :param dict_key:
        :return:
        """
        try:
            keys = inner_dict.keys()
            for key in keys:
                if isinstance(inner_dict[key], MutableMapping):
                    inner_dict[key] = clean_dictionary_recursively(inner_dict[key])
                elif inner_dict[key] == 'NaN':
                    inner_dict = 'null'
                else:
                    pass
        except:
            print(traceback.format_exc())
        return inner_dict

    clean_convos = []
    try:
        for msg in messages:
            clean_conversation = {}
            keys = msg.keys()

            for key in keys:
                if key == "from_addr" or key == "to_addr":
                    clean_conversation[key] = retain_msisdn(str(msg[key]))
                elif key == "_id":
                    pass
                elif isinstance(msg[key], MutableMapping):
                    clean_conversation[key] = clean_dictionary_recursively(msg[key])
                else:
                    clean_conversation[key] = msg[key]

            clean_conversation['poi'] = poi
            clean_convos.append(clean_conversation)
    except:
        print traceback.format_exc()

    return clean_convos


def retain_msisdn(msisdn):
    """
    This is a function written by ryan that seems to modify some msisdn. I am unclear on the exact reason why these
    modifications are necessary. Aaron can be consulted regarding this
    """
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


def clean_conversation_insert_in_mongo(address, index):
    """
    This is remade from Ryan's multi function. Meant to be mapped as part of a multiprocessed execution.
    See ConversationBuild notebook for the original code. This is necessary to insert the poi that is part of
    later scripts
    :param address: distinct address for one user
    :return: NA (inserts into mongo)
    """
    if (index % 100) == 0:
        print('now processing msg {}'.format(index))
    try:
        client, db, raw_data_collection, clean_conversation = mongo_connect()

        convos = raw_data_collection.find({"$or": [{"from_addr": address}, {"to_addr": address}]}).sort('timestamp', 1)

        cc = list(convos)
        cleaned_up_convo = clean_messages_and_add_poi(cc, poi=address)

        insert = clean_conversation.insert(cleaned_up_convo)

        client.close()
    except:
        print traceback.format_exc()
        print index
    return insert


if __name__ == "__main__":
    client, db, raw_data_collection, clean_collection = mongo_connect()

    addresses = list(raw_data_collection.distinct('from_addr'))
    client.close()
    index = list(range(len(addresses)))

    print 'found cleaned collection'

    with ProcessPoolExecutor() as executor:
        cleaned_addresses = executor.map(clean_conversation_insert_in_mongo, addresses, index)

    client.close()

