import pymongo


def make_collection_of_messages(conversation_collection, destination_collection):
    """
    turns a collection of conversations into a collection of individual messages.
    This format seems to be necessary to use some of Ryan's functions
    :param conversation_collection: collection with conversation objects as documents
    :param destination_collection: collection to load messages into
    :return:
    """
    for conversation in conversation_collection.find({}):
        message_objects = conversation['conversation']
        for value in message_objects.itervalues():
            destination_collection.insert_one(value)
            print 'one insertion completed'


if __name__ == "__main__":
    client = pymongo.MongoClient('localhost', 27017)  # local ip and mongo port on remote server
    db = client['test']
    conversations = db.conversation_collections
    message_collection = db.message_collection

    make_collection_of_messages(conversations, message_collection)
    client.close()
