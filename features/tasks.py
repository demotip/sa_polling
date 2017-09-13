from sshtunnel import SSHTunnelForwarder
import pymongo
from features.utils import make_payload, get_conversation
from features.settings import *
from time import sleep


def make_feature(address):
    with SSHTunnelForwarder(
        MONGO_HOST,
        ssh_username=MONGO_USER,
        ssh_pkey=MONGO_KEY,
        remote_bind_address=('localhost', 27016)
    ) as server:
        client = pymongo.MongoClient('localhost', server.local_bind_port)
        name = 'SA_Voting_Data'
        db = client[name]
        conversations = db.conversations_collection
        raw = db.raw_collection
        features = db.features_collection
        conversation_df = get_conversation(conversations=conversations, poi=address)

    p = make_payload(conversation_df=conversation_df)
    return p


