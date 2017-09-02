import pandas as pd
import pymongo
from features.utils import make_payload, setup_conversation
from sshtunnel import SSHTunnelForwarder
import os
TIME_STAMP_FORMAT = "%Y-%m-%d %H:%M:%S.%f"
MONGO_HOST = "206.12.98.21"
MONGO_DB = "SA_Voting_Data"
MONGO_USER = "rsampana"
MONGO_KEY = "/home/rsampana/.ssh/id_rsa"

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
        current_file = os.path.basename(__file__)
        number = current_file.split('.')[0].split('_')[-1]
        todos = pd.read_csv('./ADDRESSES/ad_{}.csv'.format(number))
        print('getting conversations')
        c = conversations.find({'poi': {'$in': todos.addresses.tolist()[0:10000]}})
        df = pd.DataFrame(list(c))
        print('pandas processing')
        if len(df['poi'].unique()) == len(todos):
            for address in todos.addresses.tolist():
                conversation_df = setup_conversation(df[df['poi'] == address])
                p = make_payload(conversation_df=conversation_df)
        else:
            print('fucking RIP in peps')